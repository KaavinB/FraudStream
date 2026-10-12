import argparse
import importlib.metadata
import json
import platform
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from ml.evaluation.metrics import evaluate, threshold_at_fpr
from ml.features.reference import FEATURE_NAMES, FEATURE_VERSION, ReferenceFeatures, epoch


def git_commit() -> str:
    result = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else "unavailable-no-git"


def build_features(
    raw: Path, processed: Path
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    processed.mkdir(parents=True, exist_ok=True)
    n = json.loads((raw / "metadata.json").read_text())["transactions"]
    x = np.lib.format.open_memmap(
        processed / "features.npy", mode="w+", dtype="float64", shape=(n, len(FEATURE_NAMES))
    )
    y, timestamps, available = np.zeros(n, dtype=int), np.zeros(n), np.zeros(n)
    reference = ReferenceFeatures()
    with (raw / "events.jsonl").open() as events, (raw / "labels.jsonl").open() as labels:
        for i, (event_line, label_line) in enumerate(zip(events, labels, strict=True)):
            event, label = json.loads(event_line), json.loads(label_line)
            if event["transaction_id"] != label["transaction_id"]:
                raise ValueError("label/event join mismatch")
            x[i] = reference.process(event)
            # Durable replay dedup is a serving concern; bound offline oracle's per-event cache.
            reference.results.clear()
            y[i], timestamps[i] = label["fraud"], epoch(event)
            available[i] = datetime.fromisoformat(label["available_at"]).timestamp()
    return x, y, timestamps, available


def masks(t: np.ndarray, available: np.ndarray) -> tuple[list[np.ndarray], dict]:
    start, end = t.min(), t.max()
    train_end, val_end = start + 0.6 * (end - start), start + 0.8 * (end - start)
    # Model fit is as of train_end, candidate selection/calibration as of val_end.
    train = (t < train_end) & (available <= train_end)
    validation = (t >= train_end) & (t < val_end) & (available <= val_end)
    test = t >= val_end  # evaluated later, after all test labels have arrived
    if any(mask.sum() < 100 for mask in [train, validation, test]):
        raise ValueError("insufficient temporal split; increase days/customers (label delay is 7d)")
    return [train, validation, test], {
        "train_cutoff": datetime.fromtimestamp(train_end, UTC).isoformat(),
        "validation_cutoff": datetime.fromtimestamp(val_end, UTC).isoformat(),
        "evaluation_as_of": datetime.fromtimestamp(available.max(), UTC).isoformat(),
        "label_delay_days": 7,
        "purged_unavailable_labels": int((~(train | validation | test)).sum()),
        "counts": {
            "train": int(train.sum()),
            "validation": int(validation.sum()),
            "test": int(test.sum()),
        },
    }


def train(
    raw: Path,
    out: Path,
    allow_missing: bool = False,
    mlflow_enabled: bool = False,
    target_fpr: float = 0.01,
    configured_block_threshold: float = 0.9,
) -> dict:
    if not 0 <= target_fpr < 1 or not 0 <= configured_block_threshold <= 1:
        raise ValueError("FPR target must be in [0,1), BLOCK threshold in [0,1]")
    x, y, t, available = build_features(raw, raw.parent / "processed")
    split, split_info = masks(t, available)
    tr, va, te = split
    # Validation is split chronologically into calibration and operating-point selection.
    validation_indices = np.flatnonzero(va)
    midpoint_time = t[validation_indices[len(validation_indices) // 2]]
    cal = va & (t < midpoint_time)
    tune = va & (t >= midpoint_time)
    if any(len(np.unique(y[m])) < 2 for m in [tr, cal, tune, te]):
        raise ValueError("each temporal partition must contain both classes")
    weights = np.where(y[tr] == 1, (sum(y[tr] == 0) / sum(y[tr] == 1)), 1.0)
    candidates: dict[str, Any] = {
        "logistic_regression": make_pipeline(
            StandardScaler(),
            LogisticRegression(max_iter=500, class_weight="balanced", random_state=42),
        ),
        "hist_gradient_boosting": HistGradientBoostingClassifier(
            max_iter=120, max_leaf_nodes=15, l2_regularization=1.0, random_state=42
        ),
    }
    skipped = []
    try:
        from xgboost import XGBClassifier

        candidates["xgboost"] = XGBClassifier(
            n_estimators=150,
            max_depth=4,
            learning_rate=0.08,
            n_jobs=2,
            tree_method="hist",
            random_state=42,
            scale_pos_weight=float(weights.max()),
            eval_metric="logloss",
        )
    except ImportError:
        if not allow_missing:
            raise RuntimeError(
                "XGBoost is required; use --allow-missing-xgboost only for an explicitly limited local run"
            ) from None
        skipped.append("xgboost: dependency unavailable; not evaluated")
    out.mkdir(parents=True, exist_ok=True)
    models: dict[str, dict[str, Any]] = {}
    rows: dict[str, dict[str, Any]] = {}
    timing_samples: dict[str, list[float]] = {}
    for name, estimator in candidates.items():
        started = time.perf_counter()
        if name == "hist_gradient_boosting":
            estimator.fit(x[tr], y[tr], sample_weight=weights)
        else:
            estimator.fit(x[tr], y[tr])
        # Platt calibration on later, disjoint, naturally imbalanced data.
        p = np.clip(estimator.predict_proba(x[cal])[:, 1], 1e-7, 1 - 1e-7)
        calibrator = LogisticRegression(C=1000, random_state=42).fit(
            np.log(p / (1 - p)).reshape(-1, 1), y[cal]
        )
        bundle = {"estimator": estimator, "calibrator": calibrator}
        scores = predict(bundle, x[tune])
        threshold = threshold_at_fpr(y[tune], scores, target_fpr)
        samples = []
        timing_input = x[tune][:500]
        for warmup in range(20):
            predict(
                bundle, timing_input[warmup % len(timing_input) : warmup % len(timing_input) + 1]
            )
        for i in range(min(500, sum(tune))):
            begin = time.perf_counter_ns()
            predict(bundle, timing_input[i : i + 1])
            samples.append((time.perf_counter_ns() - begin) / 1e6)
        artifact = out / f"{name}.joblib"
        joblib.dump(bundle, artifact)
        rows[name] = {
            "validation": evaluate(y[tune], scores, threshold),
            "training_and_validation_seconds": time.perf_counter() - started,
            "inference_p50_ms": float(np.percentile(samples, 50)),
            "inference_p95_ms": float(np.percentile(samples, 95)),
            "artifact_bytes": artifact.stat().st_size,
            "hyperparameters": {k: str(v) for k, v in estimator.get_params().items()},
        }
        timing_samples[name] = samples
        models[name] = bundle
    # Freeze model choice and threshold using validation evidence BEFORE test predictions.
    selected = max(rows, key=lambda name: rows[name]["validation"]["average_precision"])
    threshold = rows[selected]["validation"]["threshold"]
    for name, bundle in models.items():
        rows[name]["test"] = evaluate(
            y[te], predict(bundle, x[te]), rows[name]["validation"]["threshold"]
        )
    metadata = json.loads((raw / "metadata.json").read_text())
    experiment_id = datetime.now(UTC).strftime("ml-%Y%m%dT%H%M%SZ")
    version = selected + "-" + metadata["dataset_version"]
    report = {
        "experiment_id": experiment_id,
        "date": datetime.now(UTC).isoformat(),
        "git_commit": git_commit(),
        "dataset": metadata,
        "split": split_info,
        "selected_model": selected,
        "model_version": version,
        "feature_version": FEATURE_VERSION,
        "feature_names": FEATURE_NAMES,
        "review_threshold": threshold,
        "block_threshold": max(threshold, configured_block_threshold),
        "validation_target_fpr": target_fpr,
        "models": rows,
        "skipped": skipped,
        "seed": 42,
        "hardware": {"platform": platform.platform(), "processor": platform.machine()},
        "software": {p: importlib.metadata.version(p) for p in ["numpy", "scikit-learn", "pandas"]},
        "limitations": [
            "synthetic rule-based fraud",
            "single machine CPU timing",
            "seven-day delayed labels",
            "no test-based model selection",
        ],
    }
    production = {**models[selected], "info": report}
    joblib.dump(production, out / "model.joblib")
    (out / "model-info.json").write_text(json.dumps(report, indent=2) + "\n")
    predictions = predict(models[selected], x[te])
    np.savez_compressed(
        out / "heldout-predictions.npz", timestamps=t[te], labels=y[te], scores=predictions
    )
    (out / "inference-samples.json").write_text(json.dumps(timing_samples) + "\n")
    if mlflow_enabled:
        import mlflow

        mlflow.set_experiment("fraudstream-temporal")
        with mlflow.start_run(run_name=experiment_id):
            mlflow.log_params(
                {
                    "dataset_version": metadata["dataset_version"],
                    "model": selected,
                    "feature_version": FEATURE_VERSION,
                    "seed": 42,
                    "git_commit": git_commit(),
                }
            )
            mlflow.log_metrics(
                {
                    "test_" + k: v
                    for k, v in rows[selected]["test"].items()
                    if isinstance(v, (int, float))
                }
            )
            mlflow.log_artifacts(str(out))
    return report


def predict(bundle: dict, x: np.ndarray) -> np.ndarray:
    p = np.clip(bundle["estimator"].predict_proba(x)[:, 1], 1e-7, 1 - 1e-7)
    return bundle["calibrator"].predict_proba(np.log(p / (1 - p)).reshape(-1, 1))[:, 1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path, default=Path("ml/data/raw"))
    parser.add_argument("--out", type=Path, default=Path("ml/artifacts"))
    parser.add_argument("--allow-missing-xgboost", action="store_true")
    parser.add_argument("--mlflow", action="store_true")
    parser.add_argument("--target-fpr", type=float, default=0.01)
    parser.add_argument("--block-threshold", type=float, default=0.9)
    args = parser.parse_args()
    result = train(
        args.raw,
        args.out,
        args.allow_missing_xgboost,
        args.mlflow,
        args.target_fpr,
        args.block_threshold,
    )
    print(
        json.dumps(
            {
                "selected": result["selected_model"],
                "models": {
                    k: {
                        m: v["test"][m]
                        for m in [
                            "average_precision",
                            "recall",
                            "actual_fpr",
                            "precision",
                            "brier_score",
                        ]
                    }
                    for k, v in result["models"].items()
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
