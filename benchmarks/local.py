"""Real CPU inference timings; explicitly no Kafka/Redis/network throughput claim."""

import argparse
import json
import time
from pathlib import Path

import joblib
import numpy as np
from fastapi.testclient import TestClient

from benchmarks.common import metadata, summary
from ml.evaluation.drift import detect
from ml.features.reference import FEATURE_NAMES, ReferenceFeatures
from ml.training.train import predict
from services.inference.app.main import app


def measure(count: int = 2000) -> dict:
    bundle = joblib.load("ml/artifacts/model.joblib")
    info = bundle["info"]
    with Path("ml/data/raw/events.jsonl").open() as stream:
        events = [json.loads(line) for _, line in zip(range(count), stream, strict=False)]
    oracle = ReferenceFeatures()
    features, feature_samples = [], []
    for event in events:
        began = time.perf_counter_ns()
        features.append(oracle.process(event))
        feature_samples.append((time.perf_counter_ns() - began) / 1e6)
    x = np.asarray(features)
    samples = []
    for _ in range(50):
        predict(bundle, x[:1])
    for i in range(len(events)):
        began = time.perf_counter_ns()
        predict(bundle, x[i : i + 1])
        samples.append((time.perf_counter_ns() - began) / 1e6)
    batch_results = []
    for size in [1, 16, 64, 256]:
        raw = []
        for _ in range(100):
            began = time.perf_counter_ns()
            predict(bundle, x[:size])
            raw.append((time.perf_counter_ns() - began) / 1e6)
        batch_results.append(
            {
                "batch_size": size,
                "latency": summary(raw),
                "raw_ms": raw,
                "compute_predictions_per_second": size * len(raw) / (sum(raw) / 1000),
            }
        )
    api_samples = []
    with TestClient(app) as client:
        for i in range(min(count, 500)):
            began = time.perf_counter_ns()
            response = client.post(
                "/predict",
                json={"transaction_id": events[i]["transaction_id"], "features": x[i].tolist()},
            )
            response.raise_for_status()
            api_samples.append((time.perf_counter_ns() - began) / 1e6)
    rng = np.random.default_rng(42)
    baseline = rng.normal(0, 1, 5000)
    windows = [rng.normal(0, 1, 5000), rng.normal(0, 1, 5000), rng.normal(2, 1, 5000)]
    drift = [detect(baseline, window) for window in windows]
    return {
        **metadata("local"),
        "status": "executed",
        "model_version": info["model_version"],
        "dataset_version": info["dataset"]["dataset_version"],
        "transaction_count": len(events),
        "feature_names": FEATURE_NAMES,
        "concurrency": 1,
        "kafka_partitions": None,
        "consumer_workers": None,
        "actual_ml_inference": True,
        "mock_inference": False,
        "synthetic_transaction_data": True,
        "warmup_predictions": 50,
        "single_compute": summary(samples),
        "single_compute_raw_ms": samples,
        "batch_compute": batch_results,
        "in_process_fastapi": summary(api_samples),
        "in_process_fastapi_raw_ms": api_samples,
        "python_feature_oracle": summary(feature_samples),
        "python_feature_oracle_raw_ms": feature_samples,
        "drift_controlled_experiment": {
            "windows": drift,
            "shift_injected_window": 2,
            "false_alarms": sum(d["status"] == "input_shift" for d in drift[:2]),
            "detection_window": next(
                (i for i, d in enumerate(drift) if d["status"] == "input_shift"), None
            ),
        },
        "limitations": [
            "CPU component-only measurement",
            "FastAPI TestClient uses no TCP",
            "Python oracle timings are not Redis latency",
            "no queueing-delay benefit inferred from batch compute",
            "single run, no cross-machine generalization",
        ],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=2000)
    args = parser.parse_args()
    if args.count < 256:
        parser.error("use at least 256 events for the configured batch sizes")
    report = measure(args.count)
    directory = Path("benchmarks/results")
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / (report["experiment_id"] + ".json")
    target.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                "artifact": str(target),
                "single_compute": report["single_compute"],
                "in_process_fastapi": report["in_process_fastapi"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
