import json
from pathlib import Path

import numpy as np
import pytest

from ml.evaluation.drift import detect
from ml.evaluation.metrics import evaluate, threshold_at_fpr
from ml.training.train import masks


def test_threshold_ties_fpr():
    y = np.array([0] * 100 + [1] * 5)
    scores = np.array([0.1] * 99 + [0.9] + [0.95] * 5)
    threshold = threshold_at_fpr(y, scores)
    report = evaluate(y, scores, threshold)
    assert report["actual_fpr"] <= 0.01
    assert report["recall"] == 1
    zero = threshold_at_fpr(y, scores, 0)
    assert evaluate(y, scores, zero)["actual_fpr"] == 0


def test_temporal_splits_only_available_labels():
    t = np.linspace(0, 60 * 86400, 10000)
    available = t + 7 * 86400
    split, metadata = masks(t, available)
    tr, va, te = split
    train_cutoff = 0.6 * t.max()
    validation_cutoff = 0.8 * t.max()
    assert available[tr].max() <= train_cutoff
    assert available[va].max() <= validation_cutoff
    assert t[tr].max() < t[va].min() < t[te].min()
    assert not (tr & va).any() and not (va & te).any()
    assert metadata["purged_unavailable_labels"] > 0


def test_drift_distinguishes_unlabeled_shift():
    rng = np.random.default_rng(42)
    baseline = rng.normal(size=5000)
    stable = detect(baseline, rng.normal(size=5000))
    shifted = detect(baseline, rng.normal(2, size=5000))
    assert stable["status"] == "stable"
    assert shifted["status"] == "input_shift"
    assert shifted["quality_deterioration"] is None
    assert detect(baseline, np.array([1, 2]))["status"] == "insufficient_samples"


def test_executed_artifact_temporal_contract():
    path = Path("ml/artifacts/model-info.json")
    if not path.exists():
        pytest.skip("train a model before artifact validation")
    report = json.loads(path.read_text())
    assert report["selected_model"] in report["models"]
    selected = report["models"][report["selected_model"]]
    assert selected["validation"]["actual_fpr"] <= 0.01
    assert report["review_threshold"] == selected["validation"]["threshold"]
    assert selected["test"]["samples"] >= 100
