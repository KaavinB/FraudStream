import json

import fakeredis
import numpy as np
import pytest

from benchmarks.common import summary
from benchmarks.features import BaselineRedis
from ml.evaluation.delayed import available_quality
from ml.features.reference import ReferenceFeatures
from tests.test_features import event, lua


def test_redis_baseline_correctness():
    client = fakeredis.FakeRedis()
    baseline = BaselineRedis(client, "baseline")
    oracle = ReferenceFeatures()
    for i in range(100):
        ev = event(i, i * 180, 10 + i, f"c{i % 3}", f"m{i % 2}")
        expected = oracle.process(ev)
        actual = baseline.snapshot(ev)
        np.testing.assert_allclose(expected, actual, atol=1e-7, rtol=1e-9)
        np.testing.assert_allclose(
            expected, json.loads(lua(client, ev, "SNAPSHOT")), atol=1e-7, rtol=1e-9
        )
        baseline.apply(ev)
        baseline.apply(ev)
        lua(client, ev, "APPLY")


def test_delayed_labels_are_not_reported_early():
    times = np.array([0, 100, 200, 300])
    labels = np.array([0, 1, 0, 1])
    scores = np.array([0.1, 0.8, 0.2, 0.9])
    early = available_quality(times, labels, scores, 100, 0.5)
    assert early["available_labels"] == 0 and early["quality"] is None
    late = available_quality(times, labels, scores, 7 * 86400 + 150, 0.5)
    assert late["available_labels"] == 2 and late["pending_labels"] == 2
    assert late["quality"]["recall"] == 1


def test_quantile_sample_size_boundary():
    assert summary([])["p95_ms"] is None
    assert summary([1] * 100)["p99_ms"] is None
    assert summary([1] * 1000)["p99_ms"] == pytest.approx(1)
