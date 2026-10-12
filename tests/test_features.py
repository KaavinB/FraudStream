import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import fakeredis
import numpy as np
import pytest

from ml.features.reference import LateEvent, ReferenceFeatures, epoch

SCRIPT = Path("internal/features/rolling.lua").read_text()


def event(i=0, seconds=0, amount=10.0, customer="c1", merchant="m1"):
    return {
        "transaction_id": f"t{i}",
        "source_event_id": f"t{i}",
        "schema_version": 1,
        "customer_id": customer,
        "merchant_id": merchant,
        "amount": amount,
        "timestamp": (datetime(2025, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds)).isoformat(),
        "currency": "USD",
        "transaction_type": "PAYMENT",
    }


def lua(cache, ev, mode):
    keys = [
        f"c:{ev['customer_id']}:history",
        f"c:{ev['customer_id']}:meta",
        f"m:{ev['merchant_id']}:history",
        f"m:{ev['merchant_id']}:meta",
    ]
    stamp = datetime.fromisoformat(ev["timestamp"])
    return cache.eval(
        SCRIPT,
        4,
        *keys,
        mode,
        epoch(ev),
        ev["amount"],
        ev["customer_id"],
        ev["merchant_id"],
        ev["transaction_id"],
        stamp.hour,
        stamp.weekday(),
    )


@pytest.mark.parametrize("seconds", [0, 300, 301, 3600, 3601, 86400, 86401, 172801])
def test_lua_parity_boundaries_and_duplicates(seconds):
    oracle, cache = ReferenceFeatures(), fakeredis.FakeRedis()
    first, second = event(), event(1, seconds, 25)
    np.testing.assert_allclose(json.loads(lua(cache, first, "SNAPSHOT")), oracle.process(first))
    lua(cache, first, "APPLY")
    lua(cache, first, "APPLY")  # duplicate materialization is atomic ZADD
    actual = json.loads(lua(cache, second, "SNAPSHOT"))
    np.testing.assert_allclose(actual, oracle.process(second), rtol=1e-10, atol=1e-8)
    assert oracle.process(first) == oracle.results["t0"][1]


def test_no_current_or_future_history():
    oracle = ReferenceFeatures()
    before = oracle.process(event())
    assert before[3:9] == [0] * 6
    oracle.process(event(1, 1, 100000))
    assert before == oracle.results["t0"][1]


def test_late_is_quarantined_without_mutation():
    cache, oracle = fakeredis.FakeRedis(), ReferenceFeatures()
    lua(cache, event(1, 100), "APPLY")
    oracle.process(event(1, 100))
    with pytest.raises(LateEvent):
        oracle.process(event(2, 99))
    with pytest.raises(Exception, match="LATE_EVENT"):
        lua(cache, event(2, 99), "SNAPSHOT")
    assert cache.zcard("c:c1:history") == 1


def test_conflicting_duplicate():
    oracle = ReferenceFeatures()
    oracle.process(event())
    with pytest.raises(ValueError, match="conflicting"):
        oracle.process(event(amount=999))


def test_generated_replay_lua_parity():
    oracle, cache = ReferenceFeatures(), fakeredis.FakeRedis()
    rng = np.random.default_rng(24)
    for i in range(500):
        ev = event(
            i,
            i * 283,
            round(float(rng.uniform(1, 400)), 2),
            f"c{rng.integers(20)}",
            f"m{rng.integers(10)}",
        )
        expected = oracle.process(ev)
        np.testing.assert_allclose(
            json.loads(lua(cache, ev, "SNAPSHOT")), expected, rtol=1e-9, atol=1e-7
        )
        lua(cache, ev, "APPLY")


def test_no_labels_in_generated_serving_events(tmp_path):
    from ml.data.generate import generate

    metadata = generate(tmp_path, customers=20, days=14)
    assert metadata["transactions"] > 0
    for line in (tmp_path / "events.jsonl").read_text().splitlines():
        assert not {"fraud", "label", "scenario", "available_at"}.intersection(json.loads(line))
    assert (tmp_path / "labels.jsonl").exists()


def test_generator_is_deterministic(tmp_path):
    from ml.data.generate import generate

    a = generate(tmp_path / "a", 20, 14)
    b = generate(tmp_path / "b", 20, 14)
    assert a == b
