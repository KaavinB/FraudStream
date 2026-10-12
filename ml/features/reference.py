"""Prior-history features. Interval [t-window,t], excluding the current event.

Equal timestamps follow stream order. Regressions on either entity are quarantined.
This independent Python implementation is the oracle for the Redis Lua script.
"""

from collections import defaultdict, deque
from datetime import UTC, datetime
from typing import Any

FEATURE_VERSION = "prior-v1"
FEATURE_NAMES = [
    "amount",
    "hour",
    "day_of_week",
    "customer_count_5m",
    "customer_count_1h",
    "customer_count_24h",
    "customer_sum_1h",
    "customer_sum_24h",
    "customer_mean_24h",
    "amount_ratio",
    "seconds_since_previous",
    "distinct_merchants_24h",
    "merchant_count_1h",
    "merchant_mean_1h",
    "merchant_distinct_customers_1h",
]


def epoch(event: dict[str, Any]) -> float:
    stamp = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("timezone required")
    return stamp.timestamp()


class LateEvent(ValueError):
    pass


class ReferenceFeatures:
    def __init__(self) -> None:
        self.customers: Any = defaultdict(deque)
        self.merchants: Any = defaultdict(deque)
        self.latest: dict[str, float] = {}
        self.results: dict[str, tuple[dict[str, Any], list[float]]] = {}

    def process(self, event: dict[str, Any]) -> list[float]:
        identifier = event["transaction_id"]
        if identifier in self.results:
            original, values = self.results[identifier]
            if event != original:
                raise ValueError("conflicting transaction ID")
            return values.copy()
        t = epoch(event)
        ckey, mkey = "c:" + event["customer_id"], "m:" + event["merchant_id"]
        if any(t < self.latest.get(k, t) for k in (ckey, mkey)):
            raise LateEvent("event timestamp precedes an entity watermark")
        c, m = self.customers[ckey], self.merchants[mkey]
        while c and c[0][0] < t - 86400:
            c.popleft()
        while m and m[0][0] < t - 3600:
            m.popleft()
        c5 = [x for x in c if x[0] >= t - 300]
        c1 = [x for x in c if x[0] >= t - 3600]
        total = sum(x[1] for x in c)
        mean = total / len(c) if c else 0.0
        dt = datetime.fromtimestamp(t, tz=UTC)
        values = [
            float(event["amount"]),
            float(dt.hour),
            float(dt.weekday()),
            float(len(c5)),
            float(len(c1)),
            float(len(c)),
            sum(x[1] for x in c1),
            total,
            mean,
            float(event["amount"]) / mean if mean else 0.0,
            t - self.latest[ckey] if ckey in self.latest else 86400.0,
            float(len({x[2] for x in c})),
            float(len(m)),
            sum(x[1] for x in m) / len(m) if m else 0.0,
            float(len({x[2] for x in m})),
        ]
        c.append((t, float(event["amount"]), event["merchant_id"]))
        m.append((t, float(event["amount"]), event["customer_id"]))
        self.latest[ckey] = self.latest[mkey] = t
        self.results[identifier] = (event.copy(), values)
        return values.copy()
