"""Redis benchmark: simple client aggregation vs atomic server-side Lua.

Requires real Redis. Single caller, isolated namespaces, same workload and history.
The baseline is deliberately non-concurrent; Lua's atomicity is tested separately.
"""

import argparse
import json
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import redis

from benchmarks.common import metadata, summary


class BaselineRedis:
    def __init__(self, client, namespace):
        self.client, self.namespace = client, namespace

    def keys(self, event):
        root = self.namespace
        return [
            root + ":c:" + event["customer_id"] + ":history",
            root + ":c:" + event["customer_id"] + ":meta",
            root + ":m:" + event["merchant_id"] + ":history",
            root + ":m:" + event["merchant_id"] + ":meta",
        ]

    def args(self, event):
        t = datetime.fromisoformat(event["timestamp"])
        return [
            t.timestamp(),
            event["amount"],
            event["customer_id"],
            event["merchant_id"],
            event["transaction_id"],
            t.hour,
            t.weekday(),
        ]

    def snapshot(self, event):
        keys = self.keys(event)
        t, amount, _, _, _, hour, weekday = self.args(event)
        previous = self.client.hget(keys[1], "last")
        merchant_last = self.client.hget(keys[3], "last")
        if any(value is not None and float(value) > t for value in [previous, merchant_last]):
            raise ValueError("LATE_EVENT")
        c = [
            (json.loads(member), timestamp)
            for member, timestamp in self.client.zrangebyscore(
                keys[0], t - 86400, t, withscores=True
            )
        ]
        m = [json.loads(member) for member in self.client.zrangebyscore(keys[2], t - 3600, t)]
        c1 = [row for row in c if row[1] >= t - 3600]
        total = sum(row[0][1] for row in c)
        mean = total / len(c) if c else 0
        return [
            amount,
            hour,
            weekday,
            sum(row[1] >= t - 300 for row in c),
            len(c1),
            len(c),
            sum(row[0][1] for row in c1),
            total,
            mean,
            amount / mean if mean else 0,
            t - float(previous) if previous is not None else 86400,
            len({row[0][2] for row in c}),
            len(m),
            sum(row[1] for row in m) / len(m) if m else 0,
            len({row[2] for row in m}),
        ]

    def apply(self, event):
        keys = self.keys(event)
        t, amount, customer, merchant, identifier, _, _ = self.args(event)
        self.client.zadd(keys[0], {json.dumps([identifier, amount, merchant]): t})
        self.client.zadd(keys[2], {json.dumps([identifier, amount, customer]): t})
        self.client.zremrangebyscore(keys[0], "-inf", "(" + str(t - 86400))
        self.client.zremrangebyscore(keys[2], "-inf", "(" + str(t - 3600))
        self.client.hset(keys[1], "last", t)
        self.client.hset(keys[3], "last", t)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--entities", type=int, nargs="+", default=[10, 100, 1000])
    args = parser.parse_args()
    if args.count < 1 or any(n < 1 for n in args.entities):
        parser.error("count and entities must be positive")
    report = {**metadata("features"), "status": "blocked", "backend": "real Redis", "trials": []}
    try:
        client = redis.Redis.from_url(
            os.environ.get("REDIS_URL", "redis://localhost:6379"),
            socket_timeout=2,
            socket_connect_timeout=2,
        )
        client.ping()
        script = client.register_script(Path("internal/features/rolling.lua").read_text())
        base_time = datetime(2025, 1, 1, tzinfo=UTC)
        for entities in args.entities:
            events = [
                {
                    "transaction_id": f"t{i}",
                    "customer_id": f"c{i % entities}",
                    "merchant_id": f"m{i % entities}",
                    "amount": float(i % 300 + 1),
                    "timestamp": (base_time + timedelta(seconds=i)).isoformat(),
                }
                for i in range(args.count)
            ]
            outputs = {}
            for implementation in ["baseline_client", "lua_server"]:
                store = BaselineRedis(
                    client, report["experiment_id"] + f":{entities}:" + implementation
                )
                lookup, update, values = [], [], []
                began = time.perf_counter()
                for event in events:
                    before = time.perf_counter_ns()
                    if implementation == "baseline_client":
                        values.append(store.snapshot(event))
                    else:
                        values.append(
                            json.loads(
                                script(
                                    keys=store.keys(event), args=["SNAPSHOT", *store.args(event)]
                                )
                            )
                        )
                    lookup.append((time.perf_counter_ns() - before) / 1e6)
                    before = time.perf_counter_ns()
                    if implementation == "baseline_client":
                        store.apply(event)
                    else:
                        script(keys=store.keys(event), args=["APPLY", *store.args(event)])
                    update.append((time.perf_counter_ns() - before) / 1e6)
                outputs[implementation] = values
                report["trials"].append(
                    {
                        "entities": entities,
                        "implementation": implementation,
                        "events": len(events),
                        "events_per_second": len(events) / (time.perf_counter() - began),
                        "lookup": summary(lookup),
                        "update": summary(update),
                        "raw_lookup_ms": lookup,
                        "raw_update_ms": update,
                    }
                )
            np.testing.assert_allclose(
                outputs["baseline_client"], outputs["lua_server"], atol=1e-7, rtol=1e-9
            )
        report["status"] = "executed"
        report["redis_info"] = {
            k: v
            for k, v in client.info().items()
            if k in ["redis_version", "used_memory", "used_memory_peak"]
        }
        report["limitations"] = [
            "Single caller, excludes durable SQL journal",
            "reported Redis memory is whole instance",
            "isolated benchmark keys retained for inspection",
        ]
    except Exception as exc:
        report["error"] = str(exc)
    directory = Path("benchmarks/results")
    directory.mkdir(parents=True, exist_ok=True)
    artifact = directory / (report["experiment_id"] + ".json")
    artifact.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {"status": report["status"], "artifact": str(artifact), "error": report.get("error")}
        )
    )
    if report["status"] != "executed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
