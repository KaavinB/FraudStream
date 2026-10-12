"""Closed-loop load. Unique decisions/sec includes backlog drain; HTTP acceptance is separate."""

import argparse
import asyncio
import json
import os
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import httpx

from benchmarks.common import metadata, summary
from streaming.replay import api_key, decision


def load_events(count: int) -> list[dict]:
    identifier = uuid.uuid4().hex[:12]
    stamp = datetime.now(UTC).isoformat()
    with Path("ml/data/raw/events.jsonl").open() as stream:
        events = [json.loads(line) for _, line in zip(range(count), stream, strict=False)]
    for i, event in enumerate(events):
        event["transaction_id"] = event["source_event_id"] = f"bench-{identifier}-{i}"
        # Equal event timestamps prevent arrival-order regressions under HTTP concurrency.
        event["timestamp"] = stamp
    return events


async def completed_decisions(client: httpx.AsyncClient) -> int:
    response = await client.get("/api/v1/analytics/overview")
    response.raise_for_status()
    return int(response.json()["completed_unique_decisions"])


async def drain(concurrency: int, count: int) -> dict:
    """Submit without per-request polling, then watch one aggregate counter until the backlog drains.

    Isolates pipeline throughput from the closed-loop client's own decision-polling load.
    """
    events = load_events(count)
    slots = asyncio.Semaphore(concurrency)
    errors: list[str] = []
    async with httpx.AsyncClient(
        base_url=os.environ.get("API_URL", "http://localhost:8080"),
        headers={"Authorization": "Bearer " + api_key()},
        timeout=10,
        limits=httpx.Limits(max_connections=concurrency + 5),
    ) as client:
        baseline = await completed_decisions(client)

        async def send(event):
            async with slots:
                try:
                    response = await client.post("/api/v1/transactions", json=event)
                    if response.status_code != 202:
                        errors.append("http_" + str(response.status_code))
                except httpx.HTTPError as exc:
                    errors.append(type(exc).__name__)

        began = time.perf_counter()
        await asyncio.gather(*(send(event) for event in events))
        submitted = time.perf_counter() - began
        target = baseline + count - len(errors)
        deadline = time.monotonic() + 300
        done = baseline
        while done < target and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
            done = await completed_decisions(client)
        elapsed = time.perf_counter() - began
    drained = done - baseline
    return {
        "mode": "drain",
        "concurrency": concurrency,
        "transaction_count": count,
        "submission_seconds": submitted,
        "accepted_requests_per_second": (count - len(errors)) / submitted,
        "elapsed_seconds": elapsed,
        "completed_unique_decisions": drained,
        "completed_unique_decisions_per_second": drained / elapsed,
        "counter_poll_interval_s": 0.1,
        "error_count": len(errors) + (count - len(errors) - drained),
        "errors": sorted(set(errors)),
    }


async def trial(concurrency: int, count: int) -> dict:
    events = load_events(count)
    slots = asyncio.Semaphore(concurrency)
    raw = []
    trial_begin = time.perf_counter()
    async with httpx.AsyncClient(
        base_url=os.environ.get("API_URL", "http://localhost:8080"),
        headers={"Authorization": "Bearer " + api_key()},
        timeout=10,
        limits=httpx.Limits(max_connections=concurrency + 5),
    ) as client:

        async def send(event):
            async with slots:
                begin = time.perf_counter()
                row = None
                try:
                    response = await client.post("/api/v1/transactions", json=event)
                    confirmed = time.perf_counter()
                    if response.status_code != 202:
                        raw.append(
                            {
                                "id": event["transaction_id"],
                                "error": "http_" + str(response.status_code),
                            }
                        )
                        return
                    row = {
                        "id": event["transaction_id"],
                        "accepted_elapsed_s": confirmed - trial_begin,
                        "http_acceptance_ms": (confirmed - begin) * 1000,
                    }
                    raw.append(row)
                    stored = await decision(client, event["transaction_id"], timeout=90)
                    completed = time.perf_counter()
                    row.update(
                        {
                            "id": event["transaction_id"],
                            "accepted_elapsed_s": confirmed - trial_begin,
                            "http_acceptance_ms": (confirmed - begin) * 1000,
                            "decision_after_http_response_ms": (completed - confirmed) * 1000,
                            "server_precommit_ms": stored["precommit_ms"],
                            "completed_elapsed_s": completed - trial_begin,
                        }
                    )
                except (httpx.HTTPError, TimeoutError) as exc:
                    if row is None:
                        raw.append({"id": event["transaction_id"], "error": type(exc).__name__})
                    else:
                        row["error"] = type(exc).__name__

        await asyncio.gather(*(send(event) for event in events))
    elapsed = time.perf_counter() - trial_begin
    completed = [r for r in raw if "completed_elapsed_s" in r]
    accepted = [r for r in raw if "accepted_elapsed_s" in r]
    return {
        "mode": "closed",
        "concurrency": concurrency,
        "transaction_count": len(events),
        "elapsed_seconds": elapsed,
        "completed_unique_decisions_per_second": len({r["id"] for r in completed}) / elapsed,
        "accepted_requests_per_second": len(accepted)
        / max((r["accepted_elapsed_s"] for r in accepted), default=elapsed),
        "server_precommit": summary([r["server_precommit_ms"] for r in completed]),
        "client_post_ack_to_observed_decision": summary(
            [r["decision_after_http_response_ms"] for r in completed]
        ),
        "error_count": len(events) - len(completed),
        "error_rate": 1 - len(completed) / len(events),
        "raw": raw,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 10, 25, 50, 100])
    parser.add_argument("--count", type=int, default=500)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument(
        "--mode",
        choices=["closed", "drain"],
        default="closed",
        help="closed: each request polls its own decision; drain: submit then watch one counter",
    )
    args = parser.parse_args()
    if args.count < 1 or args.repetitions < 1 or any(c < 1 for c in args.concurrency):
        parser.error("count, repetitions and concurrency must be positive")
    report = {
        **metadata("pipeline"),
        "status": "executed",
        "actual_ml_inference": True,
        "mock_inference": False,
        "synthetic_transaction_data": True,
        "kafka_partitions": int(os.environ.get("KAFKA_PARTITIONS", "1")),
        "consumer_workers": int(os.environ.get("CONSUMER_WORKERS", "1")),
        "model_version": json.loads(Path("ml/artifacts/model-info.json").read_text())[
            "model_version"
        ],
        "dataset_version": json.loads(Path("ml/data/raw/metadata.json").read_text())[
            "dataset_version"
        ],
        "mode": args.mode,
        "workload": "Original amounts/entities, unique IDs, same wall-clock event timestamp per trial; "
        + (
            "closed loop includes per-request decision polling every 20ms"
            if args.mode == "closed"
            else "drain: unpolled submission, completion from aggregate decision counter (0.1s resolution)"
        ),
        "conditions": {
            "compose_resource_limits": "docker-compose.yml",
            "partition_count_source": "operator-declared; verify before comparison",
        },
        "trials": [],
    }
    for concurrency in args.concurrency:
        for repetition in range(args.repetitions):
            runner = trial if args.mode == "closed" else drain
            trial_result = asyncio.run(runner(concurrency, args.count))
            report["trials"].append({"repetition": repetition, **trial_result})
            print(json.dumps({k: v for k, v in trial_result.items() if k != "raw"}))
    path = Path("benchmarks/results")
    path.mkdir(parents=True, exist_ok=True)
    if args.mode == "drain":
        report["experiment_id"] = report["experiment_id"].replace("pipeline-", "pipeline-drain-")
    (path / (report["experiment_id"] + ".json")).write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
