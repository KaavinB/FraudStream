"""Real-container smoke, duplicate, lateness and crash-point assertions.

No emulator fallback. A denied Docker socket yields a blocked artifact, not a pass.
Uses a fresh run ID and current event time; existing user dataset/state is preserved.
"""

import argparse
import asyncio
import json
import subprocess
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from benchmarks.common import metadata
from streaming.replay import api_key, decision


def docker(*args: str, check: bool = True):
    result = subprocess.run(["docker", *args], capture_output=True, text=True)
    if check and result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result


def sql(query: str):
    result = docker(
        "compose",
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        "fraudstream",
        "-d",
        "fraudstream",
        "-Atc",
        query,
    )
    return result.stdout.strip()


def event(identifier: str, offset: int = 0):
    return {
        "schema_version": 1,
        "transaction_id": identifier,
        "source_event_id": identifier,
        "customer_id": identifier + "-customer",
        "merchant_id": identifier + "-merchant",
        "amount": 10000,
        "currency": "USD",
        "transaction_type": "PAYMENT",
        "timestamp": (datetime.now(UTC) + timedelta(seconds=offset)).isoformat(),
    }


async def smoke(identifier: str):
    async with httpx.AsyncClient(
        base_url="http://localhost:8080",
        headers={"Authorization": "Bearer " + api_key()},
        timeout=10,
    ) as client:
        ev = event(identifier)
        response = await client.post("/api/v1/transactions", json=ev)
        assert response.status_code == 202, response.text
        first = await decision(client, identifier)
        response = await client.post("/api/v1/transactions", json=ev)
        assert response.status_code == 202
        repeated = await decision(client, identifier)
        assert first["risk_score"] == repeated["risk_score"]
        assert first["persisted_at"] == repeated["persisted_at"]
        # Ensure Kafka duplicates are drained before asserting feature counts.
        await asyncio.sleep(1)
        assert sql(f"SELECT count(*) FROM decisions WHERE transaction_id='{identifier}'") == "1"
        assert (
            sql(f"SELECT count(*) FROM processing_state WHERE transaction_id='{identifier}'") == "1"
        )
        assert sql(f"SELECT count(*) FROM fraud_alerts WHERE transaction_id='{identifier}'") in {
            "0",
            "1",
        }
        late = {
            **ev,
            "transaction_id": identifier + "-late",
            "source_event_id": identifier + "-late",
            "timestamp": (
                datetime.fromisoformat(ev["timestamp"]) - timedelta(seconds=1)
            ).isoformat(),
        }
        response = await client.post("/api/v1/transactions", json=late)
        assert response.status_code == 202
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if (
                int(
                    sql(
                        f"SELECT count(*) FROM quarantined_events WHERE payload LIKE '%{identifier}-late%'"
                    )
                )
                > 0
            ):
                break
            await asyncio.sleep(0.2)
        else:
            raise AssertionError("late event was not durably quarantined")
        assert (
            sql(f"SELECT count(*) FROM decisions WHERE transaction_id='{identifier}-late'") == "0"
        )
        return {"unique_decisions": 1, "duplicates_submitted": 1, "late_events_quarantined": 1}


async def crash_test(stage: str, identifier: str):
    docker("compose", "stop", "processor")
    container = "fraudstream-fault-" + uuid.uuid4().hex[:8]
    publisher = stage == "during_alert_publication"
    if publisher:
        docker("compose", "stop", "alerts")
        docker("compose", "start", "processor")
    service = "alerts" if publisher else "processor"
    docker(
        "compose",
        "run",
        "-d",
        "--name",
        container,
        "-e",
        "CRASH_AT=" + stage,
        "-e",
        "CRASH_TRANSACTION_ID=" + identifier,
        service,
    )
    began = time.monotonic()
    try:
        async with httpx.AsyncClient(
            base_url="http://localhost:8080",
            headers={"Authorization": "Bearer " + api_key()},
            timeout=10,
        ) as client:
            response = await client.post("/api/v1/transactions", json=event(identifier))
            assert response.status_code == 202
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                result = docker(
                    "inspect", "--format", "{{.State.ExitCode}} {{.State.Status}}", container
                ).stdout.strip()
                if result == "86 exited":
                    break
                await asyncio.sleep(0.2)
            else:
                raise AssertionError("configured crash point did not execute: " + stage)
            recovered_at = time.monotonic()
            docker("compose", "start", service)
            await decision(client, identifier)
            # Drain alert outbox and wait for consumer observation when a logical alert exists.
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                pending = int(
                    sql(
                        f"SELECT count(*) FROM outbox WHERE event_id IN ('alert-{identifier}','score-{identifier}') AND published_at IS NULL"
                    )
                )
                unobserved = int(
                    sql(
                        f"SELECT count(*) FROM fraud_alerts WHERE transaction_id='{identifier}' AND observed_at IS NULL"
                    )
                )
                if pending == 0 and unobserved == 0:
                    break
                await asyncio.sleep(0.1)
            assert pending == 0 and unobserved == 0
            assert sql(f"SELECT count(*) FROM decisions WHERE transaction_id='{identifier}'") == "1"
            alerts = int(
                sql(f"SELECT count(*) FROM fraud_alerts WHERE transaction_id='{identifier}'")
            )
            assert alerts <= 1
            return {
                "scenario": stage,
                "transaction_id": identifier,
                "recovery_seconds": time.monotonic() - recovered_at,
                "total_seconds": time.monotonic() - began,
                "expected_events": 1,
                "missing_durable_decisions": 0,
                "duplicate_durable_decisions": 0,
                "duplicate_logical_alerts": 0,
                "logical_alert_count": alerts,
            }
    finally:
        docker("rm", container, check=False)  # only the explicitly created stopped test container
        docker("compose", "start", "processor", "alerts", check=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke-only", action="store_true")
    args = parser.parse_args()
    report = {**metadata("reliability"), "status": "blocked", "scenarios": []}
    try:
        docker("info", "--format", "{{.ServerVersion}}")
        run_id = "integration-" + uuid.uuid4().hex[:12]
        report["smoke"] = asyncio.run(smoke(run_id))
        if not args.smoke_only:
            for stage in [
                "before_features",
                "after_feature_intent",
                "after_features",
                "before_inference",
                "after_inference",
                "after_persistence",
                "before_offset_commit",
                "during_alert_publication",
            ]:
                report["scenarios"].append(asyncio.run(crash_test(stage, run_id + "-" + stage)))
        report["status"] = "passed"
    except Exception as exc:
        report["error"] = str(exc)
        report["status"] = "blocked" if "permission denied" in str(exc).lower() else "failed"
    directory = Path("benchmarks/results")
    directory.mkdir(parents=True, exist_ok=True)
    artifact = directory / (report["experiment_id"] + ".json")
    artifact.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {"status": report["status"], "artifact": str(artifact), "error": report.get("error")},
            indent=2,
        )
    )
    if report["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
