import argparse
import asyncio
import json
import os
import time
from pathlib import Path

import httpx


def api_key() -> str:
    if os.environ.get("API_KEY"):
        return os.environ["API_KEY"]
    if Path(".env").exists():
        for line in Path(".env").read_text().splitlines():
            if line.startswith("API_KEY="):
                return line.split("=", 1)[1]
    raise RuntimeError("API_KEY missing; run make setup")


async def decision(client: httpx.AsyncClient, identifier: str, timeout: float = 60) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = await client.get(f"/api/v1/transactions/{identifier}/decision")
        if response.status_code == 200 and response.json().get("decision"):
            return response.json()
        if response.status_code not in (200, 202, 404, 503):
            raise RuntimeError(f"decision query status {response.status_code}")
        await asyncio.sleep(0.02)
    raise TimeoutError("durable decision unavailable: " + identifier)


async def replay(
    path: Path, count: int, concurrency: int, wait_each: bool, duplicates: bool, reverse: bool
) -> None:
    with path.open() as source:
        events = [json.loads(line) for _, line in zip(range(count), source, strict=False)]
    if reverse:
        events.reverse()
    if duplicates:
        events = [event for event in events for _ in range(2)]
    slots = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(
        base_url=os.environ.get("API_URL", "http://localhost:8080"),
        headers={"Authorization": "Bearer " + api_key()},
        timeout=10,
    ) as client:

        async def send(event):
            async with slots:
                response = await client.post("/api/v1/transactions", json=event)
                if response.status_code != 202:
                    raise RuntimeError(f"acceptance failed {response.status_code}: {response.text}")
                if wait_each:
                    await decision(client, event["transaction_id"])

        began = time.monotonic()
        await asyncio.gather(*(send(event) for event in events))
        print(
            json.dumps(
                {
                    "submitted_requests": len(events),
                    "elapsed_seconds": time.monotonic() - began,
                    "durable_decisions_waited": wait_each,
                    "concurrency": concurrency,
                    "duplicates": duplicates,
                    "reverse_event_time": reverse,
                }
            )
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=Path, default=Path("ml/data/raw/events.jsonl"))
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--wait-each", action="store_true")
    parser.add_argument("--duplicates", action="store_true")
    parser.add_argument("--reverse", action="store_true")
    args = parser.parse_args()
    if args.count < 1 or args.concurrency < 1:
        parser.error("count and concurrency must be positive")
    asyncio.run(
        replay(
            args.path, args.count, args.concurrency, args.wait_each, args.duplicates, args.reverse
        )
    )


if __name__ == "__main__":
    main()
