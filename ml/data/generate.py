"""Original simulator inspired by the Handbook's behavior/fraud scenarios.

Not a byte-for-byte reproduction of its dataset or implementation.
Generation holds one day's events in memory, then writes separate events and labels.
"""

import argparse
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np


def generate(out: Path, customers: int = 1200, days: int = 60, seed: int = 42) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    means = rng.uniform(5, 100, customers)
    rates = rng.uniform(0.5, 4, customers)
    locations = rng.uniform(0, 100, (customers, 2))
    terminals = rng.uniform(0, 100, (max(100, customers // 2), 2))
    accessible = [np.where(np.linalg.norm(terminals - p, axis=1) <= 20)[0] for p in locations]
    start = datetime(2025, 1, 1, tzinfo=UTC)
    count = fraud_count = 0
    active_customers, active_merchants = set(), set()
    first = last = None
    with (out / "events.jsonl").open("w") as events, (out / "labels.jsonl").open("w") as labels:
        for day in range(days):
            compromised_merchants = set(
                rng.choice(len(terminals), max(1, len(terminals) // 100), replace=False)
            )
            compromised_customers = set(
                rng.choice(customers, max(1, customers // 100), replace=False)
            )
            daily = []
            for customer in range(customers):
                if len(accessible[customer]) == 0:
                    continue
                for _ in range(int(rng.poisson(rates[customer]))):
                    second = int(rng.normal(43200, 20000))
                    if not 0 < second < 86400:
                        continue
                    merchant = int(rng.choice(accessible[customer]))
                    amount = float(rng.normal(means[customer], means[customer] / 2))
                    if amount <= 0:
                        amount = float(rng.uniform(0.01, means[customer] * 2))
                    scenario = 0
                    if amount > 220:
                        scenario = 1
                    if merchant in compromised_merchants and rng.random() < 0.1:
                        scenario = 2
                    if customer in compromised_customers and rng.random() < 0.3:
                        amount *= 5
                        scenario = 3
                    daily.append(
                        (second, customer, merchant, max(0.01, round(amount, 2)), scenario)
                    )
            daily.sort(key=lambda x: (x[0], x[1], x[2]))
            for second, customer, merchant, amount, scenario in daily:
                identifier = f"s{seed}-t{count:09d}"
                stamp = start + timedelta(days=day, seconds=second)
                event = {
                    "schema_version": 1,
                    "transaction_id": identifier,
                    "source_event_id": identifier,
                    "customer_id": f"c{customer}",
                    "merchant_id": f"m{merchant}",
                    "amount": amount,
                    "currency": "USD",
                    "transaction_type": "PAYMENT",
                    "timestamp": stamp.isoformat(),
                }
                events.write(json.dumps(event, separators=(",", ":")) + "\n")
                labels.write(
                    json.dumps(
                        {
                            "transaction_id": identifier,
                            "fraud": int(scenario > 0),
                            "scenario": scenario,
                            "available_at": (stamp + timedelta(days=7)).isoformat(),
                        }
                    )
                    + "\n"
                )
                first = first or stamp.isoformat()
                last = stamp.isoformat()
                count += 1
                fraud_count += int(scenario > 0)
                active_customers.add(customer)
                active_merchants.add(merchant)
    with (out / "events.jsonl").open("rb") as generated:
        digest = hashlib.file_digest(generated, "sha256").hexdigest()
    meta = {
        "dataset_version": "handbook-inspired-v1-" + digest[:12],
        "seed": seed,
        "transactions": count,
        "fraud_count": fraud_count,
        "fraud_prevalence": fraud_count / count if count else 0,
        "customer_count": len(active_customers),
        "merchant_count": len(active_merchants),
        "start": first,
        "end": last,
        "days": days,
        "event_sha256": digest,
        "label_delay_days": 7,
        "source": "original handbook-inspired synthetic simulator",
        "license": "MIT (original implementation and generated synthetic records)",
    }
    (out / "metadata.json").write_text(json.dumps(meta, indent=2) + "\n")
    return meta


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=Path("ml/data/raw"))
    parser.add_argument("--customers", type=int, default=1200)
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.customers < 10 or args.days < 14:
        parser.error("use at least 10 customers and 14 days")
    print(json.dumps(generate(args.out, args.customers, args.days, args.seed), indent=2))


if __name__ == "__main__":
    main()
