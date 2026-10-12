"""Evaluate only labels available by an explicit simulated observation time."""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np

from ml.evaluation.metrics import evaluate


def available_quality(timestamps, labels, scores, as_of, threshold, delay_seconds=7 * 86400):
    present = timestamps + delay_seconds <= as_of
    count = int(present.sum())
    result: dict[str, Any] = {
        "available_labels": count,
        "pending_labels": len(labels) - count,
        "quality": None,
    }
    if count and len(np.unique(labels[present])) == 2:
        result["quality"] = evaluate(labels[present], scores[present], threshold)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", required=True, help="ISO8601 timestamp with timezone")
    args = parser.parse_args()
    observation = datetime.fromisoformat(args.as_of)
    if observation.tzinfo is None:
        parser.error("timezone required")
    info = json.loads(Path("ml/artifacts/model-info.json").read_text())
    with np.load("ml/artifacts/heldout-predictions.npz") as predictions:
        report = available_quality(
            predictions["timestamps"],
            predictions["labels"],
            predictions["scores"],
            observation.timestamp(),
            info["review_threshold"],
        )
    print(json.dumps({"as_of": args.as_of, **report}, indent=2))


if __name__ == "__main__":
    main()
