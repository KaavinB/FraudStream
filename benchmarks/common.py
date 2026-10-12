import hashlib
import importlib.metadata
import os
import platform
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from ml.training.train import git_commit


def summary(samples: list[float]) -> dict:
    if not samples:
        return {"samples": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None, "mean_ms": None}
    return {
        "samples": len(samples),
        "p50_ms": float(np.percentile(samples, 50)),
        "p95_ms": float(np.percentile(samples, 95)),
        "p99_ms": float(np.percentile(samples, 99)) if len(samples) >= 1000 else None,
        "mean_ms": float(np.mean(samples)),
    }


def metadata(kind: str) -> dict:
    digest = hashlib.sha256()
    sources = sorted(
        p
        for root in ["internal", "services", "ml", "benchmarks", "streaming"]
        for p in Path(root).rglob("*")
        if p.suffix in {".go", ".py", ".lua"} and "__pycache__" not in p.parts
    )
    for path in sources:
        digest.update(str(path).encode())
        digest.update(path.read_bytes())
    return {
        "experiment_id": kind + "-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
        "date": datetime.now(UTC).isoformat(),
        "git_commit": git_commit(),
        "source_sha256": digest.hexdigest(),
        "seed": 42,
        "hardware": {
            "platform": platform.platform(),
            "architecture": platform.machine(),
            "logical_cpus": os.cpu_count(),
            "memory_bytes": None,
        },
        "software": {
            "python": platform.python_version(),
            **{p: importlib.metadata.version(p) for p in ["numpy", "scikit-learn", "fastapi"]},
        },
        "cpu_limits": {
            "OMP_NUM_THREADS": os.environ.get("OMP_NUM_THREADS"),
            "OPENBLAS_NUM_THREADS": os.environ.get("OPENBLAS_NUM_THREADS"),
        },
    }
