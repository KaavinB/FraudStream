"""Input drift is not quality deterioration; quality requires available labels."""

import numpy as np
from scipy.stats import ks_2samp


def detect(baseline: np.ndarray, recent: np.ndarray, minimum: int = 200) -> dict:
    if min(len(baseline), len(recent)) < minimum:
        return {"status": "insufficient_samples", "quality_deterioration": None}
    edges = np.unique(np.quantile(baseline, np.linspace(0, 1, 11)))
    if len(edges) < 3:
        return {"status": "insufficient_variation", "quality_deterioration": None}
    edges[0], edges[-1] = -np.inf, np.inf
    a = np.histogram(baseline, edges)[0] + 0.5
    b = np.histogram(recent, edges)[0] + 0.5
    a, b = a / a.sum(), b / b.sum()
    psi = float(np.sum((b - a) * np.log(b / a)))
    ks = ks_2samp(baseline, recent)
    return {
        "status": "input_shift" if psi > 0.2 else "stable",
        "psi": psi,
        "ks_statistic": float(ks.statistic),
        "ks_pvalue": float(ks.pvalue),
        "baseline_samples": len(baseline),
        "recent_samples": len(recent),
        "psi_threshold": 0.2,
        "quality_deterioration": None,
    }
