from typing import Any

import numpy as np
from sklearn.calibration import calibration_curve
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    precision_recall_curve,
    precision_recall_fscore_support,
    roc_auc_score,
)


def threshold_at_fpr(y: np.ndarray, scores: np.ndarray, target: float = 0.01) -> float:
    """Maximize validation recall subject to empirical FPR <= target; ties are atomic."""
    legitimate = np.sort(scores[y == 0])
    if len(legitimate) == 0 or not 0 <= target < 1:
        raise ValueError("invalid FPR target or no legitimate samples")
    allowed = int(np.floor(target * len(legitimate)))
    cutoff = legitimate[-allowed - 1]
    return float(np.nextafter(cutoff, np.inf))


def evaluate(y: np.ndarray, scores: np.ndarray, threshold: float) -> dict[str, Any]:
    predicted = scores >= threshold
    tn, fp, fn, tp = confusion_matrix(y, predicted, labels=[0, 1]).ravel()
    precision, recall, f1, _ = precision_recall_fscore_support(
        y, predicted, average="binary", zero_division=0
    )
    pr, re, _ = precision_recall_curve(y, scores)
    fraction, mean_score = calibration_curve(y, scores, n_bins=10, strategy="quantile")
    stride = max(1, len(pr) // 300)
    return {
        "average_precision": float(average_precision_score(y, scores)),
        "roc_auc": float(roc_auc_score(y, scores)) if len(np.unique(y)) == 2 else None,
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "actual_fpr": float(fp / (fp + tn)) if fp + tn else None,
        "false_positives_per_1000": float(1000 * fp / (fp + tn)) if fp + tn else None,
        "brier_score": float(brier_score_loss(y, scores)),
        "threshold": threshold,
        "confusion_matrix": [[int(tn), int(fp)], [int(fn), int(tp)]],
        "samples": int(len(y)),
        "fraud_samples": int(sum(y)),
        "pr_curve": {"precision": pr[::stride].tolist(), "recall": re[::stride].tolist()},
        "calibration": {"fraction_positive": fraction.tolist(), "mean_score": mean_score.tolist()},
        "score_histogram": np.histogram(scores, bins=np.linspace(0, 1, 21))[0].tolist(),
    }
