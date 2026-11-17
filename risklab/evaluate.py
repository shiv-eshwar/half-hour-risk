"""Exact ranking and operational alert metrics for the small forecast table."""

import numpy as np


def average_precision(y, scores):
    y, scores = np.asarray(y, dtype=int), np.asarray(scores, dtype=float)
    total = y.sum()
    if not total:
        return None
    order = np.argsort(-scores, kind="stable")
    sorted_scores = scores[order]
    ends = np.r_[np.flatnonzero(sorted_scores[:-1] != sorted_scores[1:]), len(scores) - 1]
    hits = np.cumsum(y[order])[ends]
    return float(np.dot(np.diff(np.r_[0, hits]), hits / (ends + 1)) / total)


def alert_cutoff(scores):
    return float(np.quantile(scores, 0.9, method="higher"))


def summarize(y, scores, cutoff, days, probabilities):
    y, scores = np.asarray(y, dtype=int), np.asarray(scores, dtype=float)
    flagged = scores > cutoff
    tp = int(y[flagged].sum())
    fp = int(flagged.sum()) - tp
    fn = int(y.sum()) - tp
    return {"windows": len(y), "events": int(y.sum()), "prevalence": float(y.mean()),
            "average_precision": average_precision(y, scores), "alert_cutoff": cutoff,
            "alerts": int(flagged.sum()), "alert_rate": float(flagged.mean()),
            "tp": tp, "fp": fp, "fn": fn, "tn": len(y) - tp - fp - fn,
            "precision": tp / (tp + fp) if tp + fp else None,
            "recall": tp / (tp + fn) if tp + fn else None, "false_alerts_per_day": fp / days,
            "brier": float(np.mean((scores - y) ** 2)) if probabilities else None}


def paired_days(y, first, second, days, n=2000, seed=417):
    y, first, second, days = map(np.asarray, (y, first, second, days))
    unique = np.unique(days)
    groups = [np.flatnonzero(days == day) for day in unique]
    rng = np.random.default_rng(seed)
    diffs = []
    for _ in range(n):
        idx = np.concatenate([groups[i] for i in rng.integers(len(groups), size=len(groups))])
        if y[idx].sum():
            diffs.append(average_precision(y[idx], first[idx]) - average_precision(y[idx], second[idx]))
    return {"unit": "UTC day", "blocks": len(groups), "draws": n, "valid_draws": len(diffs),
            "seed": seed, "delta_ap": average_precision(y, first) - average_precision(y, second),
            "percentile_95": np.quantile(diffs, [0.025, 0.975]).tolist()}
