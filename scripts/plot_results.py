"""Render the held-out precision–recall comparison from published forecast scores."""

import csv
import gzip
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import PercentFormatter

ROOT = Path(__file__).resolve().parents[1] / "results"
report = json.loads((ROOT / "holdout.json").read_text())
with gzip.open(ROOT / "forecasts.csv.gz", "rt") as file:
    rows = list(csv.DictReader(file))
y = np.array([int(r["event"]) for r in rows])
variants = [(report["selected_tape"], "Bars plus trade tape", "#136a62"),
            (report["matched_twin"], "Matched bars only", "#ac4d1b"),
            ("persistence", "Volatility persistence", "#536683")]
fig, ax = plt.subplots(figsize=(8.8, 5.4), layout="constrained")
for column, title, color in variants:
    scores = np.array([float(r[column]) for r in rows])
    order = np.argsort(-scores, kind="stable")
    ends = np.r_[np.flatnonzero(np.diff(scores[order]) != 0), len(y) - 1]
    tp = np.cumsum(y[order])[ends]
    precision, recall = tp / (ends + 1), tp / y.sum()
    stats = report["metrics"][column]
    ax.step(recall, precision, where="pre", color=color, lw=1.35, alpha=0.92,
            label=f"{title}  ·  AP {stats['average_precision']:.3f}")
    if stats["precision"] is not None:
        ax.scatter(stats["recall"], stats["precision"], facecolor=color, edgecolor="white", s=55, zorder=5)
ax.axhline(y.mean(), color="#727878", linewidth=1, linestyle="--", label=f"Event prevalence  ·  {y.mean():.1%}")
ax.set(xlim=(0, 1), ylim=(0, 1.02), xlabel="Share of stress windows detected (recall)",
       ylabel="Share of warnings followed by stress (precision)")
ax.xaxis.set_major_formatter(PercentFormatter(1))
ax.yaxis.set_major_formatter(PercentFormatter(1))
ax.spines[["right", "top"]].set_visible(False)
ax.grid(axis="y", alpha=0.15)
ax.legend(frameon=False, loc="upper right", fontsize=9)
ax.set_title("Do trade-level features improve Bitcoin risk warnings?",
             loc="left", pad=24, fontsize=15, fontweight="bold")
ax.text(0, 1.025, "May–June 2024 holdout  ·  dots mark score cutoffs chosen in April",
        transform=ax.transAxes, fontsize=9, color="#41464a")
fig.savefig(ROOT / "precision-recall.png", dpi=170)
