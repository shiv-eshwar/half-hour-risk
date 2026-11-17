"""Detect stale models, thresholds or evaluation tables in the public forecast ledger."""

import csv
import gzip
import json
import unittest
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "results"


class PublishedForecastTest(unittest.TestCase):
    def test_ledger_reproduces_every_heldout_score(self):
        with gzip.open(ROOT / "forecasts.csv.gz", "rt") as f:
            rows = list(csv.DictReader(f))
        selection = json.loads((ROOT / "validation.json").read_text())
        report = json.loads((ROOT / "holdout.json").read_text())
        times = [datetime.fromisoformat(row["issue_utc"]) for row in rows]
        self.assertEqual(len(times), 61 * 48)
        self.assertTrue(all((b - a).total_seconds() == 1800 for a, b in zip(times, times[1:])))
        labels = [int(r["event"]) for r in rows]
        self.assertEqual(labels, [int(float(r["target_rv"]) > selection["train_stress_threshold"]) for r in rows])
        total = sum(labels)
        for name, result in report["metrics"].items():
            scores = [float(row[name]) for row in rows]
            cutoff = selection["validation"][name]["alert_cutoff"]
            self.assertEqual(cutoff, result["alert_cutoff"])
            counts = Counter((score > cutoff, y) for score, y in zip(scores, labels))
            self.assertEqual([counts[True, 1], counts[True, 0], counts[False, 1], counts[False, 0]],
                             [result[k] for k in ("tp", "fp", "fn", "tn")])
            # Independently integrate the threshold-by-threshold precision increments.
            groups = Counter(scores)
            hits = Counter(s for s, y in zip(scores, labels) if y)
            cum_hits = cum_alerts = 0
            ap = 0.0
            for score in sorted(groups, reverse=True):
                cum_hits += hits[score]
                cum_alerts += groups[score]
                ap += (hits[score] / total) * (cum_hits / cum_alerts)
            self.assertAlmostEqual(result["average_precision"], ap, places=12)
