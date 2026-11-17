"""Chronological model selection, matched ablation and held-out finance evaluation."""

import csv
import gzip
import io
from datetime import datetime, timezone

import numpy as np
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from .core import MAY, END, RESULTS, RUN, SEED, write_json
from .demo import package_model
from .evaluate import alert_cutoff, average_precision, paired_days, summarize
from .panel import BARS, TAPE

GRID = {"logistic": [0.01, 0.1], "gbt": [2, 4]}
BASELINE = "persistence"


def build(family, param, tape):
    vec = VectorAssembler(inputCols=BARS + (TAPE if tape else []), outputCol="features")
    if family == "logistic":
        estimator = LogisticRegression(regParam=param, elasticNetParam=0.0, standardization=True, maxIter=100)
    else:
        estimator = GBTClassifier(maxDepth=param, maxIter=25, stepSize=0.1, seed=SEED, maxBins=32)
    return Pipeline(stages=[vec, estimator])


def forecast(data, model=None):
    scored = (data.withColumn("score", F.col("past_rv")) if model is None else
              model.transform(data).withColumn("score", vector_to_array("probability")[1]))
    rows = scored.select("issue_s", "label", "score").orderBy("issue_s").collect()
    return tuple(np.array([r[col] for r in rows]) for col in ("issue_s", "label", "score"))


def utc(sec):
    return datetime.fromtimestamp(int(sec), timezone.utc).isoformat()


def train(spark):
    data = spark.read.parquet(str(RUN / "panel"))
    train_cut, regime_cut = data.filter("split='train'").selectExpr(
        "percentile(target_rv, 0.9)", "percentile(rv24h, 0.5)").first()
    data = data.withColumn("label", (F.col("target_rv") > train_cut).cast("double"))
    training = (data.filter("split='train'").repartition(4, "issue_s").sortWithinPartitions("issue_s").cache())
    train_counts = training.agg(F.count("*"), F.sum("label")).first()
    validation = data.filter("split='validation'").cache()
    dest = RUN / "models"
    dest.mkdir(parents=True, exist_ok=True)
    val = {}

    def validate(name, model):
        ts, y, pred = forecast(validation, model)
        val[name] = summarize(y, pred, alert_cutoff(pred), len(np.unique(ts // 86400)), model is not None)

    validate(BASELINE, None)
    settings = {}
    for family, params in GRID.items():
        for param in params:
            for with_tape in (False, True):
                group = "tape" if with_tape else "bars"
                name = f"{group}_{family}_{param}"
                print(f"Fitting {name}", flush=True)
                model = build(family, param, with_tape).fit(training)
                model.write().overwrite().save(str(dest / name))
                validate(name, model)
                settings[name] = {"group": group, "family": family, "parameter": param}
    training.unpersist()
    validation.unpersist()

    def best(names):
        return min(names, key=lambda n: (-val[n]["average_precision"], n))

    by_family = {f"{g}_{f}": best([n for n, s in settings.items() if s["group"] == g and s["family"] == f])
                 for g in ("bars", "tape") for f in GRID}
    full = best([by_family[f"tape_{f}"] for f in GRID])
    bars = best([by_family[f"bars_{f}"] for f in GRID])
    twin = full.replace("tape_", "bars_", 1)
    write_json(RESULTS / "validation.json", {
        "train_stress_threshold": train_cut, "train_regime_median": regime_cut,
        "train_windows": train_counts[0], "train_events": int(train_counts[1]),
        "settings": settings, "validation": val, "best_by_family": by_family,
        "selected_tape": full, "selected_bars": bars, "matched_twin": twin})

    package_model(dest / full)
    test = data.filter("split='test'").cache()
    test_rows = test.orderBy("issue_s").collect()
    issue = np.array([r.issue_s for r in test_rows])
    y = np.array([r.label for r in test_rows], dtype=int)
    pre_risk = np.array([r.rv24h for r in test_rows])
    predictions, metrics = {}, {}
    names = list(dict.fromkeys([BASELINE, *by_family.values(), twin]))
    for name in names:
        model = None if name == BASELINE else PipelineModel.load(str(dest / name))
        ts, labels, scores = forecast(test, model)
        assert np.array_equal(issue, ts) and np.array_equal(y, labels)
        assert np.all(np.isfinite(scores))
        predictions[name] = scores
        metrics[name] = summarize(y, scores, val[name]["alert_cutoff"], (END - MAY) // 86400, model is not None)
    test.unpersist()
    slices = {}
    for key, mask in (("calm", pre_risk <= regime_cut), ("active", pre_risk > regime_cut)):
        slices[key] = {"windows": int(mask.sum()), "events": int(y[mask].sum()),
                       "ap": {n: average_precision(y[mask], predictions[n][mask]) for n in names}}
    interval = paired_days(y, predictions[full], predictions[twin], issue // 86400)
    write_json(RESULTS / "holdout.json", {"selected_tape": full, "matched_twin": twin, "best_bars": bars,
               "metrics": metrics, "regimes": slices, "paired_tape_minus_matched_bars": interval})
    # Keep gzip output deterministic and compact for independent re-scoring.
    with (RESULTS / "forecasts.csv.gz").open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as zipped:
            with io.TextIOWrapper(zipped, encoding="utf-8", newline="") as out:
                writer = csv.writer(out)
                writer.writerow(["issue_utc", "target_rv", "event", "past_rv24h", *names])
                for i, r in enumerate(test_rows):
                    writer.writerow([utc(issue[i]), r.target_rv, int(y[i]), pre_risk[i],
                                     *[predictions[n][i] for n in names]])
    missed = (predictions[full] <= val[full]["alert_cutoff"]) & (y == 1)
    picked = {"first_alert": np.flatnonzero(predictions[full] > val[full]["alert_cutoff"])[:1],
              "largest_missed_event": [np.argmax(np.where(missed, [r.target_rv for r in test_rows], -1))]
              if missed.any() else []}
    cases = {}
    for kind, indices in picked.items():
        for i in indices:
            r = test_rows[i]
            cases[kind] = {"issue_utc": utc(r.issue_s), "end_utc": utc(r.issue_s + 1800),
                           "previous_rv": r.past_rv, "target_rv": r.target_rv, "observed_event": int(r.label),
                           "stress_threshold": train_cut, "score": float(predictions[full][i]),
                           "alert_cutoff": val[full]["alert_cutoff"], "model": full,
                           "feature_values": {col: r[col] for col in BARS + TAPE}}
    write_json(RESULTS / "case_studies.json", cases)
