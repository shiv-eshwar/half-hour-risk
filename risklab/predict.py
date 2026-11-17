"""Issue a historical warning directly from the market data known at the boundary."""

import argparse
import json
from datetime import datetime, timezone

from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from .core import END, MAY, RESULTS, minute_tape, read_tape
from .demo import released_model
from .panel import bars, blocks, issue_features, minute_table


def issue_time(value):
    try:
        point = datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("Supply an ISO 8601 timestamp with a UTC offset.") from error
    if point.tzinfo is None or point.microsecond:
        raise argparse.ArgumentTypeError("Supply a half-hour timestamp with a UTC offset.")
    seconds = int(point.timestamp())
    if seconds % 1800 or not MAY <= seconds <= END:
        raise argparse.ArgumentTypeError("Choose a half-hour from 2024-05-01T00:00Z through 2024-07-01T00:00Z.")
    return seconds


def predict(spark, issue):
    minute = issue // 60
    # The oldest return requires the closing price one minute before the 24-hour lookback.
    interval = (minute - 1441, minute)
    btc, _ = bars(spark, "BTCUSDT", interval=interval)
    eth, _ = bars(spark, "ETHUSDT", interval=interval)
    start = issue - 1800
    month = datetime.fromtimestamp(start, timezone.utc).month
    raw = read_tape(spark, month, first_month=month).filter(F.col("ms").between(start * 1000, issue * 1000 - 1))
    mins = minute_table(spark, minute_tape(raw), btc, eth, interval=interval)
    features = issue_features(blocks(mins)).filter(F.col("issue_s") == issue)
    selection = json.loads((RESULTS / "validation.json").read_text())
    name = selection["selected_tape"]
    cutoff = selection["validation"][name]["alert_cutoff"]
    with released_model() as model:
        row = model.transform(features).select(vector_to_array("probability")[1].alias("score")).first()
    if row is None:
        raise ValueError("Incomplete price history or unreconciled trades at the requested boundary.")
    return {"issue_utc": datetime.fromtimestamp(issue, timezone.utc).isoformat(),
            "end_utc": datetime.fromtimestamp(issue + 1800, timezone.utc).isoformat(),
            "model": name, "score": row.score, "alert_cutoff": cutoff,
            "alert": row.score > cutoff, "stress_threshold": selection["train_stress_threshold"]}
