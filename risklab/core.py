import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

from pyspark.sql import SparkSession, functions as F

PARQUET = Path("data/parquet")
RUN = Path("runs")
RESULTS = Path("results")
SEED = 417


def epoch(date):
    return int(datetime.fromisoformat(date).replace(tzinfo=timezone.utc).timestamp())


START, APR, MAY, END = map(epoch, ("2024-01-01", "2024-04-01", "2024-05-01", "2024-07-01"))


def spark_session(stage):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.appName(f"half-hour-risk-{stage}")
             .master(os.environ.get("SPARK_MASTER", "local[4]"))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "24"))
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as out:
        json.dump(data, out, indent=2, allow_nan=False)
        out.write("\n")
        temp = Path(out.name)
    temp.replace(path)


def read_tape(spark, months=6, *, first_month=1):
    # Attach source-month bounds once per file so row validation uses integer comparisons.
    parts = []
    for month in range(first_month, months + 1):
        date = f"2024-{month:02}"
        lo = epoch(f"{date}-01") * 1000
        hi = epoch(f"2024-{month + 1:02}-01") * 1000
        part = (spark.read.parquet(str(PARQUET / "tape" / f"{date}.parquet"))
                .withColumn("month", F.lit(date))
                .withColumn("wrong_month", ~F.col("ms").between(lo, hi - 1)))
        parts.append(part)
    tape = parts[0]
    for part in parts[1:]:
        tape = tape.unionByName(part)
    required = ["ms", "price", "qty", "agg_id", "first_id", "last_id", "buyer_maker"]
    missing = F.lit(False)
    for name in required:
        missing = missing | F.col(name).isNull()
    invalid_number = (F.isnan("price") | F.isnan("qty") | (F.col("price") <= 0) | (F.col("qty") <= 0)
                      | (F.col("price") == float("inf")) | (F.col("qty") == float("inf")))
    status = (F.when(missing, "missing").when(F.col("wrong_month"), "wrong_month")
              .when(invalid_number, "invalid_price_or_size")
              .when((F.col("agg_id") < 0) | (F.col("first_id") < 0) |
                    (F.col("last_id") < F.col("first_id")), "invalid_ids").otherwise("accepted"))
    return tape.withColumn("status", status)


def trade_values(frame):
    value = F.col("price") * F.col("qty")
    return (frame.withColumn("value", value).withColumn("sq", F.col("value") * F.col("value"))
            .withColumn("buy_value", F.when(~F.col("buyer_maker"), F.col("value")).otherwise(0.0))
            .withColumn("signed_sq", F.when(F.col("buyer_maker"), -F.col("sq")).otherwise(F.col("sq"))))


def moments():
    return [F.count("*").alias("prints"), F.sum("value").alias("tape_quote"),
            F.sum("buy_value").alias("tape_buy_quote"), F.sum("sq").alias("sum_sq"),
            F.sum("signed_sq").alias("sum_signed_sq"),
            F.sum(F.col("last_id") - F.col("first_id") + 1).alias("native_fills"),
            F.max_by("price", F.struct("ms", "agg_id")).alias("last_price")]


def minute_tape(tape):
    values = trade_values(tape.filter("status='accepted'"))
    return values.groupBy(F.floor(F.col("ms") / 60000).cast("long").alias("minute")).agg(*moments())
