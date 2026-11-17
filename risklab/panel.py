"""Build a dense UTC calendar and align backward-looking evidence with future labels."""

import json
import math
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
from pyspark.sql import Window, functions as F

from .core import APR, END, MAY, PARQUET, RESULTS, RUN, START, minute_tape, read_tape, write_json

BARS = ["log_rv30", "log_rv6h", "log_rv24h", "btc_abs_return", "btc_log_quote", "btc_log_trades",
        "btc_imbalance", "eth_log_rv30", "eth_abs_return", "eth_imbalance", "eth_log_quote",
        "btc_eth_corr", "clock_sin", "clock_cos", "weekend"]
TAPE = ["log_concentration", "quadratic_imbalance", "imbalance_gap"]


def id_audit():
    output = []
    last = None
    for f in sorted((PARQUET / "tape").glob("*.parquet")):
        gaps = 0
        rows = 0
        first = None
        for batch in pq.ParquetFile(f).iter_batches(batch_size=500000, columns=["agg_id"]):
            ids = batch.column(0).to_numpy()
            if first is None:
                first = int(ids[0])
            diffs = np.diff(ids)
            if np.any(diffs <= 0) or (last is not None and int(ids[0]) <= last):
                raise ValueError(f"Aggregate trade identifiers overlap or regress in {f}")
            gaps += int((diffs - 1).sum()) + (int(ids[0]) - last - 1 if last is not None else 0)
            last = int(ids[-1])
            rows += len(ids)
        output.append({"month": f.stem, "rows": rows, "first_id": first, "last_id": last, "skipped_ids": gaps})
    return output


def bars(spark, symbol, *, interval=None):
    raw = spark.read.parquet(str(PARQUET / symbol))
    if interval is not None:
        start_minute, end_minute = interval
        raw = raw.filter(F.col("ms").between(start_minute * 60000, end_minute * 60000 - 1))
    minute = F.floor(F.col("ms") / 60000).cast("long")
    good = ((F.pmod("ms", F.lit(60000)) == 0) & (F.col("close_ms") == F.col("ms") + 59999)
            & F.col("ms").between(START * 1000, END * 1000 - 1)
            & (F.col("close") > 0) & (F.col("low") > 0)
            & F.col("close").between(F.col("low"), F.col("high"))
            & (F.col("quote") >= 0) & F.col("buy_quote").between(0, F.col("quote"))
            & (F.col("trades") >= 0))
    audited = raw.select(minute.alias("minute"), good.alias("valid"), "close", "quote", "buy_quote", "trades")
    a = audited.agg(F.count("*").alias("rows"), F.countDistinct("minute").alias("unique_minutes"),
                    F.sum(F.when(F.col("valid"), 0).otherwise(1)).alias("invalid_rows")).first().asDict()
    if a["unique_minutes"] != a["rows"]:
        raise ValueError(f"Non-unique minute in {symbol}")
    prefix = symbol[:3].lower()
    clean = audited.filter("valid").select("minute", *[F.col(c).alias(f"{prefix}_{c}")
                                                       for c in ("close", "quote", "buy_quote", "trades")])
    return clean, a


def near(x, y):
    return F.abs(x - y) <= F.lit(1e-5) + F.lit(1e-8) * F.abs(y)


def minute_table(spark, tape_minutes, btc, eth, *, interval=(START // 60, END // 60)):
    calendar = spark.range(*interval).select(F.col("id").alias("minute"))
    prices = calendar.join(btc, "minute", "left").join(eth, "minute", "left")
    previous = prices.select((F.col("minute") + 1).alias("minute"),
                             F.col("btc_close").alias("prior_b"), F.col("eth_close").alias("prior_e"))
    joined = (prices.join(previous, "minute", "left").join(tape_minutes, "minute", "left")
              .withColumn("rb", F.log(F.col("btc_close") / F.col("prior_b")))
              .withColumn("re", F.log(F.col("eth_close") / F.col("prior_e"))))
    empty = F.col("prints").isNull() & (F.col("btc_trades") == 0)
    match = ((F.col("native_fills") == F.col("btc_trades"))
             & near(F.col("tape_quote"), F.col("btc_quote"))
             & near(F.col("tape_buy_quote"), F.col("btc_buy_quote"))
             & near(F.col("last_price"), F.col("btc_close")))
    return joined.withColumn("matched", F.coalesce(empty | match, F.lit(False)))


def blocks(mins):
    return (mins.groupBy(F.floor(F.col("minute") / 30).cast("long").alias("block"))
            .agg(F.count("rb").alias("btc_returns"), F.count("re").alias("eth_returns"),
                 F.sum(F.col("matched").cast("int")).alias("matched_minutes"),
                 F.sum(F.col("rb") ** 2).alias("rv"), F.sum(F.col("re") ** 2).alias("erv"),
                 F.sum(F.col("rb") * F.col("re")).alias("cross"),
                 F.sum("rb").alias("btc_ret"), F.sum("re").alias("eth_ret"),
                 *[F.sum(c).alias(c) for c in ("btc_quote", "btc_buy_quote", "btc_trades", "eth_quote",
                                              "eth_buy_quote", "prints", "sum_sq", "sum_signed_sq", "tape_quote")]))


def issue_features(frame):
    # Only 48 blocks per day reach this ordered window after distributed reduction.
    seq = Window.orderBy("block")
    day = seq.rangeBetween(-47, 0)
    history = (frame.withColumn("issue_s", (F.col("block") + 1) * 1800)
               .withColumn("complete_history", F.sum((F.col("btc_returns") == 30).cast("int")).over(day))
               .withColumn("rv6h", F.avg("rv").over(seq.rowsBetween(-11, 0)))
               .withColumn("rv24h", F.avg("rv").over(day)))
    h = (history.filter((F.col("complete_history") == 48) & (F.col("eth_returns") == 30)
                        & (F.col("matched_minutes") == 30))
         .filter((F.col("btc_quote") > 0) & (F.col("eth_quote") > 0) & (F.col("sum_sq") > 0)))
    signed = 2 * F.col("btc_buy_quote") / F.col("btc_quote") - 1
    quad = F.col("sum_signed_sq") / F.col("sum_sq")
    angle = F.pmod("issue_s", F.lit(86400)) * (2 * math.pi / 86400)

    def log_rv(col):
        return F.log(F.greatest(F.col(col), F.lit(1e-14)))

    return h.select("issue_s", F.col("rv").alias("past_rv"), "rv24h",
                     log_rv("rv").alias("log_rv30"), log_rv("rv6h").alias("log_rv6h"),
                     log_rv("rv24h").alias("log_rv24h"), F.abs("btc_ret").alias("btc_abs_return"),
                     F.log1p("btc_quote").alias("btc_log_quote"), F.log1p("btc_trades").alias("btc_log_trades"),
                     signed.alias("btc_imbalance"), log_rv("erv").alias("eth_log_rv30"),
                     F.abs("eth_ret").alias("eth_abs_return"),
                     (2 * F.col("eth_buy_quote") / F.col("eth_quote") - 1).alias("eth_imbalance"),
                     F.log1p("eth_quote").alias("eth_log_quote"),
                     F.coalesce(F.col("cross") / F.sqrt(F.col("rv") * F.col("erv")), F.lit(0.)).alias("btc_eth_corr"),
                     F.sin(angle).alias("clock_sin"), F.cos(angle).alias("clock_cos"),
                     F.dayofweek(F.timestamp_seconds("issue_s")).isin(1, 7).cast("double").alias("weekend"),
                     F.log(F.col("prints") * F.col("sum_sq") / F.col("tape_quote") ** 2).alias("log_concentration"),
                     quad.alias("quadratic_imbalance"), F.abs(quad - signed).alias("imbalance_gap"))


def examples(frame):
    targets = frame.filter(F.col("btc_returns") == 30).select(
        (F.col("block") * 1800).alias("issue_s"), F.col("rv").alias("target_rv"))
    return (issue_features(frame).join(targets, "issue_s")
            .withColumn("split", F.when(F.col("issue_s") < APR, "train")
                        .when(F.col("issue_s") < MAY, "validation").otherwise("test")))


def prepare(spark):
    id_stats = id_audit()
    raw = read_tape(spark)
    dispositions = [r.asDict() for r in raw.groupBy("month", "status").count().orderBy("month", "status").collect()]
    manifest = json.loads(Path("sources/manifest.json").read_text())["files"]
    expected = sum(r["rows"] for r in manifest if r["group"] == "tape")
    assert sum(r["count"] for r in dispositions) == sum(r["rows"] for r in id_stats) == expected
    minute_path = str(RUN / "tape_minutes")
    minute_tape(raw).write.mode("overwrite").parquet(minute_path)
    btc, btc_checks = bars(spark, "BTCUSDT")
    eth, eth_checks = bars(spark, "ETHUSDT")
    mins = minute_table(spark, spark.read.parquet(minute_path), btc, eth).cache()
    quality = mins.agg(F.count("*").alias("expected_minutes"),
                       F.sum(F.col("matched").cast("int")).alias("reconciled_btc_minutes"),
                       F.sum("prints").alias("accepted_prints"), F.sum("native_fills").alias("native_fills"),
                       F.sum(F.when(F.col("prints").isNull(), 1).otherwise(0)).alias("no_print_minutes")
                       ).first().asDict()
    assert quality["accepted_prints"] == sum(r["count"] for r in dispositions if r["status"] == "accepted")
    dest = str(RUN / "panel")
    examples(blocks(mins)).write.mode("overwrite").parquet(dest)
    mins.unpersist()
    splits = [r.asDict() for r in spark.read.parquet(dest).groupBy("split").agg(
        F.count("*").alias("windows"), F.min("issue_s").alias("first_utc_s"),
        F.max("issue_s").alias("last_utc_s")).orderBy("first_utc_s").collect()]
    for r in splits:
        for key in ("first_utc_s", "last_utc_s"):
            r[key.removesuffix("_s")] = datetime.fromtimestamp(r.pop(key), timezone.utc).isoformat()
    write_json(RESULTS / "data_quality.json", {"trade_rows": expected, "id_checks": id_stats,
               "dispositions": dispositions, "btc_bars": btc_checks, "eth_bars": eth_checks,
               "reconciliation": quality, "splits": splits})
