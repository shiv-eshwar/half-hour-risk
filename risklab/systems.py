"""Measure the distributed aggregation and compare streaming and batch event windows."""

import math
import os
import platform
import shutil
import statistics
import time
from pathlib import Path
from tempfile import mkdtemp

from pyspark.sql import functions as F

from .core import PARQUET, RESULTS, RUN, epoch, minute_tape, moments, read_tape, trade_values, write_json

SUMS = ["tape_quote", "tape_buy_quote", "sum_sq", "sum_signed_sq"]


def benchmark(spark, months):
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    raw = read_tape(spark, months)
    input_partitions = raw.rdd.getNumPartitions()

    def execute():
        minute = minute_tape(raw)
        return minute.agg(F.count("*").alias("minutes"), *[F.sum(c).alias(c) for c in
                          ["prints", "native_fills", "last_price", *SUMS]]).first().asDict()

    check = execute()
    times = []
    for _ in range(3):
        start = time.perf_counter()
        again = execute()
        times.append(time.perf_counter() - start)
        assert all(math.isclose(check[k], again[k], rel_tol=1e-9, abs_tol=1e-5) for k in check)
    master = spark.sparkContext.master
    parts = int(spark.conf.get("spark.sql.shuffle.partitions"))
    tag = "".join(c if c.isalnum() else "-" for c in master).strip("-")
    physical_bytes = sum((PARQUET / "tape" / f"2024-{m:02}.parquet").stat().st_size
                         for m in range(1, months + 1))
    write_json(RESULTS / "scaling" / f"{months}m-{tag}-p{parts}.json",
               {"months": months, "master": master, "input_partitions": input_partitions,
                "shuffle_partitions": parts, "adaptive_execution": False, "spark_version": spark.version,
                "architecture": platform.machine(), "cpu": platform.processor(), "os": platform.system(),
                "host_logical_cpus": os.cpu_count(), "parquet_bytes": physical_bytes,
                "warmups": 1, "seconds": times, "median_seconds": statistics.median(times), **check})


def replay(spark):
    RUN.mkdir(exist_ok=True)
    work = Path(mkdtemp(prefix="replay-", dir=RUN)).resolve()
    stage, inbox, sink = (work / n for n in ("stage", "inbox", "sink"))
    inbox.mkdir()
    start = epoch("2024-01-11T14:00")
    start_ms = start * 1000
    ms = F.col("ms")
    lag = F.when(F.pmod(F.col("agg_id"), F.lit(11)) == 0, 45000).otherwise(0)
    raw = read_tape(spark, 1).filter("status='accepted'").filter(
        (ms >= start_ms) & (ms < start_ms + 63 * 60000))
    events = (raw.select("agg_id", "ms", "price", "qty", "first_id", "last_id", "buyer_maker")
              .withColumn("delay_ms", lag)
              .withColumn("arrival_batch", F.floor((ms + F.col("delay_ms") - start_ms) / 300000)))
    events.repartition("arrival_batch").write.partitionBy("arrival_batch").parquet(str(stage))
    files = sorted(stage.glob("arrival_batch=*/part-*.parquet"),
                   key=lambda p: int(p.parent.name.split("=")[1]))
    assert len(files) >= 12
    schema = events.drop("arrival_batch").schema
    runs = []

    def publish(batch):
        for source in batch:
            i = int(source.parent.name.split("=")[1])
            path = inbox / f"{i:02}-{source.name}"
            shutil.copyfile(source, path)
            os.utime(path, (start + i, start + i))

    def drain():
        stream = spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).parquet(str(inbox))
        values = trade_values(stream).withColumn("event", F.timestamp_millis("ms"))
        counts = values.withWatermark("event", "2 minutes").groupBy(F.window("event", "1 minute")).agg(*moments())
        query = (counts.writeStream.format("parquet").outputMode("append")
                 .option("checkpointLocation", str(work / "checkpoint")).trigger(availableNow=True).start(str(sink)))
        query.awaitTermination()
        runs.append([{"batch_id": p["batchId"], "input_rows": p["numInputRows"],
                      "watermark": p.get("eventTime", {}).get("watermark"),
                      "dropped_by_watermark": sum(s.get("numRowsDroppedByWatermark", 0)
                                                  for s in p.get("stateOperators", []))}
                     for p in query.recentProgress])

    publish(files[:6])
    drain()
    publish(files[6:])
    drain()
    drain()
    assert sum(p["input_rows"] for p in runs[-1]) == 0
    observed = (spark.read.parquet(str(sink))
                .withColumn("minute", F.floor(F.col("window.start").cast("long") / 60).cast("long"))
                .filter(F.col("minute") < start // 60 + 60).drop("window"))
    reference = minute_tape(raw.filter(ms < start_ms + 3600000))
    want = {r.minute: r.asDict() for r in reference.collect()}
    got = observed.collect()
    assert {r.minute for r in got} == set(want) and len(got) == len(want)
    max_scaled_error = 0
    for r in got:
        target = want[r.minute]
        for key in ("prints", "native_fills", "last_price"):
            assert r[key] == target[key]
        for key in SUMS:
            error = abs(r[key] - target[key]) / max(1.0, abs(target[key]))
            max_scaled_error = max(error, max_scaled_error)
            assert error < 1e-8
    inputs = sum(p["input_rows"] for run in runs for p in run)
    assert inputs == events.count()
    dropped = sum(p["dropped_by_watermark"] for run in runs for p in run)
    assert dropped == 0
    injection = events.agg(F.sum((F.col("delay_ms") > 0).cast("int")).alias("delayed"),
                           F.sum((F.col("arrival_batch") > F.floor((ms - start_ms) / 300000))
                                 .cast("int")).alias("cross_batch")).first()
    assert injection.cross_batch > 0
    write_json(RESULTS / "streaming.json",
               {"start_utc": "2024-01-11T14:00:00Z", "evaluated_minutes": len(got),
                "buffer_minutes": 3, "watermark_minutes": 2, "events_including_buffer": inputs,
                "evaluated_prints": sum(r.prints for r in got), "delivery_delay_seconds": 45,
                "delayed_delivery_events": injection.delayed, "delivered_in_later_batch": injection.cross_batch,
                "different_integer_windows": 0, "max_scaled_moment_error": max_scaled_error,
                "dropped_by_watermark": dropped, "query_progress_by_start": runs})
