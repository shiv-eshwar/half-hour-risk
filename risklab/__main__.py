import argparse
import json

from .core import spark_session
from .predict import issue_time


def main():
    args = argparse.ArgumentParser(description="Half-Hour Risk experiment")
    args.add_argument("stage", choices=["prepare", "train", "replay", "benchmark", "demo", "predict"])
    args.add_argument("--months", type=int, choices=range(1, 7), default=1)
    args.add_argument("--at", type=issue_time, help="UTC half-hour issue time for predict")
    opts = args.parse_args()
    if (opts.stage == "predict") != (opts.at is not None):
        args.error("Use --at with the predict stage.")
    spark = spark_session(opts.stage)
    try:
        if opts.stage == "prepare":
            from .panel import prepare
            prepare(spark)
        elif opts.stage == "train":
            from .models import train
            train(spark)
        elif opts.stage == "predict":
            from .predict import predict
            print(json.dumps(predict(spark, opts.at), indent=2))
        elif opts.stage == "demo":
            from .demo import demo
            demo(spark)
        elif opts.stage == "replay":
            from .systems import replay
            replay(spark)
        else:
            from .systems import benchmark
            benchmark(spark, opts.months)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
