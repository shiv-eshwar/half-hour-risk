import argparse

from .core import spark_session


def main():
    args = argparse.ArgumentParser(description="Half-Hour Risk experiment")
    args.add_argument("stage", choices=["prepare", "train", "replay", "benchmark"])
    args.add_argument("--months", type=int, choices=range(1, 7), default=1)
    opts = args.parse_args()
    spark = spark_session(opts.stage)
    try:
        if opts.stage == "prepare":
            from .panel import prepare
            prepare(spark)
        elif opts.stage == "train":
            from .models import train
            train(spark)
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
