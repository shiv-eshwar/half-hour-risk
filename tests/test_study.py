import math
import unittest

from pyspark.sql import functions as F

from risklab.core import START, moments, spark_session, trade_values
from risklab.evaluate import alert_cutoff, average_precision, paired_days
from risklab.panel import BARS, TAPE, examples, issue_features
from risklab.predict import issue_time
from argparse import ArgumentTypeError


class EvaluationTest(unittest.TestCase):
    def test_equal_scores_share_one_precision_threshold(self):
        self.assertEqual(average_precision([0, 1, 1, 0], [4, 4, 4, 4]), 0.5)
        self.assertAlmostEqual(average_precision([1, 0, 1], [2, 1, 1]), (1 + 2 / 3) / 2)
        self.assertIsNone(average_precision([0, 0], [1, 2]))
        self.assertEqual(alert_cutoff([0, 0, 0, 1]), 1)

    def test_only_post_selection_half_hours_in_the_archive_can_be_issued(self):
        self.assertEqual(issue_time("2024-07-01T00:00Z"), issue_time("2024-06-30T20:00-04:00"))
        for invalid in ("2024-04-30T23:30Z", "2024-07-01T00:30Z", "2024-06-11T10:01Z", "2024-06-11T10:00"):
            with self.subTest(invalid=invalid), self.assertRaises(ArgumentTypeError):
                issue_time(invalid)

    def test_pairing_identical_models(self):
        y = [0, 1, 1, 0]
        interval = paired_days(y, y, y, [0, 0, 1, 1], n=20)
        self.assertEqual(interval["percentile_95"], [0, 0])


class TimeAndTapeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session("regression-tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_same_bars_different_tape(self):
        events = [(group, i, 10, float(v), 1.0, i, i, i > 1)
                  for group, values in (("A", [4, 4, 4, 4]), ("B", [7, 1, 4, 4]))
                  for i, v in enumerate(values)]
        # Price stays at 1. Quantity gives each event's quote value.
        raw = self.spark.createDataFrame(events, ["group", "agg_id", "ms", "qty", "price",
                                                  "first_id", "last_id", "buyer_maker"])
        a, b = trade_values(raw).groupBy("group").agg(*moments()).orderBy("group").collect()
        for key in ("last_price", "native_fills", "tape_quote", "tape_buy_quote"):
            self.assertEqual(a[key], b[key])
        self.assertEqual([a.sum_sq, b.sum_sq, a.sum_signed_sq, b.sum_signed_sq], [64, 82, 0, 18])

    def test_future_shock_changes_label_only_and_missing_target_is_dropped(self):
        first = START // 1800
        blocks = self.spark.range(first, first + 52).withColumnRenamed("id", "block")
        values = {"btc_returns": 30, "eth_returns": 30, "matched_minutes": 30, "rv": 0.001,
                  "erv": 0.002, "cross": 0.0002, "btc_ret": -0.002, "eth_ret": 0.003,
                  "btc_quote": 16., "eth_quote": 20., "btc_buy_quote": 8., "eth_buy_quote": 11.,
                  "btc_trades": 4, "prints": 4, "sum_sq": 82., "sum_signed_sq": 18., "tape_quote": 16.}
        blocks = blocks.select("*", *[F.lit(v).alias(k) for k, v in values.items()])
        shock = first + 49
        issue = shock * 1800
        normal = examples(blocks).filter(F.col("issue_s") == issue).first()
        available = issue_features(blocks.filter(F.col("block") < shock)).filter(F.col("issue_s") == issue).first()
        self.assertNotIn("target_rv", available.asDict())
        self.assertEqual([normal[k] for k in BARS + TAPE], [available[k] for k in BARS + TAPE])
        changed = blocks.withColumn("rv", F.when(F.col("block") == shock, 0.5).otherwise(F.col("rv")))
        observation = examples(changed).filter(F.col("issue_s") == issue).first()
        self.assertEqual(observation.target_rv, 0.5)
        self.assertEqual([normal[k] for k in BARS + TAPE], [observation[k] for k in BARS + TAPE])
        self.assertAlmostEqual(observation.log_concentration, math.log(4 * 82 / 256))
        gap = blocks.withColumn("btc_returns", F.when(F.col("block") == shock, 29).otherwise(30))
        self.assertEqual(examples(gap).filter(F.col("issue_s") == issue).count(), 0)


if __name__ == "__main__":
    unittest.main()
