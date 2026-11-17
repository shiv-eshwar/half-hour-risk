# Experiment protocol

## Time, labels and decision

Use UTC throughout. Score one BTC/USDT half-hour at each :00 and :30. Input evidence ends strictly before the issue time. The first target return runs from the last one-minute close before issue to the close of the first target minute. The final return ends with the last close inside the target block. Use disjoint target blocks. Require complete time coverage for the features and the outcome.

| Split | UTC issue dates in 2024 | Purpose |
| --- | --- | --- |
| Train | January 1 through March 31, after warm-up | Learn the stress cutoff, fit transformations and models, determine the historical regime boundary |
| Validate | April 1 through April 30 | Select model settings and calibrate the alert cutoff |
| Test | May 1 through June 30 | Evaluate frozen models, paired differences and operational warning behavior |

A label must finish by the end of its assigned split. A forecast at the next split boundary can use the preceding split's completed history. The stress cutoff is the training 90th percentile of future realized variance, with strict exceedance. The regime slice uses the training median of trailing 24-hour variance. No future volatility enters that slice.

## Comparisons fixed before training

1. **Persistence.** Rank windows by realized BTC variance over the immediately preceding 30 minutes.
2. **Bars only.** Use BTC variance over several lookbacks, price change, quote volume, trade count, signed taker volume, Ethereum controls and UTC calendar features.
3. **Bars plus tape.** Add size concentration, squared-size signed imbalance and its absolute deviation from ordinary signed volume imbalance.
4. **Two model families.** Fit Spark MLlib logistic regression with L2 regularization 0.01 or 0.1, and gradient-boosted classifiers with depth 2 or 4. Use 25 boosting rounds, step size 0.1 and seed 417. Tune both feature sets. Select the tape model by validation average precision, breaking exact ties by model name. Compare it to its bars-only twin at the same hyperparameters and to the independently tuned bars-only winner.
5. **Alert cutoff.** Choose a separate score cutoff per candidate from April. Use the higher empirical 90th percentile and a strict greater-than rule. Tied scores can yield fewer validation alerts. Apply that numeric cutoff unchanged in May and June. The realized alert rate can move with the market.

## Reporting rules

- Primary measure is exact, non-interpolated average precision (AP). Group equal scores at one threshold. Report holdout prevalence as the constant-score reference.
- Report alert rate, precision, recall, confusion counts and false alerts per calendar day. Brier score evaluates probabilistic classifiers. For persistence use its uncalibrated variance score for ranking and alerts.
- Resample whole paired UTC test days with replacement 2,000 times for the AP difference between the selected tape model and its matched bars-only twin. Use the same sampled days for each, preserve within-day observations, and report the percentile interval. Serial dependence across days and a single venue limit generalization.
- Report AP and event counts separately above and below the historical regime boundary. Show one named held-out forecast with its inputs, risk score, cutoff and realized outcome.
- Benchmark the same trade aggregation after warm-up across input sizes, worker thread counts and shuffle partition settings. Record actual medians and full samples. Replay real events through Structured Streaming and reconcile closed minute windows to batch counts and signed moments.

## Delivery gates

Acquire and checksum the complete source inventory → reconcile the tape with provider bars → build and test the as-of table → freeze validation choices → evaluate May–June → benchmark and verify replay → publish the technical report and viva walkthrough. A negative incremental result answers the research question and will be reported as observed.
