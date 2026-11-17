# Project progress

The dated entries refer to 18 November 2026 in the project workspace. The market-data timestamps and experiment boundaries use UTC.

| Milestone | Result | Evidence |
| --- | --- | --- |
| Finance topic and protocol | Fixed the market-risk question, 30-minute target, 90th-percentile event definition, chronological split, model grid and matched ablation before fitting. | [Protocol commit](https://github.com/shiv-eshwar/half-hour-risk/commit/e64c227) |
| Literature and data selection | Verified the closest Bitcoin forecasting studies and selected two public exchange products across BTC and ETH. | [Literature](literature.md) · [Data card](datasets.md) |
| Acquisition | Verified 18 publisher checksums, normalized 243,917,258 aggregate trades and 524,160 minute bars, then repeated acquisition against the pinned snapshot. | [Source manifest](sources/manifest.json) |
| Preprocessing | All 262,080 BTC minutes reconciled. The original and optimized Spark preparation runs produced identical data-quality summaries and split sizes. | [Executed audit](results/data_quality.json) |
| MLlib | Fitted eight Spark pipelines. April selected logistic regularization 0.01 for both feature sets, and depth 2 for both boosted-tree models. | [Validation record](results/validation.json) |
| Evaluation | Scored all 2,928 held-out windows. The tape logistic model reached AP 0.2162. Its matched bars-only fit reached 0.2076. Published the per-window values and an independent metric check. | [Holdout](results/holdout.json) · [Forecast ledger](results/forecasts.csv.gz) · [Metric check](tests/test_published_results.py) |
| Streaming | Processed 314,003 real prints including a closing buffer, verified all 60 target minutes against batch and consumed zero additional input on the final checkpoint restart. | [Query progress](results/streaming.json) |
| Demonstration | Loaded the published MLlib model and regenerated the first held-out alert and the largest missed event from their feature vectors. | [Model](results/selected-model.zip) · [Cases](results/case_studies.json) |
| Scaling | Completed the one-month single-host thread and shuffle comparisons. Larger volume and two-worker measurements are running. | [Timing records](results/scaling) |

The [technical report](report.md) interprets the AP gain, false-alert burden and cases. Each major milestone has a public commit. [Project history](https://github.com/shiv-eshwar/half-hour-risk/commits/main/)
