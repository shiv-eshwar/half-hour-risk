# Project progress

**Complete and publicly reproducible.** The study, model comparison, execution measurements, report and viva demo were completed on 18 November 2026 in the project workspace. The market-data timestamps and experiment boundaries use UTC.

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
| Scaling | Completed six timing configurations. Four local threads aggregated all six months in a median 43.44 seconds. Two executor JVMs on the same host completed 70 and 63 tasks in the January run. | [Timing records](results/scaling) |
| Fresh-clone verification | Created a new environment from the public repository, installed the pinned core requirements, reproduced both demonstration scores without raw market data and passed all five tests. | [Demo code](risklab/demo.py) · [Test suite](tests) |

The [technical report](report.md) interprets the AP gain, false-alert burden and cases. Each major milestone has a public commit. [Project history](https://github.com/shiv-eshwar/half-hour-risk/commits/main/)


## End-to-end rebuild

In a separate checkout, acquisition rechecked the 18 cached source ZIPs against the exchange and pinned SHA-256 values, then regenerated the typed Parquet files. Spark rebuilt the feature panel, refitted all eight candidates, repeated the replay and all six benchmarks, and regenerated the figure and model. Every test label and alert decision agreed across all 2,928 forecast times. The largest absolute score difference was 4.45 × 10⁻¹⁵ or less. The source audit, validation selection, paired bootstrap and regime comparisons passed the cross-run checks. All six regression tests passed. [Machine-readable comparison](results/reproduction.json)

The raw-data forecast command reproduced the first held-out decision, the first alert and the largest miss. It also scored 2024-07-01 00:00 UTC at 0.002686 versus the 0.301167 alert cutoff using observations through June. Its target interval is outside the study archive. [Boundary forecast](results/archive-boundary-forecast.json)

The six-page submission report was typeset and visually checked with all tables and the precision–recall figure. [Submission report](output/pdf/half-hour-risk.pdf)

Team 7’s [presentation](output/presentation/half-hour-risk.pptx) follows the course sample and includes the trade-size example, forecast clock, held-out model comparison, two market cases, measured scaling and streaming replay.
