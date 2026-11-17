# Viva and demonstration

## Five-minute demonstration

1. **Pose the decision.** At 14:00 UTC a risk desk needs a warning about the next 30 minutes. Two markets can show identical ordinary bars while the distribution of value across trades differs. Work the 4+4 versus 7+1 example from the [project brief](whatthis.md).
2. **Establish the scale.** Open the 18-file source manifest. Identify a single archive, its publisher digest, number of prints and corresponding BTC and ETH bars. Then show how many raw prints survive the validity audit and how many minutes reconcile. [Dataset card](datasets.md)
3. **Trace the clock.** Show the 13:59 close feeding the last historical block and anchoring the first target return. The target completes at 14:30. Show the test that perturbs the future and leaves features identical. [Architecture](architecture.md) · [Tests](tests/test_study.py)
4. **Load the model.** Run `scripts/run demo` from a fresh checkout after installing the core requirements. It loads the published MLlib pipeline, scores the saved feature vectors and prints the warning decision and eventual outcome. The examples are selected after evaluation to illustrate a warning and, when available, the largest missed event.
5. **Issue a forecast from market records.** With the source archives prepared, run `scripts/run predict --at 2024-07-01T00:00Z`. The selected model must give a score using June data alone. Explain why no realized outcome appears in the output.
6. **Defend the result.** Use the held-out precision–recall figure, the matched feature ablation and its uncertainty interval. Show how the raw workload scales and how many late deliveries the checkpoint replay handled. [Technical report](report.md)

## Likely examiner questions

**Why do you need hundreds of millions of records to make fewer than nine thousand decisions?** Each half-hour is one decision. Its trade-size moments require the distribution of value across all constituent prints. If that distribution adds little predictive value, the result tells the desk when the cheaper ordinary bars suffice.

**What does an aggressive buy mean here?** The buyer took resting sell-side liquidity. Binance marks the buyer as maker on the opposite case, an aggressive sale. Squared-size imbalance emphasizes prints with large quote value. The records identify no trader or parent trading strategy.

**How could a good backtest accidentally see the future?** Joining the 14:00–14:29 candle features to a forecast issued at 14:00 hands the model its own target period. Estimating the stress cutoff on six months also communicates future market conditions. The feature allowlist, chronological splits, verified block boundaries and training-only percentiles address both errors.

**Why include two baselines?** Last-period variance tests volatility persistence at almost zero engineering cost. The bars-only model tests whether ordinary price, activity, trade count, signed flow and ETH co-movement already contain the signal. Matching its model family and hyperparameters to the tape model isolates the contribution of the added feature group.

**Why average precision?** A high score should move stressed windows toward the top of a scarce-attention queue. AP is the average of precisions at increases in recall, grouping tied scores. A constant ranking's AP equals event prevalence. The alert cutoff then translates a ranking into false alarms per day and detection rate.

**How did you use Spark?** The driver constructs the job and coordinates execution. Spark executors scan Parquet partitions, compute local moment sums, shuffle by minute and merge. The half-hour reduction makes chronological windows small. MLlib then runs parameterized estimator pipelines on the training frame.

**What does the streaming checkpoint protect?** It retains source offsets, state and sink progress across query restarts. Window assignment uses exchange timestamps. A watermark bounds state retention and finalization. After restarting on the same checkpoint, the query consumes zero new rows and the sink retains one copy of each closed minute.

**Can this system provide a warning at exactly :00 in production?** The retrospective experiment assumes prompt availability of trades and finalized minute bars. The replay validates aggregation with a two-minute watermark. Production would require a measured latency budget, provisional or delayed scoring policy, and monitoring against late corrections.

**What could change the conclusion?** A different venue, year, sampling frequency, aggregation rule or volatility regime can change the incremental value of large prints. Quote currency, market structure and single-venue coverage matter. The model's target is volatility, and no trading or profit simulation was performed.
