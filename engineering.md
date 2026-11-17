# Engineering guide

## Reproduce the study

Run the following commands from the repository root. Use Python 3.11, a Java 17 JDK, and curl. The pinned runtime uses PySpark 3.5.3, NumPy, pandas and PyArrow. The launcher detects Homebrew's Java 17 on Apple Silicon. On other systems set JAVA_HOME to the installed JDK. Allow roughly 15 GB of working disk for source ZIPs, normalized data and Spark temporary files.

```bash
uv venv --python 3.11
uv pip install -r requirements.txt
.venv/bin/python -m risklab.acquire
scripts/run prepare
scripts/run train
SHUFFLE_PARTITIONS=8 scripts/run replay
```

The acquisition command verifies every compressed file against the publisher and [pinned manifest](sources/manifest.json). It reuses verified inputs, stages new downloads under temporary names and atomically renames completed files. A changed publisher digest stops acquisition. The optional manifest-recording flag is for creating a new study snapshot. A source revision calls for a new recorded experiment.

## Stage contracts

| Command | Reads | Produces |
| --- | --- | --- |
| acquire | Eighteen publisher ZIPs and checksum sidecars | Verified ZIPs in data/raw and typed Parquet in data/parquet, grouped by tape, BTCUSDT and ETHUSDT |
| prepare | Normalized Parquet and source manifest | Minute statistics in runs/tape_minutes, forecast panel in runs/panel and results/data_quality.json |
| train | Forecast panel | Fitted Spark PipelineModels in runs/models, validation choices, holdout metrics, cases and compressed per-window forecasts in results |
| replay | January BTC tape | A fresh run directory for the inbox, state checkpoint and Parquet sink, plus results/streaming.json |
| demo | Published model ZIP and case feature vectors | Recomputed MLlib scores for use in the viva |
| benchmark | The first N normalized tape months | One timing record per input-size, local-thread and shuffle combination in results/scaling |

A rerun of a batch stage rebuilds its derived tables. Run dependent stages in order and give each active run its own checkout/output directory. The raw archives and large derived tables stay on local disk. Publishing the small forecast ledger allows independent evaluation without downloading the raw tape.

## Data quality and numerical rules

- Stream the original CSV using the eight aggregate-trade or twelve kline fields in publisher order. The 2024 timestamps use **milliseconds**. Price and amount use double precision for statistical aggregation. SHA-256 protects the original bytes.
- Verify aggregate IDs strictly increase across the six chronological files, report skipped identifiers, and keep an exclusive disposition count for missing fields, wrong month, invalid values and invalid ID ranges.
- Require unique aligned BTC and ETH minute keys, 59,999 ms between kline open and close times, valid prices, nonnegative trade counts and consistent buy/total quote amounts. Validate the actual closing price against the published bar.
- Reconcile sum of underlying fill counts, total quote notional, buy quote notional and the closing trade. Floating comparisons use absolute tolerance 0.00001 plus relative tolerance 0.00000001. A minute with zero published trades and no tape prints is valid. The dense calendar preserves genuinely missing minutes.
- Require 30 reconciled minutes for the trailing tape block, 30 BTC and ETH returns in the current feature block, a complete 48-block BTC return history and 30 BTC returns in the target. A missing minute cannot silently become a zero return. Apply the same eligibility mask to both feature sets.
- Keep label construction separate from the model input allowlist. Clip past variance to a small positive floor only when taking logarithms. Calculate the event cutoff on the original realized-variance scale. [Pipeline code](risklab/panel.py)

## Models and evidence

Spark VectorAssembler builds either the 15 bar features or the same features plus three tape moments. Logistic regression uses MLlib standardization and L2 regularization. Gradient boosting uses 25 shallow trees with fixed seed 417. Both estimators learn from the same ordered period. Four parameter settings across two feature sets create eight fitted pipelines. Validation fixes the model identities and numerical alert thresholds before test scores are evaluated. [Training](risklab/models.py) · [Evaluation](risklab/evaluate.py)

## Timing and streaming

```bash
SPARK_MASTER='local[1]' SHUFFLE_PARTITIONS=24 scripts/run benchmark --months 1
SPARK_MASTER='local[4]' SHUFFLE_PARTITIONS=24 scripts/run benchmark --months 1
SPARK_MASTER='local[4]' SHUFFLE_PARTITIONS=8 scripts/run benchmark --months 1
SPARK_MASTER='local[4]' SHUFFLE_PARTITIONS=24 scripts/run benchmark --months 3
SPARK_MASTER='local[4]' SHUFFLE_PARTITIONS=24 scripts/run benchmark --months 6
```

Run `scripts/benchmarks` to execute these settings plus a one-month same-host Spark cluster with two executor workers. Each process performs one warm-up and three measured projected scans, filters and aggregations. A terminal action consumes all moment columns, including the closing price. Adaptive coalescing is disabled for the timing experiment. Local data may be in the OS file cache. Each result records Spark version, input partitions, shuffle partitions, logical CPUs, physical Parquet bytes and all timings. For a standalone Spark cluster, use its master URL in SPARK_MASTER and mount the project and data at the same absolute paths on driver and workers. Cluster execution also requires a compatible Python runtime on each node.

The replay takes 60 minutes of real January trades plus a three-minute closing buffer. One in eleven IDs gets a synthetic 45-second delivery delay. Five-minute delivery batches arrive as separate Parquet files. Spark groups by original one-minute event windows, applies a two-minute watermark and persists state. The query stops between two file groups and restarts on the same checkpoint, followed by a no-input restart. Integer values and last prices must agree exactly with the batch reference. Relative errors in floating moments must stay below 1e−8. [Systems code](risklab/systems.py)

## Verification

```bash
.venv/bin/python -m unittest discover -s tests -v
```

Set JAVA_HOME first when Java is outside the system search path. The published-ledger test reconstructs the confusion counts and AP for every held-out candidate. Additional tests cover average-precision ties, identical paired predictions, taker direction, equal ordinary bars with different squared moments, future-label isolation, and gaps in a future return window. To regenerate the report figure, install matplotlib 3.9.4 in the same environment and run `.venv/bin/python scripts/plot_results.py`. Cross-checks against the complete published BTC kline archive and the replay supplement these fixtures.
