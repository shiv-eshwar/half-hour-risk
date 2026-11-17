# Can large aggressive trades warn of the next Bitcoin volatility spike?

## Decision and scope

A market-risk analyst watching BTC/USDT every half-hour has to decide whether the next thirty minutes deserve closer attention. At 14:00 UTC, the system takes the trades and closed bars available through 13:59:59.999 and issues a risk score for 14:00–14:30. The outcome becomes measurable at 14:30. The audience is a risk desk monitoring short-horizon volatility on one spot venue.

The project covers January through June 2024, continuously across weekends and time zones. It uses the entire selected Bitcoin aggregate-trade archive and matching one-minute Bitcoin and Ethereum price/volume bars. USDT is the quote currency. A Binance aggregate trade can combine multiple fills at the same price for one taker order. The analysis uses aggregate trades as its unit of order-flow evidence. [Exchange archive and field definitions](https://github.com/binance/binance-public-data) · [Spot market data API](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints)

## The finance question

Volatility often persists. An ordinary candle already tells an analyst how far prices moved, how much traded, how many underlying trades occurred and how much quote volume came from aggressive buyers. After accounting for these signals and contemporaneous Ethereum conditions, does knowing **how trading value was distributed across individual Bitcoin aggregate trades** improve the next warning?

Consider four aggregate trades, each representing one fill at the same price. In market A the buy-initiated values are 4 and 4, and the sell-initiated values are 4 and 4. In market B the buys are 7 and 1, and the sells are 4 and 4. Both bars have the same OHLC prices, total value, buy value and trade count. The sum of squared trade values increases from 64 to 82. The signed squared sum rises from zero to 18. This is information that can justify scanning the large tape. Whether it improves a forecast is an empirical question.

## Outcome and contribution

For each non-overlapping half-hour, compute the sum of the thirty squared one-minute BTC close-to-close log returns. A high-volatility event exceeds the 90th percentile of this quantity in the training period. The cutoff stays fixed through validation and testing. This measures realized price variation over a fixed horizon. Direction, execution price, fees and trading profit are separate outcomes.

The contribution is a controlled test of the extra data's value. At the same time boundaries, compare the same model with and without three tape-derived features. Hold the ordinary bar trade count, quote flow, volatility history and Ethereum controls in both fits. Measure out-of-time warning quality, the burden of false alerts, uncertainty across days, the cost of the Spark aggregation and whether a streaming replay reproduces it. Short-horizon Bitcoin volatility prediction itself has substantial prior work. [Literature and closest prior art](literature.md)

## Questions to defend

1. Does the best tape-enhanced model improve average precision over a matched bars-only model and simple volatility persistence on later months?
2. At an alert cutoff calibrated on April, how many future stress windows are detected in May and June, and how many quiet windows are flagged per day?
3. Does any improvement hold in both calm and active conditions as measured before the forecast, and how large is the day-to-day uncertainty?
4. How much work does Spark do to turn six unsplittable monthly trade ZIPs into minute statistics? Can an event-time replay with a checkpoint produce those same statistics?

## What the experiment found

Spark processed **243,917,258 aggregate trades** representing 311,557,031 underlying fills. The reconstructed BTC tape agreed with the exchange on all 262,080 minutes. Eight pipelines were fitted on early-2024 data and tuned on April. The held-out months contained 83 stressed half-hours among 2,928 decisions.

The selected logistic model with trade features achieved AP **0.2162**, compared with **0.2076** for the same model using bars and **0.1772** for volatility persistence. At the calibrated cutoffs, the tape model caught 23 events with 62 false warnings. Bars alone caught 24 with 65 false warnings. The lower end of the paired uncertainty interval was essentially zero. This is a modest improvement in ranking with a measurable data cost. [Results and interpretation](report.md)

The [six-page submission report](output/pdf/half-hour-risk.pdf) brings the research, methods, results and execution measurements together.

For the viva, `scripts/run demo` loads the published Spark model and reproduces a correct warning and the largest missed event. A separate checkpointed replay verified event-time aggregations on real trades. The four-thread Spark job aggregated all six months in a median 43.44 seconds after warm-up. A second experiment distributed January tasks across two executor processes on the same machine. [Engineering guide](engineering.md) · [Viva walkthrough](viva.md)

## Fit to the 30-mark assessment

| Component | Marks | Evidence |
| --- | ---: | --- |
| Problem definition and literature | 2 | Risk-desk decision, exact horizon, competing explanations and direct comparison with published Bitcoin volatility research |
| Data collection and preprocessing | 4 | First-party checksums, typed conversion, trade audit, BTC bar reconciliation, gap handling and timestamp joins |
| Apache Spark MLlib implementation | 7 | Spark DataFrames, feature transformations, vector assembly, logistic and boosted-tree pipelines, chronological tuning |
| Model evaluation and comparison | 3 | Persistence, matched ablation, holdout average precision, alert errors and paired uncertainty |
| Scalability and streaming | 4 | Timed scans and shuffles, volume/parallelism comparisons, event-time watermark and checkpoint replay |
| Viva voce | 10 | Work a real alert, explain the label and the squared-size example, trace the shuffle, then defend the limits |

The [dataset card](datasets.md), [architecture](architecture.md), [engineering guide](engineering.md), [experiment protocol](plan.md) and [progress record](progress.md) cover the rest of the study.
