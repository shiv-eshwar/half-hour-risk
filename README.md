# Half-Hour Risk

**Can the concentration of aggressive Bitcoin trades flag an unusually volatile half-hour before it begins?**

A quantitative finance study of BTC/USDT spot trades on Binance from January through June 2024. Apache Spark aggregates the full trade tape. Spark MLlib compares volatility persistence, logistic regression and gradient-boosted trees. The experiment measures whether trade-level size and direction add warning value beyond ordinary minute bars from Bitcoin and Ethereum.

Data and the question are fixed before looking at held-out results. The intended use is market-risk monitoring. [Binance public archive](https://github.com/binance/binance-public-data)
