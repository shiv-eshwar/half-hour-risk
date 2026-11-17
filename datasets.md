# Final datasets

The study uses the Binance **spot** market from 2024-01-01 00:00 UTC up to 2024-07-01 00:00 UTC. Six monthly BTC/USDT aggregate-trade files provide the raw large-scale input. Twelve one-minute kline files supply the BTC control and target prices and the contemporaneous ETH/USDT control. Each source is downloadable directly without an account or API key. [Publisher archive](https://data.binance.vision/) · [Archive schema, changes and checksums](https://github.com/binance/binance-public-data)

| Series | Files | Verified records | ZIP bytes | Uncompressed CSV bytes |
| --- | ---: | ---: | ---: | ---: |
| BTCUSDT aggregate trades | 6 | 243,917,258 | 3,265,659,937 | 20,356,816,604 |
| BTCUSDT one-minute bars | 6 | 262,080 | 12,688,337 | 39,325,668 |
| ETHUSDT one-minute bars | 6 | 262,080 | 12,107,834 | 38,608,457 |

The 18 ZIPs expand to **20,434,750,729 bytes** of CSV. Record counts come from the typed conversion of the downloaded, digest-verified files. The raw observation count and the number of actionable forecast times differ by four orders of magnitude. A study of 182 days yields at most 8,736 disjoint half-hour targets before warm-up and quality checks.

## Monthly inventory

| UTC month | Aggregate prints | BTC minutes | ETH minutes | Direct publisher downloads |
| --- | ---: | ---: | ---: | --- |
| 2024-01 | 41,962,464 | 44,640 | 44,640 | [BTC trades](https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01.zip) · [BTC bars](https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip) · [ETH bars](https://data.binance.vision/data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2024-01.zip) |
| 2024-02 | 37,492,760 | 41,760 | 41,760 | [BTC trades](https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-02.zip) · [BTC bars](https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-02.zip) · [ETH bars](https://data.binance.vision/data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2024-02.zip) |
| 2024-03 | 59,681,084 | 44,640 | 44,640 | [BTC trades](https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-03.zip) · [BTC bars](https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-03.zip) · [ETH bars](https://data.binance.vision/data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2024-03.zip) |
| 2024-04 | 44,987,078 | 43,200 | 43,200 | [BTC trades](https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-04.zip) · [BTC bars](https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-04.zip) · [ETH bars](https://data.binance.vision/data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2024-04.zip) |
| 2024-05 | 32,019,547 | 44,640 | 44,640 | [BTC trades](https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-05.zip) · [BTC bars](https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-05.zip) · [ETH bars](https://data.binance.vision/data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2024-05.zip) |
| 2024-06 | 27,774,325 | 43,200 | 43,200 | [BTC trades](https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-06.zip) · [BTC bars](https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-06.zip) · [ETH bars](https://data.binance.vision/data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2024-06.zip) |

Each archive has a sidecar at its download URL with the suffix `.CHECKSUM`. The manifest records every URL, SHA-256, CSV size, ZIP size and row count. The acquisition step checks both the current publisher digest and the pinned one. [Machine-readable manifest](sources/manifest.json) · [Acquisition code](risklab/acquire.py)

## Field contract

| Source fields | Units and use |
| --- | --- |
| Aggregate trade ID, first trade ID, last trade ID | Distinguish aggregate prints from underlying fills. For this archive, sum last − first + 1 and reconcile against the publisher's count each minute. |
| Price, quantity | Price in USDT per BTC and quantity in BTC. Their product is one aggregate print's quote notional. |
| Trade timestamp | UTC Unix milliseconds in the selected 2024 files. The exchange switched spot files to microseconds in 2025. |
| Was the buyer the maker? | True implies an aggressive seller. False implies an aggressive buyer. The best-match flag is read from the archive and has no role in modeling. |
| Kline open and close timestamps, OHLC | One-minute candles. The last trade price is checked against BTC close. Adjacent closes define one-minute log returns. |
| Quote asset volume, number of trades, taker-buy quote volume | Supply control features and an independent aggregation check. The kline trade count counts underlying trades. |

A monthly kline file includes 43,200 rows for a 30-day month. A trade file has one row per exchange aggregate and can grow rapidly in active markets. Public bars and tape share the same exchange, so their agreement checks our processing and the archive's internal consistency. It provides no independent-venue price validation. [Spot API reference](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints)

## Scope and access

The quote currency is USDT. BTC is the sole forecasting target and ETH is a feature source. The spot archive contains executed trades. It has no identities, outstanding order-book depth, cancellations, liquidation flags or real-time delivery receipts. The replay adds its own labeled delivery delay. The repository publishes links, checksums, code and derived evaluation artifacts; the large original inputs are fetched from the publisher. [Public data repository](https://github.com/binance/binance-public-data)

## What passed the full-data checks

All 243,917,258 aggregate-trade records passed the validity rules. Their IDs increased continuously from 2,807,759,891 through 3,051,677,148. Together they represent 311,557,031 underlying fills. Both bar series contained 262,080 distinct valid UTC minutes. Every BTC minute reconciled on the close, fill count and two quote-volume measures.

The forecast panel contains 4,319 training, 1,440 validation and 2,928 test windows. The first 49 candidate issue times are unavailable because the first minute has no preceding close in the selected archive and the model needs a full day of valid past returns. The first scored window begins on 2 January at 00:30 UTC. [Executed quality audit](results/data_quality.json)
