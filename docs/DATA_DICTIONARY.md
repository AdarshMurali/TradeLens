# TradeLens — Data Dictionary

Fill/adjust as schemas firm up. Keep this in sync with the code.

## raw / bronze: market (OHLCV)
| column | type | notes |
|---|---|---|
| symbol | string | ticker |
| dt | string(date) | partition key |
| open/high/low/close | double | prices |
| volume | long | shares |
| _ingested_at | timestamp | lineage (bronze) |
| _source_file | string | lineage (bronze) |

## raw / bronze: orders (order events)
| column | type | notes |
|---|---|---|
| event_id | string | unique event |
| order_id | string | order lifecycle key |
| account_id | string | trading account |
| symbol | string | ticker |
| event_type | string | NEW/MODIFY/CANCEL/FILL |
| side | string | BUY/SELL |
| price | double | |
| quantity | long | |
| event_time | timestamp | |
| dt | string(date) | partition key |
| parent_order_id | string | for layering chains (nullable) |

## raw: orders_labels (ground truth for detection scoring)
| column | type | notes |
|---|---|---|
| order_id | string | |
| label | string | normal / abuse |
| pattern | string | spoofing / wash_trade / layering / null |

## silver: market_data
| column | type | notes |
|---|---|---|
| symbol, dt | | keys; deduped, latest `_ingested_at` wins |
| open/high/low/close | double | |
| volume | long | |
| daily_return | double | (close - prev_close) / prev_close, per symbol |
| is_outlier | boolean | \|daily_return\| beyond `quality.outlier_stddev_threshold` symbol stddevs |

## silver: orders
| column | type | notes |
|---|---|---|
| (all bronze order-event columns) | | deduped on `event_id` |
| sequence_no | int | per-order_id ordering by `event_time` |
| final_status | string | FILL / CANCEL / OPEN — terminal state of the order lifecycle |

## silver: securities_master (SCD Type 2)
| column | type | notes |
|---|---|---|
| symbol | string | business key |
| name / sector / status | string | tracked attributes |
| valid_from / valid_to | date | version validity |
| is_current | boolean | latest version flag |

## gold: market_analytics
| column | type | notes |
|---|---|---|
| symbol, dt | | keys |
| close, volume | | |
| vwap | double | rolling window |
| volatility | double | rolling stddev of log returns |

## gold: surveillance_alerts
| column | type | notes |
|---|---|---|
| account_id, symbol | | |
| pattern | string | partition key |
| risk_score | double | 0..1 (Pandas UDF) |
