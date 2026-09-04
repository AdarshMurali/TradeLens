"""Gold: market analytics (VWAP, rolling volatility, spread, daily rollups).

Techniques: SQL window functions, broadcast join to securities master.

A separate daily-rollup aggregation isn't implemented here: this pipeline's
raw ingestion is daily-bar granularity end to end (config.yaml date_range),
so the gold grain (one row per symbol/dt) already *is* the daily rollup —
there's no finer-grained input to aggregate up from. If ingestion is ever
extended to intraday bars (config.yaml documents interval="1m"/"5m"), a
groupBy("symbol", "dt") aggregation (first(open)/max(high)/min(low)/
last(close) ordered by an intraday timestamp, sum(volume)) belongs here.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def rolling_vwap(df: DataFrame, window_rows: int = 20) -> DataFrame:
    """VWAP over a trailing row window per symbol. Pure transformation.

    VWAP = sum(price*volume) / sum(volume) over the window.
    """
    w = Window.partitionBy("symbol").orderBy("dt").rowsBetween(-(window_rows - 1), 0)
    pv = F.col("close") * F.col("volume")
    return df.withColumn("vwap", F.sum(pv).over(w) / F.sum("volume").over(w))


def rolling_volatility(df: DataFrame, window_rows: int = 20) -> DataFrame:
    """Rolling stddev of log returns per symbol. Pure transformation.

    log_return = ln(close / lag(close)); volatility = stddev(log_return)
    over a trailing row window.
    """
    w_lag = Window.partitionBy("symbol").orderBy("dt")
    df = df.withColumn("prev_close", F.lag("close").over(w_lag))
    df = df.withColumn("log_return", F.log(F.col("close") / F.col("prev_close")))
    w_roll = Window.partitionBy("symbol").orderBy("dt").rowsBetween(-(window_rows - 1), 0)
    return df.withColumn("volatility", F.stddev("log_return").over(w_roll))


def spread_proxy(df: DataFrame) -> DataFrame:
    """High-low based bid/ask spread proxy. Pure transformation.

    True bid/ask isn't available from OHLCV bars; (high - low) / close is a
    standard proxy for intraday spread/liquidity when only bars are known.
    """
    return df.withColumn("spread_proxy", (F.col("high") - F.col("low")) / F.col("close"))


def enrich_with_securities(df: DataFrame, securities_master: DataFrame) -> DataFrame:
    """Broadcast join to the small securities dimension. Pure transformation."""
    return df.join(F.broadcast(securities_master), on="symbol", how="left")


def build_market_analytics(market: DataFrame, securities: DataFrame,
                           vwap_window: int, vol_window: int) -> DataFrame:
    df = rolling_vwap(market, vwap_window)
    df = rolling_volatility(df, vol_window)
    df = spread_proxy(df)
    df = enrich_with_securities(df, securities)
    return df
