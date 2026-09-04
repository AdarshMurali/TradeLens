"""Gold: market analytics (VWAP, rolling volatility, spread, daily rollups).

Techniques: SQL window functions, broadcast join to securities master.
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

    TODO:
      - log_return = ln(close / lag(close))  (use lag window)
      - volatility = stddev(log_return) over trailing window
    """
    w_lag = Window.partitionBy("symbol").orderBy("dt")
    df = df.withColumn("prev_close", F.lag("close").over(w_lag))
    df = df.withColumn("log_return", F.log(F.col("close") / F.col("prev_close")))
    w_roll = Window.partitionBy("symbol").orderBy("dt").rowsBetween(-(window_rows - 1), 0)
    return df.withColumn("volatility", F.stddev("log_return").over(w_roll))


def enrich_with_securities(df: DataFrame, securities_master: DataFrame) -> DataFrame:
    """Broadcast join to the small securities dimension. Pure transformation."""
    return df.join(F.broadcast(securities_master), on="symbol", how="left")


def build_market_analytics(market: DataFrame, securities: DataFrame,
                           vwap_window: int, vol_window: int) -> DataFrame:
    df = rolling_vwap(market, vwap_window)
    df = rolling_volatility(df, vol_window)
    df = enrich_with_securities(df, securities)
    return df
