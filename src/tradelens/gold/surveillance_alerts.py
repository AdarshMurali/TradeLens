"""Gold: trade surveillance alerts (spoofing, wash trading, rapid ordering).

Techniques (the showcase): self-join, windowed aggregations, Pandas UDF risk
score, data-skew handling via salting.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def detect_spoofing(events: DataFrame, cancel_ms: int = 500) -> DataFrame:
    """Flag NEW orders cancelled within `cancel_ms`. Pure transformation.

    Technique: SELF-JOIN NEW events to their CANCEL events on order_id, compute
    the placement->cancel latency, flag large fast-cancelled orders.

    TODO:
      - split events into news = event_type=='NEW', cancels = event_type=='CANCEL'
      - join on order_id; latency_ms = cancel_time - new_time
      - flag where latency_ms <= cancel_ms AND quantity is unusually large
    """
    news = events.filter(F.col("event_type") == "NEW").alias("n")
    cancels = events.filter(F.col("event_type") == "CANCEL").alias("c")
    joined = news.join(cancels, F.col("n.order_id") == F.col("c.order_id"), "inner")
    latency = (F.col("c.event_time").cast("double") - F.col("n.event_time").cast("double")) * 1000
    return (
        joined.withColumn("latency_ms", latency)
        .filter(F.col("latency_ms") <= cancel_ms)
        .select(F.col("n.order_id"), F.col("n.account_id"), F.col("n.symbol"),
                F.col("n.quantity"), F.col("latency_ms"))
        .withColumn("pattern", F.lit("spoofing"))
    )


def detect_wash_trades(events: DataFrame) -> DataFrame:
    """Flag same-account BUY & SELL fills at same price/time. Pure transformation.

    TODO:
      - filter FILL events
      - group by (account_id, symbol, price, time-bucket) having both BUY & SELL
    """
    fills = events.filter(F.col("event_type") == "FILL")
    grp = (
        fills.groupBy("account_id", "symbol", "price",
                      F.window("event_time", "10 seconds"))
        .agg(F.collect_set("side").alias("sides"))
        .filter(F.array_contains("sides", "BUY") & F.array_contains("sides", "SELL"))
    )
    return grp.select("account_id", "symbol", "price").withColumn("pattern", F.lit("wash_trade"))


def rapid_order_counts_salted(events: DataFrame, salt_buckets: int = 8) -> DataFrame:
    """Windowed order counts per account/symbol with SALTING to fix skew.

    Technique: high-volume symbols create skewed partitions. Add a salt column,
    aggregate at the salted grain, then re-aggregate to remove the salt.

    TODO:
      - salt = (rand()*salt_buckets) or hash-based bucket
      - stage 1: groupBy(account, symbol, salt, time-window).count()
      - stage 2: groupBy(account, symbol, time-window).sum(count)
      - compare .explain() before vs after salting for the writeup
    """
    salted = events.withColumn("salt", (F.rand() * salt_buckets).cast("int"))
    stage1 = salted.groupBy("account_id", "symbol", "salt",
                            F.window("event_time", "60 seconds")).count()
    stage2 = stage1.groupBy("account_id", "symbol", "window").agg(F.sum("count").alias("order_count"))
    return stage2


def compute_risk_score(alerts: DataFrame) -> DataFrame:
    """Combine signals into a risk_score with a Pandas UDF. Pure transformation.

    TODO: implement a @pandas_udf that weights cancel-latency, order-burst count,
    and pattern type into a 0..1 score; flag >= threshold.
    """
    # Placeholder linear score; replace with a pandas_udf.
    return alerts.withColumn("risk_score", F.lit(0.5))
