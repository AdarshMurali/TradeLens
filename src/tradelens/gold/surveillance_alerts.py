"""Gold: trade surveillance alerts (spoofing, wash trading, rapid ordering).

Techniques (the showcase): self-join, windowed aggregations, data-skew
handling via salting, Pandas UDF risk score.

Time bucketing here uses plain integer-division on the epoch (unix_timestamp
// window_sec), NOT `pyspark.sql.functions.window` — that's the Structured
Streaming event-time window construct, reserved for the streaming stretch
module per CLAUDE.md SS7. This is a batch pipeline.
"""
from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType


def detect_spoofing(events: DataFrame, cancel_ms: int, min_quantity: int) -> DataFrame:
    """Flag large NEW orders cancelled within `cancel_ms`. Pure transformation.

    Technique: SELF-JOIN NEW events to their CANCEL events on order_id, compute
    the placement->cancel latency, flag large fast-cancelled orders. Latency
    alone isn't enough — plenty of ordinary orders get cancelled quickly too —
    so this also requires the order to be unusually large (`min_quantity`),
    matching the injected spoofing pattern's signature.
    """
    news = events.filter(F.col("event_type") == "NEW").alias("n")
    cancels = events.filter(F.col("event_type") == "CANCEL").alias("c")
    joined = news.join(cancels, F.col("n.order_id") == F.col("c.order_id"), "inner")
    latency = (F.col("c.event_time").cast("double") - F.col("n.event_time").cast("double")) * 1000
    return (
        joined.withColumn("latency_ms", latency)
        .filter((F.col("latency_ms") <= cancel_ms) & (F.col("n.quantity") >= min_quantity))
        .select(
            F.col("n.order_id").alias("order_id"),
            F.col("n.account_id").alias("account_id"),
            F.col("n.symbol").alias("symbol"),
            F.col("latency_ms"),
        )
        .withColumn("pattern", F.lit("spoofing"))
    )


def detect_wash_trades(events: DataFrame, window_sec: int) -> DataFrame:
    """Flag same-account BUY & SELL fills at the same price within a short
    time bucket. Pure transformation.

    Groups FILL events by (account_id, symbol, price, time-bucket); a bucket
    with both a BUY and a SELL side is a wash trade — no real counterparty,
    no economic change. Exploded back to one row per contributing order_id.
    """
    fills = events.filter(F.col("event_type") == "FILL")
    bucketed = fills.withColumn(
        "_time_bucket", (F.unix_timestamp("event_time") / window_sec).cast("long")
    )
    grp = (
        bucketed.groupBy("account_id", "symbol", "price", "_time_bucket")
        .agg(
            F.collect_set("side").alias("sides"),
            F.collect_set("order_id").alias("order_ids"),
        )
        .filter(F.array_contains("sides", "BUY") & F.array_contains("sides", "SELL"))
    )
    return (
        grp.select("account_id", "symbol", F.explode("order_ids").alias("order_id"))
        .withColumn("pattern", F.lit("wash_trade"))
    )


def rapid_order_counts_salted(
    events: DataFrame, salt_buckets: int, window_sec: int, count_threshold: int
) -> DataFrame:
    """Flag accounts/symbols with an abnormal burst of NEW/CANCEL activity
    (layering's signature: several orders placed then pulled in seconds).

    Technique: high-volume symbols dominate a plain groupBy's partitions
    (skew). Salt the key with a random bucket, aggregate at the salted grain
    (stage 1 — spread across partitions), then sum back down to the true
    (account, symbol, time-bucket) grain (stage 2). Exploded back to one row
    per contributing order_id.
    """
    relevant = events.filter(F.col("event_type").isin("NEW", "CANCEL"))
    salted = relevant.withColumn("salt", (F.rand() * salt_buckets).cast("int"))
    bucketed = salted.withColumn(
        "_time_bucket", (F.unix_timestamp("event_time") / window_sec).cast("long")
    )
    stage1 = bucketed.groupBy("account_id", "symbol", "salt", "_time_bucket").agg(
        F.count(F.lit(1)).alias("cnt"),
        F.collect_set("order_id").alias("order_ids"),
    )
    stage2 = (
        stage1.groupBy("account_id", "symbol", "_time_bucket")
        .agg(
            F.sum("cnt").alias("order_count"),
            F.flatten(F.collect_list("order_ids")).alias("order_ids"),
        )
        .filter(F.col("order_count") > count_threshold)
    )
    exploded = stage2.select(
        "account_id", "symbol", "order_count", F.explode("order_ids").alias("order_id")
    )
    # salt is assigned per-row, not per-order, so one order's NEW and CANCEL
    # can land in different salt buckets and both carry its order_id into
    # stage1's collect_set — dedupe the one-row-per-order-per-burst result.
    return (
        exploded.dropDuplicates(["account_id", "symbol", "order_id"])
        .withColumn("pattern", F.lit("rapid_ordering"))
    )


def rapid_order_counts_unsalted(events: DataFrame, window_sec: int, count_threshold: int) -> DataFrame:
    """Same result as rapid_order_counts_salted, WITHOUT salting — a single
    groupBy("account_id", "symbol", "_time_bucket") straight to the answer.

    Exists only for the before/after skew comparison (see
    jobs/run_pipeline.py, which logs .explain() for both): a busy symbol like
    AAPL/TSLA sends the bulk of all NEW/CANCEL events to a small number of
    (account, symbol) group keys, so this single-stage plan concentrates
    those rows onto very few partitions for the shuffle — the skew that
    rapid_order_counts_salted's two-stage salt/re-aggregate avoids. Not used
    in the pipeline's actual output.
    """
    relevant = events.filter(F.col("event_type").isin("NEW", "CANCEL"))
    bucketed = relevant.withColumn(
        "_time_bucket", (F.unix_timestamp("event_time") / window_sec).cast("long")
    )
    return (
        bucketed.groupBy("account_id", "symbol", "_time_bucket")
        .agg(F.count(F.lit(1)).alias("order_count"))
        .filter(F.col("order_count") > count_threshold)
    )


def _make_risk_score_udf(cancel_ms: int, rapid_order_count: int):
    """Build a Pandas UDF closed over the surveillance thresholds.

    Weights per-pattern signals into a 0..1 score, floored at 0.7 (the
    `surveillance.risk_score_alert_threshold` default) for anything that
    already cleared its detector's primary rule-based filter — that filter
    is what decides "is this a candidate at all"; the score's job is to rank
    severity among candidates, not silently re-reject ones already found.
    Scaling up from a lower floor would mean some genuine detections score
    below the alert threshold and vanish from the alerts table for no
    reason a compliance analyst could see:
    - spoofing: faster cancels (relative to `cancel_ms`) score higher
    - wash_trade: fixed high-confidence score (both sides matched by
      construction — there's no natural gradation signal in OHLCV/order data)
    - rapid_ordering: the further order_count exceeds `rapid_order_count`,
      the higher the score
    """

    @pandas_udf(DoubleType())
    def _risk_score(pattern: pd.Series, latency_ms: pd.Series, order_count: pd.Series) -> pd.Series:
        score = pd.Series(0.0, index=pattern.index)

        is_spoof = pattern == "spoofing"
        cancel_speed = (1 - (latency_ms / cancel_ms)).clip(lower=0, upper=1)
        score = score.mask(is_spoof, 0.7 + 0.3 * cancel_speed)

        is_wash = pattern == "wash_trade"
        score = score.mask(is_wash, 0.9)

        is_rapid = pattern == "rapid_ordering"
        burst_excess = ((order_count - rapid_order_count) / rapid_order_count).clip(lower=0, upper=1)
        score = score.mask(is_rapid, 0.7 + 0.3 * burst_excess)

        return score.astype(float)

    return _risk_score


def compute_risk_score(alerts: DataFrame, cancel_ms: int, rapid_order_count: int) -> DataFrame:
    """Combine per-pattern signals into a risk_score via a Pandas UDF.

    Pure transformation. `alerts` must carry `pattern`, `latency_ms`
    (nullable — spoofing only) and `order_count` (nullable — rapid_ordering
    only) columns, as produced by the detect_* / rapid_order_counts_salted
    functions above.
    """
    risk_udf = _make_risk_score_udf(cancel_ms, rapid_order_count)
    return alerts.withColumn(
        "risk_score", risk_udf(F.col("pattern"), F.col("latency_ms"), F.col("order_count"))
    )
