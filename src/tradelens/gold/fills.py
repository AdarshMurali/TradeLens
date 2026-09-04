"""Gold: executed trades (FILL events), the natural target for late-arriving
corrections/busts — see jobs/apply_trade_corrections.py for the Delta
MERGE INTO + time-travel demo that upserts into this table.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FILLS_COLUMNS = [
    "event_id", "order_id", "account_id", "symbol", "side",
    "price", "quantity", "event_time", "dt",
]


def build_fills(orders: DataFrame) -> DataFrame:
    """Extract executed trades from the normalized order-event stream.

    Pure transformation. Adds `is_busted` (defaults false) — the field a
    later correction MERGE flips for a cancelled/reversed trade.
    """
    return (
        orders.filter(F.col("event_type") == "FILL")
        .select(*FILLS_COLUMNS)
        .withColumn("is_busted", F.lit(False))
    )
