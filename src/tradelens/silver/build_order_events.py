"""Silver: normalize the order-event stream and reconstruct order lifecycles.

Techniques: dedup, out-of-order handling (ordering by business event_time
rather than arrival order — batch equivalent of watermark-style logic),
lifecycle assembly (NEW -> [MODIFY]* -> CANCEL|FILL) per order_id.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def normalize_orders(df: DataFrame) -> DataFrame:
    """Dedup order events and derive per-order lifecycle state.

    - dropDuplicates on event_id (idempotent re-ingestion / retry-safe)
    - sequence_no: per-order event ordering by event_time, not arrival order,
      so a late-arriving event still lands in its correct lifecycle position
    - final_status: FILL/CANCEL/OPEN — the terminal state of each order,
      derived via a last() window over the full order partition
    """
    deduped = df.dropDuplicates(["event_id"])

    order_window = Window.partitionBy("order_id").orderBy("event_time")
    with_sequence = deduped.withColumn("sequence_no", F.row_number().over(order_window))

    full_order_window = order_window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    last_event_type = F.last("event_type").over(full_order_window)
    return with_sequence.withColumn(
        "final_status",
        F.when(last_event_type == "FILL", F.lit("FILL"))
        .when(last_event_type == "CANCEL", F.lit("CANCEL"))
        .otherwise(F.lit("OPEN")),
    )
