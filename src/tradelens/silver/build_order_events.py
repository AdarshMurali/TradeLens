"""Silver: normalize the order-event stream and reconstruct order lifecycles.

Techniques: out-of-order handling (watermark-style), dedup, lifecycle assembly
(NEW -> [MODIFY]* -> CANCEL|FILL) per order_id.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def normalize_orders(df: DataFrame) -> DataFrame:
    """Dedup and order events per order_id by event_time. Pure transformation.

    TODO:
      - cast event_time to timestamp
      - dropDuplicates on event_id
      - add sequence_no = row_number over (order_id order by event_time)
      - derive final_status per order (FILL/CANCEL/OPEN) via last() window
    """
    w = Window.partitionBy("order_id").orderBy("event_time")
    return df.withColumn("sequence_no", F.row_number().over(w))
