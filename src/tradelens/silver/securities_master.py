"""Silver: securities master dimension with SCD Type 2.

Technique focus: Slowly Changing Dimension Type 2 — track history of symbol
attributes (name, sector, status) with valid_from / valid_to / is_current.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_scd2(current_dim: DataFrame, incoming: DataFrame) -> DataFrame:
    """Merge incoming attribute changes into an SCD2 dimension. Pure transformation.

    TODO (classic SCD2 logic):
      - detect changed rows by comparing hash of tracked attributes
      - close old versions: set valid_to = change_date, is_current = false
      - insert new versions: valid_from = change_date, valid_to = null, is_current = true
      - carry unchanged rows forward untouched
    Consider implementing the physical upsert with Delta MERGE in serving/, but keep
    the record-shaping logic here and unit-test it.
    """
    return incoming.withColumn("is_current", F.lit(True))
