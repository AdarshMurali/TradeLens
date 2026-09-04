"""Silver: securities master dimension with SCD Type 2.

Technique focus: Slowly Changing Dimension Type 2 — track history of symbol
attributes (name, sector, status) with valid_from / valid_to / is_current.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TRACKED_ATTRS = ["name", "sector", "status"]


def build_scd2(current_dim: DataFrame, incoming: DataFrame, as_of: str) -> DataFrame:
    """Merge an incoming attribute snapshot into an SCD2 dimension. Pure transformation.

    `current_dim` is the existing dimension (symbol, name, sector, status,
    valid_from, valid_to, is_current) — pass an empty DataFrame with that
    schema to bootstrap. `incoming` is the latest known attributes per symbol
    (symbol, name, sector, status). `as_of` is the effective date (string,
    "YYYY-MM-DD") for any new/closed versions.

    - unchanged symbols: the current version carries forward untouched
    - changed symbols: close the current version (valid_to=as_of,
      is_current=false) and open a new one (valid_from=as_of, valid_to=null,
      is_current=true)
    - new symbols: open a first version
    """
    current_active = current_dim.filter(F.col("is_current"))
    current_history = current_dim.filter(~F.col("is_current"))

    def _with_hash(source: DataFrame) -> DataFrame:
        return source.withColumn("_attr_hash", F.sha2(F.concat_ws("||", *TRACKED_ATTRS), 256))

    incoming_h = _with_hash(incoming).select("symbol", *TRACKED_ATTRS, "_attr_hash")
    active_h = _with_hash(current_active).select("symbol", "_attr_hash")

    joined = incoming_h.alias("inc").join(active_h.alias("cur"), on="symbol", how="left")

    changed_or_new_symbols = joined.filter(
        F.col("cur._attr_hash").isNull() | (F.col("cur._attr_hash") != F.col("inc._attr_hash"))
    ).select("inc.symbol", *[f"inc.{c}" for c in TRACKED_ATTRS])

    unchanged_symbols = joined.filter(
        F.col("cur._attr_hash").isNotNull() & (F.col("cur._attr_hash") == F.col("inc._attr_hash"))
    ).select(F.col("inc.symbol").alias("symbol"))

    new_versions = changed_or_new_symbols.select(
        "symbol",
        *TRACKED_ATTRS,
        F.lit(as_of).cast("date").alias("valid_from"),
        F.lit(None).cast("date").alias("valid_to"),
        F.lit(True).alias("is_current"),
    )

    closed_versions = (
        current_active.join(changed_or_new_symbols.select("symbol"), on="symbol", how="inner")
        .withColumn("valid_to", F.lit(as_of).cast("date"))
        .withColumn("is_current", F.lit(False))
    )

    unchanged_active = current_active.join(unchanged_symbols, on="symbol", how="inner")

    return current_history.unionByName(closed_versions).unionByName(unchanged_active).unionByName(new_versions)
