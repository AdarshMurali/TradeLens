"""Bronze layer: land raw files as immutable Delta tables with ingest metadata.

Technique focus: schema enforcement, partitioning, append-only immutable landing.
Transformations are pure (DataFrame -> DataFrame); I/O lives in run_pipeline.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from tradelens.common.logging_utils import get_logger

logger = get_logger(__name__)


def add_ingest_metadata(df: DataFrame) -> DataFrame:
    """Attach lineage columns. Pure transformation."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


def to_bronze_market(raw_df: DataFrame) -> DataFrame:
    """Enforce the OHLCV schema (see docs/DATA_DICTIONARY.md) before landing."""
    typed = (
        raw_df.withColumn("open", F.col("open").cast("double"))
        .withColumn("high", F.col("high").cast("double"))
        .withColumn("low", F.col("low").cast("double"))
        .withColumn("close", F.col("close").cast("double"))
        .withColumn("volume", F.col("volume").cast("long"))
        .withColumn("symbol", F.col("symbol").cast("string"))
        .withColumn("dt", F.col("dt").cast("date"))
    )
    return add_ingest_metadata(typed)


def to_bronze_orders(raw_df: DataFrame) -> DataFrame:
    """Enforce the order-event schema (see docs/DATA_DICTIONARY.md) before landing."""
    typed = (
        raw_df.withColumn("price", F.col("price").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("long"))
        .withColumn("event_time", F.col("event_time").cast("timestamp"))
        .withColumn("parent_order_id", F.col("parent_order_id").cast("string"))
        .withColumn("symbol", F.col("symbol").cast("string"))
        .withColumn("dt", F.col("dt").cast("date"))
    )
    return add_ingest_metadata(typed)
