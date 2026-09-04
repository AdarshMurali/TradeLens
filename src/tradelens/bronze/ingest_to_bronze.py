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
    # TODO: cast columns to a strict schema (open/high/low/close: double, volume: long).
    return add_ingest_metadata(raw_df)


def to_bronze_orders(raw_df: DataFrame) -> DataFrame:
    # TODO: cast price->double, quantity->long, event_time->timestamp.
    return add_ingest_metadata(raw_df)
