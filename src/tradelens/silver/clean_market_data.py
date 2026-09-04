"""Silver: clean & conform market data.

Techniques: null handling, type casts, dedup (dropDuplicates on natural key +
latest _ingested_at), outlier flagging.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def clean_market(df: DataFrame) -> DataFrame:
    """Return cleaned, deduplicated market rows. Pure transformation.

    TODO:
      - drop rows with null close/volume
      - dedup: keep latest _ingested_at per (symbol, dt) using a row_number window
      - flag outliers where |return| > N stddev (add boolean column is_outlier)
    """
    w = Window.partitionBy("symbol", "dt").orderBy(F.col("_ingested_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
