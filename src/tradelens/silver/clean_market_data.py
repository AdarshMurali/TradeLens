"""Silver: clean & conform market data.

Techniques: null handling, dedup (dropDuplicates-equivalent via row_number
window on the (symbol, dt) natural key, keeping the latest _ingested_at),
and rolling window functions (lag, windowed stddev) to flag return outliers.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def clean_market(df: DataFrame, outlier_stddev_threshold: float) -> DataFrame:
    """Return cleaned, deduplicated market rows with an `is_outlier` flag.

    - drops rows with a null close or volume
    - dedups on the (symbol, dt) natural key, keeping the latest _ingested_at
    - flags a day where |daily return| exceeds `outlier_stddev_threshold`
      symbol-level stddevs of daily return (lag + windowed stddev)
    """
    dedup_window = Window.partitionBy("symbol", "dt").orderBy(F.col("_ingested_at").desc())
    deduped = (
        df.filter(F.col("close").isNotNull() & F.col("volume").isNotNull())
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    symbol_window = Window.partitionBy("symbol").orderBy("dt")
    with_return = deduped.withColumn(
        "daily_return",
        (F.col("close") - F.lag("close").over(symbol_window)) / F.lag("close").over(symbol_window),
    )

    stddev_window = Window.partitionBy("symbol")
    with_stddev = with_return.withColumn(
        "_return_stddev", F.stddev("daily_return").over(stddev_window)
    )
    return (
        with_stddev.withColumn(
            "is_outlier",
            F.when(
                F.col("daily_return").isNotNull() & F.col("_return_stddev").isNotNull(),
                F.abs(F.col("daily_return")) > (F.lit(outlier_stddev_threshold) * F.col("_return_stddev")),
            ).otherwise(F.lit(False)),
        )
        .drop("_return_stddev")
    )
