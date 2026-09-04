"""SparkSession factory with Delta Lake configured for both local and AWS runs.

Local: writes Delta tables to the local filesystem.
AWS (EMR Serverless): S3 access + Glue catalog are provided by the runtime; the
same Delta configs apply. Keep this the single place a SparkSession is created.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from tradelens.common.logging_utils import get_logger

logger = get_logger(__name__)


def get_spark(app_name: str = "tradelens") -> SparkSession:
    env = os.getenv("TRADELENS_ENV", "local")
    logger.info("Creating SparkSession (env=%s, app=%s)", env, app_name)

    builder = (
        SparkSession.builder.appName(app_name)
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Sensible production defaults
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    if env == "local":
        # configure_spark_with_delta_pip auto-adds the delta jars locally.
        try:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
        except Exception as exc:  # pragma: no cover
            logger.warning("Delta pip config unavailable (%s); ensure jars present.", exc)
        builder = builder.master(os.getenv("SPARK_MASTER", "local[*]"))
        # local[*] runs the whole pipeline (driver + all "executors") in one
        # JVM, on whatever this laptop actually has (commonly 4-8 cores,
        # 8-16GB RAM) — not a cluster. The base shuffle.partitions=64 above
        # is sized for a real cluster; locally it means far more concurrent
        # tasks (and, for the Pandas UDF stage, Python worker subprocesses)
        # than there are cores, which starves them of memory and times out
        # ("TimeoutError: timed out" reading from the Python worker) under
        # this pipeline's self-joins/salted aggregations over ~5M order
        # events. Cut both down for local; AWS/EMR Serverless sizes its own
        # driver/parallelism via the sparkSubmit job config, not these.
        builder = (
            builder.config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
