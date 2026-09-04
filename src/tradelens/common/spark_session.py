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

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
