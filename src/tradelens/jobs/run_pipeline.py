"""Batch entrypoint: the spark-submit job that runs the whole pipeline and exits.

Runs bronze -> silver -> gold, applies DQ gates, and writes Delta tables. This is
the file submitted to EMR Serverless and run locally via `make run-local`.

Reads/writes happen HERE; transformation modules stay pure and I/O-free.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, StringType, StructField, StructType

from tradelens.common.config import load_config
from tradelens.common.logging_utils import get_logger
from tradelens.common.spark_session import get_spark
from tradelens.bronze import ingest_to_bronze as bronze
from tradelens.silver import clean_market_data as s_market
from tradelens.silver import build_order_events as s_orders
from tradelens.silver import securities_master as s_secmaster
from tradelens.gold import market_analytics as g_market
from tradelens.gold import surveillance_alerts as g_surv
from tradelens.quality import data_quality as dq

logger = get_logger(__name__)

SECURITIES_MASTER_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("name", StringType()),
        StructField("sector", StringType()),
        StructField("status", StringType()),
        StructField("valid_from", DateType()),
        StructField("valid_to", DateType()),
        StructField("is_current", BooleanType()),
    ]
)


def _read(spark, fmt: str, path: str) -> DataFrame:
    return spark.read.format(fmt).load(path)


def _write_delta(df: DataFrame, path: str, partition_by: list[str], mode: str = "overwrite") -> None:
    (df.write.format("delta").mode(mode)
       .partitionBy(*partition_by).save(path))


def _read_or_empty_securities_dim(spark, path: str) -> DataFrame:
    """DeltaTable.isDeltaTable works against local paths and s3a:// alike, so
    this is safe to call on both a fresh run and a re-run in either env."""
    if DeltaTable.isDeltaTable(spark, path):
        return spark.read.format("delta").load(path)
    return spark.createDataFrame([], SECURITIES_MASTER_SCHEMA)


def main() -> None:
    cfg = load_config()
    spark = get_spark("tradelens-pipeline")

    raw, bronze_p = cfg.path("raw"), cfg.path("bronze")
    silver_p, gold_p = cfg.path("silver"), cfg.path("gold")

    # --- BRONZE ---
    raw_market = spark.read.parquet(f"{raw}/market")
    raw_orders = spark.read.parquet(f"{raw}/orders")
    b_market = bronze.to_bronze_market(raw_market)
    b_orders = bronze.to_bronze_orders(raw_orders)
    dq.expect_non_empty(b_market, "bronze_market", cfg["quality"]["fail_on_error"])
    dq.expect_non_empty(b_orders, "bronze_orders", cfg["quality"]["fail_on_error"])
    # Bronze is an immutable, append-only landing zone — never overwrite history.
    _write_delta(b_market, f"{bronze_p}/market", ["dt", "symbol"], mode="append")
    _write_delta(b_orders, f"{bronze_p}/orders", ["dt", "symbol"], mode="append")

    # --- SILVER ---
    sm = s_market.clean_market(b_market, cfg["quality"]["outlier_stddev_threshold"])
    so = s_orders.normalize_orders(b_orders)

    # Referential integrity: every non-NEW event must belong to an order that
    # actually had a NEW event (no orphan FILL/CANCEL/MODIFY).
    new_order_ids = so.filter(F.col("event_type") == "NEW").select("order_id").distinct()
    non_new_events = so.filter(F.col("event_type") != "NEW")
    dq.expect_referential_integrity(
        non_new_events, new_order_ids, "order_id", "orders_have_new_parent",
        cfg["quality"]["fail_on_error"],
    )

    _write_delta(sm, f"{silver_p}/market", ["dt", "symbol"])
    _write_delta(so, f"{silver_p}/orders", ["dt", "symbol"])

    securities_master_path = f"{silver_p}/securities_master"
    current_securities_dim = _read_or_empty_securities_dim(spark, securities_master_path)
    incoming_securities = spark.createDataFrame(
        [(sym, attrs["name"], attrs["sector"], attrs["status"])
         for sym, attrs in cfg["securities_reference"].items()],
        ["symbol", "name", "sector", "status"],
    )
    securities_master = s_secmaster.build_scd2(
        current_securities_dim, incoming_securities,
        as_of=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    )
    _write_delta(securities_master, securities_master_path, [])

    # --- GOLD: analytics ---
    securities = securities_master.filter(F.col("is_current")).select("symbol", "name", "sector")
    analytics = g_market.build_market_analytics(
        sm, securities, cfg["analytics"]["vwap_window"], cfg["analytics"]["volatility_window"]
    )
    _write_delta(analytics, f"{gold_p}/market_analytics", ["dt"])

    # --- GOLD: surveillance ---
    spoof = g_surv.detect_spoofing(so, cfg["surveillance"]["spoof_cancel_ms"])
    wash = g_surv.detect_wash_trades(so)
    alerts = spoof.select("account_id", "symbol", "pattern").unionByName(
        wash.select("account_id", "symbol", "pattern")
    )
    alerts = g_surv.compute_risk_score(alerts)
    _write_delta(alerts, f"{gold_p}/surveillance_alerts", ["pattern"])

    logger.info("Pipeline complete. Gold tables written under %s", gold_p)
    spark.stop()


if __name__ == "__main__":
    os.environ.setdefault("TRADELENS_ENV", "local")
    main()
