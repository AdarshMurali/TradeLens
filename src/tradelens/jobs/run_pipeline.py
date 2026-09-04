"""Batch entrypoint: the spark-submit job that runs the whole pipeline and exits.

Runs bronze -> silver -> gold, applies DQ gates, and writes Delta tables. This is
the file submitted to EMR Serverless and run locally via `make run-local`.

Reads/writes happen HERE; transformation modules stay pure and I/O-free.
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame

from tradelens.common.config import load_config
from tradelens.common.logging_utils import get_logger
from tradelens.common.spark_session import get_spark
from tradelens.bronze import ingest_to_bronze as bronze
from tradelens.silver import clean_market_data as s_market
from tradelens.silver import build_order_events as s_orders
from tradelens.gold import market_analytics as g_market
from tradelens.gold import surveillance_alerts as g_surv
from tradelens.quality import data_quality as dq

logger = get_logger(__name__)


def _read(spark, fmt: str, path: str) -> DataFrame:
    return spark.read.format(fmt).load(path)


def _write_delta(df: DataFrame, path: str, partition_by: list[str], mode: str = "overwrite") -> None:
    (df.write.format("delta").mode(mode)
       .partitionBy(*partition_by).save(path))


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
    sm = s_market.clean_market(b_market)
    so = s_orders.normalize_orders(b_orders)
    _write_delta(sm, f"{silver_p}/market", ["dt"])
    _write_delta(so, f"{silver_p}/orders", ["dt"])
    # TODO: build securities_master (SCD2) and write to silver.

    # --- GOLD: analytics ---
    # TODO: build a real securities dimension; placeholder derived from symbols here.
    securities = sm.select("symbol").distinct()
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
