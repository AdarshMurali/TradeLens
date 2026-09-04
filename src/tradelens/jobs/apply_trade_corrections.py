"""Demo job: Delta Lake MERGE INTO for late-arriving trade corrections/busts,
plus a time-travel (VERSION AS OF) read for the audit/compliance narrative.

Run after run_pipeline.py has produced gold/fills at least once:
    PYTHONPATH=src python -m tradelens.jobs.apply_trade_corrections

This is a separate, on-demand entrypoint (not part of every batch run) since
corrections are an occasional operation on already-landed trades, not
something to replay on every pipeline recompute. Synthesizes a tiny
correction batch against real fills already in the table — a real system
would receive these from a compliance/ops feed.

Reads/writes happen HERE; gold/fills.py stays a pure transformation.
"""
from __future__ import annotations

import os

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from tradelens.common.config import load_config
from tradelens.common.logging_utils import get_logger
from tradelens.common.spark_session import get_spark

logger = get_logger(__name__)


def main() -> None:
    cfg = load_config()
    spark = get_spark("tradelens-trade-corrections")
    fills_path = f"{cfg.path('gold')}/fills"

    if not DeltaTable.isDeltaTable(spark, fills_path):
        logger.error("No fills table at %s — run the batch pipeline first.", fills_path)
        spark.stop()
        return

    pre_merge_version = DeltaTable.forPath(spark, fills_path).history(1).collect()[0]["version"]
    logger.info("gold/fills is at Delta version %s before corrections", pre_merge_version)

    # A real feed would supply these; synthesize against a few real fills so
    # the merge has something to match.
    sample = (
        spark.read.format("delta").load(fills_path)
        .orderBy("event_time")
        .limit(3)
        .select("event_id", "quantity")
        .collect()
    )
    if not sample:
        logger.warning("gold/fills is empty — nothing to correct.")
        spark.stop()
        return

    corrections_rows = []
    for i, row in enumerate(sample):
        if i == 0:
            # a bust: the trade is voided
            corrections_rows.append((row["event_id"], row["quantity"], True))
        else:
            # a quantity amendment
            corrections_rows.append((row["event_id"], max(row["quantity"] - 50, 0), False))
    corrections = spark.createDataFrame(corrections_rows, ["event_id", "quantity", "is_busted"])

    target = DeltaTable.forPath(spark, fills_path)
    (
        target.alias("t")
        .merge(corrections.alias("c"), "t.event_id = c.event_id")
        .whenMatchedUpdate(set={"quantity": "c.quantity", "is_busted": "c.is_busted"})
        .execute()
    )

    post_merge_version = DeltaTable.forPath(spark, fills_path).history(1).collect()[0]["version"]
    logger.info("gold/fills is at Delta version %s after corrections", post_merge_version)

    corrected_ids = [r["event_id"] for r in sample]
    before = (
        spark.read.format("delta").option("versionAsOf", pre_merge_version).load(fills_path)
        .filter(F.col("event_id").isin(corrected_ids))
        .select("event_id", "quantity", "is_busted")
    )
    after = (
        spark.read.format("delta").load(fills_path)
        .filter(F.col("event_id").isin(corrected_ids))
        .select("event_id", "quantity", "is_busted")
    )
    logger.info("Audit trail — before correction (version %s):", pre_merge_version)
    before.show(truncate=False)
    logger.info("Audit trail — after correction (current version %s):", post_merge_version)
    after.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    os.environ.setdefault("TRADELENS_ENV", "local")
    main()
