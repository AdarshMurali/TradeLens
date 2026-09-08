"""One-off maintenance job: VACUUM every curated Delta table.

mode="overwrite" writes (silver/gold) leave their previous version's files
physically in S3 even after being logically superseded -- Delta keeps them
for time travel until VACUUM runs, up to the default 7-day retention. This
job reclaims that storage immediately.

Retention is forced to 0 hours (via the retention-check override) because
this is a dev/demo dataset and these specific old versions carry no time-
travel value (they predate the small-file fix in _write_delta). Do not
reuse a 0-hour retention against a table where time travel actually matters
(e.g. right after apply_trade_corrections.py's MERGE INTO demo, if the
point is to show VERSION AS OF against the pre-correction state).
"""
from __future__ import annotations

from delta.tables import DeltaTable

from tradelens.common.config import load_config
from tradelens.common.logging_utils import get_logger
from tradelens.common.spark_session import get_spark

logger = get_logger(__name__)

TABLES = [
    "bronze/market", "bronze/orders",
    "silver/market", "silver/orders", "silver/securities_master",
    "gold/market_analytics", "gold/fills", "gold/surveillance_alerts",
]


def main() -> None:
    cfg = load_config()
    spark = get_spark("tradelens-vacuum")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    roots = {"bronze": cfg.path("bronze"), "silver": cfg.path("silver"), "gold": cfg.path("gold")}

    for table in TABLES:
        layer, name = table.split("/", 1)
        path = f"{roots[layer]}/{name}"
        if not DeltaTable.isDeltaTable(spark, path):
            logger.info("Skipping %s (not written yet)", path)
            continue
        logger.info("VACUUM %s (retain 0 hours)", path)
        DeltaTable.forPath(spark, path).vacuum(0)

    logger.info("VACUUM complete for %d tables", len(TABLES))


if __name__ == "__main__":
    main()
