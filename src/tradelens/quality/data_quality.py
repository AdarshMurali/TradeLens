"""Lightweight data-quality gates. Fail the job on violations when configured.

Technique focus: row-count checks, null checks, referential integrity between
orders and fills. Swap in Great Expectations later if desired.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from tradelens.common.logging_utils import get_logger

logger = get_logger(__name__)


class DataQualityError(Exception):
    pass


def expect_non_empty(df: DataFrame, name: str, fail: bool = True) -> None:
    n = df.count()
    logger.info("DQ[%s] row_count=%s", name, n)
    if n == 0 and fail:
        raise DataQualityError(f"{name} is empty")


def expect_no_nulls(df: DataFrame, cols: list[str], name: str, fail: bool = True) -> None:
    counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).collect()[0]
    bad = {c: counts[c] for c in cols if counts[c] and counts[c] > 0}
    if bad:
        logger.warning("DQ[%s] null violations: %s", name, bad)
        if fail:
            raise DataQualityError(f"{name} null check failed: {bad}")


def expect_referential_integrity(child: DataFrame, parent: DataFrame,
                                 key: str, name: str, fail: bool = True) -> None:
    """Every child.key must exist in parent.key (e.g. every FILL has a NEW order)."""
    orphans = child.join(parent, on=key, how="left_anti").count()
    logger.info("DQ[%s] orphans=%s", name, orphans)
    if orphans > 0 and fail:
        raise DataQualityError(f"{name} referential integrity failed: {orphans} orphans")


def precision_recall(
    alerts: DataFrame, labels: DataFrame, alert_pattern: str, truth_pattern: str | None = None
) -> dict:
    """Precision/recall of `alerts` for one abuse pattern, against the
    Phase-1 synthetic ground-truth label file. Pure transformation.

    `alerts` must carry `order_id` and `pattern` columns (post risk-score
    filtering), matched on `alert_pattern`. `labels` is the ground-truth file
    (order_id, label 'normal'/'abuse', pattern), matched on `truth_pattern`
    (defaults to `alert_pattern` — pass it explicitly when the alert's
    pattern name differs from the ground truth's, e.g. "rapid_ordering"
    alerts are TradeLens's detector for the "layering" ground-truth pattern).
    Reporting-only — not a pass/fail DQ gate, since detection precision/
    recall is inherently tunable, not a hard integrity constraint.
    """
    truth_pattern = truth_pattern or alert_pattern
    flagged = alerts.filter(F.col("pattern") == alert_pattern).select("order_id").distinct()
    truth = labels.filter(F.col("pattern") == truth_pattern).select("order_id").distinct()

    tp = flagged.join(truth, "order_id", "inner").count()
    fp = flagged.join(truth, "order_id", "left_anti").count()
    fn = truth.join(flagged, "order_id", "left_anti").count()

    precision = tp / (tp + fp) if (tp + fp) else None
    recall = tp / (tp + fn) if (tp + fn) else None
    result = {
        "alert_pattern": alert_pattern, "truth_pattern": truth_pattern,
        "tp": tp, "fp": fp, "fn": fn, "precision": precision, "recall": recall,
    }
    logger.info("Precision/recall[%s] %s", alert_pattern, result)
    return result
