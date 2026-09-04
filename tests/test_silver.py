"""Unit tests for the silver layer: cleaning, order-lifecycle normalization,
and SCD2 securities master."""
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    StringType,
    StructField,
    StructType,
)

SECURITIES_SCHEMA = StructType(
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


def test_clean_market_dedups_drops_nulls_and_flags_outlier(spark):
    from tradelens.silver.clean_market_data import clean_market

    rows = [
        # AAPL: normal small daily moves, then one big outlier jump on day 5
        ("AAPL", "2023-01-01", 100.0, 1000, "2023-01-02T00:00:00"),
        ("AAPL", "2023-01-02", 101.0, 1000, "2023-01-03T00:00:00"),
        ("AAPL", "2023-01-03", 100.5, 1000, "2023-01-04T00:00:00"),
        ("AAPL", "2023-01-04", 101.5, 1000, "2023-01-05T00:00:00"),
        ("AAPL", "2023-01-05", 300.0, 1000, "2023-01-06T00:00:00"),  # huge jump
        # duplicate landing for 2023-01-01 with an earlier _ingested_at -> dropped
        ("AAPL", "2023-01-01", 999.0, 1, "2023-01-01T00:00:00"),
        # null close -> dropped
        ("AAPL", "2023-01-06", None, 1000, "2023-01-07T00:00:00"),
    ]
    df = (
        spark.createDataFrame(rows, ["symbol", "dt", "close", "volume", "_ingested_at"])
        .withColumn("_ingested_at", F.col("_ingested_at").cast("timestamp"))
    )
    out = clean_market(df, outlier_stddev_threshold=2.0)

    # null-close row dropped, duplicate resolved to the later _ingested_at
    assert out.count() == 5
    kept = {r["dt"]: r["close"] for r in out.collect()}
    assert kept["2023-01-01"] == 100.0

    outliers = {r["dt"] for r in out.filter(F.col("is_outlier")).collect()}
    assert outliers == {"2023-01-05"}


def test_normalize_orders_dedups_and_derives_final_status(spark):
    from tradelens.silver.build_order_events import normalize_orders

    rows = [
        # order O1: NEW then FILL
        ("E1", "O1", "2023-01-01T10:00:00", "NEW"),
        ("E1", "O1", "2023-01-01T10:00:00", "NEW"),  # exact duplicate event
        ("E2", "O1", "2023-01-01T10:00:01", "FILL"),
        # order O2: NEW then CANCEL
        ("E3", "O2", "2023-01-01T10:00:00", "NEW"),
        ("E4", "O2", "2023-01-01T10:00:02", "CANCEL"),
    ]
    df = (
        spark.createDataFrame(rows, ["event_id", "order_id", "event_time", "event_type"])
        .withColumn("event_time", F.col("event_time").cast("timestamp"))
    )
    out = normalize_orders(df)

    # the exact-duplicate event_id was deduped away
    assert out.count() == 4

    o1 = out.filter(F.col("order_id") == "O1").orderBy("sequence_no").collect()
    assert [r["event_type"] for r in o1] == ["NEW", "FILL"]
    assert [r["sequence_no"] for r in o1] == [1, 2]
    assert all(r["final_status"] == "FILL" for r in o1)

    o2 = out.filter(F.col("order_id") == "O2").collect()
    assert all(r["final_status"] == "CANCEL" for r in o2)


def test_build_scd2_new_symbol_opens_first_version(spark):
    from tradelens.silver.securities_master import build_scd2

    empty_dim = spark.createDataFrame([], SECURITIES_SCHEMA)
    incoming = spark.createDataFrame(
        [("AAPL", "Apple Inc.", "Technology", "active")],
        ["symbol", "name", "sector", "status"],
    )
    out = build_scd2(empty_dim, incoming, as_of="2023-01-01").collect()

    assert len(out) == 1
    row = out[0]
    assert row["is_current"] is True
    assert str(row["valid_from"]) == "2023-01-01"
    assert row["valid_to"] is None


def test_build_scd2_unchanged_symbol_carries_forward(spark):
    from tradelens.silver.securities_master import build_scd2

    current_dim = spark.createDataFrame(
        [("AAPL", "Apple Inc.", "Technology", "active", date(2023, 1, 1), None, True)],
        SECURITIES_SCHEMA,
    )
    incoming = spark.createDataFrame(
        [("AAPL", "Apple Inc.", "Technology", "active")],
        ["symbol", "name", "sector", "status"],
    )
    out = build_scd2(current_dim, incoming, as_of="2023-06-01").collect()

    # no new version opened; the single existing row is untouched
    assert len(out) == 1
    assert str(out[0]["valid_from"]) == "2023-01-01"
    assert out[0]["is_current"] is True


def test_build_scd2_changed_attribute_closes_old_opens_new(spark):
    from tradelens.silver.securities_master import build_scd2

    current_dim = spark.createDataFrame(
        [("AAPL", "Apple Inc.", "Technology", "active", date(2023, 1, 1), None, True)],
        SECURITIES_SCHEMA,
    )
    incoming = spark.createDataFrame(
        [("AAPL", "Apple Inc.", "Technology", "delisted")],  # status changed
        ["symbol", "name", "sector", "status"],
    )
    out = build_scd2(current_dim, incoming, as_of="2023-06-01").collect()

    assert len(out) == 2
    by_current = {r["is_current"]: r for r in out}
    closed = by_current[False]
    opened = by_current[True]

    assert closed["status"] == "active"
    assert str(closed["valid_from"]) == "2023-01-01"
    assert str(closed["valid_to"]) == "2023-06-01"

    assert opened["status"] == "delisted"
    assert str(opened["valid_from"]) == "2023-06-01"
    assert opened["valid_to"] is None
