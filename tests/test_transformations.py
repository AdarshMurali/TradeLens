"""Unit tests for pure transformations — no AWS, no I/O.

These are examples to build on. Each core transformation should get a test that
proves the Spark technique behaves correctly on a tiny hand-built DataFrame.
"""
from pyspark.sql import functions as F


def test_rolling_vwap_basic(spark):
    from tradelens.gold.market_analytics import rolling_vwap

    df = spark.createDataFrame(
        [("AAPL", "2023-01-01", 10.0, 100),
         ("AAPL", "2023-01-02", 20.0, 100)],
        ["symbol", "dt", "close", "volume"],
    )
    out = rolling_vwap(df, window_rows=2).orderBy("dt").collect()
    # day1 vwap = 10; day2 vwap = (10*100 + 20*100)/200 = 15
    assert abs(out[0]["vwap"] - 10.0) < 1e-6
    assert abs(out[1]["vwap"] - 15.0) < 1e-6


def test_detect_spoofing_flags_fast_cancel(spark):
    from tradelens.gold.surveillance_alerts import detect_spoofing

    rows = [
        ("E1", "O1", "ACC1", "AAPL", "NEW", "SELL", 101.0, 10000, "2023-01-01T10:00:00"),
        ("E2", "O1", "ACC1", "AAPL", "CANCEL", "SELL", 101.0, 10000, "2023-01-01T10:00:00.2"),
        ("E3", "O2", "ACC1", "AAPL", "NEW", "BUY", 100.0, 500, "2023-01-01T10:00:01"),
    ]
    cols = ["event_id", "order_id", "account_id", "symbol", "event_type", "side",
            "price", "quantity", "event_time"]
    df = spark.createDataFrame(rows, cols).withColumn(
        "event_time", F.col("event_time").cast("timestamp"))
    flagged = detect_spoofing(df, cancel_ms=500).collect()
    assert any(r["order_id"] == "O1" for r in flagged)


# TODO: add tests for dedup, SCD2 open/close, wash-trade detection, salting parity.
