"""Unit tests for gold/surveillance_alerts.py and gold/fills.py."""
from pyspark.sql import functions as F

EVENT_COLS = ["event_id", "order_id", "account_id", "symbol", "event_type", "side",
              "price", "quantity", "event_time"]


def _events(spark, rows):
    return spark.createDataFrame(rows, EVENT_COLS).withColumn(
        "event_time", F.col("event_time").cast("timestamp")
    )


def test_detect_spoofing_requires_both_fast_cancel_and_large_quantity(spark):
    from tradelens.gold.surveillance_alerts import detect_spoofing

    rows = [
        # O1: large + fast cancel -> spoofing
        ("E1", "O1", "ACC1", "AAPL", "NEW", "SELL", 101.0, 10000, "2023-01-01T10:00:00"),
        ("E2", "O1", "ACC1", "AAPL", "CANCEL", "SELL", 101.0, 10000, "2023-01-01T10:00:00.2"),
        # O2: fast cancel but ordinary size -> NOT spoofing
        ("E3", "O2", "ACC1", "AAPL", "NEW", "BUY", 100.0, 500, "2023-01-01T10:00:01"),
        ("E4", "O2", "ACC1", "AAPL", "CANCEL", "BUY", 100.0, 500, "2023-01-01T10:00:01.1"),
        # O3: large but slow cancel -> NOT spoofing
        ("E5", "O3", "ACC1", "AAPL", "NEW", "SELL", 102.0, 9000, "2023-01-01T10:00:02"),
        ("E6", "O3", "ACC1", "AAPL", "CANCEL", "SELL", 102.0, 9000, "2023-01-01T10:00:10"),
    ]
    df = _events(spark, rows)
    flagged = {r["order_id"] for r in detect_spoofing(df, cancel_ms=500, min_quantity=2000).collect()}

    assert flagged == {"O1"}


def test_detect_wash_trades_same_account_both_sides(spark):
    from tradelens.gold.surveillance_alerts import detect_wash_trades

    rows = [
        # wash trade: ACC1 buys and sells AAPL at the same price, 1s apart
        ("E1", "O1", "ACC1", "AAPL", "FILL", "BUY", 100.0, 1000, "2023-01-01T11:00:00"),
        ("E2", "O2", "ACC1", "AAPL", "FILL", "SELL", 100.0, 1000, "2023-01-01T11:00:01"),
        # not a wash trade: different accounts
        ("E3", "O3", "ACC2", "AAPL", "FILL", "BUY", 105.0, 500, "2023-01-01T12:00:00"),
        ("E4", "O4", "ACC3", "AAPL", "FILL", "SELL", 105.0, 500, "2023-01-01T12:00:01"),
    ]
    df = _events(spark, rows)
    flagged = {r["order_id"] for r in detect_wash_trades(df, window_sec=10).collect()}

    assert flagged == {"O1", "O2"}


def test_rapid_order_counts_salted_flags_burst_above_threshold(spark):
    from tradelens.gold.surveillance_alerts import rapid_order_counts_salted

    # 4 orders, NEW+CANCEL each, all within a couple seconds -> 8 events
    rows = []
    for i in range(4):
        oid = f"O{i}"
        rows.append((f"E{i}N", oid, "ACC1", "AAPL", "NEW", "SELL", 100.0, 1000,
                     f"2023-01-01T13:00:0{i}"))
        rows.append((f"E{i}C", oid, "ACC1", "AAPL", "CANCEL", "SELL", 100.0, 1000,
                     f"2023-01-01T13:00:0{i}"))
    # unrelated quiet account: should never be flagged
    rows.append(("E9N", "O9", "ACC2", "AAPL", "NEW", "BUY", 100.0, 100, "2023-01-01T14:00:00"))

    df = _events(spark, rows)
    out = rapid_order_counts_salted(df, salt_buckets=4, window_sec=10, count_threshold=6).collect()

    flagged_orders = {r["order_id"] for r in out}
    assert flagged_orders == {"O0", "O1", "O2", "O3"}
    assert all(r["order_count"] == 8 for r in out)
    assert all(r["pattern"] == "rapid_ordering" for r in out)
    # no duplicate rows despite per-row salt possibly splitting an order's
    # NEW/CANCEL across different salt buckets
    assert len(out) == len(flagged_orders)


def test_rapid_order_counts_unsalted_matches_salted_totals(spark):
    from tradelens.gold.surveillance_alerts import (
        rapid_order_counts_salted,
        rapid_order_counts_unsalted,
    )

    rows = []
    for i in range(4):
        oid = f"O{i}"
        rows.append((f"E{i}N", oid, "ACC1", "AAPL", "NEW", "SELL", 100.0, 1000,
                     f"2023-01-01T13:00:0{i}"))
        rows.append((f"E{i}C", oid, "ACC1", "AAPL", "CANCEL", "SELL", 100.0, 1000,
                     f"2023-01-01T13:00:0{i}"))
    df = _events(spark, rows)

    salted_count = {r["order_id"] for r in rapid_order_counts_salted(
        df, salt_buckets=4, window_sec=10, count_threshold=6).collect()}
    unsalted = rapid_order_counts_unsalted(df, window_sec=10, count_threshold=6).collect()

    # salting is purely a shuffle/partitioning strategy — the salted variant's
    # flagged order_ids and the unsalted variant's raw order_count must agree
    assert len(salted_count) == 4
    assert unsalted[0]["order_count"] == 8


def test_compute_risk_score_weights_by_pattern(spark):
    from tradelens.gold.surveillance_alerts import compute_risk_score

    rows = [
        ("spoofing", 0.0, None),      # instant cancel -> max spoof score
        ("spoofing", 500.0, None),    # at the cancel_ms threshold -> floor spoof score
        ("wash_trade", None, None),
        ("rapid_ordering", None, 6),   # right at the threshold -> floor rapid score
        ("rapid_ordering", None, 12),  # 2x threshold -> higher rapid score
    ]
    df = spark.createDataFrame(rows, ["pattern", "latency_ms", "order_count"])
    out = {
        (r["pattern"], r["latency_ms"], r["order_count"]): r["risk_score"]
        for r in compute_risk_score(df, cancel_ms=500, rapid_order_count=6).collect()
    }

    # the floor matches surveillance.risk_score_alert_threshold (0.7) so a
    # detection that only just cleared its primary filter is never silently
    # dropped by the downstream alert-threshold filter
    assert out[("spoofing", 0.0, None)] == 1.0
    assert out[("spoofing", 500.0, None)] == 0.7
    assert out[("wash_trade", None, None)] == 0.9
    assert out[("rapid_ordering", None, 6)] == 0.7
    assert out[("rapid_ordering", None, 12)] > out[("rapid_ordering", None, 6)]


def test_build_fills_filters_to_fill_events_and_defaults_not_busted(spark):
    from tradelens.gold.fills import build_fills

    rows = [
        ("E1", "O1", "ACC1", "AAPL", "NEW", "BUY", 100.0, 500, "2023-01-01T10:00:00", "2023-01-01"),
        ("E2", "O1", "ACC1", "AAPL", "FILL", "BUY", 100.0, 500, "2023-01-01T10:00:01", "2023-01-01"),
    ]
    df = spark.createDataFrame(rows, EVENT_COLS + ["dt"])
    out = build_fills(df).collect()

    assert len(out) == 1
    assert out[0]["event_id"] == "E2"
    assert out[0]["is_busted"] is False
