"""Unit tests for gold/market_analytics.py, validated against hand-calc'd
(plain-Python) reference values on tiny samples, per docs/PROJECT_PLAN.md
Phase 4."""
import math

import pytest
from pyspark.sql import functions as F


def test_rolling_vwap_matches_hand_calc(spark):
    from tradelens.gold.market_analytics import rolling_vwap

    # window_rows=2: day1 vwap = 10 (only itself); day2 vwap = weighted avg of both
    df = spark.createDataFrame(
        [("AAPL", "2023-01-01", 10.0, 100), ("AAPL", "2023-01-02", 20.0, 100)],
        ["symbol", "dt", "close", "volume"],
    )
    out = rolling_vwap(df, window_rows=2).orderBy("dt").collect()

    assert out[0]["vwap"] == pytest.approx(10.0)
    assert out[1]["vwap"] == pytest.approx((10.0 * 100 + 20.0 * 100) / (100 + 100))


def test_rolling_volatility_matches_hand_calc(spark):
    from tradelens.gold.market_analytics import rolling_volatility

    rows = [
        ("AAPL", "2023-01-01", 100.0),
        ("AAPL", "2023-01-02", 110.0),
        ("AAPL", "2023-01-03", 99.0),
    ]
    df = spark.createDataFrame(rows, ["symbol", "dt", "close"])
    out = rolling_volatility(df, window_rows=3).orderBy("dt").collect()

    log_returns = [math.log(110.0 / 100.0), math.log(99.0 / 110.0)]
    mean = sum(log_returns) / len(log_returns)
    sample_variance = sum((r - mean) ** 2 for r in log_returns) / (len(log_returns) - 1)
    expected_vol = math.sqrt(sample_variance)

    assert out[0]["log_return"] is None  # no prior close on day 1
    assert out[0]["volatility"] is None  # fewer than 2 non-null returns in window
    assert out[1]["log_return"] == pytest.approx(log_returns[0])
    assert out[2]["log_return"] == pytest.approx(log_returns[1])
    assert out[2]["volatility"] == pytest.approx(expected_vol)


def test_spread_proxy_matches_hand_calc(spark):
    from tradelens.gold.market_analytics import spread_proxy

    df = spark.createDataFrame(
        [("AAPL", "2023-01-01", 105.0, 95.0, 100.0)],
        ["symbol", "dt", "high", "low", "close"],
    )
    out = spread_proxy(df).collect()

    assert out[0]["spread_proxy"] == pytest.approx((105.0 - 95.0) / 100.0)


def test_enrich_with_securities_broadcast_join(spark):
    from tradelens.gold.market_analytics import enrich_with_securities

    market = spark.createDataFrame(
        [("AAPL", "2023-01-01", 100.0), ("ZZZZ", "2023-01-01", 5.0)],
        ["symbol", "dt", "close"],
    )
    securities = spark.createDataFrame(
        [("AAPL", "Apple Inc.", "Technology")], ["symbol", "name", "sector"]
    )
    out = {r["symbol"]: r for r in enrich_with_securities(market, securities).collect()}

    assert out["AAPL"]["name"] == "Apple Inc."
    assert out["AAPL"]["sector"] == "Technology"
    # left join: a symbol missing from the (broadcast) securities dim still
    # comes through, just without enrichment
    assert out["ZZZZ"]["name"] is None


def test_build_market_analytics_produces_all_expected_columns(spark):
    from tradelens.gold.market_analytics import build_market_analytics

    market = spark.createDataFrame(
        [
            ("AAPL", "2023-01-01", 101.0, 99.0, 100.0, 1000),
            ("AAPL", "2023-01-02", 106.0, 100.0, 105.0, 1200),
        ],
        ["symbol", "dt", "high", "low", "close", "volume"],
    )
    securities = spark.createDataFrame(
        [("AAPL", "Apple Inc.", "Technology")], ["symbol", "name", "sector"]
    )
    out = build_market_analytics(market, securities, vwap_window=20, vol_window=20)

    expected_cols = {"vwap", "log_return", "volatility", "spread_proxy", "name", "sector"}
    assert expected_cols <= set(out.columns)
    assert out.count() == 2
