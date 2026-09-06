-- Sample analyst queries against the Athena tables registered by
-- create_tables.sql. Ad-hoc, pay-per-scan exploration — the Athena side of
-- the deliberate Athena-vs-Redshift serving trade-off (CLAUDE.md SS1).

-- Alert volume by pattern (spoofing / wash_trade / rapid_ordering).
SELECT pattern, count(*) AS alert_count
FROM tradelens_db.surveillance_alerts
GROUP BY pattern
ORDER BY alert_count DESC;

-- Top flagged accounts, across all patterns.
SELECT account_id, count(*) AS alert_count, array_agg(DISTINCT pattern) AS patterns
FROM tradelens_db.surveillance_alerts
GROUP BY account_id
ORDER BY alert_count DESC
LIMIT 20;

-- Highest-severity alerts (risk_score close to 1.0), most recent first.
SELECT pattern, account_id, symbol, risk_score, latency_ms, order_count
FROM tradelens_db.surveillance_alerts
ORDER BY risk_score DESC
LIMIT 50;

-- Daily VWAP + volatility trend for one symbol.
SELECT dt, close, vwap, volatility, spread_proxy
FROM tradelens_db.market_analytics
WHERE symbol = 'AAPL'
ORDER BY dt;

-- Sector-level average volatility on the latest date loaded.
SELECT sector, avg(volatility) AS avg_volatility, avg(spread_proxy) AS avg_spread
FROM tradelens_db.market_analytics
WHERE dt = (SELECT max(dt) FROM tradelens_db.market_analytics)
GROUP BY sector
ORDER BY avg_volatility DESC;

-- Tableau data source: full alert detail with a date attached. One row per
-- order_id (surveillance_alerts' own grain) — build every panel (by pattern,
-- top accounts, risk-score distribution, daily trend, detail table) off this
-- in Tableau rather than re-querying Athena per chart.
--
-- surveillance_alerts has no timestamp of its own, so this joins back to
-- orders on order_id to recover a date. orders (not fills) is the right join
-- target — it has every event type, so spoofing/rapid_ordering order_ids
-- (cancelled, never filled) resolve here too, not just wash_trade's.
--
-- IMPORTANT: orders is event-grain (one row per NEW/MODIFY/CANCEL/FILL), so
-- a plain `JOIN orders ON order_id` fans out — an order with a NEW + CANCEL
-- event joins to 2 rows, silently doubling every alert. Confirmed by trial:
-- a naive join here doubled every pattern's count exactly (2500 vs the true
-- 1250 for spoofing, etc.). Dedupe orders to one row per order_id FIRST
-- (order_dates below), then join to that — never join alerts to orders
-- directly.
WITH order_dates AS (
    SELECT order_id, MIN(dt) AS dt
    FROM tradelens_db.orders
    GROUP BY order_id
)
SELECT
    a.order_id,
    a.account_id,
    a.symbol,
    a.pattern,
    a.risk_score,
    a.latency_ms,
    a.order_count,
    o.dt
FROM tradelens_db.surveillance_alerts a
JOIN order_dates o ON a.order_id = o.order_id;

-- Busted trades from the MERGE INTO corrections demo (jobs/apply_trade_corrections.py).
SELECT symbol, count(*) AS busted_count, sum(quantity) AS busted_quantity
FROM tradelens_db.fills
WHERE is_busted = true
GROUP BY symbol
ORDER BY busted_count DESC;
