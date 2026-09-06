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

-- Busted trades from the MERGE INTO corrections demo (jobs/apply_trade_corrections.py).
SELECT symbol, count(*) AS busted_count, sum(quantity) AS busted_quantity
FROM tradelens_db.fills
WHERE is_busted = true
GROUP BY symbol
ORDER BY busted_count DESC;
