-- Load gold Parquet from S3 into staging, then MERGE (upsert) into the target.
-- Replace <curated-bucket> and <redshift-copy-role-arn>.

TRUNCATE market_analytics_staging;

COPY market_analytics_staging
FROM 's3://<curated-bucket>/gold/market_analytics/'
IAM_ROLE '<redshift-copy-role-arn>'
FORMAT AS PARQUET;

-- Upsert: update existing (dt,symbol), insert new.
MERGE INTO market_analytics AS t
USING market_analytics_staging AS s
ON t.dt = s.dt AND t.symbol = s.symbol
WHEN MATCHED THEN UPDATE SET
    close = s.close, volume = s.volume, vwap = s.vwap, volatility = s.volatility
WHEN NOT MATCHED THEN INSERT
    (dt, symbol, close, volume, vwap, volatility)
    VALUES (s.dt, s.symbol, s.close, s.volume, s.vwap, s.volatility);

-- Optional: pre-aggregated materialized view for the dashboard.
-- CREATE MATERIALIZED VIEW mv_daily_symbol AS
--   SELECT dt, symbol, avg(vwap) vwap, avg(volatility) vol
--   FROM market_analytics GROUP BY dt, symbol;
