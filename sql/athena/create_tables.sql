-- Athena external tables over the gold S3 data (schema-on-read).
-- Replace <curated-bucket> with your bucket. Run in the Athena console or via boto3.
-- These read the Delta/Parquet gold output written by the Spark job.

CREATE DATABASE IF NOT EXISTS tradelens_db;

-- If gold is written as plain Parquet, a standard external table works directly.
-- For Delta, prefer a Glue crawler or use Athena's Delta support / a manifest.

CREATE EXTERNAL TABLE IF NOT EXISTS tradelens_db.surveillance_alerts (
    account_id   string,
    symbol       string,
    risk_score   double
)
PARTITIONED BY (pattern string)
STORED AS PARQUET
LOCATION 's3://<curated-bucket>/gold/surveillance_alerts/';

MSCK REPAIR TABLE tradelens_db.surveillance_alerts;

CREATE EXTERNAL TABLE IF NOT EXISTS tradelens_db.market_analytics (
    symbol      string,
    close       double,
    volume      bigint,
    vwap        double,
    volatility  double
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://<curated-bucket>/gold/market_analytics/';

MSCK REPAIR TABLE tradelens_db.market_analytics;

-- Example analyst queries:
-- SELECT pattern, count(*) FROM tradelens_db.surveillance_alerts GROUP BY pattern;
-- SELECT account_id, count(*) c FROM tradelens_db.surveillance_alerts
--   GROUP BY account_id ORDER BY c DESC LIMIT 20;
