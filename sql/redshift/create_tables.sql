-- Redshift Serverless: curated warehouse table for the analytics dashboard.
-- Demonstrates deliberate distribution + sort key choices (MPP knowledge).

CREATE TABLE IF NOT EXISTS market_analytics (
    dt          date        NOT NULL,
    symbol      varchar(16) NOT NULL,
    close       double precision,
    volume      bigint,
    vwap        double precision,
    volatility  double precision
)
DISTKEY(symbol)      -- co-locate rows of the same symbol (joins/aggregations by symbol)
SORTKEY(dt);         -- time-range scans prune efficiently

-- Staging table for MERGE-based incremental loads.
CREATE TABLE IF NOT EXISTS market_analytics_staging (LIKE market_analytics);
