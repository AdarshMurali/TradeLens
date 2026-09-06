-- Athena external tables over the gold (and one silver) S3 data (schema-on-read).
--
-- Gold is written by the Spark job as DELTA (not plain Parquet) — see
-- jobs/run_pipeline.py's _write_delta(). Athena engine v3 reads a Delta
-- table's transaction log directly via TBLPROPERTIES ('table_type'='DELTA'):
-- no column list to keep in sync by hand, no MSCK REPAIR TABLE (partition
-- info comes from the Delta log too, not a directory listing), and it stays
-- correct even after a MERGE INTO leaves tombstoned files behind (a plain
-- "STORED AS PARQUET" external table would happily read those and double
-- count, since it has no concept of Delta's transaction log). This is the
-- one thing to change if the gold layer ever moves to a non-Delta Parquet
-- write instead.
--
-- ${TRADELENS_CURATED_BUCKET} is substituted by serving/athena_ddl.py from
-- .env before these statements are submitted — this file is a template, not
-- meant to be pasted into the console verbatim (though you can, replacing
-- the placeholder by hand).

CREATE DATABASE IF NOT EXISTS tradelens_db;

CREATE EXTERNAL TABLE IF NOT EXISTS tradelens_db.market_analytics
LOCATION 's3://${TRADELENS_CURATED_BUCKET}/gold/market_analytics/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS tradelens_db.fills
LOCATION 's3://${TRADELENS_CURATED_BUCKET}/gold/fills/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS tradelens_db.surveillance_alerts
LOCATION 's3://${TRADELENS_CURATED_BUCKET}/gold/surveillance_alerts/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- silver, not gold: surveillance_alerts has no timestamp column (it's
-- order_id-grain, pre-filtered to risk_score >= threshold), so this is
-- exposed for Tableau/analysts to join alerts back to event_time by
-- order_id. orders is the right join target for that — NOT fills: fills is
-- FILL events only, and spoofing/rapid_ordering order_ids were cancelled,
-- never filled, so they'd join to nothing there. wash_trade's order_ids
-- *are* FILL events, so either table recovers time for that one pattern,
-- but orders is the only one that works for all three.
CREATE EXTERNAL TABLE IF NOT EXISTS tradelens_db.orders
LOCATION 's3://${TRADELENS_CURATED_BUCKET}/silver/orders/'
TBLPROPERTIES ('table_type' = 'DELTA');
