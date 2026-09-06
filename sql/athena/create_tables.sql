-- Athena external tables over the gold S3 data (schema-on-read).
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
