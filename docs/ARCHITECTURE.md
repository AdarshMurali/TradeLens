# TradeLens — Architecture Notes

## Design principles
1. **One processing engine (Spark), many consumers.** Athena and Redshift are two
   serving choices on the *same* gold data — never duplicate the pipeline.
2. **Medallion layering.** bronze (raw, immutable) → silver (clean/conform) →
   gold (business-ready). Each layer is a Delta table in S3.
3. **Pure transformations.** Every function is `DataFrame -> DataFrame` with no
   I/O, so it's unit-testable locally without AWS.
4. **Local-first, cloud-for-demo.** Same code runs on the local filesystem and on
   S3 (path selection via `TRADELENS_ENV`).
5. **Cost scales to zero.** Serverless everything; the only continuous cost is
   Redshift storage, which is torn down between demos.

## Data flow
1. **Ingestion (Python):** yfinance OHLCV + synthetic order events (with injected
   spoofing/wash/layering + ground-truth labels) → raw files partitioned by `dt`.
2. **Bronze (Spark):** schema-enforced, lineage-tagged, immutable Delta.
3. **Silver (Spark):** cleaning, dedup, out-of-order handling, SCD2 securities
   master.
4. **Gold (Spark):**
   - `market_analytics` — VWAP, volatility (window functions), broadcast-join
     enrichment.
   - `surveillance_alerts` — spoofing (self-join), wash trades, rapid ordering
     (salted windowed counts), Pandas-UDF risk score. Delta `MERGE` for trade
     corrections + time travel for audit.
5. **Serving:** Athena (ad-hoc) and Redshift Serverless (BI) over gold.
6. **Viz:** Tableau Desktop → Athena / Redshift / Parquet extract.

## Why these choices (interview talking points)
- **EMR Serverless vs a cluster:** pay per job-second; no idle burn.
- **Athena vs Redshift:** schema-on-read + pay-per-scan flexibility vs structured,
  indexed, faster-for-repeated-BI MPP warehouse. Built both to speak to the trade-off.
- **Delta vs plain Parquet:** ACID upserts (trade corrections are real and common),
  time travel for compliance/audit, schema evolution.
- **Salting:** hot symbols (AAPL/TSLA) skew partitions; salt→aggregate→re-aggregate.

## Streaming (optional stretch)
`jobs/run_streaming.py` — Structured Streaming file-source (or Kinesis free tier),
event-time `F.window()` + watermark for late corrections. Clearly a bonus that
reuses the same schema.
