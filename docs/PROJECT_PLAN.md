# TradeLens — Project Plan

Build in this order. Each phase is independently demoable and ends with a commit.
Do **all of Phase 0–5 locally** before touching AWS in Phase 6+.

Legend: `[ ]` todo · `[~]` in progress · `[x]` done

---

## Phase 0 — Project setup
- [x] Create venv, install `requirements.txt` + `requirements-dev.txt` (`make setup`)
- [x] `pip install -e .` (or set `PYTHONPATH=src`) so `import tradelens` works
- [x] Verify local Spark + Delta Lake works: run `common/spark_session.py` smoke test
- [x] Fill in `config/config.yaml` and copy `.env.example` → `.env`
- **Done when:** `make test` runs (even with only placeholder tests) and a local
  SparkSession with Delta support starts.

## Phase 1 — Ingestion (local, free)
- [x] `ingestion/fetch_market_data.py`: pull OHLCV for the configured symbols via
      yfinance, write raw CSV/Parquet to `data/raw/market/dt=YYYY-MM-DD/`
- [x] `ingestion/generate_order_events.py`: synthesize order events
      (placed/modified/cancelled/filled) from the OHLCV, injecting:
      - spoofing: large order placed then cancelled within ms, opposite side
        of a real fill
      - wash trading: same beneficial owner on both sides, no economic change
      - layering: multiple orders stacked then pulled
      Write to `data/raw/orders/dt=.../`
- [x] Make injected-pattern rate/volume configurable; record ground-truth labels
      in a side file for validating detection precision/recall later
- **Done when:** `make ingest` produces raw market + order data locally, and the
  ground-truth label file exists.

## Phase 2 — Bronze
- [x] `bronze/ingest_to_bronze.py`: read raw files, add ingestion metadata
      (`_ingested_at`, `_source_file`), enforce a schema, write Delta partitioned
      by `dt` (and `symbol` where sensible). Immutable append.
- [x] Unit test: schema + partition columns present, row count preserved.
- **Done when:** bronze Delta tables exist for market and orders.

## Phase 3 — Silver (clean / conform / dedup / SCD2)
- [x] `silver/clean_market_data.py`: null handling, type casts, outlier flags,
      dedup (`dropDuplicates` on natural key + latest `_ingested_at`).
- [x] `silver/build_order_events.py`: normalize event stream, order-lifecycle
      reconstruction, out-of-order handling (watermark-style logic).
- [x] `silver/securities_master.py`: **SCD Type 2** dimension (symbol, name,
      sector, valid_from/valid_to, is_current) — demonstrate history tracking.
- [x] Unit tests for dedup, SCD2 open/close-record logic.
- **Done when:** clean, conformed silver Delta tables + SCD2 dimension exist.

## Phase 4 — Gold: market analytics
- [ ] `gold/market_analytics.py`:
      - rolling **VWAP** (window function)
      - rolling **volatility** (stddev of log returns over rolling window)
      - spread proxy, daily OHLCV rollups
      - **broadcast join** to securities master for enrichment
- [ ] Write Delta partitioned by `dt`; unit tests on VWAP/volatility math.
- **Done when:** `market_analytics` gold Delta table validated against hand-calc
      on a tiny sample.

## Phase 5 — Gold: surveillance alerts
- [ ] `gold/surveillance_alerts.py`:
      - **self-join** cancels ↔ originating orders (spoofing)
      - windowed rapid order/cancel counts per account/symbol
      - **wash-trade** detection (same owner both sides)
      - **Pandas UDF** combining signals into a `risk_score`
      - **salting** on hot symbols to fix skew; document before/after with `.explain()`
- [ ] `MERGE INTO` path for trade corrections/busts (upsert) + a time-travel demo
- [ ] Validate detection against the Phase-1 ground-truth labels (precision/recall)
- [ ] Unit tests for spoofing self-join and wash-trade logic.
- **Done when:** `surveillance_alerts` gold table flags the injected patterns.

## Phase 6 — AWS foundation (first AWS spend — be careful)
- [ ] `infra/README.md`: set **AWS Budgets $5 alarm FIRST**
- [ ] Create S3 buckets (`tradelens-raw-*`, `tradelens-curated-*`), Glue DB,
      EMR Serverless application, IAM role (`scripts/setup_aws.sh`)
- [ ] Generate a larger demo-scale dataset locally (more symbols / longer
      range / higher order density than the local-dev config — see below),
      then upload it + code to S3. Not a literal "sample": EMR Serverless
      bills per job-second, not idle time, so a bigger one-off run is cheap.
- [ ] Before generating at demo scale: vectorize `generate_order_events.py`'s
      per-order Python loop (numpy/pandas vectorized ops) — the current
      dataclass-per-order approach is fine at ~5M rows (~2 min) but won't
      scale to tens of millions in reasonable local time.
- **Done when:** buckets/role/app exist and billing alarm is active.

## Phase 7 — Run on EMR Serverless
- [ ] `scripts/submit_emr_serverless.sh`: package `src/`, submit `jobs/run_pipeline.py`
- [ ] Confirm gold Delta tables land in `tradelens-curated-*`
- [ ] Register schemas in Glue (crawler or explicit DDL)
- **Done when:** the same pipeline that ran locally completes on EMR Serverless
      and the app scales back to zero afterward.

## Phase 8 — Serving A: Athena
- [ ] `sql/athena/create_tables.sql`: external tables over the gold S3 data
- [ ] Sample analyst queries (top flagged accounts, alerts over time)
- **Done when:** Athena returns results over gold data.

## Phase 9 — Serving B: Redshift Serverless
- [ ] `sql/redshift/create_tables.sql`: DDL with **dist key + sort key** chosen
      and justified
- [ ] `sql/redshift/copy_and_merge.sql`: `COPY` gold Parquet from S3; `MERGE`
      for incremental loads; optional materialized view for the dashboard query
- [ ] `serving/redshift_load.py`: orchestrate load (boto3 Redshift Data API)
- [ ] **Tear down / snapshot** Redshift when done (avoid idle storage cost)
- **Done when:** `market_analytics` is queryable in Redshift; teardown documented.

## Phase 10 — Tableau dashboards
- [ ] Analytics dashboard (VWAP/volatility/spread) — connect to Redshift or extract
- [ ] Surveillance dashboard (alerts by type/symbol/time) — connect to Athena
- [ ] Export packaged workbooks (.twbx) + screenshots into `docs/` for the README
- **Done when:** two dashboards exist with screenshots in the repo.

## Phase 11 — Polish for the resume
- [ ] README: architecture diagram, screenshots, the resume bullets with real
      numbers (rows processed, runtime, skew-fix improvement %)
- [ ] Fill `docs/DATA_DICTIONARY.md`
- [ ] Record a short run/demo GIF or notes
- [ ] Final cost check; delete everything not needed for demos

## Optional stretch — Streaming module
- [ ] `jobs/run_streaming.py`: Structured Streaming file-source (or Kinesis free
      tier) with `F.window()` event-time windows + watermark for late corrections
- [ ] Clearly labeled as a bonus; reuses the same domain/schema

---

## Suggested resume bullets (fill the [X] after real runs)
- Built an end-to-end Spark data pipeline processing **[X]M+** order events on AWS
  EMR Serverless, producing surveillance alerts and market analytics into a Delta
  Lake lakehouse on S3.
- Detected spoofing/wash-trade patterns using session-windowed aggregations,
  self-joins, and a Pandas-UDF risk score; cut job runtime **[X]%** by resolving
  skew on high-volume symbols with key salting.
- Implemented Delta `MERGE`-based upserts with time travel to handle trade
  corrections with a full audit trail.
- Delivered dual serving layers (Athena for ad-hoc, Redshift Serverless for BI)
  over one curated dataset; built Tableau dashboards for both use cases.
