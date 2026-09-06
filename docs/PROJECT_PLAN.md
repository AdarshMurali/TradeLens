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
- [x] `gold/market_analytics.py`:
      - rolling **VWAP** (window function)
      - rolling **volatility** (stddev of log returns over rolling window)
      - spread proxy, daily OHLCV rollups (rollup is a no-op here — see the
        module docstring for why: ingestion is daily-bar granularity, so the
        gold grain already is the daily rollup; the aggregation logic to add
        if intraday ingestion ever lands is documented there)
      - **broadcast join** to securities master for enrichment
- [x] Write Delta partitioned by `dt` (+ `symbol`, per CLAUDE.md SS9); unit
      tests on VWAP/volatility math.
- **Done when:** `market_analytics` gold Delta table validated against hand-calc
      on a tiny sample.

## Phase 5 — Gold: surveillance alerts
- [x] `gold/surveillance_alerts.py`:
      - **self-join** cancels ↔ originating orders (spoofing) — also requires
        unusually large quantity (`spoof_min_quantity`), not latency alone
      - windowed rapid order/cancel counts per account/symbol
        (`rapid_order_counts_salted`) — TradeLens's detector for layering
      - **wash-trade** detection (same owner both sides)
      - **Pandas UDF** combining signals into a `risk_score`, floored at the
        alert threshold for anything that already cleared its detector
      - **salting** on hot symbols to fix skew (`rapid_order_counts_salted`
        vs the unsalted `rapid_order_counts_unsalted` kept only for the
        comparison); before/after `.explain()` logged from `run_pipeline.py`
- [x] `MERGE INTO` path for trade corrections/busts (upsert) + a time-travel
      demo — `jobs/apply_trade_corrections.py`, targeting the new `gold/fills`
      table
- [x] Validate detection against the Phase-1 ground-truth labels
      (precision/recall) — logged every run via `quality.precision_recall`.
      Real numbers from a full local run: spoofing precision 1.00 / recall
      1.00; wash_trade precision 0.95 / recall 1.00; layering (via
      rapid_ordering) precision 0.98 / recall 0.92.
- [x] Unit tests for spoofing self-join and wash-trade logic.
- **Done when:** `surveillance_alerts` gold table flags the injected patterns.

## Phase 6 — AWS foundation (first AWS spend — be careful)
- [x] `infra/README.md`: set **AWS Budgets $5 alarm FIRST** — automated in
      `scripts/setup_aws.sh` via `aws budgets create-budget`
- [x] Create S3 buckets (`tradelens-raw-*`, `tradelens-curated-*`), Glue DB,
      EMR Serverless application, IAM role (`scripts/setup_aws.sh`) — the EMR
      Serverless application needs an explicit VPC (S3 gateway endpoint) or
      its default networking cannot reach S3 at all; see the script's
      comments and the Sep-2026 debugging notes in memory
- [ ] Generate a larger demo-scale dataset locally (more symbols / longer
      range / higher order density than the local-dev config — see below),
      then upload it + code to S3. Not a literal "sample": EMR Serverless
      bills per job-second, not idle time, so a bigger one-off run is cheap.
      **Deferred** — Phase 6/7 validated end-to-end against the existing
      local-dev dataset first, deliberately isolating "does the AWS plumbing
      work" from "does the vectorized generator produce correct big data."
- [ ] Before generating at demo scale: vectorize `generate_order_events.py`'s
      per-order Python loop (numpy/pandas vectorized ops) — the current
      dataclass-per-order approach is fine at ~5M rows (~2 min) but won't
      scale to tens of millions in reasonable local time.
- **Done when:** buckets/role/app exist and billing alarm is active. (Demo-
      scale data generation carried forward as its own follow-up.)

## Phase 7 — Run on EMR Serverless
- [x] `scripts/submit_emr_serverless.sh`: package `src/`, submit `jobs/run_pipeline.py`
      — needs the Delta jars + `delta`/`yaml`/`dotenv` Python packages
      pre-staged to S3 (no internet egress to resolve them at submit time),
      and explicit executor sizing kept under the account's EMR Serverless
      vCPU quota (16 in ap-south-1 as of 2026-09-05)
- [x] Confirm gold Delta tables land in `tradelens-curated-*` — verified:
      `fills`, `market_analytics`, `surveillance_alerts` all present; the
      same precision/recall numbers as the local run (spoofing 1.00/1.00,
      wash_trade 0.95/1.00, layering 0.98/0.92)
- [ ] Register schemas in Glue (crawler or explicit DDL) — carried forward to
      Phase 8 (Athena needs this; the batch job itself doesn't)
- **Done when:** the same pipeline that ran locally completes on EMR Serverless
      and the app scales back to zero afterward. Confirmed: full run
      succeeded (~54 min, ~$0.85), `autoStopConfiguration` (15 min idle
      timeout) scales the application to $0 with no action needed.

## Phase 8 — Serving A: Athena
- [x] `sql/athena/create_tables.sql`: external tables over the gold S3 data —
      uses Athena engine v3's native Delta Lake table type
      (`TBLPROPERTIES ('table_type'='DELTA')`), not `STORED AS PARQUET` +
      `MSCK REPAIR TABLE`: schema and partitions come straight from the Delta
      transaction log, which also stays correct after a future `MERGE INTO`
      leaves tombstoned files behind (a plain Parquet external table has no
      concept of that and would double-count). `scripts/setup_aws.sh` creates
      the workgroup (`tradelens-athena-wg`, engine v3 explicit); `src/tradelens/
      serving/athena_ddl.py` submits the DDL via the Athena Data API.
- [x] Sample analyst queries — `sql/athena/sample_queries.sql` (alert volume
      by pattern, top flagged accounts, highest-severity alerts, VWAP/
      volatility trend by symbol, sector volatility, busted-trade rollup)
- **Done when:** Athena returns results over gold data. Confirmed: queried
      `surveillance_alerts` live — spoofing 1250, wash_trade 1338,
      rapid_ordering 3076 alert rows, matching the pipeline's own TP counts.

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
