# CLAUDE.md — TradeLens

This file is the single source of truth for anyone (human or AI) working on this
repo. Read it fully before generating or editing code. When in doubt, follow the
conventions here and update this file if a decision changes.

---

## 1. What this project is

**TradeLens** is a resume/portfolio **data engineering** project: an end-to-end,
production-style big-data pipeline in the capital-markets domain. It ingests
historical and synthetic trade/order data, processes it at scale with **Apache
Spark**, and produces two gold-layer outputs:

1. **Market analytics** — VWAP, rolling volatility, spread, daily aggregates.
2. **Trade surveillance alerts** — detection of market-abuse patterns
   (spoofing, layering, wash trading) from order-event data.

Both are served two ways on top of the **same** curated data lake, to
demonstrate the trade-off deliberately:
- **Athena** — schema-on-read, pay-per-scan, ad-hoc exploration on S3.
- **Redshift Serverless** — a curated MPP warehouse for repeated BI queries.

Dashboards are built in **Tableau Desktop** (no Tableau Server — connect Desktop
directly to Athena/Redshift or to a Parquet extract).

The goal is to **showcase real, production-grade Spark and AWS data-engineering
skills** for interviews — not to run continuously. It must run cleanly on demand.

---

## 2. Hard constraints (do not violate)

- **No paid APIs. No paid data.** Only free, no-key data sources.
- **Cost must scale to (near) zero when idle.** Use serverless / pay-per-use AWS
  services only. Never provision anything that bills hourly while idle
  (no persistent EMR cluster, no provisioned Redshift cluster, no MWAA).
- **Local-first development.** All logic must run and be testable locally against
  the local filesystem before it ever touches AWS. AWS is for the final
  run/demo, not the dev loop.
- **A billing alarm (AWS Budgets, ~$5) must exist before any AWS resource is
  created.** Documented in `infra/README.md`.
- The pipeline is a **batch `spark-submit` job that runs to completion and
  exits.** Streaming is an optional, clearly-labeled stretch module only.

---

## 3. Architecture

```
                         INGESTION (Python, local, free)
   yfinance (OHLCV) ─┐
   Kaggle datasets  ─┼─► raw files ──► S3 bronze (raw, immutable, partitioned)
   synthetic order  ─┘                         │
   event generator                             ▼
                                    SPARK (EMR Serverless / local)
                                    ┌─────────────────────────────┐
                          bronze ──►│ silver: clean, conform,      │
                                    │ dedup, SCD2 securities master│
                                    └──────────────┬──────────────┘
                                                   ▼
                                    ┌─────────────────────────────┐
                                    │ gold:                        │
                                    │  • market_analytics          │
                                    │  • surveillance_alerts       │
                                    │ (Delta Lake, partitioned)    │
                                    └───────┬──────────────┬───────┘
                                            ▼              ▼
                                   S3 curated (Delta/Parquet)
                                            │              │
                              Glue Data Catalog (schemas)   │
                                            │              │
                                  ┌─────────┴───┐   ┌──────┴────────┐
                                  │  Athena     │   │ Redshift      │
                                  │ (ad-hoc)    │   │ Serverless    │
                                  └──────┬──────┘   │ COPY + MERGE  │
                                         │          └──────┬────────┘
                                         └──────┬──────────┘
                                                ▼
                                        Tableau Desktop
```

**Medallion layers:** bronze (raw) → silver (cleaned/conformed) → gold (curated,
business-ready). Every layer lives in S3; Spark is the only processing engine.

---

## 4. Tech stack

| Concern            | Choice                                             |
|--------------------|----------------------------------------------------|
| Language           | Python 3.10+                                        |
| Processing         | Apache Spark 3.5.x (PySpark)                         |
| Table format       | Delta Lake (`delta-spark`, standalone — no Databricks) |
| Cloud compute      | AWS EMR Serverless                                  |
| Storage            | Amazon S3                                           |
| Catalog            | AWS Glue Data Catalog                              |
| Query (lakehouse)  | Amazon Athena                                       |
| Warehouse          | Amazon Redshift Serverless                          |
| Viz                | Tableau Desktop                                     |
| Local S3 (optional)| MinIO via Docker (for `s3a://` testing offline)     |
| Orchestration      | Manual / local script (optionally local Airflow in Docker) |
| Testing            | pytest + chispa (Spark DataFrame assertions)        |
| Data quality       | Custom assertions (optionally Great Expectations)   |

---

## 5. Data sources (all free, no key)

- **yfinance** — historical OHLCV for any ticker. Primary "real" market data.
- **Kaggle** — e.g. Huge Stock Market Dataset, LOBSTER order-book samples
  (one-time manual download into `data/` locally; never commit large data).
- **Synthetic order-event generator** (`ingestion/generate_order_events.py`) —
  produces order placed/modified/cancelled/filled events layered on real OHLCV,
  with **injected abuse patterns** (spoofing, wash trades) for surveillance to
  detect. This is the primary source for the surveillance use case and is fully
  under our control (perfect for on-demand runs). Always disclose it as synthetic.

---

## 6. Spark techniques to demonstrate (the whole point)

Implement these deliberately and note them in the README so they map to resume
bullets. Do not remove them to "simplify" — they are the deliverable.

- **SQL window functions** (`Window.partitionBy(...).orderBy(...)`): rolling
  VWAP, rolling order counts, moving volatility, lag/lead, rank. (Batch, not
  streaming — see §7.)
- **Self-joins**: match cancel events back to their originating order (spoofing).
- **Broadcast joins**: large events × small securities-reference dimension.
- **Skew handling via salting**: high-volume symbols (AAPL/TSLA) dominate
  partitions — salt the key before aggregation and document the before/after.
- **UDF / Pandas UDF**: custom spoofing/wash-trade scoring function.
- **Deduplication + out-of-order handling**: `dropDuplicates`, watermark-style
  logic even in batch.
- **SCD Type 2**: securities master (ticker changes, delistings).
- **Delta Lake `MERGE INTO`**: upsert trade corrections/busts (late-arriving
  amendments) — plus **time travel** (`VERSION AS OF`) for the audit/compliance
  narrative.
- **Partitioning strategy**: `write.partitionBy("dt", "symbol")`; discuss
  partition pruning.
- **Performance reasoning**: use `.explain()`, `.cache()`/`.persist()` with
  justification; enable Adaptive Query Execution.
- **Data quality gates**: row counts, null checks, referential integrity between
  orders and trades — fail the job on violation.

---

## 7. Window functions vs streaming — READ THIS

Two different things share the word "window":
- **SQL window functions** (`.over(Window...)`) — batch analytical construct,
  used heavily here. NOT streaming.
- **Structured Streaming event-time windows** (`F.window("event_time", ...)`) —
  only in the streaming stretch module, requires a streaming source + sink.

The core pipeline is **batch**. Only `jobs/run_streaming.py` (optional stretch)
uses Structured Streaming (file-source `readStream` from S3, or Kinesis free
tier), with watermarking for late trade corrections.

---

## 8. Repository layout

```
TradeLens/
├── CLAUDE.md                 # this file
├── README.md                 # public-facing overview + resume bullets
├── docs/
│   ├── PROJECT_PLAN.md       # phased build plan (follow this order)
│   ├── ARCHITECTURE.md       # deeper architecture + data flow notes
│   └── DATA_DICTIONARY.md    # schemas for every table/layer
├── config/
│   └── config.yaml           # paths, symbols, thresholds, env switches
├── .env.example              # AWS_REGION, bucket names, etc. (copy to .env)
├── requirements.txt          # runtime deps
├── requirements-dev.txt      # dev/test deps
├── Makefile                  # common commands
├── src/tradelens/
│   ├── common/               # spark session, config, logging (implemented)
│   ├── ingestion/            # yfinance fetch + synthetic generator
│   ├── bronze/               # raw → bronze landing
│   ├── silver/               # clean, conform, dedup, SCD2
│   ├── gold/                 # market_analytics + surveillance_alerts
│   ├── serving/              # athena DDL registration + redshift load
│   ├── quality/              # data quality checks
│   └── jobs/                 # run_pipeline.py (batch entrypoint), run_streaming.py
├── sql/
│   ├── athena/               # external table DDL
│   └── redshift/             # DDL + COPY + MERGE
├── scripts/                  # aws setup, emr submit, run_local
├── tests/                    # pytest unit tests for transformations
├── infra/                    # IaC notes, billing alarm, IAM, teardown steps
└── notebooks/                # exploration only (not part of the pipeline)
```

Data lives under a local `data/` dir (gitignored) and in S3 — never commit data.

---

## 9. Coding conventions

- Package importable as `tradelens` (`src/` layout; installed with `pip install -e .`
  or `PYTHONPATH=src`).
- **Every transformation is a pure function** `DataFrame -> DataFrame` that takes
  and returns Spark DataFrames, with **no I/O inside it**. Read/write happens only
  in `jobs/` and layer entrypoints. This makes transformations unit-testable
  without AWS.
- Config comes from `config/config.yaml` + `.env`; never hardcode bucket names,
  paths, or thresholds in transformation code.
- All AWS resource names use the `tradelens-` prefix (see §10).
- Type hints on public functions. Docstrings state the Spark technique used.
- Logging via `common.logging_utils.get_logger(__name__)`, never bare `print`.
- Keep transformations engine-agnostic about storage: read paths from config so
  the same code runs local (`file://`/local path) and on S3 (`s3a://`).

---

## 10. Naming convention (AWS + repo)

```
tradelens-raw-<suffix>         # S3 bucket: bronze/raw zone (globally unique suffix)
tradelens-curated-<suffix>     # S3 bucket: silver+gold zone
tradelens-emr-job-role         # IAM role for EMR Serverless
tradelens-glue-db              # Glue database
tradelens-athena-wg            # Athena workgroup
tradelens-redshift-ns          # Redshift Serverless namespace
tradelens-redshift-wg          # Redshift Serverless workgroup
```
Only S3 bucket names need the unique suffix; keep everything else clean.

---

## 11. How to run

**Local (dev loop — no AWS):**
```
make setup            # create venv, install deps
make ingest           # fetch yfinance + generate synthetic order events -> data/raw
make run-local        # run full batch pipeline against local filesystem
make test             # pytest
```

**AWS (demo/final):**
```
# one-time: set up S3, Glue, IAM, billing alarm (see infra/README.md)
make deploy-code      # sync src + config to the code bucket
make submit-emr       # submit the batch job to EMR Serverless
# then register Athena tables (sql/athena) and/or load Redshift (sql/redshift)
```

Prefer running against a **small symbol set + short date range** while iterating;
scale up only for the final run.

---

## 12. Definition of done (per component)

- Runs locally end-to-end producing gold Delta tables.
- Has at least one pytest unit test for its core transformation.
- Passes data-quality checks.
- Named per §10, configured via §9 (no hardcoding).
- The Spark technique it demonstrates is noted in its docstring and README.

---

## 13. What NOT to do

- Do not add paid services or paid data.
- Do not create long-running / hourly-billed AWS resources.
- Do not put I/O inside transformation functions.
- Do not commit data, `.env`, credentials, or large artifacts.
- Do not duplicate the whole pipeline for Athena vs Redshift — they are two
  serving choices on the *same* gold data.
- Do not silently drop a demonstrated Spark technique to make code shorter.
