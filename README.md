# TradeLens

> Big data pipeline for trade surveillance & market analytics — Apache Spark, AWS lakehouse, Delta Lake.

TradeLens is an end-to-end data engineering pipeline that ingests historical and
synthetic trade/order data, processes it at scale using **Apache Spark** on **AWS
EMR Serverless**, and surfaces both **market-abuse surveillance alerts** and
standard **trading analytics** (VWAP, volatility, spread) through a **Delta
Lake**–backed data lake, queryable via **Athena** and **Redshift Serverless**,
with dashboards built in **Tableau**.

> ℹ️ This is a portfolio project designed to run **on demand** (not continuously)
> and to cost **near-zero when idle** — everything uses free data and serverless /
> pay-per-use AWS. See `CLAUDE.md` for the full engineering brief and constraints.

## Architecture

```
ingestion (yfinance + synthetic order-event generator)
        │
        ▼
   S3 bronze ──► Spark: silver (clean/dedup/SCD2) ──► gold (Delta)
                                                        ├─ market_analytics
                                                        └─ surveillance_alerts
                                                        │
                              ┌─────────────────────────┴───────────────┐
                              ▼                                          ▼
                        Athena (ad-hoc)                      Redshift Serverless (BI)
                              └──────────────┬───────────────────────────┘
                                             ▼
                                      Tableau Desktop
```

## Tech stack
Python · PySpark 3.5 · Delta Lake · AWS (S3, EMR Serverless, Glue, Athena,
Redshift Serverless) · Tableau Desktop · pytest.

## Key data engineering techniques demonstrated
Window functions · self-joins · broadcast joins · **data-skew handling via
salting** · Pandas UDFs · SCD Type 2 · **Delta `MERGE` upserts + time travel** ·
partitioning & pruning · data-quality gates · medallion (bronze/silver/gold)
architecture · dual serving (lakehouse vs warehouse).

## Quickstart (local, no AWS)
```bash
make setup      # venv + deps
make ingest     # fetch market data + generate synthetic order events -> data/raw
make run-local  # run the full batch pipeline locally
make test       # unit tests
```

## AWS run (demo)
See `infra/README.md` (set the billing alarm first), then:
```bash
make deploy-code && make submit-emr
```

## Repo layout
See `CLAUDE.md` §8 and `docs/PROJECT_PLAN.md` for the phased build plan.

## Data sources (all free, no API key)
- yfinance (historical OHLCV)
- Kaggle datasets (one-time download; not committed)
- Synthetic order-event generator with injected abuse patterns (clearly disclosed)
