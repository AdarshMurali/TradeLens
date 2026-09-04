#!/usr/bin/env bash
# Convenience local run (equivalent to `make ingest && make run-local`).
set -euo pipefail
export TRADELENS_ENV=local
export PYTHONPATH=src
python -m tradelens.ingestion.fetch_market_data
python -m tradelens.ingestion.generate_order_events
python -m tradelens.jobs.run_pipeline
