# TradeLens developer commands. Windows: run these in Git Bash, or use the
# equivalent commands directly. Requires Python 3.10+ and Java 11/17 for Spark.

PYTHON := python
VENV := .venv
PYTHONPATH := src

.PHONY: help setup ingest run-local run-streaming test lint format deploy-code submit-emr clean

help:
	@echo "setup        - create venv and install dependencies"
	@echo "ingest       - fetch market data + generate synthetic order events"
	@echo "run-local    - run the full batch pipeline locally"
	@echo "run-streaming- run the optional streaming module locally"
	@echo "test         - run pytest"
	@echo "lint         - ruff checks"
	@echo "format       - black + ruff --fix"
	@echo "deploy-code  - sync src+config to the S3 code bucket"
	@echo "submit-emr   - submit the batch job to EMR Serverless"
	@echo "clean        - remove local build/warehouse artifacts"

setup:
	$(PYTHON) -m venv $(VENV)
	. $(VENV)/bin/activate && pip install --upgrade pip && pip install -r requirements-dev.txt && pip install -e .

# Unset SPARK_HOME/PYTHONPATH before every run: a stray global SPARK_HOME or
# PYTHONPATH pointing at an unrelated Spark install (e.g. from another local
# project) silently shadows the venv's pip-installed PySpark and breaks the
# Delta Lake version match. See docs/PROJECT_PLAN.md Phase 0.
ENV_GUARD := env -u SPARK_HOME PYTHONPATH=$(PYTHONPATH)

ingest:
	$(ENV_GUARD) $(PYTHON) -m tradelens.ingestion.fetch_market_data
	$(ENV_GUARD) $(PYTHON) -m tradelens.ingestion.generate_order_events

run-local:
	$(ENV_GUARD) TRADELENS_ENV=local $(PYTHON) -m tradelens.jobs.run_pipeline

run-streaming:
	$(ENV_GUARD) TRADELENS_ENV=local $(PYTHON) -m tradelens.jobs.run_streaming

test:
	$(ENV_GUARD) pytest -q

lint:
	ruff check src tests

format:
	black src tests && ruff check --fix src tests

deploy-code:
	bash scripts/deploy_code.sh

submit-emr:
	bash scripts/submit_emr_serverless.sh

clean:
	rm -rf spark-warehouse metastore_db derby.log .pytest_cache **/__pycache__
