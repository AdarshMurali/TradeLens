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

ingest:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m tradelens.ingestion.fetch_market_data
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m tradelens.ingestion.generate_order_events

run-local:
	PYTHONPATH=$(PYTHONPATH) TRADELENS_ENV=local $(PYTHON) -m tradelens.jobs.run_pipeline

run-streaming:
	PYTHONPATH=$(PYTHONPATH) TRADELENS_ENV=local $(PYTHON) -m tradelens.jobs.run_streaming

test:
	PYTHONPATH=$(PYTHONPATH) pytest -q

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
