PY ?= python
export PYTHONPATH := .
export PEP_DUCKDB_PATH ?= $(CURDIR)/warehouse/events.duckdb
DBT = cd dbt && dbt
VARS ?= {"pipeline_as_of": "2026-10-01 00:00:00"}

.PHONY: all install generate ingest ingest-partial dbt-build dq sql figures pipeline test lint clean

all: pipeline

install:
	$(PY) -m pip install -r requirements.txt

generate:            ## synthetic events -> data/landing (hourly NDJSON queue)
	$(PY) -m src.generator

ingest-partial:      ## micro-batches received before the cut-over hour
	$(PY) -m src.ingester --until 2026-09-29T00

ingest:              ## drain the whole landing queue (idempotent)
	$(PY) -m src.ingester

dbt-build:           ## models + tests
	$(DBT) build --profiles-dir . --vars '$(VARS)'

dq:
	$(PY) -m src.dq_monitor

sql:
	$(PY) -m src.run_sql

figures:
	$(PY) -m src.figures

pipeline:            ## everything end to end, from scratch -> results/pipeline_run.json
	$(PY) -m src.pipeline

test:
	$(PY) -m pytest -q tests

clean:
	rm -rf data/landing warehouse dbt/target dbt/logs
