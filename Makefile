PYTHON ?= $(shell if command -v python >/dev/null; then echo python; elif test -x .venv/bin/python; then echo .venv/bin/python; else echo python3; fi)

.PHONY: install test lint check integration db-init observability-up observability-serve api websocket-demo frontend-install frontend-dev frontend-test frontend-build release-check real-demo-download

install:
	$(PYTHON) -m pip install -e ".[dev]"

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m ruff check .

check: lint test

integration:
	$(PYTHON) -m pytest -q -o addopts='' -m integration

db-init:
	$(PYTHON) -m matchstream.streaming.cli init-db

observability-up:
	docker compose up -d prometheus

observability-serve:
	$(PYTHON) -m matchstream.streaming.cli serve-observability

api:
	$(PYTHON) -m matchstream.api.server

websocket-demo:
	$(PYTHON) -m matchstream.api.demo_client $(URL)

frontend-install:
	npm --prefix frontend install

frontend-dev:
	npm --prefix frontend run dev

frontend-test:
	npm --prefix frontend test

frontend-build:
	npm --prefix frontend run build

release-check: check frontend-test frontend-build

real-demo-download:
	$(PYTHON) scripts/fetch_real_demo.py
