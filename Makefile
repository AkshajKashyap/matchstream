.PHONY: install test lint check integration db-init

install:
	python -m pip install -e ".[dev]"

test:
	pytest -q

lint:
	ruff check .

check: lint test

integration:
	pytest -q -o addopts='' -m integration

db-init:
	python -m matchstream.streaming.cli init-db
