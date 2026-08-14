.PHONY: install test lint check integration

install:
	python -m pip install -e ".[dev]"

test:
	pytest -q

lint:
	ruff check .

check: lint test

integration:
	pytest -q -o addopts='' -m integration
