.PHONY: test lint format

PYTHON ?= .venv/bin/python

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m black --check .

format:
	$(PYTHON) -m ruff check . --fix
	$(PYTHON) -m black .
