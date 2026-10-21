.PHONY: check lint format types test

# Run everything CI runs
check: lint types test

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff format .
	uv run ruff check --fix .

types:
	uv run mypy

test:
	uv run pytest
