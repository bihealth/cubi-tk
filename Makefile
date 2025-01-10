.PHONY: all
all: lint format

.PHONY: format
format:
	uv run ruff format .

.PHONY: format-check
format-check:
	uv run ruff format --check .

.PHONY: lint
lint:
	uv run ruff check

.PHONY: pytest
pytest:
	uv run pytest
