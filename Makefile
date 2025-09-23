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

.PHONY: sphinx-check
sphinx-check:
	@TMPDIR=$$(mktemp -d); \
	trap 'rm -rf "$$TMPDIR"' EXIT; \
	uv run sphinx-build --fail-on-warning --show-traceback --keep-going -D language=de docs_manual "$$TMPDIR"
