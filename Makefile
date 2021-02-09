.PHONY: all
all: black

.PHONY: black
black:
	black -l 100 .

.PHONY: test
test:
	black -l 100 --check .

.PHONY: pytest
pytest:
	pytest .

.PHONY: lint-all
lint-all: bandit pyflakes pep257 prospector

.PHONY: bandit
bandit:
	bandit -c bandit.yml -r cubi_tk

.PHONY: pyflakes
pyflakes:
	pyflakes cubi_tk tests

.PHONY: pep257
pep257:
	pep257

.PHONY: flake8
flake8:
	flake8

.PHONY: prospector
prospector:
	prospector
