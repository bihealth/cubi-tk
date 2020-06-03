.PHONY: default black flake8 mypy test test-v test-vv

default: black flake8 mypy test

black:
	black -l 100 .

black-check:
	black -l 100 --check .

flake8:
	flake8 .

mypy:
	mypy cubi_tk

test:
	pytest --disable-pytest-warnings

test-v:
	pytest -v --disable-pytest-warnings

test-vv:
	pytest -vv --disable-pytest-warnings
