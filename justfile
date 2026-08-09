set shell := ["zsh", "-cu"]

default:
    just --list

install:
    uv sync --all-extras --all-groups

run:
    uv run pyspark-schemas

test:
    uv run pytest

lint:
    uv run ruff check . --fix

types:
    uv run pyrefly check

format:
    uv run ruff format .

check: lint types format test

docs-serve:
    zensical serve

docs-build:
    zensical build
