set shell := ["zsh", "-cu"]

default:
    just --list

install:
    uv sync --all-extras --all-groups

run:
    uv run spark-joinery

test:
    uv run pytest --ignore=docs_src/

lint:
    uv run ruff check . --fix

types:
    uv run pyrefly check

format:
    uv run ruff format .

check: lint types format test

docs-serve:
    uv run zensical build --clean
    uv run zensical serve

docs-build:
    uv run zensical build

docs-examples:
    uv run python scripts/run_docs_examples.py
