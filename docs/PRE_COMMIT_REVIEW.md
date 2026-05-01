# Pre-Commit Review Notes

Review date: 2026-05-01

## Environment

- The project `.venv` is the validation environment.
- `fastapi`, `uvicorn`, and `httpx` must be installed through `python -m pip install -r requirements.txt` before running tests.
- The earlier `ModuleNotFoundError: No module named 'fastapi'` was caused by dependencies being installed in a different Python environment, not by a missing declaration in the manifests.

## Architecture

- Existing FX commands and tables remain backward compatible.
- The market-data expansion is layered as provider -> ingest/analysis services -> SQLite -> FastAPI.
- The future Java front-end must consume the local HTTP API documented in `docs/API_CONTRACT.md`; it should not read SQLite directly.

## Dependency Decision

- `httpx` remains in the main dependency files for now because FastAPI's `TestClient` requires it and the project has no separate dev dependency group yet.
- A future packaging cleanup can move test-only dependencies into an optional dev extra.

## Follow-Up Refactor Candidates

- `db_sqlite.py` now owns both FX and market-data persistence and is large enough to split later.
- Recommended future split: keep schema initialization in one module and move stock, quote, analysis, and dashboard query helpers into focused repository modules.
- This split is not required before the current commit because tests are passing and behavior is cohesive.

## Pre-Commit Checklist

- Run `.\.venv\Scripts\python.exe -m pytest -q`.
- Run demo-mode smoke commands for watchlist import, stock backfill, quote polling, analysis, and `serve --help`.
- Confirm `git status --short` does not include `.env`, generated SQLite databases, cache JSON, or log files.
