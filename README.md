# FX Rates Ingestion: Frankfurter API -> SQLite -> Power BI

`fx_rates` is a distributable Python CLI that fetches FX rates from the public Frankfurter API, normalizes them into a tabular dataset, persists them into SQLite with idempotent UPSERTs, and keeps the result ready for Power BI via ODBC.

## What This Project Delivers

- Daily FX rate ingestion with no API key
- Historical backfill over a configurable date range
- Validation for latest and time-series API payloads
- SQLite persistence with `fx_rates` and `ingest_runs`
- Local cache for raw API responses in `cache/`
- Structured logs in `logs/app.log`
- Power BI connection instructions for Windows
- Buildable `wheel` and `sdist` artifacts

## Requirements

- Python 3.11+
- Network access to `https://api.frankfurter.dev`
- Windows PowerShell for the provided `.ps1` helpers

## Installation

### Runtime install

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install .
Copy-Item .env.example .env
```

### Development install

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -e .
pip install -r requirements.txt
Copy-Item .env.example .env
```

## CLI Usage

Backfill:

```powershell
python -m fx_rates backfill --start 2025-11-01 --end 2026-02-10 --base USD --symbols BRL,EUR
```

Daily update:

```powershell
python -m fx_rates daily --base USD --symbols BRL,EUR
```

Status:

```powershell
python -m fx_rates status --last 10
```

Console script entrypoint:

```powershell
fx-rates daily --base USD --symbols BRL,EUR
```

PowerShell helpers:

```powershell
.\scripts\run_backfill.ps1
.\scripts\run_daily.ps1
```

## Runtime Defaults

- Database: `data/fx.sqlite`
- Cache directory: `cache/`
- Logs: `logs/app.log`
- Timeout: `20` seconds

Global CLI options:

- `--db-path`
- `--cache-dir`
- `--no-cache`
- `--log-level`
- `--timeout`

## Build and Package

Build distributable artifacts:

```powershell
python -m build
```

This generates:

- `dist/*.whl`
- `dist/*.tar.gz`

Install the built wheel into a clean environment:

```powershell
python -m venv .pkgtest
.\.pkgtest\Scripts\activate
pip install dist\*.whl
python -m fx_rates --help
fx-rates --help
```

## Local Validation

Run the automated tests:

```powershell
python -m pytest -q
```

Run a small real backfill:

```powershell
python -m fx_rates backfill --start 2026-03-02 --end 2026-03-09 --base USD --symbols BRL,EUR
```

Run the latest business day update:

```powershell
python -m fx_rates daily --base USD --symbols BRL,EUR
```

Inspect recent runs:

```powershell
python -m fx_rates status --last 10
```

## Quick SQLite Inspection

```powershell
python -c "import sqlite3; conn = sqlite3.connect('data/fx.sqlite'); [print(row) for row in conn.execute(\"SELECT date, base, symbol, rate FROM fx_rates ORDER BY date DESC, symbol LIMIT 10;\")]; conn.close()"
```

Example SQL:

```sql
SELECT date, base, symbol, rate
FROM fx_rates
ORDER BY date DESC, symbol
LIMIT 20;
```

## Power BI via ODBC

1. Install a SQLite ODBC driver on Windows.
2. Create a DSN pointing to `data/fx.sqlite`.
3. Open Power BI Desktop.
4. Go to `Get Data -> ODBC`.
5. Select the SQLite DSN.
6. Load the `fx_rates` table.

Suggested visuals:

- Line chart with `date` on X, `rate` on Y, legend by `symbol`
- Slicer by `symbol`
- Card for the latest rate

Placeholder screenshot:

- `assets/powerbi_screenshot_placeholder.png`

## Logs, Cache, and Data

- Raw API responses are cached in `cache/` using a stable hash of URL plus query parameters.
- Structured logs go to console and `logs/app.log`.
- SQLite data is stored in `data/fx.sqlite`.
- `ingest_runs` records every `OK` and `FAIL` execution with row count and error details.

## Release Workflow

- Pull requests and pushes to `main` run CI automatically.
- Tags like `v0.1.0` trigger the release workflow.
- The release workflow builds `wheel` and `sdist`, attaches them to GitHub Releases, and generates release notes.

## Compatibility Policy

- The current SQLite schema is the supported production schema for `v0.1.x`.
- Schema changes after this version require an explicit migration strategy.
- This version does not include automated database migrations.
- Upgrades are expected to preserve `data/fx.sqlite` as long as the schema remains unchanged.

## Troubleshooting

- `python` not found:
  Install Python 3.11+ and confirm it is on the terminal PATH.
- `fx-rates` command not found:
  Activate the virtual environment and install the package with `pip install .` or `pip install -e .`.
- `pytest` or `build` not found:
  Activate the development environment and run `pip install -r requirements.txt`.
- HTTP or timeout failures:
  Retry with a larger `--timeout` and confirm access to `api.frankfurter.dev`.
- Cache corruption:
  Re-run the command with `--no-cache` or remove the broken file from `cache/`.
- Empty Power BI result:
  Confirm the DSN points to `data/fx.sqlite` and verify rows exist in `fx_rates`.
