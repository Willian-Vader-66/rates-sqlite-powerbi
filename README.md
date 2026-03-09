# FX Rates Ingestion: Frankfurter API -> SQLite -> Power BI

This project fetches foreign exchange rates from the public Frankfurter API, normalizes the payload into a tabular dataset, persists the history in SQLite with idempotent UPSERTs, and prepares the data for Power BI through ODBC.

## What This Project Delivers

- Daily FX rate ingestion with no API key.
- Historical backfill over a date range.
- Validation and normalization for latest and time-series payloads.
- SQLite persistence with run tracking in `ingest_runs`.
- Local cache for raw API responses in `cache/`.
- Structured logs in `logs/app.log`.
- Power BI connection instructions for Windows.

## Requirements

- Windows PowerShell
- Python 3.11+
- Internet access to `https://api.frankfurter.dev`

## Windows Setup

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
Copy-Item .env.example .env
```

## Commands

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

Global CLI options available on every command:

- `--db-path`
- `--cache-dir`
- `--no-cache`
- `--log-level`
- `--timeout`

## Quick SQLite Inspection

Open the database with a GUI tool or run a quick query from PowerShell:

```powershell
python -c "import sqlite3; conn = sqlite3.connect('data/fx.sqlite'); [print(row) for row in conn.execute(\"SELECT date, base, symbol, rate FROM fx_rates ORDER BY date DESC, symbol LIMIT 10;\")]; conn.close()"
```

Example SQL for recent rates:

```sql
SELECT date, base, symbol, rate
FROM fx_rates
ORDER BY date DESC, symbol
LIMIT 20;
```

## Power BI via ODBC

1. Install a SQLite ODBC driver on Windows.
2. Create a System DSN or User DSN pointing to `data/fx.sqlite`.
3. Open Power BI Desktop.
4. Go to `Get Data -> ODBC`.
5. Select the SQLite DSN.
6. Load the `fx_rates` table.

Suggested visuals:

- Line chart with `date` on X, `rate` on Y, legend by `symbol`
- Slicer by `symbol`
- Card showing the latest available rate

The placeholder screenshot file lives at `assets/powerbi_screenshot_placeholder.png`.

## Logs and Cache

- Raw API payload cache files are stored in `cache/` and keyed by a stable hash of URL plus query parameters.
- Structured application logs go to the console and `logs/app.log`.
- Every ingest run is tracked in SQLite through `ingest_runs`.

## Validation and Local Checks

Run the automated tests:

```powershell
python -m pytest
```

Run a small real backfill:

```powershell
python -m fx_rates backfill --start 2026-03-02 --end 2026-03-09 --base USD --symbols BRL,EUR
```

Inspect recent ingest runs:

```powershell
python -m fx_rates status --last 10
```

## Troubleshooting

- `python` not found:
  Install Python 3.11+ and ensure `python` is available in the terminal PATH.
- `pytest` not found:
  Activate the virtual environment and run `pip install -r requirements.txt`.
- HTTP or timeout failures:
  Retry with a larger `--timeout` value and confirm access to `api.frankfurter.dev`.
- Unexpected cached responses:
  Re-run the command with `--no-cache`.
- Empty Power BI result:
  Confirm the DSN points to `data/fx.sqlite` and verify rows exist in `fx_rates`.
