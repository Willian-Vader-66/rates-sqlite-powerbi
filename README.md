# fx-rates-sqlite-powerbi

FX rates ingestion project that fetches public exchange rates from the Frankfurter API, normalizes them into a tabular dataset, stores history in SQLite with idempotent UPSERT behavior, and prepares the data for Power BI via ODBC.

## What This Project Is

This repository provides a small but production-minded local pipeline for foreign exchange history:

- `backfill` downloads a date range
- `daily` fetches the latest available business day
- backfill/time-series responses are cached on disk
- daily/latest fetches fresh data by default
- normalized rows are persisted into SQLite
- each ingestion run is tracked in `ingest_runs`
- logs are written to console and `logs/app.log`

Default runtime paths:

- SQLite database: `data/fx.sqlite`
- Log file: `logs/app.log`
- Cache directory: `cache/`

## Repository Layout

```text
fx-rates-sqlite-powerbi/
  fx_rates/
  src/fx_rates/
  tests/
  scripts/
  data/
  cache/
  logs/
  assets/
  .env.example
  requirements.txt
  README.md
```

The repository includes a small root-level `fx_rates` wrapper package that forwards imports to `src/fx_rates`, so running `python -m fx_rates ...` from the repo root does not require setting `PYTHONPATH` or installing with `pip -e .`.

## Verified Windows Setup

These are the commands verified during release hardening in a fresh standard CPython 3.11 virtual environment:

```powershell
python -m venv .release-venv
.\.release-venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
copy .env.example .env
```

For day-to-day local use, you can replace `.release-venv` with `.venv` if you prefer.

## Configuration

Optional environment variables can be placed in `.env`:

```dotenv
API_BASE_URL=https://api.frankfurter.dev/v1
DB_PATH=data/fx.sqlite
CACHE_DIR=cache
USE_CACHE_LATEST=false
LOG_FILE=logs/app.log
LOG_LEVEL=INFO
TIMEOUT_SECONDS=20
MAX_RETRIES=3
```

CLI flags override `.env` values for the same setting.

## CLI Commands

### Backfill

```powershell
python -m fx_rates backfill --start 2026-02-01 --end 2026-02-03 --base USD --symbols BRL,EUR
```

### Daily

```powershell
python -m fx_rates daily --base USD --symbols BRL,EUR
```

`daily` bypasses the disk cache for the `latest` endpoint by default, so it does not silently reuse stale cached data forever. If you explicitly want to reuse a cached `latest` payload, add `--use-cache-latest`.

### Status

```powershell
python -m fx_rates status --last 5
```

### Common Flags

- `--db-path` default: `data/fx.sqlite`
- `--cache-dir` default: `cache`
- `--no-cache` disables disk-cache reuse
- `--log-file` default: `logs/app.log`
- `--log-level` accepts `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`
- `--timeout` default: `20`
- `--retries` default: `3`

### Daily-Only Flag

- `--use-cache-latest` re-enables cache reuse for the `latest` endpoint

### Input Rules

- `--base` is normalized to uppercase and must be a 3-letter code such as `USD`
- `--symbols` are normalized by trimming spaces, uppercasing, removing duplicates, and sorting alphabetically
- `backfill` validates `--start <= --end`

## Testing

Verified test command:

```powershell
$env:TEMP = Join-Path (Get-Location) '.release-temp'
$env:TMP = $env:TEMP
python -m pytest -q --basetemp (Join-Path $env:TEMP 'pytest-run')
```

Note: the temp overrides were needed on the hardened Windows host used for release verification because its default temp location had permission restrictions. On a normal local machine, `python -m pytest -q` is usually sufficient.

## Verified Smoke Test Commands

```powershell
python -m fx_rates backfill --start 2026-02-01 --end 2026-02-03 --base USD --symbols BRL,EUR
python -m fx_rates daily --base USD --symbols BRL,EUR
python -m fx_rates status --last 5
```

### Network Requirement

`daily` performs a live fetch from `https://api.frankfurter.dev/v1/latest`. For that command to succeed, the machine running it must have outbound network access to `api.frankfurter.dev`. If outbound access is blocked, the project should still:

- retry transient HTTP/network failures
- log retry attempts
- record a `FAIL` row in `ingest_runs`

## PowerShell Scripts

Backfill:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_backfill.ps1
```

Daily:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_daily.ps1
```

These wrappers are Windows-friendly and pass through the current CLI flags, including `-LogFile`, `-Retries`, and `-UseCacheLatest` for `daily`.

## SQLite Output

### Tables

- `fx_rates`
- `ingest_runs`

### Analytics Views

- `v_fx_daily`
- `v_fx_latest`
- `v_fx_monthly_avg`

Quick check:

```powershell
python -c "import sqlite3; conn = sqlite3.connect('data/fx.sqlite'); print(conn.execute('select count(*) from fx_rates').fetchone()[0]); conn.close()"
```

## Power BI Connection Note

1. Install an SQLite ODBC driver on Windows.
2. Create a DSN that points to `data/fx.sqlite`.
3. In Power BI Desktop, go to `Home -> Get Data -> ODBC`.
4. Select the DSN and load one or more of:
   - `v_fx_daily`
   - `v_fx_latest`
   - `v_fx_monthly_avg`

Suggested use:

- `v_fx_daily` for daily line charts
- `v_fx_latest` for current-rate cards
- `v_fx_monthly_avg` for month-over-month summaries

## Verified Demo Run

Release hardening was completed with a fresh standard CPython virtual environment and the following outcomes:

- `pip install -r requirements.txt` completed successfully in `.release-venv`
- `pytest` passed: `18 passed`
- `backfill` completed successfully and reused the cached time-series payload for `2026-02-01..2026-02-03`
- `daily` completed successfully with `use_cache_latest=False`, fetched live data, and wrote `2` rows
- `status --last 5` showed the recent `backfill` and `daily` runs correctly
- SQLite contains data in `data/fx.sqlite`, including `fx_rates`, `ingest_runs`, and the analytics views
- `logs/app.log` was updated during the smoke run

At the end of the verified demo run, the local database contained:

- `fx_rates`: `8` rows
- `ingest_runs`: `9` rows

## Troubleshooting

### Timeout or connectivity issues

- increase `--timeout`
- increase `--retries`
- retry with `--no-cache` if you suspect stale time-series cache content
- verify outbound access to `https://api.frankfurter.dev`

### HTTP failure on `daily`

- the process exits non-zero
- the latest row in `ingest_runs` is marked `FAIL`
- inspect `logs/app.log` for stack traces and retry events

### Re-running a backfill

Re-running the same range is safe. The project uses SQLite UPSERT and updates the existing `(date, base, symbol)` row instead of inserting duplicates.
