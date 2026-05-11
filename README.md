# fx-rates-sqlite-powerbi

Local financial market data backend that fetches public exchange rates from the Frankfurter API, stores FX, stock, crypto, and macro history in SQLite with idempotent UPSERT behavior, runs lightweight analysis over stored data, and exposes a local HTTP API for JavaFX or future front-ends.

## What This Project Is

This repository started as a small but production-minded local pipeline for foreign exchange history and now includes a broader market-data backend:

- `backfill` downloads a date range
- `daily` fetches the latest available business day
- `stocks backfill` and `stocks daily` ingest daily stock history from an editable watchlist
- `crypto backfill`, `crypto daily`, and `crypto quotes` add crypto history and latest quotes
- `macro backfill`, `macro daily`, and `macro status` add macro indicators such as Selic
- `quotes poll` refreshes latest quotes through safe polling for selected symbols
- `analyze now` creates stock, FX, crypto, and macro analysis snapshots from data already stored in SQLite
- `dashboard prepare-demo` prepares a complete local SQLite dataset for the JavaFX Finance Monitor
- `dashboard audit` reports dashboard readiness, coverage, missing quotes/analysis, and duplicate instrument keys
- `serve` exposes a local HTTP API for Java or other front-ends
- backfill/time-series responses are cached on disk
- daily/latest fetches fresh data by default
- normalized rows are persisted into SQLite
- each ingestion run is tracked in `ingest_runs`
- logs are written to console and `logs/app.log`

This is not a professional trading platform. Quote collection is near-real-time polling, not a tick-by-tick feed, and provider rate limits should be respected.

## Data Modes

Market-data origin is explicit:

- `demo`: deterministic mock data for local demos and offline validation.
- `live`: data fetched from configured real providers.
- `mixed`: demo and live records are both present.
- `unknown`: origin cannot be trusted yet.

Prepare demo data explicitly:

```powershell
python -m fx_rates dashboard prepare-demo --years 4 --demo
```

Check providers and audits:

```powershell
python -m fx_rates providers status
python -m fx_rates dashboard audit
python -m fx_rates dashboard audit-market
```

Live preparation fetches supported providers, writes `data_mode=live`, and does not silently fall back to demo:

```powershell
python -m fx_rates dashboard prepare-live --years 4
python -m fx_rates dashboard prepare-live --years 4 --allow-mixed
python -m fx_rates dashboard prepare-live --years 4 --asset-type FX --symbols BRL,EUR
python -m fx_rates dashboard prepare-live --years 4 --asset-type STOCK --symbols AAPL,MSFT --replace-demo
```

See `docs/DATA_MODE_STRATEGY.md` and `docs/LIVE_PROVIDERS_SETUP.md`.

## Architecture

```text
Frankfurter API -> FX ingest CLI -----------+
                                            |
Twelve Data API -> stock/quote providers -> Python backend -> SQLite
CoinGecko API  -> crypto provider ----------+        |
BCB SGS API    -> macro provider -----------+        |
Mock providers -> demo/test data -----------+        |
                                                     v
                                           FastAPI local HTTP API
                                                     |
                                                     v
                                      future Java front-end / dashboards
```

The Java front-end must consume the Python HTTP API. It should not read SQLite directly. The API contract lives in `docs/API_CONTRACT.md`.

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
TWELVE_DATA_API_KEY=
MARKET_DATA_PROVIDER=twelvedata
MARKET_DATA_DEMO_MODE=false
API_HOST=127.0.0.1
API_PORT=8000
```

CLI flags override `.env` values for the same setting.

For stock data, create a Twelve Data API key and set `TWELVE_DATA_API_KEY`. Macro data uses Banco Central SGS and crypto data uses CoinGecko when demo mode is off. For local demos, tests, and UI work without external API access, set:

```dotenv
MARKET_DATA_DEMO_MODE=true
```

Demo mode and `--demo` use deterministic synthetic data designed for portfolio demos and local validation without API keys.
It is visually plausible, but it is not real market history.
Stocks use `MockMarketDataProvider`.
Macro and crypto demo data come from `MockMacroProvider` and `MockCryptoProvider`; the Java UI never hardcodes fake market data.

Dependency note: `httpx` is listed with the main requirements because FastAPI's `TestClient` needs it and this project does not yet split runtime and development dependency groups.

## CLI Commands

### Prepare The Local Dashboard

Use this before opening the JavaFX Finance Monitor on a fresh or sparse SQLite database. It imports the curated dashboard instruments, creates deterministic historical demo data, writes latest quotes, and generates analysis snapshots in the configured SQLite database.

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi
.\.venv\Scripts\Activate.ps1
$env:MARKET_DATA_DEMO_MODE='true'
python -m fx_rates dashboard prepare-demo --years 4 --demo
python -m fx_rates serve --host 127.0.0.1 --port 8000
```

In another terminal:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi\frontend-java
mvn javafx:run
```

Validate the API:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/system/status
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/summary
```

The default prepare command loads the first 32 active stocks from `data/reference/top100_stocks.csv`, all active currencies from `data/reference/currencies.csv` as USD-based FX series, BTC/ETH and the other curated crypto assets, and macro indicators including Selic. Use `--stock-limit 100` when you want the full stock reference list.

Audit the prepared data:

```powershell
python -m fx_rates dashboard audit
```

Expected demo readiness is at least 68 instruments, no missing latest quotes, no missing analysis snapshots, and four-year coverage for USD/BRL, USD/EUR, BTC, ETH, Selic, and the major stocks.

### Como resolver dashboard sem dados

If JavaFX connects to the API but the dashboard is empty, confirm that `prepare-demo` and `serve` are using the same SQLite file. Relative `DB_PATH` values from `.env`, such as `data/fx.sqlite`, are resolved from the project root; explicit `--db-path` values are honored by each command.

PowerShell:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi
.\.venv\Scripts\Activate.ps1
python -m fx_rates dashboard prepare-demo --years 4 --demo
python -m fx_rates dashboard audit
python -m fx_rates serve --host 127.0.0.1 --port 8000
```

In another terminal:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/system/status
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/summary
```

Check that `/api/system/status.db_path` matches the path printed by `dashboard audit`. If `is_empty` is `true`, rerun `prepare-demo`, restart the backend, and check the same endpoint again.

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

### Import Stock Watchlist

```powershell
python -m fx_rates instruments import --file data/reference/top100_stocks.csv
```

Editable watchlists:

- `data/reference/top100_stocks.csv`
- `data/reference/sample_stocks.csv`

### Stock Daily Ingestion

```powershell
python -m fx_rates stocks daily --watchlist data/reference/top100_stocks.csv
```

### Stock Historical Backfill

```powershell
python -m fx_rates stocks backfill --start 2026-01-01 --end 2026-04-25 --watchlist data/reference/sample_stocks.csv
```

### Quote Polling

```powershell
python -m fx_rates quotes poll --symbols AAPL,MSFT,NVDA,TSLA --interval-seconds 30 --duration-minutes 5
```

Poll a small selected set of symbols. Do not poll the full 100-stock watchlist every few seconds.

### Crypto History And Quotes

```powershell
python -m fx_rates crypto backfill --start 2026-01-01 --end 2026-04-25 --symbols BTC,ETH
python -m fx_rates crypto daily
python -m fx_rates crypto quotes --symbols BTC,ETH,SOL
```

Crypto references live in `data/reference/crypto_assets.csv`. Demo mode uses deterministic backend-generated data. Live crypto support uses CoinGecko's public API and should be used with reasonable cadence.

### Macro Indicators

```powershell
python -m fx_rates macro backfill --start 2026-01-01 --end 2026-04-25
python -m fx_rates macro daily
python -m fx_rates macro status
```

Macro references live in `data/reference/macro_indicators.csv`. The initial seeds are Selic daily, monthly, and annualized monthly using Banco Central SGS provider codes.

### Analysis

```powershell
python -m fx_rates analyze now --symbols AAPL,MSFT,NVDA,TSLA
python -m fx_rates analyze now --asset-type FX
python -m fx_rates analyze now --asset-type STOCK
```

Analysis uses stored SQLite history, not invented values. It calculates last close, daily return, SMA 20, SMA 50, 20-day volatility, 30-day min/max, trend, and signal.

### API Server

```powershell
python -m fx_rates serve --host 127.0.0.1 --port 8000
```

Useful endpoints:

- `GET /health`
- `GET /api/system/status`
- `GET /api/instruments`
- `GET /api/stocks/history?symbol=AAPL&start=2026-01-01&end=2026-04-25`
- `GET /api/fx/history?base=USD&symbol=BRL&start=2026-01-01&end=2026-04-25`
- `GET /api/quotes/latest?symbols=AAPL,MSFT&asset_type=STOCK`
- `GET /api/analysis/latest?asset_type=STOCK`
- `GET /api/dashboard/summary`
- `GET /api/dashboard/market-overview`
- `GET /api/dashboard/fixed-charts`
- `GET /api/dashboard/top-stocks-30d`
- `GET /api/crypto/history?symbol=BTC&start=2026-01-01&end=2026-04-25`
- `GET /api/macro/history?indicator_code=SELIC_DAILY&start=2026-01-01&end=2026-04-25`

See `docs/API_CONTRACT.md` for JSON examples.

### JavaFX Product Dashboard

The JavaFX app is a corporate dark-mode desktop dashboard that consumes the FastAPI backend over HTTP only. It includes populated Overview, Markets, Stocks, FX & Crypto, Macro, Watchlist, and Settings pages.

Current productized UI features:

- populated summary and market overview cards after `dashboard prepare-demo`
- dedicated cross-asset Markets, Stocks, FX & Crypto, and Macro views
- fixed 30-day overview mini charts
- Top 10 Companies table with financial formatting
- Watchlist filters and selected instrument details
- interactive charts with 30D, 90D, 6M, 1Y, and 4Y ranges
- hover tooltip, crosshair, last-value marker, loading/empty/error states
- Settings page showing `/api/system/status` diagnostics

Frontend productization docs:

- `docs/FRONTEND_PRODUCTIZATION_AUDIT.md`
- `docs/FRONTEND_PRODUCTIZATION_PLAN.md`
- `docs/FRONTEND_VISUAL_QA_CHECKLIST.md`

### Local Visual Test Runner

From the repository root, `run_visual_test.ps1` can prepare data, start the FastAPI backend, validate `/api/system/status`, and open JavaFX:

```powershell
.\run_visual_test.ps1
.\run_visual_test.ps1 -PrepareDemo
.\run_visual_test.ps1 -PrepareDemo -SkipTests
.\run_visual_test.ps1 -NoFrontend
.\run_visual_test.ps1 -KeepBackendAlive
```

If Windows blocks script execution, use:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_visual_test.ps1 -PrepareDemo
```

Visual test logs are written to:

- `logs/backend-visual-test.log`
- `logs/frontend-visual-test.log`

See `docs/VISUAL_TEST_RUNNER.md` for parameters and empty-database troubleshooting.

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
- `instruments`
- `stock_prices_daily`
- `market_quotes_latest`
- `analysis_snapshots`

### Analytics Views

- `v_fx_daily`
- `v_fx_latest`
- `v_fx_monthly_avg`

Quick check:

```powershell
python -c "import sqlite3; conn = sqlite3.connect('data/fx.sqlite'); print(conn.execute('select count(*) from fx_rates').fetchone()[0]); conn.close()"
```

## Power BI / ODBC setup on Windows

1. Make sure the SQLite database exists by running a backfill or daily load:

```powershell
python -m fx_rates backfill --start 2026-02-01 --end 2026-02-03 --base USD --symbols BRL,EUR
```

2. Create or refresh the USER DSN:

```powershell
.\scripts\setup_sqlite_odbc_dsn.ps1
```

If the execution policy blocks the script, run it explicitly with Windows PowerShell:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_sqlite_odbc_dsn.ps1
```

3. Test the DSN:

```powershell
.\scripts\test_sqlite_odbc_dsn.ps1
```

If the execution policy blocks the script, run it explicitly with Windows PowerShell:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\test_sqlite_odbc_dsn.ps1
```

4. In Power BI Desktop, use `Home -> Get Data -> ODBC`, select `FX_SQLITE`, choose `fx_rates`, then select `Transform Data`.

Warning: do not select files from the SQLite ODBC driver installation folder such as `adddsn.exe`, `addsysdsn.exe`, `sqlite.exe`, or `sqlite3.exe`. The database file is the project file: `data\fx.sqlite`.

Suggested analytics views:

- `v_fx_daily` for daily line charts
- `v_fx_latest` for current-rate cards
- `v_fx_monthly_avg` for month-over-month summaries

### ODBC troubleshooting

- If no SQLite ODBC driver is detected, install the 64-bit SQLite ODBC driver and rerun `.\scripts\setup_sqlite_odbc_dsn.ps1`.
- If Power BI Desktop is 64-bit, the SQLite ODBC driver must also be 64-bit.
- If `.\scripts\test_sqlite_odbc_dsn.ps1` fails with "Cannot add type" or "assemblies are missing", run the test explicitly with Windows PowerShell:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\test_sqlite_odbc_dsn.ps1
```

- If `data\fx.sqlite` does not exist, run the project smoke command first:

```powershell
python -m fx_rates backfill --start 2026-02-01 --end 2026-02-03 --base USD --symbols BRL,EUR
```

- If the DSN already exists and points to the wrong file, rerun `.\scripts\setup_sqlite_odbc_dsn.ps1`.

## Verified Demo Run

Release hardening was completed with a fresh standard CPython virtual environment and the following outcomes:

- `pip install -r requirements.txt` completed successfully in `.release-venv`
- `pytest` passed: `26 passed`
- `backfill` completed successfully and reused the cached time-series payload for `2026-02-01..2026-02-03`
- `daily` completed successfully with `use_cache_latest=False`, fetched live data, and wrote `2` rows
- `status --last 5` showed the recent `backfill` and `daily` runs correctly
- SQLite contains data in `data/fx.sqlite`, including `fx_rates`, `ingest_runs`, and the analytics views
- `logs/app.log` was updated during the smoke run

At the end of the verified demo run, the local database contained:

- `fx_rates`: `8` rows
- `ingest_runs`: `9` rows

## Troubleshooting

### Data & Display Consistency

Finance Monitor uses SQLite as the local source of truth. Demo data is deterministic/local and is designed to be visually plausible for portfolio validation, but it is synthetic and is not financial advice.

Prepare and audit the dashboard before visual QA:

```powershell
python -m fx_rates dashboard prepare-demo --years 4 --demo
python -m fx_rates dashboard audit
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_visual_test.ps1 -PrepareDemo -SkipTests
```

The audit checks quote/history ratios, suspicious stock prices, non-positive FX/crypto values, missing macro units, duplicate instruments/quotes, and expected 4-year coverage. Dashboard API responses include display metadata such as `display_pair`, `display_unit`, `value_format`, `chart_title`, `axis_label`, and `tooltip_label` so charts can explicitly show USD, FX pair direction, crypto quote currency, and macro units.

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
