# fx-rates-sqlite-powerbi

Finance Monitor is a LIVE-FIRST local financial dashboard: it builds a validated SQLite database with the last 365 days of real FX, stock, crypto, and macro history, refreshes recent data incrementally, validates samples against external providers, and exposes a local HTTP API for the JavaFX frontend. Advanced history up to 10 years is reserved for paid providers that explicitly support longer ranges.

## What This Project Is

This repository started as a small but production-minded local pipeline for foreign exchange history and now includes a broader market-data backend:

- `backfill` downloads a date range
- `daily` fetches the latest available business day
- `stocks backfill` and `stocks daily` ingest daily stock history from an editable watchlist
- `crypto backfill`, `crypto daily`, and `crypto quotes` add crypto history and latest quotes
- `macro backfill`, `macro daily`, and `macro status` add macro indicators such as Selic
- `quotes poll` refreshes latest quotes through safe polling for selected symbols
- `analyze now` creates stock, FX, crypto, and macro analysis snapshots from data already stored in SQLite
- `dashboard build-live-db` creates a fresh real-data staging database for the JavaFX Finance Monitor
- `dashboard refresh-live` incrementally updates an existing live SQLite database
- `dashboard validate-samples` checks stored values against provider samples
- `dashboard audit-live` proves the candidate DB is coherent before promotion
- `dashboard promote-live` safely promotes a validated staging DB to `data/fx.sqlite` with backup
- `dashboard prepare-demo` remains available only for dev/test/offline work
- `dashboard audit` reports dashboard readiness, coverage, missing quotes/analysis, and duplicate instrument keys
- `serve` exposes a local HTTP API for Java or other front-ends
- backfill/time-series responses are cached on disk
- daily/latest fetches fresh data by default
- normalized rows are persisted into SQLite
- each ingestion run is tracked in `ingest_runs`
- logs are written to console and `logs/app.log`

This is not a professional trading platform. Quote collection is near-real-time polling, not a tick-by-tick feed, and provider rate limits should be respected.

## Live-First Data Modes

Market-data origin is explicit:

- `demo`: deterministic mock data for local demos and offline validation.
- `live`: data fetched from configured real providers.
- `mixed`: demo and live records are both present.
- `unknown`: origin cannot be trusted yet.

## Recommended Local Operation

Start the local product with one PowerShell command:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_finance_monitor.ps1
```

This opens the JavaFX Finance Monitor with a `Control Center` tab. After startup, use the JavaFX UI to enter the Twelve Data key for the current session, validate providers, run the LIVE 365D pipeline, inspect reports/logs, start or stop the backend, audit databases, and manually promote a candidate database.

Useful startup options:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_finance_monitor.ps1 -CheckOnly
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_finance_monitor.ps1 -StartBackend
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_finance_monitor.ps1 -LiveMode
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_finance_monitor.ps1 -DemoMode
```

`-CheckOnly` validates paths and tools without opening the UI, asking for secrets, starting the backend, building data, or touching SQLite.

The Control Center keeps `TWELVE_DATA_API_KEY` only in JavaFX memory and passes it to child Python commands through environment variables. It does not save the key to `.env`, docs, reports, logs, or committed files. Promotion is never automatic: Step 8 requires a passed dry-run plus manual confirmation and creates a backup.

If the app is launched from a PowerShell session that already has `TWELVE_DATA_API_KEY`, the Control Center recognizes that environment key without filling the password field or showing the value. The provider table and pipeline badges share provider-status parsing, so Twelve Data `external_test=pass` is shown as Provider Validation passed, not as a missing secret.

Pipeline status badges distinguish operational blockers from data quality failures: Passed, Passed with Warnings, Failed, Blocked by Missing Secret, Blocked by Provider/TLS, Ready for Dry Run, and Ready for Promotion. A missing or implausible Twelve Data key blocks stock validation before any provider call. TLS/CA or provider connectivity failures are shown separately so the user can tell environment issues from candidate DB defects.

Build and validate the live candidate database first:

```powershell
python -m fx_rates providers status --external-test
python -m fx_rates dashboard build-live-db --days 365 --db-path .tmp/live-main-candidate.sqlite --external-test
python -m fx_rates dashboard validate-samples --db-path .tmp/live-main-candidate.sqlite --samples-per-symbol 5 --external-test
python -m fx_rates dashboard audit-live --db-path .tmp/live-main-candidate.sqlite
python -m fx_rates api smoke-live --db-path .tmp/live-main-candidate.sqlite --port 8001
```

`validate-samples` separates internal DB checks from external provider confirmation. A clean run returns `READY`; provider rate limit or transient external validation gaps return `READY_WITH_WARNINGS` only when the internal candidate, audit-live, and API smoke-live gates are clean and no required key is missing. Data problems, missing keys, failed audit/smoke gates, non-live/mixed history, invalid `data_health`, or critical quote/history divergence return `NOT_READY` or `FAIL`.

Check providers and audits:

```powershell
python -m fx_rates providers status
python -m fx_rates dashboard audit
python -m fx_rates dashboard audit-market
```

Promote only after the candidate DB passes validation:

```powershell
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --dry-run
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --to-db data/fx.sqlite --backup
```

Demo data can still be created explicitly for tests and local UI development, but it is not the product mode. See `docs/VISUAL_CONTROL_CENTER.md`, `docs/LIVE_DATA_SCOPE.md`, `docs/LIVE_FIRST_PRODUCT_SCOPE.md`, `docs/LIVE_FULL_TEST_PLAN.md`, and `docs/LIVE_PROMOTION_GUIDE.md`.

## Architecture

```text
Frankfurter API -> FX ingest CLI -----------+
                                            |
Twelve Data API -> stock/quote providers -> Python backend -> SQLite
CoinGecko API  -> crypto provider ----------+        |
BCB SGS API    -> macro provider -----------+        |
Test providers -> dev/test data ------------+        |
                                                     v
                                          FastAPI local HTTP API
                                                     |
                                                     v
                                      JavaFX Finance Monitor / dashboards
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
COINGECKO_API_PLAN=public
COINGECKO_DEMO_API_KEY=
COINGECKO_PRO_API_KEY=
MARKET_DATA_PROVIDER=twelvedata
MARKET_DATA_DEMO_MODE=false
API_HOST=127.0.0.1
API_PORT=8000
```

CLI flags override `.env` values for the same setting.

For stock data, create a Twelve Data API key and set `TWELVE_DATA_API_KEY` in your shell environment. Crypto data uses CoinGecko public access by default; if you use a CoinGecko demo or pro key, set `COINGECKO_API_PLAN` plus the matching key environment variable. Macro data uses Banco Central SGS. Do not store API keys in committed files, committed `.env` files, docs, logs, or reports.

If provider checks fail because of Windows TLS/CA validation, run the environment doctor before rebuilding the live DB:

```powershell
python -m fx_rates env doctor
```

Optional Windows trust store support is available through:

```powershell
$env:FX_RATES_USE_TRUSTSTORE="1"
python -m pip install --upgrade certifi truststore
```

For a local PowerShell setup that configures certifi/truststore and asks for the Twelve Data key without writing it to disk, prefer the one-shot pipeline:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_live_pipeline.ps1
```

`run_live_pipeline.ps1` prompts for `TWELVE_DATA_API_KEY` with hidden input, keeps TLS settings and the key in the same PowerShell process, runs provider checks, builds the staging DB, validates samples, audits, runs API smoke, and finishes with promotion dry-run only. It prints only `present`, `key_length`, and a masked preview such as `abcd****`.

If the key appeared in a screenshot, terminal transcript, log, or shell history, rotate it in the Twelve Data dashboard before running release validation again.

If you only want to configure the current terminal session, dot-source the setup script:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi-git
Set-ExecutionPolicy -Scope Process Bypass -Force
. .\scripts\setup_live_env.ps1
```

Running the setup script with `powershell.exe -File` is valid for the commands inside that script, but variables created there do not persist back to the original terminal:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_live_env.ps1
```

See `docs/POWERSHELL_SESSION_GUIDE.md` for details.

Manual live validation is also valid, but the key must be set in the same terminal session that runs the commands:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi-git
$secure = Read-Host "Paste Twelve Data API key" -AsSecureString
$bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
try { $env:TWELVE_DATA_API_KEY = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) } finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }

.\.venv\Scripts\python.exe -m fx_rates providers status --external-test
.\.venv\Scripts\python.exe -m fx_rates dashboard validate-samples --db-path .tmp\live-main-candidate.sqlite --samples-per-symbol 5 --external-test
.\.venv\Scripts\python.exe -m fx_rates dashboard promote-live --candidate-db .tmp\live-main-candidate.sqlite --dry-run
```

For automated tests, offline local development, and UI experiments without external API access, set:

```dotenv
MARKET_DATA_DEMO_MODE=true
```

Demo mode and `--demo` use deterministic synthetic data.
It is not real market history and must not be used as the final product database.
Stocks use `MockMarketDataProvider`.
Macro and crypto demo data come from `MockMacroProvider` and `MockCryptoProvider`; the Java UI never hardcodes fake market data.

Dependency note: `httpx` is listed with the main requirements because FastAPI's `TestClient` needs it and this project does not yet split runtime and development dependency groups.

## CLI Commands

### Como Rodar Com Dados Reais

Use this flow before opening the JavaFX Finance Monitor for release or portfolio use. It creates a staging SQLite database with real data, validates it, then promotes it to the main local DB only after checks pass.

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi-git
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_live_pipeline.ps1
.\.venv\Scripts\python.exe -m fx_rates dashboard promote-live --candidate-db .tmp\live-main-candidate.sqlite --to-db data\fx.sqlite --backup
.\.venv\Scripts\python.exe -m fx_rates serve --host 127.0.0.1 --port 8000
```

In another terminal:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi-git\frontend-java
mvn javafx:run
```

Validate the API:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/system/status
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/summary
```

The default live-first scope is intentionally small and reliable: USD/BRL, USD/EUR, USD/GBP, USD/JPY, USD/CAD, USD/CHF; BTC, ETH, SOL, BNB, XRP; AAPL, MSFT, NVDA, AMZN, GOOGL, META, TSLA, JPM, KO, AMD; SELIC_DAILY, CDI_DAILY, and IPCA_MONTHLY. The versioned release scope lives in `data/reference/live_release_scope.csv`.

`IPCA_MONTHLY` is a monthly macro series. It is not expected to have 365 points or to publish up to the current daily market date. The LIVE 365D gate validates it by monthly frequency, minimum monthly point count, valid values, no future dates, duplicate checks, and a freshness window of 75 days. A latest monthly value inside that window is OK even when its calendar range is shorter than daily FX/stock/crypto ranges.

Refresh recent live data after promotion:

```powershell
python -m fx_rates dashboard refresh-live --db-path data/fx.sqlite
python -m fx_rates dashboard refresh-live --db-path data/fx.sqlite --dry-run
```

`prepare-demo` remains available for automated tests and local UI experiments only.

### Como resolver dashboard sem dados

If JavaFX connects to the API but the dashboard is empty, confirm that `build-live-db`, `promote-live`, and `serve` are using the expected SQLite files. Relative `DB_PATH` values from `.env`, such as `data/fx.sqlite`, are resolved from the project root; explicit `--db-path` values are honored by each command.

PowerShell:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi-git
.\.venv\Scripts\Activate.ps1
python -m fx_rates dashboard build-live-db --days 365 --db-path .tmp/live-main-candidate.sqlite --external-test
python -m fx_rates dashboard audit-live --db-path .tmp/live-main-candidate.sqlite
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --to-db data/fx.sqlite --backup
python -m fx_rates serve --host 127.0.0.1 --port 8000
```

In another terminal:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/system/status
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/summary
```

Check that `/api/system/status.db_path` matches the promoted `data/fx.sqlite`. If `is_empty` is `true`, rerun the live staging flow, promote only after checks pass, restart the backend, and check the same endpoint again.

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

- populated summary and market overview cards after live DB promotion
- dedicated cross-asset Markets, Stocks, FX & Crypto, and Macro views
- fixed 30-day overview mini charts
- Top 10 Companies table with financial formatting
- Watchlist filters and selected instrument details
- interactive charts with 7D, 30D, 90D, 180D, and 365D ranges
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

## Verified Local Smoke Run

Release hardening was completed with a fresh standard CPython virtual environment and the following outcomes:

- `pip install -r requirements.txt` completed successfully in `.release-venv`
- `pytest` passed: `26 passed`
- `backfill` completed successfully and reused the cached time-series payload for `2026-02-01..2026-02-03`
- `daily` completed successfully with `use_cache_latest=False`, fetched live data, and wrote `2` rows
- `status --last 5` showed the recent `backfill` and `daily` runs correctly
- SQLite contains data in `data/fx.sqlite`, including `fx_rates`, `ingest_runs`, and the analytics views
- `logs/app.log` was updated during the smoke run

At the end of the verified smoke run, the local database contained:

- `fx_rates`: `8` rows
- `ingest_runs`: `9` rows

## Troubleshooting

### Data & Display Consistency

Finance Monitor uses SQLite as the local source of truth. For product use, the database should be promoted from a validated live candidate. Demo data is deterministic/local and is limited to tests and development.

Build, validate, and audit the live dashboard before visual QA:

```powershell
python -m fx_rates dashboard build-live-db --days 365 --db-path .tmp/live-main-candidate.sqlite --external-test
python -m fx_rates dashboard validate-samples --db-path .tmp/live-main-candidate.sqlite --samples-per-symbol 5 --external-test
python -m fx_rates dashboard audit-live --db-path .tmp/live-main-candidate.sqlite
python -m fx_rates api smoke-live --db-path .tmp/live-main-candidate.sqlite --port 8001
```

The audit checks quote/history ratios, suspicious stock prices, non-positive FX/crypto values, missing macro units, duplicate instruments/quotes, and expected 365-day coverage. Dashboard API responses include display metadata such as `display_pair`, `display_unit`, `value_format`, `chart_title`, `axis_label`, and `tooltip_label` so charts can explicitly show USD, FX pair direction, crypto quote currency, and macro units.

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

### Safe Live Ingestion Flow

`prepare-live` uses a two-phase flow. Phase A validates provider configuration, rejects placeholder API keys, fetches all requested live history, checks latest quotes against history, and keeps everything staged outside the permanent dataset. Phase B opens a SQLite transaction and only then applies `--replace-demo`, inserts live rows, writes latest quotes, analysis snapshots, and the ingest result.

This means `--replace-demo` never deletes demo/local rows before the live fetch is complete and validated. If Twelve Data returns an invalid key error, a rate-limit response, an unsupported symbol, or an incomplete payload, the command aborts before DB mutation and existing data stays in place.

Useful commands:

```powershell
python -m fx_rates providers status --external-test
python -m fx_rates dashboard prepare-live --years 1 --asset-type STOCK --symbols AAPL,MSFT,NVDA --db-path .tmp/live-stock-test.sqlite
python -m fx_rates dashboard audit --db-path .tmp/live-stock-test.sqlite
python -m fx_rates dashboard audit-market --db-path .tmp/live-stock-test.sqlite
python -m fx_rates dashboard prepare-live --years 1 --asset-type STOCK --symbols AAPL,MSFT,NVDA --replace-demo
python -m fx_rates dashboard audit
python -m fx_rates dashboard audit-market
```

Use only environment variables for provider credentials: `TWELVE_DATA_API_KEY`, `COINGECKO_DEMO_API_KEY`, `COINGECKO_PRO_API_KEY`, and any future provider key. Do not place API keys in source files, docs, `.env` committed to Git, logs, reports, or command output.

Do not commit `data/*.sqlite`; SQLite files are runtime state, not source code.

