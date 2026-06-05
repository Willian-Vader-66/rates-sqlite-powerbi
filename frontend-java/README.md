# Finance Dashboard JavaFX Front-End

Local desktop dashboard for the `rates-sqlite-powerbi` financial market data backend.

The app consumes the Python backend over HTTP. It does not connect to SQLite and does not read `data/fx.sqlite` directly.

## Prerequisites

- Java 21
- Maven
- Python backend dependencies installed
- Backend API running locally or reachable through a configured URL

## Recommended Start

From the repository root, start the product with:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_finance_monitor.ps1
```

This opens the JavaFX app and the `Control Center` tab. PowerShell is only the bootstrap step; backend start/stop, provider validation, key entry, LIVE 365D pipeline steps, candidate DB validation, promotion, reports, and logs are operated in JavaFX.

Use `-StartBackend` if you want the script to start the local API before opening JavaFX:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_finance_monitor.ps1 -StartBackend
```

Use `-CheckOnly` to validate local paths and tools without opening the UI or touching SQLite.

## Manual Backend Start

From the repository root:

```powershell
python -m fx_rates serve --host 127.0.0.1 --port 8000
```

Default backend URL:

```text
http://127.0.0.1:8000
```

## Run the Java App

For direct frontend development from this directory:

```powershell
cd frontend-java
mvn clean javafx:run
```

Run tests:

```powershell
mvn clean test
```

## Configuration

The bundled default lives in:

```text
src/main/resources/application.properties
```

Defaults:

```properties
api.base.url=http://127.0.0.1:8000
refresh.interval.seconds=30
http.timeout.seconds=30
history.lookback.days=120
```

Override the API URL for another local or remote backend with either option:

```powershell
$env:FINANCE_API_BASE_URL='https://finance-api.example.com'
mvn javafx:run
```

or create:

```text
config/application.properties
```

with:

```properties
api.base.url=https://finance-api.example.com
refresh.interval.seconds=30
http.timeout.seconds=30
```

The environment variable `FINANCE_API_BASE_URL` takes precedence over the properties file.

## Features

- Overview tab with summary cards, market overview cards, fixed 30-day mini charts, and top company performance.
- Navigation structure for Overview, Markets, Stocks, FX & Crypto, Macro, Watchlist, Settings, and Control Center.
- Control Center for System Status, Data Mode, Secret Keys, Providers, Live 365D Pipeline, Local API Server, Database, Reports & Logs, and future Advanced History.
- Dashboard summary cards for total instruments, active stocks, active FX, active crypto, macro indicators, latest quotes, failed runs, and last successful ingest.
- Watchlist table combining instruments, latest quotes, and latest analysis snapshots.
- Filters for search, asset type, signal, trend, exchange, sector, and active instruments.
- Instrument detail panel with latest quote, analysis snapshot, data coverage, and an interactive historical chart.
- Chart ranges: 7D, 30D, 90D, 180D, and 365D for the current standard scope. 3Y, 5Y, and 10Y remain disabled/future advanced history.
- Hover tooltip, nearest-point crosshair, last-value marker, and client-side downsampling for long ranges.
- Settings page showing API base URL, timeout, refresh interval, DB path, DB size, historical rows, and date coverage from `/api/system/status`.
- Background polling every `refresh.interval.seconds`, defaulting to 30 seconds.
- HTTP requests use `http.timeout.seconds`, defaulting to 30 seconds.
- Manual refresh and pause/resume controls are available in the dashboard.
- Friendly offline banner if the backend is unavailable.
- Control Center can start the local backend when the dashboard is offline.
- API base URL centralized through `AppConfig`, so future external access only requires configuration changes.

## Control Center Security

- `TWELVE_DATA_API_KEY` is accepted through a password field and kept in JavaFX memory for the current session only.
- The key is passed to child Python commands through process environment variables.
- The key is not saved to `.env`, docs, reports, logs, or committed files.
- Logs shown in the UI are redacted and display only masked previews.
- LIVE promotion is locked until promotion dry-run passes and the user confirms manually.
- The JavaFX frontend does not fetch market data from external APIs directly; it calls the Python CLI/API.

## Screenshots

Placeholder for screenshots:

- Main dashboard
- Watchlist filters
- Instrument detail chart
- Backend offline state

## Troubleshooting

### Backend offline

Start the backend first:

```powershell
python -m fx_rates serve --host 127.0.0.1 --port 8000
```

The Java app stays open and retries on the next polling interval.

### Wrong API base URL

Check `FINANCE_API_BASE_URL` and `config/application.properties`. The Java app expects the API contract documented in `../docs/API_CONTRACT.md`.

### CORS

CORS is not relevant for this JavaFX desktop app because it uses Java's built-in `HttpClient`, not a browser.

### Empty dashboard

The backend may be running with an empty SQLite database or a different DB path. Prefer the Control Center Live 365D Pipeline. The equivalent CLI commands remain:

```powershell
python -m fx_rates providers status --external-test
python -m fx_rates dashboard build-live-db --days 365 --db-path .tmp/live-main-candidate.sqlite --external-test
python -m fx_rates dashboard validate-samples --db-path .tmp/live-main-candidate.sqlite --samples-per-symbol 5 --external-test
python -m fx_rates dashboard audit-live --db-path .tmp/live-main-candidate.sqlite
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --dry-run
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --to-db data/fx.sqlite --backup
Invoke-RestMethod http://127.0.0.1:8000/api/system/status
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/summary
```

## Packaging Notes

The current development path is:

```powershell
mvn clean javafx:run
```

Future packaging can add `jlink` or installer generation once distribution requirements are clearer.
