# Finance Dashboard JavaFX Front-End

Local desktop dashboard for the `rates-sqlite-powerbi` financial market data backend.

The app consumes the Python backend over HTTP. It does not connect to SQLite and does not read `data/fx.sqlite` directly.

## Prerequisites

- Java 21
- Maven
- Python backend dependencies installed
- Backend API running locally or reachable through a configured URL

## Start the Backend

From the repository root:

```powershell
python -m fx_rates serve --host 127.0.0.1 --port 8000
```

Default backend URL:

```text
http://127.0.0.1:8000
```

## Run the Java App

From this directory:

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
- Dashboard summary cards for total instruments, active stocks, active FX, active crypto, macro indicators, latest quotes, failed runs, and last successful ingest.
- Watchlist table combining instruments, latest quotes, and latest analysis snapshots.
- Filters for search, asset type, signal, trend, exchange, sector, and active instruments.
- Instrument detail panel with latest quote, analysis snapshot, and historical line chart.
- Background polling every `refresh.interval.seconds`, defaulting to 30 seconds.
- HTTP requests use `http.timeout.seconds`, defaulting to 30 seconds.
- Manual refresh and pause/resume controls are available in the dashboard.
- Friendly offline banner if the backend is unavailable.
- API base URL centralized through `AppConfig`, so future external access only requires configuration changes.

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

The backend may be running with an empty SQLite database. Seed data in demo mode:

```powershell
$env:MARKET_DATA_DEMO_MODE='true'
python -m fx_rates instruments import --file data/reference/sample_stocks.csv
python -m fx_rates stocks backfill --start 2026-01-01 --end 2026-01-10 --watchlist data/reference/sample_stocks.csv
python -m fx_rates crypto backfill --start 2026-01-01 --end 2026-01-10 --symbols BTC,ETH
python -m fx_rates macro backfill --start 2026-01-01 --end 2026-01-10
python -m fx_rates crypto quotes --symbols BTC,ETH
python -m fx_rates quotes poll --symbols AAPL,MSFT,NVDA --interval-seconds 5 --duration-minutes 0
python -m fx_rates analyze now --symbols AAPL,MSFT,NVDA
```

## Packaging Notes

The current development path is:

```powershell
mvn clean javafx:run
```

Future packaging can add `jlink` or installer generation once distribution requirements are clearer.
