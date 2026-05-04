# Finance Dashboard QA Checklist

## Backend commands

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi
.\.venv\Scripts\python.exe -m pytest -q
$env:MARKET_DATA_DEMO_MODE='true'
.\.venv\Scripts\python.exe -m fx_rates dashboard prepare-demo --years 4 --demo
.\.venv\Scripts\python.exe -m fx_rates dashboard audit
.\.venv\Scripts\python.exe -m fx_rates serve --host 127.0.0.1 --port 8000
```

## Prepare local dashboard data

PowerShell:

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

Expected after `prepare-demo`:

- `total_instruments >= 68`
- `active_stocks >= 30`
- `active_currencies >= 5`
- `active_crypto >= 5`
- `active_macro >= 1`
- `latest_quote_count > 0`
- `latest_analysis_count` close to `total_instruments`
- `instruments_without_analysis = 0`
- `instruments_without_quotes = 0`
- fixed charts have points for `USD/BRL`, `USD/EUR`, Bitcoin, Ethereum, and Selic
- top stocks returns at least 10 items
- `dashboard audit` reports `Alerts: none`

## Como resolver dashboard sem dados

If JavaFX shows `API connected` but all cards are empty, verify the SQLite path used by the running backend.

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

Checklist:

- compare the `db_path` from `/api/system/status` with the path printed by `dashboard audit`
- confirm `is_empty = false`
- confirm `total_instruments >= 50`
- confirm `latest_quote_count > 0`
- confirm `latest_analysis_count > 0`
- restart the backend after preparing data
- open JavaFX only after the backend prints the SQLite path and non-zero counts

## Component smoke commands

```powershell
.\.venv\Scripts\python.exe -m fx_rates stocks backfill --start 2026-01-01 --end 2026-01-31 --watchlist data/reference/sample_stocks.csv
.\.venv\Scripts\python.exe -m fx_rates crypto backfill --start 2026-01-01 --end 2026-01-31 --symbols BTC,ETH
.\.venv\Scripts\python.exe -m fx_rates macro backfill --start 2026-01-01 --end 2026-01-31
.\.venv\Scripts\python.exe -m fx_rates crypto quotes --symbols BTC,ETH
.\.venv\Scripts\python.exe -m fx_rates quotes poll --symbols AAPL,MSFT,NVDA --interval-seconds 5 --duration-minutes 0
.\.venv\Scripts\python.exe -m fx_rates analyze now --asset-type STOCK
```

## API checks

Run these while the backend is serving:

```powershell
Invoke-WebRequest http://127.0.0.1:8000/health
Invoke-WebRequest http://127.0.0.1:8000/api/dashboard/summary
Invoke-WebRequest http://127.0.0.1:8000/api/dashboard/market-overview
Invoke-WebRequest http://127.0.0.1:8000/api/dashboard/fixed-charts
Invoke-WebRequest http://127.0.0.1:8000/api/dashboard/top-stocks-30d
Invoke-WebRequest "http://127.0.0.1:8000/api/crypto/history?symbol=BTC"
Invoke-WebRequest "http://127.0.0.1:8000/api/macro/history?indicator_code=SELIC_DAILY"
```

## Frontend commands

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi\frontend-java
mvn clean test
mvn -q -DskipTests compile
mvn javafx:run
```

## Visual checklist

- app opens after splash
- backend status shows connected when API is running
- backend offline banner is friendly when API is stopped
- refresh does not flicker or clear previous data
- Overview tab shows summary metrics
- fixed chart cards render or show clear empty states
- empty chart cards show `No data loaded. Run: python -m fx_rates dashboard prepare-demo --years 4 --demo`
- top stocks panel renders or shows a clear empty state
- Watchlist tab still loads instruments, quotes, analysis, and filters
- selecting a stock loads the historical chart
- selecting crypto loads crypto history when present
- selecting macro loads macro history when present
- selected instrument chart offers 30D, 90D, 1Y, and 4Y
- 4Y selected history shows coverage around 2022-05 to 2026-05
- selecting the same instrument repeatedly does not spam duplicate history calls
- most rows show useful trend/signal rather than raw UNKNOWN
- chart gridlines are subtle and readable

## Screenshot checklist

- capture Overview with populated demo data
- capture Watchlist with filters visible
- capture a selected instrument chart
- capture backend offline state
- avoid screenshots with API keys, `.env`, local secrets, or absolute personal folders

## LinkedIn portfolio checklist

- describe architecture: ingestion -> SQLite -> FastAPI -> JavaFX
- mention demo mode works without API keys
- mention near-real-time polling, not trading-grade ticks
- mention provider rate limits are respected
- include one clean dashboard screenshot
