# Finance Monitor Visual Test Runner

`run_visual_test.ps1` starts the local FastAPI backend, validates dashboard readiness, and optionally opens the JavaFX frontend from the repository root.

Run from the repo root:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi
.\run_visual_test.ps1
```

If Windows blocks the script through execution policy, run:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_visual_test.ps1 -PrepareDemo
```

## Common Commands

Prepare demo data, run tests, start backend, and open JavaFX:

```powershell
.\run_visual_test.ps1 -PrepareDemo
```

Prepare demo data but skip Python/Maven tests:

```powershell
.\run_visual_test.ps1 -PrepareDemo -SkipTests
```

Start only the backend and validate the API:

```powershell
.\run_visual_test.ps1 -NoFrontend
```

Keep the backend running after closing JavaFX:

```powershell
.\run_visual_test.ps1 -KeepBackendAlive
```

Use a custom host/port:

```powershell
.\run_visual_test.ps1 -HostAddress 127.0.0.1 -Port 8000
```

## Visual Checks Before Publishing

After JavaFX opens, verify:

- Overview has metrics, market cards, fixed charts, and Top 10 Companies.
- Markets has cross-asset cards, Market Snapshot, and positive/negative 30D rankings.
- Stocks has equity cards, a populated stock table, and stock momentum charts.
- FX & Crypto has FX/crypto cards, table, and USD/BRL, USD/EUR, BTC/USD, ETH/USD charts.
- Macro has macro cards, indicator table, and macro chart/insufficient-history messaging.
- Watchlist range selector visibly shows `1Y` by default and options `30D`, `90D`, `180D`, `1Y`, `365D`.
- Watchlist and selected chart show explicit display units: `AAPL/USD`, `USD/BRL`, `BTC/USD`, and macro units such as `% a.a.`.
- AAPL and other stock prices are plausible USD values, not values in the millions.
- Settings shows API URL, DB path, DB size, historical rows, and date range.

## Parameters

- `-PrepareDemo`: runs `.\.venv\Scripts\python.exe -m fx_rates dashboard prepare-demo --years 1 --demo`.
- `-SkipTests`: skips `pytest`, `mvn clean test`, and `mvn -q -DskipTests compile`.
- `-Port`: backend port, default `8000`.
- `-HostAddress`: backend host, default `127.0.0.1`.
- `-KeepBackendAlive`: leaves the backend process started by the script running after JavaFX exits.
- `-NoFrontend`: starts and validates only the backend.

## Logs

Logs are written to:

```text
logs\backend-visual-test.log
logs\frontend-visual-test.log
```

The script only stops the backend PID that it started. It does not kill all Python or Java processes.

## Empty Database Diagnosis

If the backend responds but the dashboard database is empty, the script prints:

```powershell
Backend is running, but dashboard database is empty.
Run:
.\run_visual_test.ps1 -PrepareDemo
```

You can also prepare data manually:

```powershell
.\.venv\Scripts\python.exe -m fx_rates dashboard prepare-demo --years 1 --demo
```

Then validate:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/system/status
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/summary
```

For a data/display consistency gate, run:

```powershell
.\.venv\Scripts\python.exe -m fx_rates dashboard audit
```

Expected result after demo preparation: `Suspicious values: 0` and `Alerts: none`.
