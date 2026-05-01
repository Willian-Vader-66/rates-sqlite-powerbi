# Java Front-End Execution Plan

## Current status

READY FOR LOCAL VALIDATION

The Java front-end structure exists under `frontend-java/`, but it cannot be marked READY until it is compiled, tested, and visually checked on a machine with Java 21 and Maven installed.

## What already exists

- Maven project in `frontend-java/`
- Java 21 compiler configuration
- JavaFX dependencies and JavaFX Maven plugin
- Jackson JSON parsing
- JUnit tests
- JavaFX entry point: `com.example.financedashboard.MainApp`
- HTTP-only API client using Java built-in `HttpClient`
- Config loading from bundled `application.properties`, optional `config/application.properties`, and `FINANCE_API_BASE_URL`
- Dashboard UI with metric cards, watchlist table, filters, details panel, history chart, polling, and friendly backend-offline banner
- Tests for API URL construction, JSON model parsing, config override behavior, and formatting utilities

## Environment requirements

- Java 21
- Maven 3.9+
- Python backend virtual environment already working
- Backend API available at `http://127.0.0.1:8000` or another URL configured through `FINANCE_API_BASE_URL`

## Backend validation steps

From Windows PowerShell:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi
.\.venv\Scripts\python.exe -m fx_rates serve --host 127.0.0.1 --port 8000
```

In a second PowerShell terminal:

```powershell
Invoke-WebRequest http://127.0.0.1:8000/health
```

Expected: HTTP 200 and a JSON response with `"status": "ok"`.

## Java validation steps

```powershell
java -version
mvn -v
cd C:\Projetos_Local\rates-sqlite-powerbi\frontend-java
mvn clean test
mvn -q -DskipTests compile
mvn javafx:run
```

You can also run the helper script:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi\frontend-java
.\scripts\verify_frontend.ps1
```

The helper script does not launch the JavaFX UI automatically; run `mvn javafx:run` manually after tests and compile pass.

## Expected success evidence

- `mvn clean test` passes.
- `mvn -q -DskipTests compile` passes.
- `mvn javafx:run` opens the desktop app.
- Backend status shows connected when the Python API is running.
- Dashboard summary cards load.
- Watchlist table loads.
- Filters work for search, asset type, signal, and active instruments.
- Selecting an instrument loads quote and analysis details.
- History chart renders.
- If the backend is stopped, the app remains open and shows the friendly backend-offline banner.

## API contract check

| Endpoint | Java usage | Status | Notes |
|---|---|---|---|
| `GET /health` | No direct app call | NOT USED YET | Connection status currently follows data endpoint success. Manual health check is part of validation. |
| `GET /api/dashboard/summary` | `MarketDataService.getDashboardSummary()` | MATCH | Maps to `DashboardSummary`. |
| `GET /api/instruments` | `getInstruments(assetType, active, search)` | MATCH | Uses `asset_type`, `active`, and `search`. |
| `GET /api/quotes/latest` | `getLatestQuotes()` | MATCH | Fetches all latest quotes and joins client-side. |
| `GET /api/analysis/latest` | `getLatestAnalysis()` | MATCH | Fetches all latest analysis snapshots and joins client-side. |
| `GET /api/stocks/history` | `getStockHistory(symbol, start, end)` | MATCH | Maps price history through `PricePoint.close`. |
| `GET /api/fx/history` | `getFxHistory("USD", symbol, start, end)` | MATCH | Maps FX history through `PricePoint.rate`; USD is the v1 default base. |

## P0 blockers

- None proven from static inspection.
- Any compile error from `mvn clean test` or `mvn -q -DskipTests compile` becomes P0.
- Any API/model mismatch that prevents dashboard loading becomes P0.

## P1 risks

- JavaFX UI has not been visually validated yet.
- `/health` is not called directly by the app.
- Empty backend data can make the dashboard appear sparse even when the app is functioning.
- Java 21 type inference for anonymous `TypeReference<>() {}` should compile, but Maven validation must confirm.

## P2 polish

- Replace the FXML placeholder with a full FXML layout later if desired.
- Improve chart axis density and tooltips.
- Add jlink or installer packaging.
- Add authentication for external deployment.
- Add WebSocket/SSE streaming if the backend supports it later.

## Fastest path to READY

1. Install Java 21 and Maven 3.9+.
2. Start the backend and verify `/health`.
3. Run `frontend-java/scripts/verify_frontend.ps1`.
4. Run `mvn javafx:run`.
5. Complete the visual checklist above.
6. If all pass, mark `JAVA FRONT-END STATUS: READY`.

## Recommended next Codex prompt

Use this only after running the local validation commands:

```text
Java 21 and Maven are installed. I ran the frontend validation commands from docs/JAVA_FRONTEND_EXECUTION_PLAN.md. Here is the output/error log: <paste output>. Please fix only the Java front-end issues needed to reach READY.
```
