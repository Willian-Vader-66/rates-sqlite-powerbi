# Java Front-End UI Redesign and Stabilization Plan

## Current status

The JavaFX app opens, but the current dashboard is too plain/light, shows a backend-unavailable banner when it cannot reach the Python API, and has reported flickering during refresh. The table and chart can appear empty when the backend is offline or the database has no loaded market data. The UI needs stronger filters, a cyberpunk/neon financial dashboard style, and an original startup splash/logo.

## Diagnosis checklist

- Backend/API: verify `FINANCE_API_BASE_URL`, `application.properties`, `/health`, and all API paths in `MarketDataService`.
- Refresh loop: confirm polling interval, request timeout, no overlapping refreshes, and no destructive data clearing on failures.
- JavaFX threading: keep network calls in `Task` worker threads and UI mutations in `Task` success/failure handlers.
- Filters: keep filtering client-side using loaded data; do not re-fetch for each filter change.
- CSS: centralize cyberpunk styling in `styles/app.css`.
- Splash: use an original Finance Monitor geometric neon logo; no Umbrella Corporation name, logo, assets, or copied identity.

## P0 blockers

- Java compile failures.
- Backend endpoint mismatch that prevents dashboard loading.
- API model mismatch that prevents JSON parsing.
- App startup crash.
- Refresh failures that crash the JavaFX Application Thread.

## P1 fixes

- Improve backend diagnostic messages with attempted URL and error type.
- Increase default refresh interval to 30 seconds.
- Increase default HTTP timeout to 30 seconds.
- Keep last known good data on failed refresh.
- Avoid overlapping refreshes.
- Add manual Refresh and Pause/Resume auto-refresh controls.
- Avoid showing a full-screen loading overlay on every background refresh.

## P2 UI enhancements

- Cyberpunk/neon visual theme.
- Original animated splash screen.
- Filter chips for asset, signal, and trend.
- Exchange and sector dropdown filters.
- Improved table, chart, and detail-panel styling.
- Later packaging with jlink or installer.

## Implementation sequence

1. Backend communication diagnostics and optional `/health` validation from the local shell.
2. Refresh/flicker stabilization with non-overlapping refreshes, 30s interval, 30s timeout, stale-data preservation, manual refresh, and pause/resume.
3. Filter controls: search, asset chips, signal chips, trend chips, exchange dropdown, sector dropdown, active toggle, clear filters.
4. Cyberpunk CSS redesign with dark panels, neon accents, readable typography, and non-flashing effects.
5. Original splash screen with a short Finance Monitor logo animation.
6. Manual validation and Maven test/compile on a machine with Java 21 and Maven.

## Acceptance criteria

- `mvn clean test` passes.
- `mvn -q -DskipTests compile` passes.
- `mvn javafx:run` opens the app.
- App shows friendly backend offline banner if backend is stopped.
- App connects when backend is running.
- No visible flickering during background refresh.
- Refresh interval defaults to 30 seconds.
- HTTP timeout defaults to 30 seconds.
- Manual refresh works.
- Pause/resume auto-refresh works.
- Filters work client-side.
- Splash screen appears and transitions to dashboard.
- No direct SQLite access.
- No use of Umbrella Corporation name, logo, assets, or copied visual identity.

## Manual test steps

Terminal 1:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi
.\.venv\Scripts\python.exe -m fx_rates serve --host 127.0.0.1 --port 8000
```

Terminal 2:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi\frontend-java
mvn clean test
mvn -q -DskipTests compile
mvn javafx:run
```

Offline test:

- Stop the backend.
- The app should keep the last loaded data, show the offline banner, and avoid flickering.

Refresh test:

- Click Refresh.
- Click Pause.
- Confirm scheduled refresh stops.
- Click Resume.
- Confirm scheduled refresh resumes.
