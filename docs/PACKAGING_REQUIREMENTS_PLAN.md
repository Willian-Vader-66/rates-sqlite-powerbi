# Finance Monitor - Future Packaging Requirements Plan

Generated: 2026-05-10

## Goal

Prepare a future Windows distribution with a single launcher for the local Finance Monitor application.

The launcher should:

- Start the local Python backend automatically.
- Wait until `/health` responds successfully.
- Open the JavaFX frontend.
- Stop the backend when the frontend closes.
- Use a local SQLite database.
- Support future activation or license checks.

Packaging is not implemented in this phase. This document only records requirements, risks, and a likely build direction.

## Proposed Runtime Shape

Recommended local data locations:

- App binaries: installation folder under `Program Files` or a user-local application folder.
- Data: `%LOCALAPPDATA%\FinanceMonitor\data`.
- Config: `%LOCALAPPDATA%\FinanceMonitor\config`.
- Logs: `%LOCALAPPDATA%\FinanceMonitor\logs`.

The packaged app should not write mutable data into the installation folder.

## Backend Packaging Options

Candidate tools:

- PyInstaller.
- Nuitka.

Backend package should include:

- `fx_rates` package.
- FastAPI/Uvicorn runtime dependencies.
- Any provider modules used by ingestion.
- Configuration defaults.
- A predictable SQLite database path.

Risks:

- Hidden imports for Uvicorn/FastAPI.
- Native dependency discovery.
- Antivirus false positives with single-file executables.
- Startup time for one-file extraction modes.
- Clear handling of logs and database path outside the executable.

## JavaFX Packaging Options

Candidate tools:

- `jlink` for a custom Java runtime.
- `jpackage` for a Windows installer or app image.

Java package should include:

- JavaFX modules.
- Application jar.
- Custom Java runtime based on Java 21.
- Frontend configuration for the local API base URL.

Risks:

- JavaFX native modules must match platform and architecture.
- Maven build must be reproducible.
- The app must handle backend startup timing and API failures gracefully.

## Launcher Options

Candidate launcher approaches:

- PowerShell launcher.
- Java launcher that owns backend process lifecycle.
- Small native launcher.

Launcher responsibilities:

- Resolve application directories.
- Create data/config/log directories if missing.
- Start backend on a local port.
- Poll `http://127.0.0.1:<port>/health`.
- Start JavaFX after backend readiness.
- Capture backend and frontend logs.
- Stop backend after frontend closes.
- Avoid orphan processes.
- Detect port conflicts and report clear errors.

## Build Script Requirements

A future reproducible build should:

- Clean previous generated build output.
- Run Python tests.
- Run Java/Maven tests.
- Prepare backend executable.
- Build JavaFX runtime/image.
- Assemble launcher and assets.
- Smoke-test `/health`.
- Produce a versioned artifact.
- Keep build output outside versioned source folders or ensure it is ignored.

## Licensing/Activation Placeholder

Future activation may require:

- License file or signed token.
- Device binding rules.
- Offline grace period.
- Backend and frontend validation path.
- Clear failure mode when activation is missing or expired.

This is intentionally out of scope for the current reinstall/setup work.

## Current Readiness

Packaging readiness is blocked until the local environment is ready:

- Git installed.
- Python environment restored.
- Backend tests passing.
- Demo SQLite data prepared.
- API validation passing.
- Java 21 installed.
- Maven tests and JavaFX compile passing.
- Visual runner passing.
