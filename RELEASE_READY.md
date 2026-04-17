# Release Readiness

## Final status
READY

## What was verified
- clean environment install in a fresh standard CPython 3.11 virtual environment created at `.release-venv`
- dependency installation with `pip install -r requirements.txt`
- tests with the fresh environment: `18 passed`
- CLI commands:
  - `python -m fx_rates backfill --start 2026-02-01 --end 2026-02-03 --base USD --symbols BRL,EUR`
  - `python -m fx_rates daily --base USD --symbols BRL,EUR`
  - `python -m fx_rates status --last 5`
- database creation and persistence in `data/fx.sqlite`
- logging to `logs/app.log`
- `ingest_runs` audit tracking for successful runs
- cache behavior:
  - backfill reused cached time-series data
  - daily ran with `use_cache_latest=False`, so latest data was fetched fresh by default
- README consistency against the verified setup, test, and smoke commands

## What changed in this hardening pass
- verified the project end-to-end in a fresh standard CPython environment instead of relying on the older local `.venv`
- hardened README to reflect only verified setup, test, and smoke-run commands, including the live-network requirement for `daily`
- added a short verified demo section with real outcomes from the release smoke run
- de-scoped Power BI screenshot evidence for v1 and removed the placeholder asset/reference
- updated `.gitignore` to cover local release-verification artifacts such as `.release-venv` and temporary verification directories

## What was intentionally deferred
- simplifying the noisy `status --help` flags, because it is cosmetic and not required for release safety
- adding a real Power BI screenshot, because shipping without placeholder evidence is safer than adding weak material

## Publish recommendation
Publish now. The project is in a credible release state: the clean install flow was verified in a fresh standard CPython environment, tests passed, the CLI and PowerShell scripts work, the database/log outputs are correct, and the stale-cache risk on `daily` is addressed and documented.
