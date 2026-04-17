# Release Audit

## 1. What is fully complete
- CLI surface is implemented and runnable with `python -m fx_rates {backfill,daily,status}`. Evidence: `python -m fx_rates --help` and subcommand `--help` all worked in the audit environment.
- Package layout is intact for repo-root execution. Evidence: `import fx_rates`, `fx_rates.cli`, `fx_rates.api_frankfurter`, and `fx_rates.db_sqlite` all imported successfully from the local `.venv`.
- SQLite persistence is working with the expected grain. Evidence: `data/fx.sqlite` contains `fx_rates`, `ingest_runs`, and the views `v_fx_daily`, `v_fx_latest`, `v_fx_monthly_avg`; duplicate-key query returned `0`.
- UPSERT behavior is working. Evidence: re-running the same backfill window did not create duplicates; the audited window still contained `6` rows total for `USD` x `{BRL, EUR}` across the expected dates.
- Audit trail is working. Evidence: `ingest_runs` recorded both `OK` and `FAIL` runs correctly, and `status --last 5` reflected the recent executions accurately.
- Logging is working to `logs/app.log`. Evidence: the file contains `run_start`, `cache_hit`, `fetch_complete`, `rows_normalized`, `db_upsert`, `run_finish`, `http_retry`, and `run_fail`.
- Cache behavior for backfill/time-series is working. Evidence: the backfill smoke run completed with `event=cache_hit` against the expected cache file.
- The known daily-cache risk appears fixed. Evidence:
  - Code: `fetch_latest()` calls `_request_json(..., use_cache=self.use_cache and self.use_cache_latest)`, and `USE_CACHE_LATEST` defaults to `false`.
  - Runtime: after successful daily runs, the cache directory still only contained the historical timeseries file; the computed `latest` cache filename was absent.
  - Tests: `tests/test_api_normalize.py` includes a test proving stale cache is ignored by default.
  - Docs: README now explicitly says daily fetches fresh data by default and documents `--use-cache-latest`.
- Windows-friendly PowerShell wrappers are working. Evidence: both `scripts/run_backfill.ps1` and `scripts/run_daily.ps1` executed successfully against the current CLI.
- Test suite is green. Evidence: `pytest -q` reported `18 passed`.
- Repo hygiene basics are present. Evidence: `.env.example` exists, `.gitignore` excludes `.env`, cache JSON, SQLite files, logs, `.venv`, and `__pycache__`, and the audit search did not find obvious secrets.

## 2. What is partially complete
- Clean-environment installation was only partially verified. The current local `.venv` can run the project and tests, but `python -m pip install -r requirements.txt` failed because this specific `.venv` lacks `pip`, and `python -m ensurepip` is also unavailable in it. This looks like an environment issue, not a repo code issue, but it means the README install path was not fully proven end-to-end inside the shipped workspace.
- Live `daily` execution is reliable only when outbound HTTPS is allowed. In the restricted sandbox, `daily` failed with connection errors, retried correctly, and wrote `FAIL` to `ingest_runs`; outside the sandbox it succeeded immediately. The code behavior is good, but demo success still depends on network access to `api.frankfurter.dev`.
- Power BI readiness is functionally supported via SQLite views and README instructions, but the evidence asset is still explicitly a placeholder (`assets/powerbi_screenshot_placeholder.png`).

## 3. What is missing
- A non-placeholder Power BI evidence asset or final demo screenshot is not present.
- A clean-room installation proof from a standard CPython environment with working `pip` is not yet captured by this audit.

## 4. Release blockers (P0)
- None found in the current codebase for the expected scope.

## 5. Release risks (P1)
- Demo environments without outbound access to `https://api.frankfurter.dev` will make `daily` fail live, even though retries and `ingest_runs` failure tracking behave correctly.
- The current local `.venv` is not a trustworthy reference for onboarding because it lacks `pip`; if someone uses this exact environment instead of creating a fresh one, the documented install command will appear broken.

## 6. Polish / post-release improvements (P2)
- Replace the placeholder Power BI screenshot with a real evidence image.
- Consider trimming irrelevant flags from `status --help` (`--cache-dir`, `--no-cache`, `--timeout`, `--retries`) to reduce UX noise.
- Capture one short “known-good demo run” transcript in the docs using the new log events and analytics views.

## 7. Fastest path to release
1. Validate the README install steps once in a standard fresh CPython virtual environment that has `pip`, and record that result.
2. Capture a real Power BI screenshot or explicitly remove the placeholder from release-facing materials.
3. Release from a clean commit after preserving the current green test state and verified CLI/script behavior.

## 8. Recommended next action
do these 1-3 things before release
