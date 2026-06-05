# LIVE 365D Release Gate Report

Generated: 2026-06-03T13:53:05.8098883-03:00

## Summary

- Macro monthly gate status: `READY`
- Overall promotion status in this Codex session: `NOT_READY`
- Root cause fixed: `IPCA_MONTHLY` was being checked with a daily range coverage rule, producing `history range shorter than expected (304d < 310d)` even though the monthly stale window was OK.
- Fix applied: monthly macro series are validated by expected frequency, monthly point count, stale window, values, data mode, duplicate checks, and future-date checks. Daily coverage range is no longer applied to `IPCA_MONTHLY`.
- Remaining blocker in this shell: `TWELVE_DATA_API_KEY` is not present, so stock external sample validation and promote dry-run are correctly blocked.

## Commands Run

- `.\.venv\Scripts\python.exe -m pytest tests\test_live_full_workflow.py -q`
- `.\.venv\Scripts\python.exe -m pytest -q`
- `cd frontend-java; mvn -U clean test`
- `cd frontend-java; mvn -q -DskipTests compile`
- `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_live_pipeline.ps1 -CheckOnly`
- `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_finance_monitor.ps1 -CheckOnly`
- `.\.venv\Scripts\python.exe -m fx_rates dashboard audit-live --db-path .tmp/live-main-candidate.sqlite`
- `.\.venv\Scripts\python.exe -m fx_rates dashboard validate-samples --db-path .tmp/live-main-candidate.sqlite --samples-per-symbol 5 --external-test`
- `.\.venv\Scripts\python.exe -m fx_rates api smoke-live --db-path .tmp/live-main-candidate.sqlite --port 8001`
- `.\.venv\Scripts\python.exe -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --dry-run`

## Test Results

- Python targeted live workflow: `35 passed`
- Python pytest: `123 passed`
- Maven tests: `30 passed`
- Maven compile: `OK`
- `run_live_pipeline.ps1 -CheckOnly`: `READY`
- `run_finance_monitor.ps1 -CheckOnly`: `READY`

## Candidate DB Status

- DB: `C:\Projetos_Local\rates-sqlite-powerbi-git\.tmp\live-main-candidate.sqlite`
- requested_days: `365`
- history_mode: `standard`
- advanced_history: `disabled`
- data_mode: `live`
- data_health: `OK`
- providers: `bcb_sgs`, `coingecko`, `frankfurter`, `twelvedata`
- historical rows: `6368`
- date range: `2025-06-01` to `2026-06-03`

## IPCA Monthly Policy

- symbol: `IPCA_MONTHLY`
- frequency: `monthly`
- point_count: `11`
- date_min: `2025-06-01`
- latest_date: `2026-04-01`
- stale_days: `63`
- allowed_stale_days: `75`
- final status: `OK`
- policy decision: `READY`

`IPCA_MONTHLY` is not expected to have 365 daily rows or to publish up to the current daily market date. A valid monthly series inside the freshness window must not fail because its calendar range is shorter than daily FX, crypto, or stock ranges.

## Audit-Live Result

- status: `OK`
- symbols checked: `24`
- critical failures: `0`
- warnings: `0`
- previous failure removed: `MACRO IPCA_MONTHLY: history range shorter than expected (304d < 310d)`
- UI/context note: `IPCA is a monthly macro series and may lag daily market data. Latest value is within allowed monthly freshness window.`

## Sample Validation Result

- status in this shell: `NOT_READY`
- internal validation: `OK=120 WARN=0 FAIL=0`
- external validation: skipped for stocks because `TWELVE_DATA_API_KEY` is missing in this shell
- reason_codes: `HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE`, `VALIDATION_OK`, `PROVIDER_KEY_MISSING`
- promotion_allowed: `false`

This blocker is unrelated to the IPCA monthly fix. In the user's provider-ready session, prior sample validation was `READY_WITH_WARNINGS` because of external rate limit, with internal validation OK.

## API Smoke Result

- status: `OK`
- endpoints tested: `15`
- failed endpoints: `-`
- checked endpoints: `/health`, `/api/system/status`, `/api/dashboard/summary`, `/api/instruments`, `/api/quotes/latest`, `/api/analysis/latest`, `/api/history/BRL`, `/api/history/EUR`, `/api/history/BTC`, `/api/history/ETH`, `/api/history/AAPL`, `/api/history/MSFT`, `/api/history/NVDA`, `/api/history/SELIC_DAILY`, `/api/history/IPCA_MONTHLY`

## Promotion Decision

- `promote-live --dry-run` in this shell: `BLOCKED`
- blocker: `TWELVE_DATA_API_KEY missing for stock external sample validation`
- IPCA/audit blocker: resolved
- real promotion run: `no`
- `data/fx.sqlite` touched: `no`

Dry-run can return `READY_WITH_WARNINGS` when audit-live has only an allowed monthly macro freshness warning and the sample/API gates are otherwise acceptable. Missing or invalid provider keys still block.

## UI Status

- Control Center Step 5 now prepends an Audit Live summary.
- Step 5 summary includes critical failures, warnings, and the IPCA monthly policy note.
- If IPCA is monthly and inside the allowed freshness window, Step 5 is shown as Passed/Passed with Warnings instead of Failed.
- Logs and report previews remain redacted.

## Security / Redaction Status

- API key saved to file: `no`
- API key printed in logs: `no`
- `.env` touched: `no`
- `data/fx.sqlite` touched: `no`
- secret scan findings: only placeholder/test redaction references; no raw API key identified
- git add/commit/push: `not run`
- real promotion: `not run`

## Safe To Commit

Review and commit only versionable source/docs/scripts:

- `README.md`
- `docs/API_LIVE_SMOKE_REPORT.md`
- `docs/ENV_DOCTOR_REPORT.md`
- `docs/LIVE_AUDIT_REPORT.md`
- `docs/LIVE_BUILD_REPORT.md`
- `docs/LIVE_FULL_TEST_PLAN.md`
- `docs/LIVE_PROMOTION_GUIDE.md`
- `docs/LIVE_SAMPLE_VALIDATION_REPORT.md`
- `docs/LIVE_365D_RELEASE_GATE_REPORT.md`
- `docs/VISUAL_CONTROL_CENTER.md`
- `frontend-java/README.md`
- `frontend-java/src/main/java/com/example/financedashboard/ops/`
- `frontend-java/src/main/java/com/example/financedashboard/ui/ControlCenterController.java`
- `frontend-java/src/main/java/com/example/financedashboard/ui/DashboardController.java`
- `frontend-java/src/main/resources/styles/app.css`
- `frontend-java/src/test/java/com/example/financedashboard/ControlCenterOperationsTest.java`
- `run_live_pipeline.ps1`
- `run_finance_monitor.ps1`
- `src/fx_rates/live_first.py`
- `src/fx_rates/live_promotion.py`
- `src/fx_rates/live_samples.py`
- `src/fx_rates/live_validation.py`
- `src/fx_rates/provider_status.py`
- `tests/test_env_doctor.py`
- `tests/test_live_full_workflow.py`
- `tests/test_powershell_live_scripts.py`

## Do Not Commit

- `.env`
- `.venv/`
- `data/*.sqlite`
- `data/*.sqlite-*`
- `data/backups/*.sqlite`
- `.tmp/`
- `logs/`
- `cache/`
- `frontend-java/target/`
- `__pycache__/`
- `.pytest_cache/`
- `*.egg-info/`
- any file containing an API key or unredacted secret

## Next Steps

1. Run from the provider-ready PowerShell session where `TWELVE_DATA_API_KEY` is available only in memory.
2. Rerun `validate-samples --external-test` and `promote-live --dry-run`.
3. Promote manually only after the full gate is `READY` or `READY_WITH_WARNINGS` with only allowed external/rate-limit/monthly macro warnings.
