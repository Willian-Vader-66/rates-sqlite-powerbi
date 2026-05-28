# Pre-Release Audit Report

Date: 2026-05-19
Project: Finance Monitor / rates-sqlite-powerbi
Repository: C:\Projetos_Local\rates-sqlite-powerbi-git

## Git

- Branch: `main`
- Remote: `https://github.com/Willian-Vader-66/rates-sqlite-powerbi`
- Remote status: expected origin fetch/push URL confirmed.

## Modified source/docs files

- `.gitignore`
- `src/fx_rates/analysis.py`
- `src/fx_rates/dashboard_audit.py`
- `src/fx_rates/dashboard_market_audit.py`
- `src/fx_rates/dashboard_prepare.py`
- `src/fx_rates/db_sqlite.py`
- `src/fx_rates/market_providers.py`
- `tests/test_dashboard_display_metadata.py`
- `tests/test_data_mode_contract.py`
- `docs/LIVE_STOCK_INGESTION_DIAGNOSIS.md`
- `docs/PRE_RELEASE_AUDIT_REPORT.md`
- `docs/LINKEDIN_RELEASE_NOTES.md`
- `docs/RELEASE_CHECKLIST_LINKEDIN.md`

## Ignored/generated files observed

These must not be committed:

- `.venv/`
- `.tmp/`
- `.pytest_cache/`
- `data/fx.sqlite`
- `cache/pytest-*`
- `logs/*.log`
- `frontend-java/target/`
- `__pycache__/`

`.gitignore` was reinforced to include `.tmp/`.

## Backend validation

- `python --version`: Python 3.13.13
- `python -m pytest -q`: 56 passed
- `python -m fx_rates --help`: OK
- `python -m fx_rates dashboard --help`: OK
- `python -m fx_rates providers status`: OK, no external calls
- `python -m fx_rates dashboard prepare-demo --years 1 --demo`: OK
- `python -m fx_rates dashboard audit`: OK, no alerts
- `python -m fx_rates dashboard audit-market`: OK, current dataset is DEMO and explicitly flagged as demo

## API smoke

Temporary backend was started on `http://127.0.0.1:8000` and stopped after validation.

Endpoints checked:

- `/health`
- `/api/system/status`
- `/api/dashboard/summary`
- `/api/instruments`
- `/api/quotes/latest`
- `/api/analysis/latest`
- `/api/history/BRL`
- `/api/history/EUR`
- `/api/history/BTC`
- `/api/history/ETH`
- `/api/history/AAPL`
- `/api/history/MSFT`
- `/api/history/NVDA`
- `/api/history/SELIC_DAILY`

Result: all responded without stack trace. `/api/system/status` reported `data_mode=demo`, `data_health=OK`, 68 instruments, and 84172 historical rows after demo preparation.

## Frontend validation

- `mvn -U clean test`: OK, 16 tests passed
- `mvn -q -DskipTests compile`: OK
- `run_visual_test.ps1 -PrepareDemo -SkipTests -NoFrontend`: OK
- Full UI/manual check: pending. The no-frontend runner confirms backend readiness and Java/Maven environment; screenshots should still be captured manually before posting.

## Data status

- Current data mode: `demo`
- Instruments: 68
- Historical rows: 84172
- Providers in current dataset: `mock`, `mock_crypto`, `mock_fx`
- Data health: OK
- Release-safe explanation: dataset is deterministic demo data for portfolio validation, not real market data and not financial advice. Live providers are prepared but require provider-specific configuration and validation.

## Risks

- Twelve Data live stock ingestion depends on a real `TWELVE_DATA_API_KEY` and should be presented as live-ready, not finalized live coverage.
- Full JavaFX visual/manual screenshot check was not executed in this audit pass.
- SQLite runtime database and logs changed during validation and must not be committed.
- Power BI/ODBC is initial scope and should not be described as complete BI productization.

## Release status

Automated backend, API smoke, Java tests, Java compile, and no-frontend visual runner passed. The project is suitable for a LinkedIn portfolio post as a local finance dashboard with explicit demo data and live-ready architecture, provided the post remains honest about demo/live scope.
