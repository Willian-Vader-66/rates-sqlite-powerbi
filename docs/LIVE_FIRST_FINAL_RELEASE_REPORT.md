# Live-First Final Release Report

Date: 2026-05-21

## Status

LIVE-FIRST DATA PIPELINE STATUS: NOT READY

The code path for the LIVE-FIRST pipeline is implemented and covered by automated tests, but the real candidate database could not be built in this environment. The release gate correctly refused promotion.

## Implemented

- Central secret redaction for provider params, exception text, and diagnostics.
- CoinGecko provider support for public/demo/pro plans, masked headers, retry/backoff, range diagnostics, UTC timestamp conversion, daily deduplication, and fallback from full range to yearly and 90-day chunks.
- Versioned live release scope in `data/reference/live_release_scope.csv`.
- LIVE-FIRST build, refresh, sample validation, live audit, API smoke, promotion dry-run, backup promotion, and restore commands.
- Live audit rules for required symbols, live-only data mode, provider consistency, quote/history consistency, future dates, duplicates, and expected 365-day coverage.
- JavaFX data-mode badge now shows `LIVE DATA` only when backend status is `data_mode=live` and `data_health=OK`.

## Release Scope

| Asset type | Required symbols |
|---|---|
| FX | BRL, EUR, GBP, JPY, CAD, CHF |
| CRYPTO | BTC, ETH, BNB, SOL, XRP |
| STOCK | AAPL, MSFT, NVDA, AMZN, GOOGL, META, TSLA, JPM, KO, AMD |
| MACRO | SELIC_DAILY, CDI_DAILY, IPCA_MONTHLY |

Demo data remains limited to tests, local development, and explicit fallback experiments.

## Providers

`python -m fx_rates providers status`:

- FX / Frankfurter: configured, external test skipped.
- Crypto / CoinGecko: configured, external test skipped.
- Stock / Twelve Data: not configured in this process; `TWELVE_DATA_API_KEY` not present.
- Macro / BCB SGS: configured, external test skipped.

`python -m fx_rates providers status --external-test`:

- FX / Frankfurter: FAIL, TLS/CA validation error.
- Crypto / CoinGecko: FAIL, TLS/CA validation error.
- Stock / Twelve Data: FAIL, `TWELVE_DATA_API_KEY` not present in this process.
- Macro / BCB SGS: FAIL, TLS/CA validation error.

No API key values were printed in the report.

## Live DB Candidate

Command:

```powershell
python -m fx_rates dashboard build-live-db --days 365 --db-path .tmp/live-main-candidate.sqlite --external-test
```

Result: FAIL before ingest.

- Path: `.tmp/live-main-candidate.sqlite`
- Instruments: 0
- Historical rows: 0
- Date min: -
- Date max: -
- Data mode: unknown
- Providers: -
- OK/WARN/FAIL: FAIL

Blockers:

- Python process does not see `TWELVE_DATA_API_KEY`.
- HTTPS provider checks fail TLS/CA validation even with `SSL_CERT_FILE` and `REQUESTS_CA_BUNDLE` pointed to the virtualenv `certifi` bundle.

## Sample Validation

Command:

```powershell
python -m fx_rates dashboard validate-samples --db-path .tmp/live-main-candidate.sqlite --samples-per-symbol 5 --external-test
```

Result: FAIL

- Samples OK: 0
- Samples WARN: 0
- Samples FAIL: 1
- Reason: no live instruments in DB
- Report: `docs/LIVE_SAMPLE_VALIDATION_REPORT.md`

## Audit Live

Command:

```powershell
python -m fx_rates dashboard audit-live --db-path .tmp/live-main-candidate.sqlite
```

Result: FAIL

- Data mode: unknown
- Historical rows: 0
- Critical failures: 27
- Main failures: empty live DB, required release symbols missing, data health FAIL.
- Report: `docs/LIVE_AUDIT_REPORT.md`

## API Smoke

Command:

```powershell
python -m fx_rates api smoke-live --db-path .tmp/live-main-candidate.sqlite --port 8001
```

Result: FAIL

- Endpoints tested: 15
- Failed endpoints: 14
- Main failures: data_mode unknown, data_health FAIL, empty instruments, quotes, analysis, and histories.
- Report: `docs/API_LIVE_SMOKE_REPORT.md`

## Promotion Gate

Command:

```powershell
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --dry-run
```

Result: FAIL as expected.

The command refused promotion because `audit-live` failed. No backup or copy was performed.

## Automated Tests

- Python: PASS, `83 passed`.
- Maven tests: PASS, `16 tests`, build success.
- Maven compile: PASS, `mvn -q -DskipTests compile`.
- Visual runner: not run because the live candidate is blocked and the script opens an interactive JavaFX window that waits for manual close.

## Secret Scan

- `git check-ignore -v .env`: PASS, `.env` is ignored by Git.
- Text scan over `README.md`, `docs/`, `src/`, `tests/`, and `logs/`: no raw provider key values found. Matches were environment variable names, empty placeholders, or fake test values.
- Provider diagnostics redact params and exception text before logging.

## Safe To Commit

Review and commit only intentional source, test, and docs files:

- `README.md`
- `docs/API_CONTRACT.md`
- `docs/LIVE_FIRST_PRODUCT_SCOPE.md`
- `docs/LIVE_FULL_TEST_PLAN.md`
- `docs/LIVE_PROMOTION_GUIDE.md`
- `docs/LIVE_STOCK_INGESTION_DIAGNOSIS.md`
- `docs/LIVE_FIRST_FINAL_RELEASE_REPORT.md`
- `data/reference/live_release_scope.csv`
- `src/fx_rates/*.py` changed for live-first, redaction, provider, audit, API, DB metadata, promotion, samples, and refresh.
- `tests/*.py` changed or added for live-first and provider behavior.
- `frontend-java/src/main/java/...` and `frontend-java/src/test/java/...` intentional UI/API contract changes.

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

## Next Commands

Run these in a PowerShell session where the Twelve Data key is actually present and TLS trust is fixed:

```powershell
$env:TWELVE_DATA_API_KEY="****"
$env:COINGECKO_API_PLAN="public"
python -m fx_rates providers status --external-test
python -m fx_rates dashboard build-live-db --days 365 --db-path .tmp/live-main-candidate.sqlite --external-test
python -m fx_rates dashboard validate-samples --db-path .tmp/live-main-candidate.sqlite --samples-per-symbol 5 --external-test
python -m fx_rates dashboard audit-live --db-path .tmp/live-main-candidate.sqlite
python -m fx_rates api smoke-live --db-path .tmp/live-main-candidate.sqlite --port 8001
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --dry-run
```

Only after every check is OK:

```powershell
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --to-db data/fx.sqlite --backup
```
