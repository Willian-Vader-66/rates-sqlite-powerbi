# Live Full Test Plan

This plan validates the LIVE-FIRST pipeline without committing SQLite databases or secrets.

## Automated Tests

```powershell
python -m pytest -q
cd frontend-java
$env:MAVEN_OPTS="-Djavax.net.ssl.trustStoreType=Windows-ROOT"
mvn -U clean test
mvn -q -DskipTests compile
cd ..
```

The Python tests use fake live providers only for deterministic test coverage. They verify:

- `build-live-db` creates a 100% live candidate DB;
- `refresh-live` writes only new rows and supports `--dry-run`;
- `validate-samples` passes internal candidate DB checks without external calls when `--external-test` is not used;
- `validate-samples --external-test` validates historical samples with historical provider endpoints, not current-price endpoints;
- crypto historical samples use CoinGecko `market_chart/range`; CoinGecko `simple/price` is used only for latest quote validation;
- external provider rate limits produce explicit reason codes such as `EXTERNAL_RATE_LIMIT` and do not become generic failures when the candidate DB is internally coherent;
- latest quote/history divergence above the configured threshold remains a FAIL;
- `audit-live` rejects demo/mock rows and accepts coherent live rows;
- `api smoke-live` validates the required backend endpoints;
- `promote-live` creates a backup;
- `promote-live` refuses invalid source databases;
- `restore-backup` restores a previous SQLite file.

## Real Provider Test Flow

Recommended one-process PowerShell flow:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_live_pipeline.ps1
```

`run_live_pipeline.ps1` keeps TLS variables and `TWELVE_DATA_API_KEY` in the same process, so the key is still available when `build-live-db` runs.

Manual equivalent after dot-sourcing `scripts/setup_live_env.ps1`:

```powershell
python -m fx_rates providers status
python -m fx_rates providers status --external-test
python -m fx_rates dashboard build-live-db --days 365 --db-path .tmp/live-main-candidate.sqlite --external-test
python -m fx_rates dashboard validate-samples --db-path .tmp/live-main-candidate.sqlite --samples-per-symbol 5 --external-test
python -m fx_rates dashboard audit-live --db-path .tmp/live-main-candidate.sqlite
python -m fx_rates api smoke-live --db-path .tmp/live-main-candidate.sqlite --port 8001
```

Expected outcome:

- READY when all scoped instruments pass.
- PARTIALLY FUNCTIONAL when `--allow-partial` produced a live DB with only reliable instruments.
- NOT READY when critical provider, audit, sample, or API checks fail.

Sample validation status policy:

- `READY`: internal DB validation passed and optional external samples passed.
- `READY_WITH_WARNINGS`: internal DB validation passed, audit-live and API smoke-live are clean, and the only unresolved items are external/transient confirmation warnings such as provider rate limit, TLS/network/provider availability, or nearest-date historical samples.
- `NOT_READY`: data or configuration is unsafe for promotion, including empty DB, non-live data, missing provider key, quote/history divergence above threshold, non-positive prices, insufficient history, duplicated dates, future dates, missing required symbols, or failed audit/smoke gates.
- `FAIL`: local execution failed, the DB could not be read, or validation could not run.

`validate-samples` prints separate `INTERNAL SAMPLE VALIDATION`, `EXTERNAL PROVIDER SAMPLE VALIDATION`, and `DATA DECISION` sections. The promotion gate is `PASS`, `PASS_WITH_WARNINGS`, or `BLOCKED`.

Reason codes include `VALIDATION_OK`, `HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE`, `HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE`, `HISTORICAL_SAMPLE_NEAREST_DATE_WARN`, `HISTORICAL_SAMPLE_DIVERGENCE_FAIL`, `CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST`, `EXTERNAL_RATE_LIMIT`, `EXTERNAL_PROVIDER_UNAVAILABLE`, `PROVIDER_TLS_ERROR`, `PROVIDER_KEY_MISSING`, `INSUFFICIENT_HISTORY_POINTS`, `MISSING_LIVE_HISTORY`, `DUPLICATED_HISTORY_DATE`, `NON_POSITIVE_PRICE`, `FUTURE_HISTORY_DATE`, `STALE_LATEST_QUOTE`, `LATEST_QUOTE_DIVERGENCE_WARN`, and `LATEST_QUOTE_DIVERGENCE_FAIL`.

Macro monthly policy:

- `IPCA_MONTHLY` is validated as a monthly macro series, not as daily market data.
- It must have enough monthly points for the LIVE 365D scope; the current minimum is 10 points.
- It does not need 365 rows and does not need a daily calendar range that reaches the current day.
- The freshness gate uses `allowed_stale_days=75`.
- Missing rows, very low point count, future dates, duplicates, invalid units/values, non-live data, or stale monthly data beyond the failure threshold remain blockers.

## Promotion Gate

Run only after sample validation, audit-live, and API smoke reports are acceptable. `READY_WITH_WARNINGS` is acceptable only when the warning is external/transient or an allowed monthly macro freshness warning, and internal validation, audit-live, and API smoke-live pass. It is not acceptable when `TWELVE_DATA_API_KEY` is missing or invalid, when `data_health` is not `OK`, when the DB is empty or non-live, when expected providers/symbols are missing, or when quote/history divergence exceeds the configured failure threshold.

```powershell
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --dry-run
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --to-db data/fx.sqlite --backup
```

Do not use `--force` in this phase. Do not commit `data/*.sqlite`, backups, `.tmp`, logs, cache, or secrets.
