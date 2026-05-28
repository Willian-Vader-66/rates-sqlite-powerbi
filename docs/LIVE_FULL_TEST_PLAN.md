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
- `validate-samples` passes when fake provider values match and fails when DB values diverge;
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

## Promotion Gate

Run only after sample validation, audit-live, and API smoke reports are acceptable:

```powershell
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --dry-run
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --to-db data/fx.sqlite --backup
```

Do not use `--force` in this phase. Do not commit `data/*.sqlite`, backups, `.tmp`, logs, cache, or secrets.
