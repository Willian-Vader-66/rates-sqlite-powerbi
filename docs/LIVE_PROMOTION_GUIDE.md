# Live Promotion Guide

Use this guide to promote a validated staging database into the main local SQLite database.

## PowerShell Session Rule

PowerShell `$env:` variables live in the current process. If you run `scripts/setup_live_env.ps1` with `powershell.exe -File`, `TWELVE_DATA_API_KEY` is visible only inside that child process and disappears when it exits.

Use one of these safe flows:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi-git
Set-ExecutionPolicy -Scope Process Bypass -Force
. .\scripts\setup_live_env.ps1
```

or run the full staging pipeline in one process:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_live_pipeline.ps1
```

See `docs/POWERSHELL_SESSION_GUIDE.md`.

## 1. Build Candidate

```powershell
python -m fx_rates dashboard build-live-db --days 365 --db-path .tmp/live-main-candidate.sqlite --external-test
```

This creates a fresh candidate DB. It does not import rows from `data/fx.sqlite` and does not use demo fallback.

## 2. Validate Samples

```powershell
python -m fx_rates dashboard validate-samples --db-path .tmp/live-main-candidate.sqlite --samples-per-symbol 5 --external-test
```

The report is written to `docs/LIVE_SAMPLE_VALIDATION_REPORT.md`.

If the candidate contains live stocks from Twelve Data and `TWELVE_DATA_API_KEY` is not configured in the current PowerShell session, this command stops immediately with `LIVE SAMPLE VALIDATION STATUS: NOT READY` instead of emitting one failure per stock sample.

## 3. Audit Live DB

```powershell
python -m fx_rates dashboard audit-live --db-path .tmp/live-main-candidate.sqlite
```

The report is written to `docs/LIVE_AUDIT_REPORT.md`.

## 4. Smoke API

```powershell
python -m fx_rates api smoke-live --db-path .tmp/live-main-candidate.sqlite --port 8001
```

The report is written to `docs/API_LIVE_SMOKE_REPORT.md`.

## 5. Promote With Backup

Run a dry-run first:

```powershell
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --dry-run
```

```powershell
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --to-db data/fx.sqlite --backup
```

The command validates the source DB, runs sample validation unless `--skip-samples` is explicit, runs API smoke unless explicitly skipped, creates `data/backups/fx-before-live-YYYYMMDD-HHMMSS.sqlite`, then copies the candidate over the main DB.

`promote-live --dry-run` also checks the Twelve Data key before calling external sample validation. A missing key is a hard blocker, not a warning, and no real promotion is performed.

## 6. Roll Back

```powershell
python -m fx_rates dashboard restore-backup --backup data/backups/NOME.sqlite
```

Rollback copies the selected backup to `data/fx.sqlite` and reinitializes schema compatibility.

## Safety Rules

- Never promote a candidate with critical FAIL.
- Prefer a smaller reliable live scope over broad but inconsistent data.
- Do not commit SQLite files, backups, `.tmp`, logs, cache, `.env`, or API keys.
- Keep provider keys in environment variables only. Logs, reports, and docs should show masked values such as `****`, never raw secrets.
- Rotate the Twelve Data key if it appeared in a screenshot, terminal transcript, command history, or log.
