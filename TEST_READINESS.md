# Test Readiness

## Status
READY FOR TESTING

## What was verified
- A fresh CPython 3.11 virtual environment was created in `.readiness-venv`, `pip install -r requirements.txt` completed successfully, and imports plus `python -m fx_rates --help` worked from the repo root without editable installation.
- `pytest -q` passed in the clean environment: `18 passed in 0.83s`.
- `python -m fx_rates backfill --start 2026-02-01 --end 2026-02-03 --base USD --symbols BRL,EUR` started correctly, reused the cached timeseries payload, wrote `6` rows, and recorded an `OK` row in `ingest_runs`.
- `python -m fx_rates daily --base USD --symbols BRL,EUR` was verified in both important modes: it failed non-zero with retries and a recorded `FAIL` row when outbound access was blocked, and it succeeded with a live fetch once network access was available, writing `2` rows and an `OK` row in `ingest_runs`.
- `python -m fx_rates status --last 5` started correctly and showed the recent runs; after the smoke checks the default SQLite database contained `10` rows in `fx_rates` and `12` rows in `ingest_runs`, and `logs/app.log` was updated.
- Cache behavior is sane for testing: the backfill command reused the existing timeseries cache file, and the successful `daily` run did not create a new `latest` cache file because `use_cache_latest` defaults to `false`.
- README instructions, CLI examples, runtime defaults, troubleshooting notes, and the actual commands tested are consistent enough for a human to run the baseline checks without guesswork.

## What is missing before tests
- No repository code changes are required before baseline testing.
- The tester needs outbound HTTPS access to `https://api.frankfurter.dev/v1/latest` for the live `daily` smoke command.

## Blockers
None.

## Risks
- `daily` is network-dependent, so restricted hosts will record a `FAIL` run unless the tester allows outbound access to `api.frankfurter.dev`.
- The default runtime paths (`data/fx.sqlite`, `logs/app.log`, and `cache/`) already contain prior artifacts in this repository, so a tester who wants a completely fresh evidence trail should clear or redirect them before re-running smoke checks.
- `status` output is log-formatted rather than tabular; it is readable, but testers should expect structured log lines rather than a report table.

## Exact commands to run
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env -Force
python -m pytest -q
python -m fx_rates backfill --start 2026-02-01 --end 2026-02-03 --base USD --symbols BRL,EUR
python -m fx_rates daily --base USD --symbols BRL,EUR
python -m fx_rates status --last 5
```

## Expected evidence of success
- `data/fx.sqlite` exists.
- `logs/app.log` exists and shows `run_start`, `fetch_complete`, `db_upsert`, and `run_finish` events.
- `fx_rates` contains rows, including the backfill dates and the successful latest-day rows.
- `ingest_runs` contains rows for the recent backfill and daily runs.
- `pytest` reports all tests passing.
- `python -m fx_rates status --last 5` prints recent run records with `OK` status and row counts.

## Recommended next action
- Run the exact command sequence above in a clean local workspace with outbound access enabled for the `daily` step.
