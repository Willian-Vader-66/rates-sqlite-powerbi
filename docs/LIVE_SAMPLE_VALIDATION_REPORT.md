# Live Sample Validation Report

Generated: 2026-05-26T15:36:02.072208+00:00
DB: `C:\Projetos_Local\rates-sqlite-powerbi-git\.tmp\live-main-candidate.sqlite`
Overall status: **NOT READY**
requested_period_days: `365`
history_mode: `standard`
advanced_history_available: `false`
Samples OK/WARN/FAIL: `0/0/0`

## Readiness

- reason: `TWELVE_DATA_API_KEY missing for stock sample validation.`
- action: `Run inside run_live_pipeline.ps1 or set TWELVE_DATA_API_KEY in the current PowerShell session.`

| symbol | asset_type | provider | endpoint | sample_date | provider_date | db_value | provider_value | delta_pct | tolerance_pct | status | note |
|---|---|---|---|---|---|---:|---:|---:|---:|---|---|

## Provider External Tests

| asset_type | provider | external_test | status | message |
|---|---|---|---|---|
| STOCK | twelvedata | not_run | not_ready | TWELVE_DATA_API_KEY missing for stock sample validation. |

## Notes

- Samples are deterministic: first, last, middle, and evenly spaced interior dates.
- Provider nearest-date matches are WARN unless the value diverges beyond tolerance.
- API keys are never written to this report.
