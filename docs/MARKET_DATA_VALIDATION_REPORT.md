# Market Data Validation Report

Generated at: 2026-05-10T20:51:57
Project path: C:\Projetos_Local\rates-sqlite-powerbi-git
Database path: C:\Projetos_Local\rates-sqlite-powerbi-git\data\fx.sqlite
Status: READY

## Dataset Summary

- Data mode: DEMO
- Providers: mock, mock_crypto, mock_fx, mock_macro
- Total instruments: 68
- Instruments by asset type: {'CRYPTO': 10, 'FX': 19, 'MACRO': 7, 'STOCK': 32}
- Historical rows: 83638
- Historical range: 2022-04-30 to 2026-05-10
- Instruments without quote: 0
- Instruments without history: 0
- Stale instruments: 0
- Demo instruments: 68

## Internal Validation

- Internal consistency status: OK
- Duplicate instruments: 0, confirmed by `dashboard audit`
- Duplicate quotes: 0, confirmed by `dashboard audit`
- Suspicious values: 0, confirmed by `dashboard audit` and `audit-market`
- 30D/90D changes: recalculated from historical series using `((last_value / first_value) - 1) * 100`
- Ranking status: OK
- Ranking bottom label: Worst 30D
- Remaining audit flags: {'DEMO_DATA_VISIBLE_AS_LIVE': 68}

## External Validation

External live validation is optional and non-blocking. This run reported `SKIPPED` because samples could not reach a public provider or do not have a configured provider/API key.

| Symbol | Asset type | App value | External value | Diff | Status | Notes |
|---|---:|---:|---:|---:|---|---|
| BRL | FX | 5.254928 | - | - | SKIPPED | LIVE_VALIDATION_SKIPPED: no internet/provider/API key or unsupported sample |
| EUR | FX | 0.96051 | - | - | SKIPPED | LIVE_VALIDATION_SKIPPED: no internet/provider/API key or unsupported sample |
| BTC | CRYPTO | 108050.215 | - | - | SKIPPED | LIVE_VALIDATION_SKIPPED: no internet/provider/API key or unsupported sample |
| ETH | CRYPTO | 5411.7653 | - | - | SKIPPED | LIVE_VALIDATION_SKIPPED: no internet/provider/API key or unsupported sample |
| AAPL | STOCK | 266.968 | - | - | SKIPPED | LIVE_VALIDATION_SKIPPED: no internet/provider/API key or unsupported sample |
| AMZN | STOCK | 149.7936 | - | - | SKIPPED | LIVE_VALIDATION_SKIPPED: no internet/provider/API key or unsupported sample |
| MSFT | STOCK | 227.1232 | - | - | SKIPPED | LIVE_VALIDATION_SKIPPED: no internet/provider/API key or unsupported sample |
| NVDA | STOCK | 315.2852 | - | - | SKIPPED | LIVE_VALIDATION_SKIPPED: no internet/provider/API key or unsupported sample |
| BRK.B | STOCK | 313.3711 | - | - | SKIPPED | LIVE_VALIDATION_SKIPPED: no internet/provider/API key or unsupported sample |
| SELIC_DAILY | MACRO | 0.0397 | - | - | SKIPPED | LIVE_VALIDATION_SKIPPED: no internet/provider/API key or unsupported sample |
| SELIC_ANNUALIZED_MONTHLY | MACRO | 10.5298 | - | - | SKIPPED | LIVE_VALIDATION_SKIPPED: no internet/provider/API key or unsupported sample |
| IPCA_MONTHLY | MACRO | 0.4068 | - | - | SKIPPED | LIVE_VALIDATION_SKIPPED: no internet/provider/API key or unsupported sample |
| FED_FUNDS_DAILY | MACRO | 5.2513 | - | - | SKIPPED | LIVE_VALIDATION_SKIPPED: no internet/provider/API key or unsupported sample |

## Semantic Fixes Applied

- Demo/mock provider records are now exposed by API as `data_mode=demo` with provider list and warning text.
- `audit-market` flags demo data so values such as USD/BRL 5.254928 are not presented as live market data.
- Macro instruments keep explicit units: `% a.d.`, `% a.m.`, `% a.a.` or `index`.
- Display metadata now includes `unit_label` and `value_label` for FX, stocks, crypto and macro.
- Watchlist history can use `/api/history/{symbol}` with aliases such as `BRL -> USD/BRL`, `EUR -> USD/EUR`, `BTC -> BTC/USD`, `ETH -> ETH/USD`.
- Top/Worst ranking now returns contextual labels: `Weakest` when all period changes are positive and `Least negative` when all are negative.

## Validation Commands Run

- `.\.venv\Scripts\python.exe -m pytest -q` -> 35 passed
- `.\.venv\Scripts\python.exe -m fx_rates dashboard audit` -> OK, no alerts
- `.\.venv\Scripts\python.exe -m fx_rates dashboard audit-market` -> OK, demo flags only
- `.\.venv\Scripts\python.exe -m fx_rates dashboard audit-market --with-live-sample` -> SKIPPED, no external provider/network sample available
- `.\.venv\Scripts\python.exe -m fx_rates dashboard prepare-demo --years 1 --demo` -> 68 instruments, 83638 historical rows
- Maven `mvn -U clean test` with Windows trust store -> 16 passed
- Maven `mvn -q -DskipTests compile` with Windows trust store -> OK
- `run_visual_test.ps1 -PrepareDemo -SkipTests -NoFrontend` -> OK, backend stopped
- API smoke endpoints -> OK for health, system status, summary, latest quote/analysis and generic history aliases

## Commit Notes

Safe to review for commit: source code, tests, docs, frontend Java files, `.gitignore` if intentionally changed.

Do not commit: `.venv/`, `data/*.sqlite`, `data/*.sqlite-*`, `logs/`, `cache/`, `frontend-java/target/`, `__pycache__/`, `.pytest_cache/`, `*.egg-info/`, temporary files.
# Market Data Validation Report

Current validation strategy separates origin before value interpretation:

- Demo records are valid for UI/testing only and are reported as `data_mode=demo`.
- Live records must come from configured providers and are reported as `data_mode=live`.
- Mixed datasets are allowed only when explicit and are reported as `data_mode=mixed`.
- Unknown origin is reported as `data_mode=unknown` and should be reviewed before analysis.

Commands:

```powershell
python -m fx_rates providers status
python -m fx_rates dashboard audit
python -m fx_rates dashboard audit-market
```

`audit-market` reports data-mode counts, provider by asset type, demo/live/unknown symbols, stale data, missing quote/history/analysis links, suspicious ranges, missing stock currency, and macro unit issues. Demo data is a warning/risk, not a fatal error.

## Live Ingestion Safety Addendum

The current live hardening adds explicit data-loss protection around `prepare-live --replace-demo`:

- API key placeholders are invalid even when an environment variable is present.
- `providers status` reports `key_present`, `key_valid_format`, `available`, `external_test`, and a summarized error without revealing secrets.
- `providers status --external-test` is the only provider diagnostic that may call external APIs.
- Live fetch and payload validation complete before any SQLite mutation.
- Demo deletion under `--replace-demo` happens inside the same transaction as live inserts.
- Rollback keeps existing demo/live rows if a write failure occurs.
- `data_health` reports important symbols with zero history, quote/history mismatches, analysis without history, live rows without provider, and demo-like providers marked live.

Validation target after a placeholder or bad Twelve Data key:

```powershell
python -m fx_rates dashboard prepare-live --days 365 --asset-type STOCK --symbols AAPL,MSFT,NVDA --replace-demo
python -m fx_rates dashboard audit-market
```

Expected result: live ingest fails before DB mutation and AAPL/MSFT/NVDA retain their existing demo history.
