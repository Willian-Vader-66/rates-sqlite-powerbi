# Live Audit Report

Generated: 2026-05-26T15:35:59.578557+00:00
DB: `C:\Projetos_Local\rates-sqlite-powerbi-git\.tmp\live-main-candidate.sqlite`
Overall status: **OK**

## Summary

- data_mode: `live`
- providers: `bcb_sgs, coingecko, frankfurter, twelvedata`
- instruments: `24`
- historical rows: `6370`
- date_min: `2025-05-01`
- date_max: `2026-05-25`
- history_mode: `standard`
- requested_days: `365`
- advanced_history_enabled: `false`
- advanced_history_max_years: `10`

## Symbol Validation

| symbol | asset_type | provider | rows | date_min | date_max | latest_value | unit_label | expected_frequency | stale_days | allowed_stale_days | stale_status | data_mode | status | notes |
|---|---|---:|---:|---|---|---:|---|---|---:|---:|---|---|---|---|
| BTC | CRYPTO | coingecko | 365 | 2025-05-26 | 2026-05-25 | 76988.1613 | USD | daily | 1 | 3 | OK | live | OK | OK |
| ETH | CRYPTO | coingecko | 365 | 2025-05-26 | 2026-05-25 | 2098.1386 | USD | daily | 1 | 3 | OK | live | OK | OK |
| BNB | CRYPTO | coingecko | 365 | 2025-05-26 | 2026-05-25 | 656.0794 | USD | daily | 1 | 3 | OK | live | OK | OK |
| SOL | CRYPTO | coingecko | 365 | 2025-05-26 | 2026-05-25 | 85.3178 | USD | daily | 1 | 3 | OK | live | OK | OK |
| XRP | CRYPTO | coingecko | 365 | 2025-05-26 | 2026-05-25 | 1.3507 | USD | daily | 1 | 3 | OK | live | OK | OK |
| BRL | FX | frankfurter | 255 | 2025-05-26 | 2026-05-25 | 5.0026 | BRL | business_daily | 1 | 10 | OK | live | OK | OK |
| EUR | FX | frankfurter | 255 | 2025-05-26 | 2026-05-25 | 0.85889 | EUR | business_daily | 1 | 10 | OK | live | OK | OK |
| GBP | FX | frankfurter | 255 | 2025-05-26 | 2026-05-25 | 0.74083 | GBP | business_daily | 1 | 10 | OK | live | OK | OK |
| JPY | FX | frankfurter | 255 | 2025-05-26 | 2026-05-25 | 158.93 | JPY | business_daily | 1 | 10 | OK | live | OK | OK |
| CHF | FX | frankfurter | 255 | 2025-05-26 | 2026-05-25 | 0.7815 | CHF | business_daily | 1 | 10 | OK | live | OK | OK |
| CAD | FX | frankfurter | 255 | 2025-05-26 | 2026-05-25 | 1.3813 | CAD | business_daily | 1 | 10 | OK | live | OK | OK |
| SELIC_DAILY | MACRO | bcb_sgs | 252 | 2025-05-26 | 2026-05-25 | 0.0534 | % a.d. | business_daily | 1 | 10 | OK | live | OK | OK |
| CDI_DAILY | MACRO | bcb_sgs | 251 | 2025-05-26 | 2026-05-22 | 0.0534 | % a.d. | business_daily | 4 | 10 | OK | live | OK | OK |
| IPCA_MONTHLY | MACRO | bcb_sgs | 12 | 2025-05-01 | 2026-04-01 | 0.67 | % a.m. | monthly | 55 | 75 | OK | live | OK | OK |
| AAPL | STOCK | twelvedata | 250 | 2025-05-27 | 2026-05-22 | 308.82 | USD | business_daily | 4 | 10 | OK | live | OK | OK |
| MSFT | STOCK | twelvedata | 250 | 2025-05-27 | 2026-05-22 | 418.57 | USD | business_daily | 4 | 10 | OK | live | OK | OK |
| NVDA | STOCK | twelvedata | 250 | 2025-05-27 | 2026-05-22 | 215.33 | USD | business_daily | 4 | 10 | OK | live | OK | OK |
| AMZN | STOCK | twelvedata | 250 | 2025-05-27 | 2026-05-22 | 266.32 | USD | business_daily | 4 | 10 | OK | live | OK | OK |
| GOOGL | STOCK | twelvedata | 250 | 2025-05-27 | 2026-05-22 | 382.97 | USD | business_daily | 4 | 10 | OK | live | OK | OK |
| META | STOCK | twelvedata | 250 | 2025-05-27 | 2026-05-22 | 610.26 | USD | business_daily | 4 | 10 | OK | live | OK | OK |
| TSLA | STOCK | twelvedata | 250 | 2025-05-27 | 2026-05-22 | 426.01 | USD | business_daily | 4 | 10 | OK | live | OK | OK |
| JPM | STOCK | twelvedata | 250 | 2025-05-27 | 2026-05-22 | 306.38 | USD | business_daily | 4 | 10 | OK | live | OK | OK |
| KO | STOCK | twelvedata | 250 | 2025-05-27 | 2026-05-22 | 81.48 | USD | business_daily | 4 | 10 | OK | live | OK | OK |
| AMD | STOCK | twelvedata | 250 | 2025-05-27 | 2026-05-22 | 467.51 | USD | business_daily | 4 | 10 | OK | live | OK | OK |

## Critical Failures

- none

## Warnings

- none

## Notes

- This report is generated from a temporary live-test SQLite database.
- Demo rows are not accepted in this validation path.
- API keys are never written to this report.
