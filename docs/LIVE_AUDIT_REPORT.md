# Live Audit Report

Generated: 2026-06-05T20:36:02.481827+00:00
DB: `C:\Projetos_Local\rates-sqlite-powerbi-git\data\fx.sqlite`
Overall status: **OK**

## Summary

- data_mode: `live`
- providers: `bcb_sgs, coingecko, frankfurter, twelvedata`
- instruments: `24`
- historical rows: `6368`
- date_min: `2025-06-01`
- date_max: `2026-06-03`
- history_mode: `standard`
- requested_days: `365`
- advanced_history_enabled: `false`
- advanced_history_max_years: `10`

## Symbol Validation

| symbol | asset_type | provider | rows | date_min | date_max | latest_value | unit_label | expected_frequency | stale_days | allowed_stale_days | stale_status | data_mode | status | notes |
|---|---|---:|---:|---|---|---:|---|---|---:|---:|---|---|---|---|
| BTC | CRYPTO | coingecko | 365 | 2025-06-04 | 2026-06-03 | 66649.8556 | USD | daily | 2 | 3 | OK | live | OK | OK |
| ETH | CRYPTO | coingecko | 365 | 2025-06-04 | 2026-06-03 | 1856.0538 | USD | daily | 2 | 3 | OK | live | OK | OK |
| BNB | CRYPTO | coingecko | 365 | 2025-06-04 | 2026-06-03 | 650.1158 | USD | daily | 2 | 3 | OK | live | OK | OK |
| SOL | CRYPTO | coingecko | 365 | 2025-06-04 | 2026-06-03 | 73.9713 | USD | daily | 2 | 3 | OK | live | OK | OK |
| XRP | CRYPTO | coingecko | 365 | 2025-06-04 | 2026-06-03 | 1.2085 | USD | daily | 2 | 3 | OK | live | OK | OK |
| BRL | FX | frankfurter | 255 | 2025-06-04 | 2026-06-03 | 5.0184 | BRL | business_daily | 2 | 10 | OK | live | OK | OK |
| EUR | FX | frankfurter | 255 | 2025-06-04 | 2026-06-03 | 0.86103 | EUR | business_daily | 2 | 10 | OK | live | OK | OK |
| GBP | FX | frankfurter | 255 | 2025-06-04 | 2026-06-03 | 0.74367 | GBP | business_daily | 2 | 10 | OK | live | OK | OK |
| JPY | FX | frankfurter | 255 | 2025-06-04 | 2026-06-03 | 159.86 | JPY | business_daily | 2 | 10 | OK | live | OK | OK |
| CHF | FX | frankfurter | 255 | 2025-06-04 | 2026-06-03 | 0.78931 | CHF | business_daily | 2 | 10 | OK | live | OK | OK |
| CAD | FX | frankfurter | 255 | 2025-06-04 | 2026-06-03 | 1.3857 | CAD | business_daily | 2 | 10 | OK | live | OK | OK |
| SELIC_DAILY | MACRO | bcb_sgs | 251 | 2025-06-04 | 2026-06-02 | 0.0534 | % a.d. | business_daily | 3 | 10 | OK | live | OK | OK |
| CDI_DAILY | MACRO | bcb_sgs | 251 | 2025-06-04 | 2026-06-02 | 0.0534 | % a.d. | business_daily | 3 | 10 | OK | live | OK | OK |
| IPCA_MONTHLY | MACRO | bcb_sgs | 11 | 2025-06-01 | 2026-04-01 | 0.67 | % a.m. | monthly | 65 | 75 | OK | live | OK | monthly macro series validated by monthly point count and stale window |
| AAPL | STOCK | twelvedata | 250 | 2025-06-04 | 2026-06-02 | 315.2 | USD | business_daily | 3 | 10 | OK | live | OK | OK |
| MSFT | STOCK | twelvedata | 250 | 2025-06-04 | 2026-06-02 | 441.31 | USD | business_daily | 3 | 10 | OK | live | OK | OK |
| NVDA | STOCK | twelvedata | 250 | 2025-06-04 | 2026-06-02 | 222.82 | USD | business_daily | 3 | 10 | OK | live | OK | OK |
| AMZN | STOCK | twelvedata | 250 | 2025-06-04 | 2026-06-02 | 256.52 | USD | business_daily | 3 | 10 | OK | live | OK | OK |
| GOOGL | STOCK | twelvedata | 250 | 2025-06-04 | 2026-06-02 | 361.85 | USD | business_daily | 3 | 10 | OK | live | OK | OK |
| META | STOCK | twelvedata | 250 | 2025-06-04 | 2026-06-02 | 597.63 | USD | business_daily | 3 | 10 | OK | live | OK | OK |
| TSLA | STOCK | twelvedata | 250 | 2025-06-04 | 2026-06-02 | 423.74 | USD | business_daily | 3 | 10 | OK | live | OK | OK |
| JPM | STOCK | twelvedata | 250 | 2025-06-04 | 2026-06-02 | 300.96 | USD | business_daily | 3 | 10 | OK | live | OK | OK |
| KO | STOCK | twelvedata | 250 | 2025-06-04 | 2026-06-02 | 78.41 | USD | business_daily | 3 | 10 | OK | live | OK | OK |
| AMD | STOCK | twelvedata | 250 | 2025-06-04 | 2026-06-02 | 521.54 | USD | business_daily | 3 | 10 | OK | live | OK | OK |

## Critical Failures

- none

## Warnings

- none

## Notes

- This report is generated from a temporary live-test SQLite database.
- Demo rows are not accepted in this validation path.
- API keys are never written to this report.
