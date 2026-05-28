# Live-First Build Report

Generated: 2026-05-26T01:16:11.797330+00:00
Final status: **CANDIDATE_READY**

## Scope

| asset_type | symbols |
|---|---|
| FX | BRL, EUR, GBP, JPY, CAD, CHF |
| CRYPTO | BTC, ETH, BNB, SOL, XRP |
| STOCK | AAPL, MSFT, NVDA, AMZN, GOOGL, META, TSLA, JPM, KO, AMD |
| MACRO | SELIC_DAILY, CDI_DAILY, IPCA_MONTHLY |

## Providers

| asset_type | provider | configured | available | external_test | status | message |
|---|---|---:|---:|---|---|---|
| FX | frankfurter | True | True | pass | configured | - |
| CRYPTO | coingecko | True | True | pass | configured | - |
| STOCK | twelvedata | True | True | pass | configured | - |
| MACRO | bcb_sgs | True | True | pass | configured | - |

## Asset Attempts

| asset_type | status | exit_code | symbols | message |
|---|---|---:|---|---|
| FX | OK | 0 | BRL,EUR,GBP,JPY,CAD,CHF | - |
| CRYPTO | OK | 0 | BTC,ETH,BNB,SOL,XRP | - |
| STOCK | OK | 0 | AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA,JPM,KO,AMD | - |
| MACRO | OK | 0 | SELIC_DAILY,CDI_DAILY,IPCA_MONTHLY | - |

## Candidate DB

- path: `C:\Projetos_Local\rates-sqlite-powerbi-git\.tmp\live-main-candidate.sqlite`
- data_mode: `live`
- instruments: `24`
- historical rows: `6370`
- date_min: `2025-05-01`
- date_max: `2026-05-25`
- providers: `bcb_sgs, coingecko, frankfurter, twelvedata`
- sample_validation_required: `true`
- sample_validation_status: `NOT_RUN`
- next command: `python -m fx_rates dashboard validate-samples --db-path .tmp/live-main-candidate.sqlite --samples-per-symbol 5 --external-test`

## Validation

- status: `OK`
- critical failures: none
- warnings: none

## Safety

- This command builds a new candidate DB and does not import demo rows.
- API keys are never written to this report.
