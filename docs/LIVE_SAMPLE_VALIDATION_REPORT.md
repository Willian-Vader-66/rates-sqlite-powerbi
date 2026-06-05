# Live Sample Validation Report

Generated: 2026-06-03T19:28:17.118768+00:00
DB: `C:\Projetos_Local\rates-sqlite-powerbi-git\.tmp\live-main-candidate.sqlite`
Overall status: **READY_WITH_WARNINGS**
Recommendation: **READY_WITH_WARNINGS**
requested_period_days: `365`
history_mode: `standard`
advanced_history_available: `false`
Samples OK/WARN/FAIL: `246/11/0`
release_gate: `PASS_WITH_WARNINGS`
promotion_allowed: `true`
reason_codes: `HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE, HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE, CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST, HISTORICAL_SAMPLE_NEAREST_DATE_WARN, EXTERNAL_RATE_LIMIT, VALIDATION_OK`

## Readiness

- reason: `EXTERNAL_RATE_LIMIT`
- action: `Provider rate limit detected; internal candidate remains valid but external confirmation is incomplete.`

## Internal Sample Validation

- total samples: `120`
- OK: `120`
- WARN: `0`
- FAIL: `0`
- duplicate count: `0`
- invalid price count: `0`
- future date count: `0`
- stale count: `0`
- insufficient history count: `0`

## External Provider Sample Validation

- provider calls attempted: `45`
- OK: `126`
- WARN: `11`
- FAIL: `0`
- RATE_LIMIT: `2`
- SKIPPED: `0`
- provider failures: `0`
- transient failures: `2`

## Data Decision

- release_gate: `PASS_WITH_WARNINGS`
- promotion_allowed: `true`
- recommendation: `READY_WITH_WARNINGS`

## Result By Asset Type

| asset_type | OK | WARN | FAIL |
|---|---:|---:|---:|
| CRYPTO | 55 | 0 | 0 |
| FX | 66 | 0 | 0 |
| MACRO | 30 | 0 | 0 |
| STOCK | 95 | 11 | 0 |

## Samples

| scope | symbol | asset_type | provider | endpoint | sample_date | provider_date | db_value | provider_value | delta_pct | tolerance_pct | status | reason_code | note |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---|---|---|
| internal | BTC | CRYPTO | coingecko | internal sqlite | 2025-06-04 | 2025-06-04 | 105434.47745144971 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | BTC | CRYPTO | coingecko | internal sqlite | 2025-09-03 | 2025-09-03 | 111190.18209845416 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | BTC | CRYPTO | coingecko | internal sqlite | 2025-12-03 | 2025-12-03 | 91344.73275150165 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | BTC | CRYPTO | coingecko | internal sqlite | 2026-03-04 | 2026-03-04 | 68321.6178484252 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | BTC | CRYPTO | coingecko | internal sqlite | 2026-06-03 | 2026-06-03 | 66649.85561928031 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | ETH | CRYPTO | coingecko | internal sqlite | 2025-06-04 | 2025-06-04 | 2595.4689570062997 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | ETH | CRYPTO | coingecko | internal sqlite | 2025-09-03 | 2025-09-03 | 4325.856306225941 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | ETH | CRYPTO | coingecko | internal sqlite | 2025-12-03 | 2025-12-03 | 2995.751149322771 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | ETH | CRYPTO | coingecko | internal sqlite | 2026-03-04 | 2026-03-04 | 1982.4582786533451 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | ETH | CRYPTO | coingecko | internal sqlite | 2026-06-03 | 2026-06-03 | 1856.053796035668 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | BNB | CRYPTO | coingecko | internal sqlite | 2025-06-04 | 2025-06-04 | 661.17332497159 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | BNB | CRYPTO | coingecko | internal sqlite | 2025-09-03 | 2025-09-03 | 851.6937445280856 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | BNB | CRYPTO | coingecko | internal sqlite | 2025-12-03 | 2025-12-03 | 876.7469880502844 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | BNB | CRYPTO | coingecko | internal sqlite | 2026-03-04 | 2026-03-04 | 633.9153172943332 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | BNB | CRYPTO | coingecko | internal sqlite | 2026-06-03 | 2026-06-03 | 650.1158236153033 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | SOL | CRYPTO | coingecko | internal sqlite | 2025-06-04 | 2025-06-04 | 155.57609294535118 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | SOL | CRYPTO | coingecko | internal sqlite | 2025-09-03 | 2025-09-03 | 209.23036715955652 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | SOL | CRYPTO | coingecko | internal sqlite | 2025-12-03 | 2025-12-03 | 138.68381774415911 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | SOL | CRYPTO | coingecko | internal sqlite | 2026-03-04 | 2026-03-04 | 87.21330452440068 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | SOL | CRYPTO | coingecko | internal sqlite | 2026-06-03 | 2026-06-03 | 73.97134364162103 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | XRP | CRYPTO | coingecko | internal sqlite | 2025-06-04 | 2025-06-04 | 2.2486280245071963 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | XRP | CRYPTO | coingecko | internal sqlite | 2025-09-03 | 2025-09-03 | 2.861352340590463 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | XRP | CRYPTO | coingecko | internal sqlite | 2025-12-03 | 2025-12-03 | 2.1570887957334866 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | XRP | CRYPTO | coingecko | internal sqlite | 2026-03-04 | 2026-03-04 | 1.36103107458941 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | XRP | CRYPTO | coingecko | internal sqlite | 2026-06-03 | 2026-06-03 | 1.2084998063574466 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | BRL | FX | frankfurter | internal sqlite | 2025-06-04 | 2025-06-04 | 5.6221 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | BRL | FX | frankfurter | internal sqlite | 2025-09-02 | 2025-09-02 | 5.4746 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | BRL | FX | frankfurter | internal sqlite | 2025-11-28 | 2025-11-28 | 5.3391 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | BRL | FX | frankfurter | internal sqlite | 2026-03-02 | 2026-03-02 | 5.2067 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | BRL | FX | frankfurter | internal sqlite | 2026-06-03 | 2026-06-03 | 5.0184 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | EUR | FX | frankfurter | internal sqlite | 2025-06-04 | 2025-06-04 | 0.87843 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | EUR | FX | frankfurter | internal sqlite | 2025-09-02 | 2025-09-02 | 0.85866 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | EUR | FX | frankfurter | internal sqlite | 2025-11-28 | 2025-11-28 | 0.8646 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | EUR | FX | frankfurter | internal sqlite | 2026-03-02 | 2026-03-02 | 0.85485 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | EUR | FX | frankfurter | internal sqlite | 2026-06-03 | 2026-06-03 | 0.86103 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | GBP | FX | frankfurter | internal sqlite | 2025-06-04 | 2025-06-04 | 0.73972 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | GBP | FX | frankfurter | internal sqlite | 2025-09-02 | 2025-09-02 | 0.74721 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | GBP | FX | frankfurter | internal sqlite | 2025-11-28 | 2025-11-28 | 0.7567 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | GBP | FX | frankfurter | internal sqlite | 2026-03-02 | 2026-03-02 | 0.74705 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | GBP | FX | frankfurter | internal sqlite | 2026-06-03 | 2026-06-03 | 0.74367 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | JPY | FX | frankfurter | internal sqlite | 2025-06-04 | 2025-06-04 | 144.19 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | JPY | FX | frankfurter | internal sqlite | 2025-09-02 | 2025-09-02 | 148.63 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | JPY | FX | frankfurter | internal sqlite | 2025-11-28 | 2025-11-28 | 156.12 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | JPY | FX | frankfurter | internal sqlite | 2026-03-02 | 2026-03-02 | 157.45 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | JPY | FX | frankfurter | internal sqlite | 2026-06-03 | 2026-06-03 | 159.86 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CHF | FX | frankfurter | internal sqlite | 2025-06-04 | 2025-06-04 | 0.82309 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CHF | FX | frankfurter | internal sqlite | 2025-09-02 | 2025-09-02 | 0.80422 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CHF | FX | frankfurter | internal sqlite | 2025-11-28 | 2025-11-28 | 0.80564 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CHF | FX | frankfurter | internal sqlite | 2026-03-02 | 2026-03-02 | 0.77936 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CHF | FX | frankfurter | internal sqlite | 2026-06-03 | 2026-06-03 | 0.78931 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CAD | FX | frankfurter | internal sqlite | 2025-06-04 | 2025-06-04 | 1.3704 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CAD | FX | frankfurter | internal sqlite | 2025-09-02 | 2025-09-02 | 1.3787 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CAD | FX | frankfurter | internal sqlite | 2025-11-28 | 2025-11-28 | 1.4015 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CAD | FX | frankfurter | internal sqlite | 2026-03-02 | 2026-03-02 | 1.367 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CAD | FX | frankfurter | internal sqlite | 2026-06-03 | 2026-06-03 | 1.3857 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | SELIC_DAILY | MACRO | bcb_sgs | internal sqlite | 2025-06-04 | 2025-06-04 | 0.054266 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | SELIC_DAILY | MACRO | bcb_sgs | internal sqlite | 2025-09-01 | 2025-09-01 | 0.055131 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | SELIC_DAILY | MACRO | bcb_sgs | internal sqlite | 2025-11-28 | 2025-11-28 | 0.055131 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | SELIC_DAILY | MACRO | bcb_sgs | internal sqlite | 2026-03-03 | 2026-03-03 | 0.055131 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | SELIC_DAILY | MACRO | bcb_sgs | internal sqlite | 2026-06-02 | 2026-06-02 | 0.0534 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CDI_DAILY | MACRO | bcb_sgs | internal sqlite | 2025-06-04 | 2025-06-04 | 0.054266 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CDI_DAILY | MACRO | bcb_sgs | internal sqlite | 2025-09-01 | 2025-09-01 | 0.055131 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CDI_DAILY | MACRO | bcb_sgs | internal sqlite | 2025-11-28 | 2025-11-28 | 0.055131 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CDI_DAILY | MACRO | bcb_sgs | internal sqlite | 2026-03-03 | 2026-03-03 | 0.055131 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | CDI_DAILY | MACRO | bcb_sgs | internal sqlite | 2026-06-02 | 2026-06-02 | 0.0534 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | IPCA_MONTHLY | MACRO | bcb_sgs | internal sqlite | 2025-06-01 | 2025-06-01 | 0.24 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | IPCA_MONTHLY | MACRO | bcb_sgs | internal sqlite | 2025-08-01 | 2025-08-01 | -0.11 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | IPCA_MONTHLY | MACRO | bcb_sgs | internal sqlite | 2025-11-01 | 2025-11-01 | 0.18 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | IPCA_MONTHLY | MACRO | bcb_sgs | internal sqlite | 2026-02-01 | 2026-02-01 | 0.7 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | IPCA_MONTHLY | MACRO | bcb_sgs | internal sqlite | 2026-04-01 | 2026-04-01 | 0.67 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AAPL | STOCK | twelvedata | internal sqlite | 2025-06-04 | 2025-06-04 | 202.82001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AAPL | STOCK | twelvedata | internal sqlite | 2025-09-03 | 2025-09-03 | 238.47 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AAPL | STOCK | twelvedata | internal sqlite | 2025-12-01 | 2025-12-01 | 283.10001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AAPL | STOCK | twelvedata | internal sqlite | 2026-03-04 | 2026-03-04 | 262.51999 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AAPL | STOCK | twelvedata | internal sqlite | 2026-06-02 | 2026-06-02 | 315.20001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | MSFT | STOCK | twelvedata | internal sqlite | 2025-06-04 | 2025-06-04 | 463.87 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | MSFT | STOCK | twelvedata | internal sqlite | 2025-09-03 | 2025-09-03 | 505.35001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | MSFT | STOCK | twelvedata | internal sqlite | 2025-12-01 | 2025-12-01 | 486.73999 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | MSFT | STOCK | twelvedata | internal sqlite | 2026-03-04 | 2026-03-04 | 405.20001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | MSFT | STOCK | twelvedata | internal sqlite | 2026-06-02 | 2026-06-02 | 441.31 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | NVDA | STOCK | twelvedata | internal sqlite | 2025-06-04 | 2025-06-04 | 141.92 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | NVDA | STOCK | twelvedata | internal sqlite | 2025-09-03 | 2025-09-03 | 170.62 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | NVDA | STOCK | twelvedata | internal sqlite | 2025-12-01 | 2025-12-01 | 179.92 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | NVDA | STOCK | twelvedata | internal sqlite | 2026-03-04 | 2026-03-04 | 183.039993 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | NVDA | STOCK | twelvedata | internal sqlite | 2026-06-02 | 2026-06-02 | 222.82001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AMZN | STOCK | twelvedata | internal sqlite | 2025-06-04 | 2025-06-04 | 207.23 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AMZN | STOCK | twelvedata | internal sqlite | 2025-09-03 | 2025-09-03 | 225.99001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AMZN | STOCK | twelvedata | internal sqlite | 2025-12-01 | 2025-12-01 | 233.88 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AMZN | STOCK | twelvedata | internal sqlite | 2026-03-04 | 2026-03-04 | 216.82001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AMZN | STOCK | twelvedata | internal sqlite | 2026-06-02 | 2026-06-02 | 256.51999 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | GOOGL | STOCK | twelvedata | internal sqlite | 2025-06-04 | 2025-06-04 | 168.050003 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | GOOGL | STOCK | twelvedata | internal sqlite | 2025-09-03 | 2025-09-03 | 230.66 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | GOOGL | STOCK | twelvedata | internal sqlite | 2025-12-01 | 2025-12-01 | 314.89001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | GOOGL | STOCK | twelvedata | internal sqlite | 2026-03-04 | 2026-03-04 | 303.13 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | GOOGL | STOCK | twelvedata | internal sqlite | 2026-06-02 | 2026-06-02 | 361.85001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | META | STOCK | twelvedata | internal sqlite | 2025-06-04 | 2025-06-04 | 687.95001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | META | STOCK | twelvedata | internal sqlite | 2025-09-03 | 2025-09-03 | 737.049988 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | META | STOCK | twelvedata | internal sqlite | 2025-12-01 | 2025-12-01 | 640.87 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | META | STOCK | twelvedata | internal sqlite | 2026-03-04 | 2026-03-04 | 667.72998 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | META | STOCK | twelvedata | internal sqlite | 2026-06-02 | 2026-06-02 | 597.63 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | TSLA | STOCK | twelvedata | internal sqlite | 2025-06-04 | 2025-06-04 | 332.049988 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | TSLA | STOCK | twelvedata | internal sqlite | 2025-09-03 | 2025-09-03 | 334.089996 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | TSLA | STOCK | twelvedata | internal sqlite | 2025-12-01 | 2025-12-01 | 430.14001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | TSLA | STOCK | twelvedata | internal sqlite | 2026-03-04 | 2026-03-04 | 405.94 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | TSLA | STOCK | twelvedata | internal sqlite | 2026-06-02 | 2026-06-02 | 423.73999 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | JPM | STOCK | twelvedata | internal sqlite | 2025-06-04 | 2025-06-04 | 264.22 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | JPM | STOCK | twelvedata | internal sqlite | 2025-09-03 | 2025-09-03 | 299.51001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | JPM | STOCK | twelvedata | internal sqlite | 2025-12-01 | 2025-12-01 | 308.92001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | JPM | STOCK | twelvedata | internal sqlite | 2026-03-04 | 2026-03-04 | 299.39001 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | JPM | STOCK | twelvedata | internal sqlite | 2026-06-02 | 2026-06-02 | 300.95999 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | KO | STOCK | twelvedata | internal sqlite | 2025-06-04 | 2025-06-04 | 71.37 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | KO | STOCK | twelvedata | internal sqlite | 2025-09-03 | 2025-09-03 | 68.99 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | KO | STOCK | twelvedata | internal sqlite | 2025-12-01 | 2025-12-01 | 71.95 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | KO | STOCK | twelvedata | internal sqlite | 2026-03-04 | 2026-03-04 | 78.099998 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | KO | STOCK | twelvedata | internal sqlite | 2026-06-02 | 2026-06-02 | 78.41 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AMD | STOCK | twelvedata | internal sqlite | 2025-06-04 | 2025-06-04 | 118.58 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AMD | STOCK | twelvedata | internal sqlite | 2025-09-03 | 2025-09-03 | 162.13 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AMD | STOCK | twelvedata | internal sqlite | 2025-12-01 | 2025-12-01 | 219.75999 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AMD | STOCK | twelvedata | internal sqlite | 2026-03-04 | 2026-03-04 | 202.070007 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| internal | AMD | STOCK | twelvedata | internal sqlite | 2026-06-02 | 2026-06-02 | 521.53998 | - | - | - | OK | HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE | internal DB history sample valid; old historical point not compared to current price |
| external | BTC | CRYPTO | coingecko | coins/bitcoin/market_chart/range | 2025-06-04 | 2025-06-04 | 105434.47745144971 | 105434.47745144971 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BTC | CRYPTO | coingecko | coins/bitcoin/market_chart/range | 2025-09-03 | 2025-09-03 | 111190.18209845416 | 111190.18209845416 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BTC | CRYPTO | coingecko | coins/bitcoin/market_chart/range | 2025-12-03 | 2025-12-03 | 91344.73275150165 | 91344.73275150165 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BTC | CRYPTO | coingecko | coins/bitcoin/market_chart/range | 2026-03-04 | 2026-03-04 | 68321.6178484252 | 68321.6178484252 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BTC | CRYPTO | coingecko | coins/bitcoin/market_chart/range | 2026-06-03 | 2026-06-03 | 66649.85561928031 | 66649.85561928031 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BTC | CRYPTO | coingecko | coingecko simple/price | 2026-06-03 | 2026-06-03T19:24:31.614310+00:00 | 66649.8556 | 65685.0 | 1.468913 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | ETH | CRYPTO | coingecko | coins/ethereum/market_chart/range | 2025-06-04 | 2025-06-04 | 2595.4689570062997 | 2595.4689570062997 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | ETH | CRYPTO | coingecko | coins/ethereum/market_chart/range | 2025-09-03 | 2025-09-03 | 4325.856306225941 | 4325.856306225941 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | ETH | CRYPTO | coingecko | coins/ethereum/market_chart/range | 2025-12-03 | 2025-12-03 | 2995.751149322771 | 2995.751149322771 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | ETH | CRYPTO | coingecko | coins/ethereum/market_chart/range | 2026-03-04 | 2026-03-04 | 1982.4582786533451 | 1982.4582786533451 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | ETH | CRYPTO | coingecko | coins/ethereum/market_chart/range | 2026-06-03 | 2026-06-03 | 1856.053796035668 | 1856.053796035668 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | ETH | CRYPTO | coingecko | coingecko simple/price | 2026-06-03 | 2026-06-03T19:24:32.790727+00:00 | 1856.0538 | 1816.26 | 2.190975 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | BNB | CRYPTO | coingecko | coins/binancecoin/market_chart/range | 2025-06-04 | 2025-06-04 | 661.17332497159 | 661.17332497159 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BNB | CRYPTO | coingecko | coins/binancecoin/market_chart/range | 2025-09-03 | 2025-09-03 | 851.6937445280856 | 851.6937445280856 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BNB | CRYPTO | coingecko | coins/binancecoin/market_chart/range | 2025-12-03 | 2025-12-03 | 876.7469880502844 | 876.7469880502844 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BNB | CRYPTO | coingecko | coins/binancecoin/market_chart/range | 2026-03-04 | 2026-03-04 | 633.9153172943332 | 633.9153172943332 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BNB | CRYPTO | coingecko | coins/binancecoin/market_chart/range | 2026-06-03 | 2026-06-03 | 650.1158236153033 | 650.1158236153033 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BNB | CRYPTO | coingecko | coingecko simple/price | 2026-06-03 | 2026-06-03T19:25:39.982118+00:00 | 650.1158 | 624.9 | 4.035174 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | SOL | CRYPTO | coingecko | coins/solana/market_chart/range | 2025-06-04 | 2025-06-04 | 155.57609294535118 | 155.57609294535118 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | SOL | CRYPTO | coingecko | coins/solana/market_chart/range | 2025-09-03 | 2025-09-03 | 209.23036715955652 | 209.23036715955652 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | SOL | CRYPTO | coingecko | coins/solana/market_chart/range | 2025-12-03 | 2025-12-03 | 138.68381774415911 | 138.68381774415911 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | SOL | CRYPTO | coingecko | coins/solana/market_chart/range | 2026-03-04 | 2026-03-04 | 87.21330452440068 | 87.21330452440068 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | SOL | CRYPTO | coingecko | coins/solana/market_chart/range | 2026-06-03 | 2026-06-03 | 73.97134364162103 | 73.97134364162103 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | SOL | CRYPTO | coingecko | coingecko simple/price | 2026-06-03 | 2026-06-03T19:26:47.164815+00:00 | 73.9713 | 72.16 | 2.510116 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | XRP | CRYPTO | coingecko | coins/ripple/market_chart/range | 2025-06-04 | 2025-06-04 | 2.2486280245071963 | 2.2486280245071963 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | XRP | CRYPTO | coingecko | coins/ripple/market_chart/range | 2025-09-03 | 2025-09-03 | 2.861352340590463 | 2.861352340590463 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | XRP | CRYPTO | coingecko | coins/ripple/market_chart/range | 2025-12-03 | 2025-12-03 | 2.1570887957334866 | 2.1570887957334866 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | XRP | CRYPTO | coingecko | coins/ripple/market_chart/range | 2026-03-04 | 2026-03-04 | 1.36103107458941 | 1.36103107458941 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | XRP | CRYPTO | coingecko | coins/ripple/market_chart/range | 2026-06-03 | 2026-06-03 | 1.2084998063574466 | 1.2084998063574466 | 0.0 | 3.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | XRP | CRYPTO | coingecko | coingecko simple/price | 2026-06-03 | 2026-06-03T19:26:48.288138+00:00 | 1.2085 | 1.21 | 0.123967 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | BRL | FX | frankfurter | frankfurter timeseries | 2025-06-04 | 2025-06-04 | 5.6221 | 5.6221 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BRL | FX | frankfurter | frankfurter timeseries | 2025-09-02 | 2025-09-02 | 5.4746 | 5.4746 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BRL | FX | frankfurter | frankfurter timeseries | 2025-11-28 | 2025-11-28 | 5.3391 | 5.3391 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BRL | FX | frankfurter | frankfurter timeseries | 2026-03-02 | 2026-03-02 | 5.2067 | 5.2067 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BRL | FX | frankfurter | frankfurter timeseries | 2026-06-03 | 2026-06-03 | 5.0184 | 5.0184 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | BRL | FX | frankfurter | frankfurter latest | 2026-06-03 | 2026-06-03 | 5.0184 | 5.0184 | 0.0 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | EUR | FX | frankfurter | frankfurter timeseries | 2025-06-04 | 2025-06-04 | 0.87843 | 0.87843 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | EUR | FX | frankfurter | frankfurter timeseries | 2025-09-02 | 2025-09-02 | 0.85866 | 0.85866 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | EUR | FX | frankfurter | frankfurter timeseries | 2025-11-28 | 2025-11-28 | 0.8646 | 0.8646 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | EUR | FX | frankfurter | frankfurter timeseries | 2026-03-02 | 2026-03-02 | 0.85485 | 0.85485 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | EUR | FX | frankfurter | frankfurter timeseries | 2026-06-03 | 2026-06-03 | 0.86103 | 0.86103 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | EUR | FX | frankfurter | frankfurter latest | 2026-06-03 | 2026-06-03 | 0.86103 | 0.86103 | 0.0 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | GBP | FX | frankfurter | frankfurter timeseries | 2025-06-04 | 2025-06-04 | 0.73972 | 0.73972 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | GBP | FX | frankfurter | frankfurter timeseries | 2025-09-02 | 2025-09-02 | 0.74721 | 0.74721 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | GBP | FX | frankfurter | frankfurter timeseries | 2025-11-28 | 2025-11-28 | 0.7567 | 0.7567 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | GBP | FX | frankfurter | frankfurter timeseries | 2026-03-02 | 2026-03-02 | 0.74705 | 0.74705 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | GBP | FX | frankfurter | frankfurter timeseries | 2026-06-03 | 2026-06-03 | 0.74367 | 0.74367 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | GBP | FX | frankfurter | frankfurter latest | 2026-06-03 | 2026-06-03 | 0.74367 | 0.74367 | 0.0 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | JPY | FX | frankfurter | frankfurter timeseries | 2025-06-04 | 2025-06-04 | 144.19 | 144.19 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | JPY | FX | frankfurter | frankfurter timeseries | 2025-09-02 | 2025-09-02 | 148.63 | 148.63 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | JPY | FX | frankfurter | frankfurter timeseries | 2025-11-28 | 2025-11-28 | 156.12 | 156.12 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | JPY | FX | frankfurter | frankfurter timeseries | 2026-03-02 | 2026-03-02 | 157.45 | 157.45 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | JPY | FX | frankfurter | frankfurter timeseries | 2026-06-03 | 2026-06-03 | 159.86 | 159.86 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | JPY | FX | frankfurter | frankfurter latest | 2026-06-03 | 2026-06-03 | 159.86 | 159.86 | 0.0 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | CHF | FX | frankfurter | frankfurter timeseries | 2025-06-04 | 2025-06-04 | 0.82309 | 0.82309 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CHF | FX | frankfurter | frankfurter timeseries | 2025-09-02 | 2025-09-02 | 0.80422 | 0.80422 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CHF | FX | frankfurter | frankfurter timeseries | 2025-11-28 | 2025-11-28 | 0.80564 | 0.80564 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CHF | FX | frankfurter | frankfurter timeseries | 2026-03-02 | 2026-03-02 | 0.77936 | 0.77936 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CHF | FX | frankfurter | frankfurter timeseries | 2026-06-03 | 2026-06-03 | 0.78931 | 0.78931 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CHF | FX | frankfurter | frankfurter latest | 2026-06-03 | 2026-06-03 | 0.78931 | 0.78931 | 0.0 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | CAD | FX | frankfurter | frankfurter timeseries | 2025-06-04 | 2025-06-04 | 1.3704 | 1.3704 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CAD | FX | frankfurter | frankfurter timeseries | 2025-09-02 | 2025-09-02 | 1.3787 | 1.3787 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CAD | FX | frankfurter | frankfurter timeseries | 2025-11-28 | 2025-11-28 | 1.4015 | 1.4015 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CAD | FX | frankfurter | frankfurter timeseries | 2026-03-02 | 2026-03-02 | 1.367 | 1.367 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CAD | FX | frankfurter | frankfurter timeseries | 2026-06-03 | 2026-06-03 | 1.3857 | 1.3857 | 0.0 | 0.5 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CAD | FX | frankfurter | frankfurter latest | 2026-06-03 | 2026-06-03 | 1.3857 | 1.3857 | 0.0 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | SELIC_DAILY | MACRO | bcb_sgs | bcb_sgs 11 | 2025-06-04 | 2025-06-04 | 0.054266 | 0.054266 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | SELIC_DAILY | MACRO | bcb_sgs | bcb_sgs 11 | 2025-09-01 | 2025-09-01 | 0.055131 | 0.055131 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | SELIC_DAILY | MACRO | bcb_sgs | bcb_sgs 11 | 2025-11-28 | 2025-11-28 | 0.055131 | 0.055131 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | SELIC_DAILY | MACRO | bcb_sgs | bcb_sgs 11 | 2026-03-03 | 2026-03-03 | 0.055131 | 0.055131 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | SELIC_DAILY | MACRO | bcb_sgs | bcb_sgs 11 | 2026-06-02 | 2026-06-02 | 0.0534 | 0.0534 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CDI_DAILY | MACRO | bcb_sgs | bcb_sgs 12 | 2025-06-04 | 2025-06-04 | 0.054266 | 0.054266 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CDI_DAILY | MACRO | bcb_sgs | bcb_sgs 12 | 2025-09-01 | 2025-09-01 | 0.055131 | 0.055131 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CDI_DAILY | MACRO | bcb_sgs | bcb_sgs 12 | 2025-11-28 | 2025-11-28 | 0.055131 | 0.055131 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CDI_DAILY | MACRO | bcb_sgs | bcb_sgs 12 | 2026-03-03 | 2026-03-03 | 0.055131 | 0.055131 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | CDI_DAILY | MACRO | bcb_sgs | bcb_sgs 12 | 2026-06-02 | 2026-06-02 | 0.0534 | 0.0534 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | IPCA_MONTHLY | MACRO | bcb_sgs | bcb_sgs 433 | 2025-06-01 | 2025-06-01 | 0.24 | 0.24 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | IPCA_MONTHLY | MACRO | bcb_sgs | bcb_sgs 433 | 2025-08-01 | 2025-08-01 | -0.11 | -0.11 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | IPCA_MONTHLY | MACRO | bcb_sgs | bcb_sgs 433 | 2025-11-01 | 2025-11-01 | 0.18 | 0.18 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | IPCA_MONTHLY | MACRO | bcb_sgs | bcb_sgs 433 | 2026-02-01 | 2026-02-01 | 0.7 | 0.7 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | IPCA_MONTHLY | MACRO | bcb_sgs | bcb_sgs 433 | 2026-04-01 | 2026-04-01 | 0.67 | 0.67 | 0.0 | 1.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | AAPL | STOCK | twelvedata | twelvedata time_series | 2025-06-04 | 2025-06-04 | 202.82001 | 202.82001 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | AAPL | STOCK | twelvedata | twelvedata time_series | 2025-09-03 | 2025-09-03 | 238.47 | 238.47 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | AAPL | STOCK | twelvedata | twelvedata time_series | 2025-12-01 | 2025-12-01 | 283.10001 | 283.10001 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | AAPL | STOCK | twelvedata | twelvedata time_series | 2026-03-04 | 2026-03-04 | 262.51999 | 262.51999 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | AAPL | STOCK | twelvedata | twelvedata time_series | 2026-06-02 | 2026-06-01 | 315.20001 | 306.31 | 2.902292 | 2.0 | WARN | HISTORICAL_SAMPLE_NEAREST_DATE_WARN | historical sample not failed because provider returned nearest date 2026-06-01; external confirmation is partial |
| external | AAPL | STOCK | twelvedata | twelvedata quote | 2026-06-02 | 2026-06-03 | 315.2 | 310.48 | 1.520227 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | MSFT | STOCK | twelvedata | twelvedata time_series | 2025-06-04 | 2025-06-04 | 463.87 | 463.87 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | MSFT | STOCK | twelvedata | twelvedata time_series | 2025-09-03 | 2025-09-03 | 505.35001 | 505.35001 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | MSFT | STOCK | twelvedata | twelvedata time_series | 2025-12-01 | 2025-12-01 | 486.73999 | 486.73999 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | MSFT | STOCK | twelvedata | twelvedata time_series | 2026-03-04 | 2026-03-04 | 405.20001 | 405.20001 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | MSFT | STOCK | twelvedata | twelvedata time_series | 2026-06-02 | 2026-06-01 | 441.31 | 460.51999 | 4.171369 | 2.0 | WARN | HISTORICAL_SAMPLE_NEAREST_DATE_WARN | historical sample not failed because provider returned nearest date 2026-06-01; external confirmation is partial |
| external | MSFT | STOCK | twelvedata | twelvedata quote | 2026-06-02 | 2026-06-03 | 441.31 | 427.72 | 3.177312 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | NVDA | STOCK | twelvedata | twelvedata time_series | 2025-06-04 | 2025-06-04 | 141.92 | 141.92 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | NVDA | STOCK | twelvedata | twelvedata time_series | 2025-09-03 | 2025-09-03 | 170.62 | 170.62 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | NVDA | STOCK | twelvedata | twelvedata time_series | 2025-12-01 | 2025-12-01 | 179.92 | 179.92 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | NVDA | STOCK | twelvedata | twelvedata time_series | 2026-03-04 | 2026-03-04 | 183.039993 | 183.039993 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | NVDA | STOCK | twelvedata | twelvedata time_series | 2026-06-02 | 2026-06-01 | 222.82001 | 224.36 | 0.686392 | 2.0 | WARN | HISTORICAL_SAMPLE_NEAREST_DATE_WARN | provider returned nearest date 2026-06-01 |
| external | NVDA | STOCK | twelvedata | twelvedata quote | 2026-06-02 | 2026-06-03 | 222.82 | 215.425 | 3.432749 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | AMZN | STOCK | twelvedata | twelvedata time_series | 2025-06-04 | 2025-06-04 | 207.23 | 207.23 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | AMZN | STOCK | twelvedata | twelvedata time_series | 2025-09-03 | 2025-09-03 | 225.99001 | 225.99001 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | AMZN | STOCK | twelvedata | twelvedata time_series | 2025-12-01 | 2025-12-01 | 233.88 | 233.88 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | AMZN | STOCK | twelvedata | twelvedata time_series | 2026-03-04 | 2026-03-04 | 216.82001 | 216.82001 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | AMZN | STOCK | twelvedata | twelvedata time_series | 2026-06-02 | 2026-06-01 | 256.51999 | 261.26001 | 1.814292 | 2.0 | WARN | HISTORICAL_SAMPLE_NEAREST_DATE_WARN | provider returned nearest date 2026-06-01 |
| external | AMZN | STOCK | twelvedata | twelvedata quote | 2026-06-02 | 2026-06-03 | 256.52 | 248.305 | 3.308431 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | GOOGL | STOCK | twelvedata | twelvedata time_series | 2025-06-04 | 2025-06-04 | 168.050003 | 168.050003 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | GOOGL | STOCK | twelvedata | twelvedata time_series | 2025-09-03 | 2025-09-03 | 230.66 | 230.66 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | GOOGL | STOCK | twelvedata | twelvedata time_series | 2025-12-01 | 2025-12-01 | 314.89001 | 314.89001 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | GOOGL | STOCK | twelvedata | twelvedata time_series | 2026-03-04 | 2026-03-04 | 303.13 | 303.13 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | GOOGL | STOCK | twelvedata | twelvedata time_series | 2026-06-02 | 2026-06-01 | 361.85001 | 376.37 | 3.857903 | 2.0 | WARN | HISTORICAL_SAMPLE_NEAREST_DATE_WARN | historical sample not failed because provider returned nearest date 2026-06-01; external confirmation is partial |
| external | GOOGL | STOCK | twelvedata | twelvedata quote | 2026-06-02 | 2026-06-03 | 361.85 | 359.67 | 0.606111 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | META | STOCK | twelvedata | twelvedata time_series | 2025-06-04 | 2025-06-04 | 687.95001 | 687.95001 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | META | STOCK | twelvedata | twelvedata time_series | 2025-09-03 | 2025-09-03 | 737.049988 | 737.049988 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | META | STOCK | twelvedata | twelvedata time_series | 2025-12-01 | 2025-12-01 | 640.87 | 640.87 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | META | STOCK | twelvedata | twelvedata time_series | 2026-03-04 | 2026-03-04 | 667.72998 | 667.72998 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | META | STOCK | twelvedata | twelvedata time_series | 2026-06-02 | 2026-06-01 | 597.63 | 600.46997 | 0.472958 | 2.0 | WARN | HISTORICAL_SAMPLE_NEAREST_DATE_WARN | provider returned nearest date 2026-06-01 |
| external | META | STOCK | twelvedata | twelvedata quote | 2026-06-02 | 2026-06-03 | 597.63 | 621.52 | 3.843802 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | TSLA | STOCK | twelvedata | twelvedata time_series | 2025-06-04 | 2025-06-04 | 332.049988 | 332.049988 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | TSLA | STOCK | twelvedata | twelvedata time_series | 2025-09-03 | 2025-09-03 | 334.089996 | 334.089996 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | TSLA | STOCK | twelvedata | twelvedata time_series | 2025-12-01 | 2025-12-01 | 430.14001 | 430.14001 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | TSLA | STOCK | twelvedata | twelvedata time_series | 2026-03-04 | 2026-03-04 | 405.94 | 405.94 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | TSLA | STOCK | twelvedata | twelvedata time_series | 2026-06-02 | 2026-06-01 | 423.73999 | 415.88 | 1.889966 | 2.0 | WARN | HISTORICAL_SAMPLE_NEAREST_DATE_WARN | provider returned nearest date 2026-06-01 |
| external | TSLA | STOCK | twelvedata | twelvedata quote | 2026-06-02 | 2026-06-03 | 423.74 | 422.45 | 0.305362 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | JPM | STOCK | twelvedata | twelvedata time_series | 2025-06-04 | 2025-06-04 | 264.22 | 264.22 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | JPM | STOCK | twelvedata | twelvedata time_series | 2025-09-03 | 2025-09-03 | 299.51001 | 299.51001 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | JPM | STOCK | twelvedata | twelvedata time_series | 2025-12-01 | 2025-12-01 | 308.92001 | 308.92001 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | JPM | STOCK | twelvedata | twelvedata time_series | 2026-03-04 | 2026-03-04 | 299.39001 | 299.39001 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | JPM | STOCK | twelvedata | twelvedata time_series | 2026-06-02 | 2026-06-01 | 300.95999 | 296.57999 | 1.476836 | 2.0 | WARN | HISTORICAL_SAMPLE_NEAREST_DATE_WARN | provider returned nearest date 2026-06-01 |
| external | JPM | STOCK | twelvedata | twelvedata quote | 2026-06-02 | 2026-06-03 | 300.96 | 301.405 | 0.147642 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |
| external | KO | STOCK | twelvedata | twelvedata time_series | 2026-06-02 | 2026-06-02 | 78.41 | - | - | 2.0 | WARN | EXTERNAL_RATE_LIMIT | EXTERNAL_RATE_LIMIT: twelvedata HTTP 429 endpoint=time_series body={"code":429,"message":"You have run out of API credits for the current minute. 12 API credits were used, with the current limit being 8. Wait for the next minute or consid... |
| external | KO | STOCK | twelvedata | twelvedata quote | 2026-06-02 | 2026-06-02 | 78.41 | - | - | 5.0 | WARN | EXTERNAL_RATE_LIMIT | EXTERNAL_RATE_LIMIT: twelvedata HTTP 429 endpoint=quote body={"code":429,"message":"You have run out of API credits for the current minute. 16 API credits were used, with the current limit being 8. Wait for the next minute or consider swi... |
| external | AMD | STOCK | twelvedata | twelvedata time_series | 2025-06-04 | 2025-06-04 | 118.58 | 118.58 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | AMD | STOCK | twelvedata | twelvedata time_series | 2025-09-03 | 2025-09-03 | 162.13 | 162.13 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | AMD | STOCK | twelvedata | twelvedata time_series | 2025-12-01 | 2025-12-01 | 219.75999 | 219.75999 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | AMD | STOCK | twelvedata | twelvedata time_series | 2026-03-04 | 2026-03-04 | 202.070007 | 202.070007 | 0.0 | 2.0 | OK | HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE | historical sample validated from provider range |
| external | AMD | STOCK | twelvedata | twelvedata time_series | 2026-06-02 | 2026-06-01 | 521.53998 | 510.13 | 2.236681 | 2.0 | WARN | HISTORICAL_SAMPLE_NEAREST_DATE_WARN | historical sample not failed because provider returned nearest date 2026-06-01; external confirmation is partial |
| external | AMD | STOCK | twelvedata | twelvedata quote | 2026-06-02 | 2026-06-03 | 521.54 | 538.11 | 3.079296 | 5.0 | OK | CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST | current price endpoint used only for latest quote validation |

## Provider External Tests

| asset_type | provider | external_test | status | reason_code | message |
|---|---|---|---|---|---|
| FX | frankfurter | configuration_only | configured | VALIDATION_OK | Provider configuration available for sample validation. |
| CRYPTO | coingecko | configuration_only | configured | VALIDATION_OK | Provider configuration available for sample validation. |
| STOCK | twelvedata | configuration_only | configured | VALIDATION_OK | Provider configuration available for sample validation. |
| MACRO | bcb_sgs | configuration_only | configured | VALIDATION_OK | Provider configuration available for sample validation. |

## Failures

- none

## Warnings

- `HISTORICAL_SAMPLE_NEAREST_DATE_WARN` STOCK AAPL 2026-06-02: historical sample not failed because provider returned nearest date 2026-06-01; external confirmation is partial
- `HISTORICAL_SAMPLE_NEAREST_DATE_WARN` STOCK MSFT 2026-06-02: historical sample not failed because provider returned nearest date 2026-06-01; external confirmation is partial
- `HISTORICAL_SAMPLE_NEAREST_DATE_WARN` STOCK NVDA 2026-06-02: provider returned nearest date 2026-06-01
- `HISTORICAL_SAMPLE_NEAREST_DATE_WARN` STOCK AMZN 2026-06-02: provider returned nearest date 2026-06-01
- `HISTORICAL_SAMPLE_NEAREST_DATE_WARN` STOCK GOOGL 2026-06-02: historical sample not failed because provider returned nearest date 2026-06-01; external confirmation is partial
- `HISTORICAL_SAMPLE_NEAREST_DATE_WARN` STOCK META 2026-06-02: provider returned nearest date 2026-06-01
- `HISTORICAL_SAMPLE_NEAREST_DATE_WARN` STOCK TSLA 2026-06-02: provider returned nearest date 2026-06-01
- `HISTORICAL_SAMPLE_NEAREST_DATE_WARN` STOCK JPM 2026-06-02: provider returned nearest date 2026-06-01
- `EXTERNAL_RATE_LIMIT` STOCK KO 2026-06-02: EXTERNAL_RATE_LIMIT: twelvedata HTTP 429 endpoint=time_series body={"code":429,"message":"You have run out of API credits for the current minute. 12 API credits were used, with the current limit being 8. Wait for the next minute or consid...
- `EXTERNAL_RATE_LIMIT` STOCK KO 2026-06-02: EXTERNAL_RATE_LIMIT: twelvedata HTTP 429 endpoint=quote body={"code":429,"message":"You have run out of API credits for the current minute. 16 API credits were used, with the current limit being 8. Wait for the next minute or consider swi...
- `HISTORICAL_SAMPLE_NEAREST_DATE_WARN` STOCK AMD 2026-06-02: historical sample not failed because provider returned nearest date 2026-06-01; external confirmation is partial

## Notes

- Samples are deterministic: first, last, middle, and evenly spaced interior dates.
- Historical samples use historical provider ranges such as `market_chart/range`, Frankfurter timeseries, or Twelve Data `time_series`.
- Current price endpoints such as CoinGecko `simple/price` are used only for latest quote validation.
- Provider nearest-date matches are WARN and do not block promotion when internal validation is clean.
- Provider rate limit or transient provider failures produce `READY_WITH_WARNINGS` when the internal candidate is coherent.
- API keys are never written to this report.
