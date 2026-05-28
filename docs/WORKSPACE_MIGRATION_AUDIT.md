# Workspace Migration Audit

## Status
READY

## Compared paths
- New repo: C:\Projetos_Local\rates-sqlite-powerbi-git
- Old folder: C:\Projetos_Local\rates-sqlite-powerbi

## Git repo validation
- pwd: C:\Projetos_Local\rates-sqlite-powerbi-git
- git root: C:/Projetos_Local/rates-sqlite-powerbi-git
- branch: main
- remote:
  - origin	https://github.com/Willian-Vader-66/rates-sqlite-powerbi (fetch)
  - origin	https://github.com/Willian-Vader-66/rates-sqlite-powerbi (push)
- git status summary: tracked modified=45; untracked safe candidate=36; untracked review manually=0; risky/sensitive=0

## Structure validation
### New repo required items
| path | exists |
| --- | --- |
| .git | True |
| src/fx_rates | True |
| frontend-java | True |
| docs | True |
| tests | True |
| README.md | True |
| pyproject.toml | True |
| requirements.txt | True |
| run_live_pipeline.ps1 | True |
| scripts | True |

### Old folder required items
| path | exists |
| --- | --- |
| src/fx_rates | True |
| frontend-java | True |
| docs | True |
| tests | True |
| README.md | True |
| pyproject.toml | True |
| requirements.txt | True |
| run_live_pipeline.ps1 | False |
| scripts | True |

## Summary
- total comparable files in new repo: 161
- total comparable files in old folder: 117
- files same: 69
- files different: 48
- files only in new: 44
- files only in old: 0
- high risk missing files: 0
- critical old-newer differences: 0
- potential secrets found: 56 reviewed; 36 FALSE POSITIVE; 20 SAFE PLACEHOLDER; 0 REAL SECRET RISK
- generated/runtime files ignored: new files=2, new dirs=10, old files=3, old dirs=9

## Important files only in old folder
_None._

## Important files different between old and new
| path | category | new size | old size | new modified | old modified | newer side | recommendation |
| --- | --- | --- | --- | --- | --- | --- | --- |
| .env.example | config | 715 | 290 | 2026-05-25 21:38:10 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| .gitignore | config | 523 | 396 | 2026-05-20 21:40:26 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| docs/API_CONTRACT.md | docs | 13087 | 12482 | 2026-05-26 12:41:24 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| docs/ENVIRONMENT_REQUIREMENTS_PLAN.md | docs | 6979 | 6956 | 2026-05-26 12:42:18 -03:00 | 2026-05-10 17:37:04 -03:00 | new | Likely keep new; review diff if behavior matters. |
| docs/FINANCE_DASHBOARD_QA_CHECKLIST.md | docs | 5944 | 5938 | 2026-05-26 12:42:18 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| docs/FRONTEND_PRODUCTIZATION_AUDIT.md | docs | 4015 | 4012 | 2026-05-26 12:41:24 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| docs/FRONTEND_PRODUCTIZATION_PLAN.md | docs | 3865 | 3856 | 2026-05-26 12:41:24 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| docs/FRONTEND_VISUAL_FINALIZATION_PLAN.md | docs | 1648 | 1644 | 2026-05-26 12:42:18 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| docs/FRONTEND_VISUAL_QA_CHECKLIST.md | docs | 2401 | 2394 | 2026-05-26 12:41:24 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| docs/VISUAL_TEST_RUNNER.md | docs | 3279 | 3275 | 2026-05-26 12:42:18 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/README.md | frontend | 4272 | 4056 | 2026-05-20 23:34:27 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/main/java/com/example/financedashboard/model/AnalysisSnapshot.java | frontend | 2214 | 1943 | 2026-05-11 14:05:19 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/main/java/com/example/financedashboard/model/DashboardCard.java | frontend | 486 | 297 | 2026-05-11 14:05:26 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/main/java/com/example/financedashboard/model/DashboardSummary.java | frontend | 1764 | 1533 | 2026-05-10 20:46:04 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/main/java/com/example/financedashboard/model/Instrument.java | frontend | 1618 | 1414 | 2026-05-11 14:04:57 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/main/java/com/example/financedashboard/model/PricePoint.java | frontend | 1889 | 1618 | 2026-05-11 14:05:11 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/main/java/com/example/financedashboard/model/Quote.java | frontend | 1616 | 1345 | 2026-05-11 14:05:04 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/main/java/com/example/financedashboard/model/SystemStatus.java | frontend | 3925 | 1196 | 2026-05-25 21:43:15 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/main/java/com/example/financedashboard/service/MarketDataService.java | frontend | 7036 | 6663 | 2026-05-25 21:43:31 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/main/java/com/example/financedashboard/ui/chart/InteractiveFinanceChart.java | frontend | 13031 | 12963 | 2026-05-20 23:27:10 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/main/java/com/example/financedashboard/ui/components/MetricCard.java | frontend | 2055 | 1237 | 2026-05-11 14:05:53 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/main/java/com/example/financedashboard/ui/components/PeriodSelector.java | frontend | 2458 | 2165 | 2026-05-25 21:43:46 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/main/java/com/example/financedashboard/ui/DashboardController.java | frontend | 66799 | 56806 | 2026-05-25 21:48:18 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/main/java/com/example/financedashboard/ui/InstrumentTableController.java | frontend | 19165 | 17594 | 2026-05-11 14:07:06 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/main/resources/styles/app.css | frontend | 16180 | 14514 | 2026-05-11 14:07:44 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/test/java/com/example/financedashboard/ApiClientTest.java | test | 5651 | 5492 | 2026-05-20 23:38:26 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| frontend-java/src/test/java/com/example/financedashboard/MarketDataServiceTest.java | test | 2943 | 2714 | 2026-05-25 21:49:05 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| pyproject.toml | config | 615 | 494 | 2026-05-21 22:57:40 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| README.md | config | 26184 | 18641 | 2026-05-26 12:41:24 -03:00 | 2026-05-11 12:58:58 -03:00 | new | Likely keep new; review diff if behavior matters. |
| requirements.txt | config | 139 | 102 | 2026-05-21 22:57:31 -03:00 | 2026-05-10 17:22:15 -03:00 | new | Likely keep new; review diff if behavior matters. |
| src/fx_rates/analysis.py | source | 17073 | 11093 | 2026-05-11 20:07:00 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| src/fx_rates/api_server.py | source | 14449 | 9563 | 2026-05-25 21:36:31 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| src/fx_rates/cli.py | source | 29156 | 13753 | 2026-05-25 21:37:49 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| src/fx_rates/config.py | source | 9464 | 5241 | 2026-05-25 21:31:05 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| src/fx_rates/crypto_ingest.py | source | 8987 | 5067 | 2026-05-25 21:32:27 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| src/fx_rates/crypto_providers.py | source | 27914 | 12552 | 2026-05-25 21:32:00 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| src/fx_rates/dashboard_audit.py | source | 24645 | 18724 | 2026-05-20 21:41:48 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| src/fx_rates/dashboard_prepare.py | source | 47344 | 16942 | 2026-05-25 21:53:15 -03:00 | 2026-05-20 19:09:07 -03:00 | new | Likely keep new; review diff if behavior matters. |
| src/fx_rates/db_sqlite.py | source | 86295 | 52394 | 2026-05-25 21:50:18 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| src/fx_rates/display_metadata.py | source | 6575 | 6003 | 2026-05-10 20:43:16 -03:00 | 2026-05-20 19:09:00 -03:00 | old | Reviewed; keep new repo canonical because it has explicit unit/value labels. |
| src/fx_rates/macro_ingest.py | source | 4448 | 4070 | 2026-05-21 10:45:25 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| src/fx_rates/macro_providers.py | source | 9164 | 9112 | 2026-05-21 10:31:39 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| src/fx_rates/market_providers.py | source | 11790 | 11233 | 2026-05-21 10:31:25 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| src/fx_rates/models.py | source | 7660 | 5919 | 2026-05-21 10:41:25 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| src/fx_rates/watchlist.py | source | 3781 | 3266 | 2026-05-21 10:44:00 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| tests/test_api_server.py | test | 5647 | 2623 | 2026-05-20 23:27:39 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |
| tests/test_cli_smoke.py | test | 8401 | 6054 | 2026-05-25 21:41:29 -03:00 | 2026-05-10 18:39:38 -03:00 | new | Likely keep new; review diff if behavior matters. |
| tests/test_dashboard_display_metadata.py | test | 5389 | 4218 | 2026-05-19 18:54:57 -03:00 | 2026-05-10 17:22:16 -03:00 | new | Likely keep new; review diff if behavior matters. |

## Important files only in new repo
| path | category | size | modified time | recommendation |
| --- | --- | --- | --- | --- |
| data/reference/live_release_scope.csv | reference data | 1902 | 2026-05-25 21:53:07 -03:00 | Keep in new repo; track only after review. |
| docs/API_LIVE_SMOKE_REPORT.md | docs | 877 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/CRYPTO_PROVIDER_STRATEGY.md | docs | 902 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/DATA_MODE_STRATEGY.md | docs | 2402 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/ENV_DOCTOR_REPORT.md | docs | 1871 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/LINKEDIN_RELEASE_NOTES.md | docs | 4385 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/LIVE_AUDIT_REPORT.md | docs | 3999 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/LIVE_BUILD_REPORT.md | docs | 1782 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/LIVE_DATA_SCOPE.md | docs | 1611 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/LIVE_FIRST_FINAL_RELEASE_REPORT.md | docs | 6382 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/LIVE_FIRST_PRODUCT_SCOPE.md | docs | 2995 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/LIVE_FULL_TEST_PLAN.md | docs | 2464 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/LIVE_PROMOTION_GUIDE.md | docs | 3275 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/LIVE_PROVIDERS_SETUP.md | docs | 4432 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/LIVE_SAMPLE_VALIDATION_REPORT.md | docs | 1172 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/LIVE_STOCK_INGESTION_DIAGNOSIS.md | docs | 4345 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/MARKET_DATA_VALIDATION_REPORT.md | docs | 7061 | 2026-05-26 12:42:18 -03:00 | Keep in new repo; track only after review. |
| docs/POST_REINSTALL_GIT_RECOVERY_REPORT.md | docs | 7119 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/POWERSHELL_SESSION_GUIDE.md | docs | 3068 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/PRE_RELEASE_AUDIT_REPORT.md | docs | 3582 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| docs/RELEASE_CHECKLIST_LINKEDIN.md | docs | 1842 | 2026-05-26 12:41:24 -03:00 | Keep in new repo; track only after review. |
| run_live_pipeline.ps1 | script | 13300 | 2026-05-26 12:30:07 -03:00 | Keep in new repo; track only after review. |
| scripts/setup_live_env.ps1 | script | 6739 | 2026-05-26 12:30:43 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/api_smoke.py | source | 9796 | 2026-05-25 21:36:52 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/dashboard_market_audit.py | source | 29355 | 2026-05-26 12:26:03 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/data_origin.py | source | 1587 | 2026-05-11 14:04:01 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/env_doctor.py | source | 12266 | 2026-05-21 22:55:54 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/live_first.py | source | 16417 | 2026-05-26 12:42:50 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/live_full_test.py | source | 10690 | 2026-05-25 21:35:58 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/live_history.py | source | 2204 | 2026-05-25 21:30:51 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/live_promotion.py | source | 5852 | 2026-05-26 12:25:50 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/live_refresh.py | source | 9084 | 2026-05-20 23:08:59 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/live_samples.py | source | 18121 | 2026-05-26 12:25:28 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/live_scope.py | source | 3305 | 2026-05-25 21:53:31 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/live_validation.py | source | 24251 | 2026-05-26 12:27:09 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/provider_status.py | source | 13263 | 2026-05-25 21:36:42 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/redaction.py | source | 2186 | 2026-05-21 10:31:05 -03:00 | Keep in new repo; track only after review. |
| src/fx_rates/tls_support.py | source | 1298 | 2026-05-21 22:54:11 -03:00 | Keep in new repo; track only after review. |
| tests/test_crypto_coingecko_provider.py | test | 10268 | 2026-05-25 21:45:17 -03:00 | Keep in new repo; track only after review. |
| tests/test_data_mode_contract.py | test | 24696 | 2026-05-25 21:41:10 -03:00 | Keep in new repo; track only after review. |
| tests/test_env_doctor.py | test | 2035 | 2026-05-21 22:58:37 -03:00 | Keep in new repo; track only after review. |
| tests/test_live_full_workflow.py | test | 19286 | 2026-05-26 12:38:30 -03:00 | Keep in new repo; track only after review. |
| tests/test_live_history_policy.py | test | 1074 | 2026-05-25 21:42:13 -03:00 | Keep in new repo; track only after review. |
| tests/test_powershell_live_scripts.py | test | 1347 | 2026-05-26 12:38:38 -03:00 | Keep in new repo; track only after review. |

## Secrets / sensitive scan
- status: reviewed; no REAL SECRET RISK remains in versionable files.
- findings redacted: values are not printed; previews are categorical only.
- classification summary: total=56; false positives=36; safe placeholders=20; real secret risks=0; generated/runtime=0
| side | path | line | type | initial risk | final class | reason | masked preview |
| --- | --- | --- | --- | --- | --- | --- | --- |
| new | docs/LIVE_FIRST_FINAL_RELEASE_REPORT.md | 181 | TWELVE_DATA_API_KEY assignment | HIGH | SAFE PLACEHOLDER | documentation reference without committed secret | [documentation] |
| new | docs/LIVE_PROVIDERS_SETUP.md | 30 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | docs/POWERSHELL_SESSION_GUIDE.md | 53 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | README.md | 196 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | run_live_pipeline.ps1 | 32 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | run_live_pipeline.ps1 | 93 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | run_live_pipeline.ps1 | 99 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | run_live_pipeline.ps1 | 112 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | run_live_pipeline.ps1 | 127 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | scripts/setup_live_env.ps1 | 46 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | scripts/setup_live_env.ps1 | 135 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | scripts/setup_live_env.ps1 | 143 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/api_server.py | 43 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/config.py | 29 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/config.py | 126 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/dashboard_prepare.py | 473 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/dashboard_prepare.py | 704 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/env_doctor.py | 237 | TWELVE_DATA_API_KEY assignment | HIGH | SAFE PLACEHOLDER | documented placeholder or redacted example | [placeholder] |
| new | src/fx_rates/market_ingest.py | 144 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/market_providers.py | 55 | API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/market_providers.py | 64 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/market_providers.py | 242 | API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/market_providers.py | 251 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/provider_status.py | 87 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/provider_status.py | 93 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/provider_status.py | 96 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/provider_status.py | 97 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/provider_status.py | 148 | API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | src/fx_rates/provider_status.py | 215 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| new | tests/test_crypto_coingecko_provider.py | 72 | long suspicious string | MEDIUM | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_crypto_coingecko_provider.py | 81 | long suspicious string | MEDIUM | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_crypto_coingecko_provider.py | 171 | long suspicious string | MEDIUM | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_crypto_coingecko_provider.py | 214 | long suspicious string | MEDIUM | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_crypto_coingecko_provider.py | 230 | long suspicious string | MEDIUM | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_crypto_coingecko_provider.py | 247 | SECRET assignment | LOW | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_data_mode_contract.py | 267 | API_KEY assignment | HIGH | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_data_mode_contract.py | 296 | API_KEY assignment | HIGH | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_data_mode_contract.py | 388 | API_KEY assignment | HIGH | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_data_mode_contract.py | 403 | API_KEY assignment | HIGH | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_data_mode_contract.py | 441 | API_KEY assignment | HIGH | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_data_mode_contract.py | 545 | API_KEY assignment | HIGH | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_env_doctor.py | 21 | SECRET assignment | HIGH | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_env_doctor.py | 29 | SECRET assignment | HIGH | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_env_doctor.py | 52 | TWELVE_DATA_API_KEY assignment | HIGH | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_live_full_workflow.py | 269 | SECRET assignment | HIGH | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_live_full_workflow.py | 388 | API_KEY assignment | HIGH | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| new | tests/test_live_full_workflow.py | 433 | API_KEY assignment | HIGH | SAFE PLACEHOLDER | test fixture value | [test fixture] |
| old | src/fx_rates/api_server.py | 40 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| old | src/fx_rates/config.py | 28 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| old | src/fx_rates/config.py | 102 | TWELVE_DATA_API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| old | src/fx_rates/dashboard_prepare.py | 124 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| old | src/fx_rates/market_ingest.py | 144 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| old | src/fx_rates/market_providers.py | 54 | API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| old | src/fx_rates/market_providers.py | 63 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| old | src/fx_rates/market_providers.py | 239 | API_KEY assignment | HIGH | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |
| old | src/fx_rates/market_providers.py | 248 | API_KEY assignment | LOW | FALSE POSITIVE | variable/env lookup, type hint, runtime assignment, or masking logic | [code reference] |

## LIVE 365D scope scan
- old 4Y references: 2 test-only guard references remain; active/high risk=0
| path | line | pattern class | category | classification |
| --- | --- | --- | --- | --- |
| tests/test_powershell_live_scripts.py | 38 | legacy years flag literal | test | TEST CASE - validates current docs/scripts do not contain the legacy command. |
| tests/test_powershell_live_scripts.py | 39 | legacy four-year wording literal | test | TEST CASE - validates current docs/scripts do not contain legacy coverage wording. |
- 365D references: present across README/docs/scripts/src/frontend/tests for LIVE 365D standard.
- risk: OK; active old 4Y command/message references were replaced with LIVE 365D standard guidance.
## Git status classification
| status | path | category | classification |
| --- | --- | --- | --- |
| M | .env.example | config | Tracked modified |
| M | .gitignore | config | Tracked modified |
| M | docs/API_CONTRACT.md | docs | Tracked modified |
| M | docs/DATA_MODE_STRATEGY.md | docs | Tracked modified |
| M | docs/ENVIRONMENT_REQUIREMENTS_PLAN.md | docs | Tracked modified |
| M | docs/FINANCE_DASHBOARD_QA_CHECKLIST.md | docs | Tracked modified |
| M | docs/FRONTEND_PRODUCTIZATION_AUDIT.md | docs | Tracked modified |
| M | docs/FRONTEND_PRODUCTIZATION_PLAN.md | docs | Tracked modified |
| M | docs/FRONTEND_VISUAL_FINALIZATION_PLAN.md | docs | Tracked modified |
| M | docs/FRONTEND_VISUAL_QA_CHECKLIST.md | docs | Tracked modified |
| M | docs/LIVE_PROVIDERS_SETUP.md | docs | Tracked modified |
| M | docs/MARKET_DATA_VALIDATION_REPORT.md | docs | Tracked modified |
| M | docs/POST_REINSTALL_GIT_RECOVERY_REPORT.md | docs | Tracked modified |
| M | docs/VISUAL_TEST_RUNNER.md | docs | Tracked modified |
| M | frontend-java/README.md | frontend | Tracked modified |
| M | frontend-java/src/main/java/com/example/financedashboard/model/SystemStatus.java | frontend | Tracked modified |
| M | frontend-java/src/main/java/com/example/financedashboard/service/MarketDataService.java | frontend | Tracked modified |
| M | frontend-java/src/main/java/com/example/financedashboard/ui/chart/InteractiveFinanceChart.java | frontend | Tracked modified |
| M | frontend-java/src/main/java/com/example/financedashboard/ui/components/PeriodSelector.java | frontend | Tracked modified |
| M | frontend-java/src/main/java/com/example/financedashboard/ui/DashboardController.java | frontend | Tracked modified |
| M | frontend-java/src/test/java/com/example/financedashboard/ApiClientTest.java | test | Tracked modified |
| M | frontend-java/src/test/java/com/example/financedashboard/MarketDataServiceTest.java | test | Tracked modified |
| M | pyproject.toml | config | Tracked modified |
| M | README.md | config | Tracked modified |
| M | requirements.txt | config | Tracked modified |
| M | src/fx_rates/analysis.py | source | Tracked modified |
| M | src/fx_rates/api_server.py | source | Tracked modified |
| M | src/fx_rates/cli.py | source | Tracked modified |
| M | src/fx_rates/config.py | source | Tracked modified |
| M | src/fx_rates/crypto_ingest.py | source | Tracked modified |
| M | src/fx_rates/crypto_providers.py | source | Tracked modified |
| M | src/fx_rates/dashboard_audit.py | source | Tracked modified |
| M | src/fx_rates/dashboard_market_audit.py | source | Tracked modified |
| M | src/fx_rates/dashboard_prepare.py | source | Tracked modified |
| M | src/fx_rates/db_sqlite.py | source | Tracked modified |
| M | src/fx_rates/macro_ingest.py | source | Tracked modified |
| M | src/fx_rates/macro_providers.py | source | Tracked modified |
| M | src/fx_rates/market_providers.py | source | Tracked modified |
| M | src/fx_rates/models.py | source | Tracked modified |
| M | src/fx_rates/provider_status.py | source | Tracked modified |
| M | src/fx_rates/watchlist.py | source | Tracked modified |
| M | tests/test_api_server.py | test | Tracked modified |
| M | tests/test_cli_smoke.py | test | Tracked modified |
| M | tests/test_dashboard_display_metadata.py | test | Tracked modified |
| M | tests/test_data_mode_contract.py | test | Tracked modified |
| ?? | data/reference/live_release_scope.csv | reference data | Untracked safe candidate |
| ?? | docs/API_LIVE_SMOKE_REPORT.md | docs | Untracked safe candidate |
| ?? | docs/CRYPTO_PROVIDER_STRATEGY.md | docs | Untracked safe candidate |
| ?? | docs/ENV_DOCTOR_REPORT.md | docs | Untracked safe candidate |
| ?? | docs/LINKEDIN_RELEASE_NOTES.md | docs | Untracked safe candidate |
| ?? | docs/LIVE_AUDIT_REPORT.md | docs | Untracked safe candidate |
| ?? | docs/LIVE_BUILD_REPORT.md | docs | Untracked safe candidate |
| ?? | docs/LIVE_DATA_SCOPE.md | docs | Untracked safe candidate |
| ?? | docs/LIVE_FIRST_FINAL_RELEASE_REPORT.md | docs | Untracked safe candidate |
| ?? | docs/LIVE_FIRST_PRODUCT_SCOPE.md | docs | Untracked safe candidate |
| ?? | docs/LIVE_FULL_TEST_PLAN.md | docs | Untracked safe candidate |
| ?? | docs/LIVE_PROMOTION_GUIDE.md | docs | Untracked safe candidate |
| ?? | docs/LIVE_SAMPLE_VALIDATION_REPORT.md | docs | Untracked safe candidate |
| ?? | docs/LIVE_STOCK_INGESTION_DIAGNOSIS.md | docs | Untracked safe candidate |
| ?? | docs/POWERSHELL_SESSION_GUIDE.md | docs | Untracked safe candidate |
| ?? | docs/PRE_RELEASE_AUDIT_REPORT.md | docs | Untracked safe candidate |
| ?? | docs/RELEASE_CHECKLIST_LINKEDIN.md | docs | Untracked safe candidate |
| ?? | run_live_pipeline.ps1 | script | Untracked safe candidate |
| ?? | scripts/setup_live_env.ps1 | script | Untracked safe candidate |
| ?? | src/fx_rates/api_smoke.py | source | Untracked safe candidate |
| ?? | src/fx_rates/env_doctor.py | source | Untracked safe candidate |
| ?? | src/fx_rates/live_first.py | source | Untracked safe candidate |
| ?? | src/fx_rates/live_full_test.py | source | Untracked safe candidate |
| ?? | src/fx_rates/live_history.py | source | Untracked safe candidate |
| ?? | src/fx_rates/live_promotion.py | source | Untracked safe candidate |
| ?? | src/fx_rates/live_refresh.py | source | Untracked safe candidate |
| ?? | src/fx_rates/live_samples.py | source | Untracked safe candidate |
| ?? | src/fx_rates/live_scope.py | source | Untracked safe candidate |
| ?? | src/fx_rates/live_validation.py | source | Untracked safe candidate |
| ?? | src/fx_rates/redaction.py | source | Untracked safe candidate |
| ?? | src/fx_rates/tls_support.py | source | Untracked safe candidate |
| ?? | tests/test_crypto_coingecko_provider.py | test | Untracked safe candidate |
| ?? | tests/test_env_doctor.py | test | Untracked safe candidate |
| ?? | tests/test_live_full_workflow.py | test | Untracked safe candidate |
| ?? | tests/test_live_history_policy.py | test | Untracked safe candidate |
| ?? | tests/test_powershell_live_scripts.py | test | Untracked safe candidate |

## Follow-up cleanup
- secrets reviewed: 56
- false positives: 36
- placeholders: 20
- real secret risks: 0
- real secret risks fixed: 0; none were real after masked review.
- remaining secret risks: 0
- active 4Y references fixed: 5 active command/message occurrences plus prepare-demo and dashboard audit defaults moved to 365D/one-year standard.
- remaining 4Y references: 2 test guard literals only; no active product scope reference remains.
- 48 different files reviewed: 48
- files needing manual review: 0

### Secret classification notes
- FALSE POSITIVE: environment lookups, type hints, runtime variable assignments, masking/redaction logic, and provider status metadata.
- SAFE PLACEHOLDER: documentation placeholders and fake test fixtures only.
- REAL SECRET RISK: none found in versionable files.

### 4Y cleanup notes
- Active build and troubleshooting commands now use --days 365 / LIVE 365D standard.
- Demo preparation now accepts --days 365; the legacy years flag is compatibility only, not current guidance.
- Dashboard audit default now checks one-year/365D readiness instead of legacy multi-year coverage.

### Different files review
| classification | count | notes |
| --- | --- | --- |
| NEW_REPO_NEWER_OR_CANONICAL | 47 | New repo side is newer/canonical; no copy from old folder recommended. |
| DIFFERENCE_EXPECTED | 1 | src/fx_rates/display_metadata.py has an older timestamp in the new repo, but new content is canonical because it adds explicit unit/value labels. |
| OLD_FOLDER_HAS_POSSIBLE_USEFUL_CHANGE | 0 | No old-only or old-newer useful changes require manual copy. |
| RUNTIME_OR_IGNORED | 0 | Runtime files were excluded from comparable difference review. |
## Recommended actions
### Safe to ignore
- Generated/runtime exclusions: .git/, .venv/, .env, .env.* except .env.example, data/*.sqlite, data/*.sqlite-*, data/backups/, .tmp/, logs/, cache/, frontend-java/target/, target/, __pycache__/, .pytest_cache/, *.egg-info/, build/, dist/, node_modules/, *.log, *.tmp, *.bak, *.backup*, *.pyc, .DS_Store, Thumbs.db.

### Review manually
- .env.example (config, newer side: new)
- .gitignore (config, newer side: new)
- docs/API_CONTRACT.md (docs, newer side: new)
- docs/ENVIRONMENT_REQUIREMENTS_PLAN.md (docs, newer side: new)
- docs/FINANCE_DASHBOARD_QA_CHECKLIST.md (docs, newer side: new)
- docs/FRONTEND_PRODUCTIZATION_AUDIT.md (docs, newer side: new)
- docs/FRONTEND_PRODUCTIZATION_PLAN.md (docs, newer side: new)
- docs/FRONTEND_VISUAL_FINALIZATION_PLAN.md (docs, newer side: new)
- docs/FRONTEND_VISUAL_QA_CHECKLIST.md (docs, newer side: new)
- docs/VISUAL_TEST_RUNNER.md (docs, newer side: new)
- frontend-java/README.md (frontend, newer side: new)
- frontend-java/src/main/java/com/example/financedashboard/model/AnalysisSnapshot.java (frontend, newer side: new)
- frontend-java/src/main/java/com/example/financedashboard/model/DashboardCard.java (frontend, newer side: new)
- frontend-java/src/main/java/com/example/financedashboard/model/DashboardSummary.java (frontend, newer side: new)
- frontend-java/src/main/java/com/example/financedashboard/model/Instrument.java (frontend, newer side: new)
- frontend-java/src/main/java/com/example/financedashboard/model/PricePoint.java (frontend, newer side: new)
- frontend-java/src/main/java/com/example/financedashboard/model/Quote.java (frontend, newer side: new)
- frontend-java/src/main/java/com/example/financedashboard/model/SystemStatus.java (frontend, newer side: new)
- frontend-java/src/main/java/com/example/financedashboard/service/MarketDataService.java (frontend, newer side: new)
- frontend-java/src/main/java/com/example/financedashboard/ui/chart/InteractiveFinanceChart.java (frontend, newer side: new)
- frontend-java/src/main/java/com/example/financedashboard/ui/components/MetricCard.java (frontend, newer side: new)
- frontend-java/src/main/java/com/example/financedashboard/ui/components/PeriodSelector.java (frontend, newer side: new)
- frontend-java/src/main/java/com/example/financedashboard/ui/DashboardController.java (frontend, newer side: new)
- frontend-java/src/main/java/com/example/financedashboard/ui/InstrumentTableController.java (frontend, newer side: new)
- frontend-java/src/main/resources/styles/app.css (frontend, newer side: new)
- frontend-java/src/test/java/com/example/financedashboard/ApiClientTest.java (test, newer side: new)
- frontend-java/src/test/java/com/example/financedashboard/MarketDataServiceTest.java (test, newer side: new)
- pyproject.toml (config, newer side: new)
- README.md (config, newer side: new)
- requirements.txt (config, newer side: new)
- src/fx_rates/analysis.py (source, newer side: new)
- src/fx_rates/api_server.py (source, newer side: new)
- src/fx_rates/cli.py (source, newer side: new)
- src/fx_rates/config.py (source, newer side: new)
- src/fx_rates/crypto_ingest.py (source, newer side: new)
- src/fx_rates/crypto_providers.py (source, newer side: new)
- src/fx_rates/dashboard_audit.py (source, newer side: new)
- src/fx_rates/dashboard_prepare.py (source, newer side: new)
- src/fx_rates/db_sqlite.py (source, newer side: new)
- src/fx_rates/display_metadata.py (source, newer side: old)
- src/fx_rates/macro_ingest.py (source, newer side: new)
- src/fx_rates/macro_providers.py (source, newer side: new)
- src/fx_rates/market_providers.py (source, newer side: new)
- src/fx_rates/models.py (source, newer side: new)
- src/fx_rates/watchlist.py (source, newer side: new)
- tests/test_api_server.py (test, newer side: new)
- tests/test_cli_smoke.py (test, newer side: new)
- tests/test_dashboard_display_metadata.py (test, newer side: new)

### Should copy after review
- None identified.

### Should not copy
- .env, SQLite databases, logs, cache, .venv, target/build outputs, and files containing real API keys or tokens.
- src/fx_rates/config.py until secret material is removed.
- src/fx_rates/market_providers.py until secret material is removed.

## Commands for user
Do not run these until after reviewing this report. They are read-only inspection commands.
```powershell
git status --short
git diff -- README.md
git diff -- src/fx_rates/api_server.py
git diff --no-index C:\Projetos_Local\rates-sqlite-powerbi\src\fx_rates C:\Projetos_Local\rates-sqlite-powerbi-git\src\fx_rates
git diff --no-index C:\Projetos_Local\rates-sqlite-powerbi\docs C:\Projetos_Local\rates-sqlite-powerbi-git\docs
```

## Audit notes
- No files were copied from the old folder.
- No git add, commit, or push was performed.
- SQLite files and .env files were not hashed or read.
