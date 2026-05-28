# Live-First Product Scope

## Product Mode

Finance Monitor is now scoped as a LIVE-FIRST local financial dashboard. The final product should be presented as:

> Finance Monitor - dashboard financeiro local com historico real dos ultimos 365 dias, atualizacao incremental e validacao automatica dos dados por amostragem.

The main SQLite database must contain validated real data only. Demo data remains available for automated tests, local development, and explicit fallback experiments, but it must not be the default product mode and must always be visible as `data_mode=demo`.

## Live Data Requirements

- Main database: `data/fx.sqlite`, promoted only from a validated staging DB.
- Staging database: `.tmp/live-main-candidate.sqlite`.
- Target history: approximately the last 365 days.
- Refresh model: controlled incremental backend refresh, not frontend direct API calls.
- Frontend source: local backend and SQLite only.
- No artificial data when an external provider fails.
- No silent mixing of demo/live inside the same symbol.
- API keys must come only from environment variables, especially `TWELVE_DATA_API_KEY`, `COINGECKO_DEMO_API_KEY`, and `COINGECKO_PRO_API_KEY`.
- Logs, docs, reports, and command output must redact secrets as `****` or equivalent masked text.

## Initial Reliable Scope

| Asset type | Symbols |
|---|---|
| FX | USD/BRL, USD/EUR, USD/GBP, USD/JPY, USD/CAD, USD/CHF |
| CRYPTO | BTC/USD, ETH/USD, SOL/USD, BNB/USD, XRP/USD |
| STOCK | AAPL, MSFT, NVDA, AMZN, GOOGL, META, TSLA, JPM, KO, AMD |
| MACRO | SELIC, CDI, IPCA |

If a provider does not support one of these instruments, the build must mark it as FAIL or unsupported and must not invent rows.

The versioned release contract lives in `data/reference/live_release_scope.csv`. It defines display labels, provider symbols, expected frequency, and minimum row counts for the 365-day release database.

Advanced History is a future mode for up to 10 years and requires a paid provider/API plan compatible with the requested range.

## Validation Requirements

The pipeline must prove data quality through:

- provider configuration checks;
- staged live ingest before DB promotion;
- quote/history consistency checks;
- sample validation against external APIs;
- `audit-live` for a full SQLite consistency check;
- API smoke test using the candidate live database.

## Frontend Signals

The JavaFX frontend should show:

- `data_mode`;
- provider summary;
- last update / covered period;
- data health;
- validation status;
- clear DEMO or MIXED warning when the DB is not fully live.

## Promotion Rule

`data/fx.sqlite` should be replaced only by:

```powershell
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --dry-run
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --to-db data/fx.sqlite --backup
```

Promotion requires a passing live audit and API smoke check, and it creates a backup under `data/backups/`.
