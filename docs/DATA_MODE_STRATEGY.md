# Data Mode Strategy

Finance Monitor separates market data origin into four canonical modes:

- `demo`: deterministic mock/synthetic data for demos, tests, and offline UI validation.
- `live`: provider-sourced data from an explicitly configured external provider.
- `mixed`: a dataset that contains both demo and live records.
- `unknown`: records or datasets without reliable origin metadata.

Every relevant SQLite table carries `data_mode`; historical/latest tables also carry `source_updated_at` where applicable. Existing databases are migrated with `ALTER TABLE` and backfilled from `provider`, `source`, and ingest run metadata.

The API never promotes demo data to live. Rows expose `data_mode`, `is_demo`, `is_live`, and `data_warning`. `/api/system/status` also exposes `providers`, `provider_summary`, `data_mode_counts`, `coverage`, `db_path`, and `recommended_prepare_command`.

Operational rule:

- Use `python -m fx_rates dashboard prepare-demo --years 4 --demo` for offline/demo datasets.
- Use `python -m fx_rates providers status` before attempting live ingestion.
- Use `python -m fx_rates dashboard prepare-live --years 4` only after providers are configured; it now fetches live history for supported providers and records `data_mode=live`.
- Use `--allow-mixed` only when a mixed dataset is intentional.
- Use `--replace-demo` only when live replacement is intentionally implemented and reviewed.

## Transactional Replace-Demo Policy

`--replace-demo` means "replace only after live data is proven usable", not "delete first". The live flow stages provider data in memory, validates origin and payload quality, then commits all mutations in one SQLite transaction. Provider validation or fetch errors abort before any delete/update. Write errors roll back the transaction.

Safety guarantees:

- Demo rows are never written as `data_mode=live`.
- Live rows are never written as `data_mode=demo`.
- Placeholder API keys are treated as invalid provider configuration.
- Partial provider success requires `--allow-mixed`; otherwise existing data is preserved.
- `audit-market` and `/api/system/status.data_health` report important symbols with missing history and suggest a repair command.

Recovery command for the common stock subset:

```powershell
python -m fx_rates dashboard prepare-demo --years 4 --demo --symbols AAPL,MSFT,NVDA
```

