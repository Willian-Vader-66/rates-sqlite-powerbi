# Live Providers Setup

Live providers are optional. The application remains usable in explicit demo mode without API keys.

Configure provider names in `.env`:

```env
FX_PROVIDER=frankfurter
CRYPTO_PROVIDER=coingecko
STOCK_PROVIDER=twelvedata
MACRO_PROVIDER=bcb_sgs
TWELVE_DATA_API_KEY=
FRED_API_KEY=
```

Run diagnostics without revealing keys:

```powershell
python -m fx_rates providers status
```

`prepare-live` validates provider configuration, fetches live history for supported providers, writes `data_mode=live`, and refuses to mix with demo data unless a flag is explicit:

```powershell
python -m fx_rates dashboard prepare-live --years 4
python -m fx_rates dashboard prepare-live --years 4 --allow-mixed
python -m fx_rates dashboard prepare-live --years 4 --asset-type FX --symbols BRL,EUR
python -m fx_rates dashboard prepare-live --years 4 --asset-type STOCK --symbols AAPL,MSFT --replace-demo
```

No command silently falls back from live to demo. Use demo only through:

```powershell
python -m fx_rates dashboard prepare-demo --years 4 --demo
```

Provider notes:

- FX uses Frankfurter-compatible public endpoints and does not require a key by default. Coverage depends on currencies supported by that provider.
- Crypto uses CoinGecko public endpoints and does not require a key by default. Public rate limits can interrupt long backfills.
- Stocks use Twelve Data and require `TWELVE_DATA_API_KEY`.
- Macro uses Banco Central SGS for Brazilian indicators. Fed Funds and US CPI remain unsupported unless a FRED provider/key is implemented.
- `fake_live` is accepted for automated tests only; it is deterministic live-marked test data and should not be used for market analysis.

## Safe Live Ingestion Flow

`prepare-live` is intentionally conservative:

1. Validate provider configuration and API key format.
2. Reject common placeholders such as `SUA_CHAVE_AQUI`, `YOUR_API_KEY`, `CHANGE_ME`, `TODO`, `test`, `fake`, `demo`, or very short/spaced values.
3. Fetch and validate all requested live payloads in staging.
4. Only after successful staging, open a SQLite transaction.
5. If `--replace-demo` is present, delete demo rows only for the symbols that were successfully fetched and validated.
6. Insert live history, latest quotes, analysis snapshots, and the ingest run result.
7. Roll back the transaction on any write failure.

Without `--external-test`, provider status never calls the internet:

```powershell
python -m fx_rates providers status
```

To test a real provider/key explicitly:

```powershell
python -m fx_rates providers status --external-test
```

A failed external test reports `external_test=fail` and masks the key. It does not mutate SQLite.

If live ingestion fails after a bad provider response, repair demo data explicitly:

```powershell
python -m fx_rates dashboard prepare-demo --years 4 --demo --symbols AAPL,MSFT,NVDA
python -m fx_rates dashboard audit-market
```

`data/*.sqlite` and SQLite sidecar files are runtime artifacts and must not be committed.

