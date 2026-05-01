# Roadmap

## v1 Current FX Pipeline

- Frankfurter API ingestion for daily and historical FX rates
- SQLite persistence in `fx_rates`
- Idempotent UPSERT behavior
- `ingest_runs` tracking
- disk cache and structured logs
- Power BI ODBC setup
- backward-compatible CLI commands:
  - `python -m fx_rates backfill ...`
  - `python -m fx_rates daily ...`
  - `python -m fx_rates status ...`

## v2 Market Data Expansion

- stock instrument watchlists in editable CSV files
- stock historical and daily ingestion into SQLite
- provider abstraction with Twelve Data and deterministic demo provider
- near-real-time quote polling for selected symbols
- analysis snapshots for stocks and FX using stored data
- local HTTP API for dashboards and the future Java front-end
- API contract in `docs/API_CONTRACT.md`

## v3 Java Front-End

- Java application consumes the Python HTTP API
- no direct SQLite reads from Java
- dashboard views for instruments, price history, latest quotes, and analysis
- polling-based quote refresh from `/api/quotes/latest`
- local demo mode support without external API keys

## v4 External Deployment / Authentication

- configurable API binding and deployment packaging
- authentication and authorization before exposing outside localhost
- provider secret management outside `.env`
- background scheduler for daily ingestion and quote polling
- monitoring, retention policy, and operational runbooks
