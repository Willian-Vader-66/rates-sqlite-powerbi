# Local Market Data API Contract

The Java front-end should consume this local HTTP API. It should not read SQLite directly.

Default local server:

```text
http://127.0.0.1:8000
```

Start command:

```powershell
python -m fx_rates serve --host 127.0.0.1 --port 8000
```

Build/promote a live SQLite database first when the dashboard is empty:

```powershell
cd C:\Projetos_Local\rates-sqlite-powerbi-git
.\.venv\Scripts\Activate.ps1
python -m fx_rates dashboard build-live-db --days 365 --db-path .tmp/live-main-candidate.sqlite --external-test
python -m fx_rates dashboard validate-samples --db-path .tmp/live-main-candidate.sqlite --samples-per-symbol 5 --external-test
python -m fx_rates dashboard audit-live --db-path .tmp/live-main-candidate.sqlite
python -m fx_rates dashboard promote-live --candidate-db .tmp/live-main-candidate.sqlite --to-db data/fx.sqlite --backup
python -m fx_rates serve --host 127.0.0.1 --port 8000
```

Readiness audit:

```powershell
python -m fx_rates dashboard audit
```

The audit reports SQLite path, instrument/quote/analysis counts by asset type, historical coverage by asset type and important symbols, missing quote/analysis counts, and duplicate instrument/quote keys.

## JavaFX Frontend Endpoint Usage

The JavaFX app remains HTTP-only and chooses history endpoints by asset type:

- `STOCK`: `GET /api/stocks/history?symbol=AAPL&start=YYYY-MM-DD&end=YYYY-MM-DD`
- `FX`: `GET /api/fx/history?base=USD&symbol=BRL&start=YYYY-MM-DD&end=YYYY-MM-DD`
- `CRYPTO`: `GET /api/crypto/history?symbol=BTC&start=YYYY-MM-DD&end=YYYY-MM-DD`
- `MACRO`: `GET /api/macro/history?indicator_code=SELIC_DAILY&start=YYYY-MM-DD&end=YYYY-MM-DD`

Supported standard frontend ranges are `7D`, `30D`, `90D`, `180D`, and `365D`. Future ranges `3Y`, `5Y`, and `10Y` require advanced history providers and are not enabled in the standard release. Overview charts default to `90D`.

## GET /health

Returns service, database, and provider status.

Example response:

```json
{
  "status": "ok",
  "db_path": "data/fx.sqlite",
  "db_exists": true,
  "provider": {
    "name": "mock",
    "configured": true,
    "demo": true
  }
}
```

## GET /api/system/status

Returns the SQLite file used by the running API plus readiness counts. Use this endpoint when the JavaFX dashboard connects but shows no data.

Example response:

```json
{
  "db_path": "C:\\Projetos_Local\\rates-sqlite-powerbi\\data\\fx.sqlite",
  "db_exists": true,
  "db_size_bytes": 13991936,
  "total_instruments": 68,
  "active_stocks": 32,
  "active_currencies": 19,
  "active_crypto": 10,
  "active_macro": 7,
  "latest_quote_count": 68,
  "latest_analysis_count": 68,
  "instruments_without_analysis": 0,
  "instruments_without_quotes": 0,
  "historical_row_count": 83250,
  "date_min": "2022-05-04",
  "date_max": "2026-05-03",
  "is_empty": false,
  "recommended_prepare_command": "python -m fx_rates dashboard build-live-db --days 365 --db-path .tmp/live-main-candidate.sqlite --external-test"
}
```

Empty database response includes:

```json
{
  "is_empty": true,
  "message": "No live data loaded. Run: python -m fx_rates dashboard build-live-db --days 365 --db-path .tmp/live-main-candidate.sqlite --external-test"
}
```

## GET /api/instruments

Query params:

- `asset_type`: optional, `STOCK`, `FX`, `CRYPTO`, or `MACRO`
- `active`: optional boolean
- `search`: optional symbol/name text

Example:

```text
GET /api/instruments?asset_type=STOCK&active=true&search=apple
```

Example response:

```json
{
  "count": 1,
  "items": [
    {
      "instrument_id": 1,
      "symbol": "AAPL",
      "name": "Apple Inc",
      "asset_type": "STOCK",
      "exchange": "NASDAQ",
      "currency": "USD",
      "sector": "Technology",
      "provider": "mock",
      "provider_symbol": "AAPL",
      "is_active": 1,
      "priority": 1,
      "created_at": "2026-05-01T12:00:00+00:00",
      "updated_at": "2026-05-01T12:00:00+00:00"
    }
  ]
}
```

## GET /api/stocks/history

Query params:

- `symbol`: required
- `start`: optional `YYYY-MM-DD`
- `end`: optional `YYYY-MM-DD`

Example:

```text
GET /api/stocks/history?symbol=AAPL&start=2026-01-01&end=2026-04-25
```

Example response:

```json
{
  "symbol": "AAPL",
  "count": 1,
  "items": [
    {
      "date": "2026-01-02",
      "symbol": "AAPL",
      "exchange": "NASDAQ",
      "open": 188.1,
      "high": 190.4,
      "low": 186.8,
      "close": 189.2,
      "adjusted_close": 189.2,
      "volume": 45000000,
      "currency": "USD",
      "provider": "mock",
      "fetched_at": "2026-05-01T12:00:00+00:00"
    }
  ]
}
```

## GET /api/fx/history

Query params:

- `base`: required, for example `USD`
- `symbol`: required, for example `BRL`
- `start`: optional `YYYY-MM-DD`
- `end`: optional `YYYY-MM-DD`

Example:

```text
GET /api/fx/history?base=USD&symbol=BRL&start=2026-01-01&end=2026-04-25
```

## GET /api/quotes/latest

Query params:

- `symbols`: optional comma-separated list
- `asset_type`: optional, `STOCK`, `FX`, `CRYPTO`, or `MACRO`

Example:

```text
GET /api/quotes/latest?symbols=AAPL,MSFT,NVDA&asset_type=STOCK
```

Example response:

```json
{
  "count": 1,
  "items": [
    {
      "symbol": "AAPL",
      "asset_type": "STOCK",
      "exchange": "NASDAQ",
      "price": 189.2,
      "bid": 189.1,
      "ask": 189.3,
      "open": 188.0,
      "high": 190.0,
      "low": 187.5,
      "previous_close": 187.9,
      "change": 1.3,
      "percent_change": 0.6919,
      "volume": 45000000,
      "quote_time": "2026-05-01T12:00:00+00:00",
      "provider": "mock",
      "fetched_at": "2026-05-01T12:00:00+00:00"
    }
  ]
}
```

## GET /api/analysis/latest

Query params:

- `symbols`: optional comma-separated list
- `asset_type`: optional, `STOCK`, `FX`, `CRYPTO`, or `MACRO`

Example:

```text
GET /api/analysis/latest?symbols=AAPL,MSFT&asset_type=STOCK
```

Example response:

```json
{
  "count": 1,
  "items": [
    {
      "snapshot_id": 10,
      "symbol": "AAPL",
      "asset_type": "STOCK",
      "exchange": "NASDAQ",
      "generated_at": "2026-05-01T12:00:00+00:00",
      "last_price": 189.2,
      "last_close": 189.2,
      "daily_return": 0.0069,
      "change_30d": 0.0418,
      "change_90d": 0.088,
      "change_1y": 0.24,
      "sma_20": 184.5,
      "sma_50": 181.2,
      "volatility_20": 0.018,
      "min_30d": 174.1,
      "max_30d": 190.0,
      "trend": "UP",
      "signal": "BREAKOUT",
      "notes": null
    }
  ]
}
```

## GET /api/dashboard/summary

Example response:

```json
{
  "total_instruments": 68,
  "active_stocks": 32,
  "active_currencies": 19,
  "active_crypto": 10,
  "active_macro": 7,
  "latest_quote_count": 68,
  "latest_analysis_count": 68,
  "instruments_without_analysis": 0,
  "instruments_without_quotes": 0,
  "last_successful_ingest_run": {
    "run_id": 12,
    "started_at": "2026-05-01T12:00:00+00:00",
    "finished_at": "2026-05-01T12:00:10+00:00",
    "mode": "dashboard_prepare_demo",
    "base": "DEM",
    "symbols": "ALL",
    "start": "2022-05-02",
    "end": "2026-05-01",
    "row_count": 83000,
    "status": "OK",
    "error": null
  },
  "failed_runs_count": 0,
  "message": null
}
```

## GET /api/crypto/history

Query params:

- `symbol`: required, for example `BTC`
- `start`: optional `YYYY-MM-DD`
- `end`: optional `YYYY-MM-DD`

Example:

```text
GET /api/crypto/history?symbol=BTC&start=2026-01-01&end=2026-04-25
```

Example response:

```json
{
  "symbol": "BTC",
  "count": 1,
  "items": [
    {
      "date": "2026-01-02",
      "symbol": "BTC",
      "name": "Bitcoin",
      "price_usd": 65000.0,
      "market_cap": 1280000000000.0,
      "volume_24h": 28000000000.0,
      "change_24h": 1.2,
      "provider": "mock_crypto",
      "fetched_at": "2026-05-01T12:00:00+00:00"
    }
  ]
}
```

## GET /api/macro/history

Query params:

- `indicator_code`: required, for example `SELIC_DAILY`
- `start`: optional `YYYY-MM-DD`
- `end`: optional `YYYY-MM-DD`

Example:

```text
GET /api/macro/history?indicator_code=SELIC_DAILY&start=2026-01-01&end=2026-04-25
```

## GET /api/dashboard/market-overview

Returns overview cards and recent notable signals.

Example response:

```json
{
  "generated_at": "2026-05-01T12:00:00+00:00",
  "cards": [
    {"label": "USD/BRL", "value": 5.0, "change": -0.25, "unit": null, "status": "down"},
    {"label": "USD/EUR", "value": 0.92, "change": 0.1, "unit": null, "status": "up"},
    {"label": "BTC/USD", "value": 65000.0, "change": 2.1, "unit": "USD", "status": "up"},
    {"label": "Selic", "value": 0.04, "change": 0.0, "unit": "% a.d.", "status": "neutral"}
  ],
  "signals": [
    {"symbol": "NVDA", "asset_type": "STOCK", "trend": "UP", "signal": "BREAKOUT"}
  ],
  "message": null
}
```

## GET /api/dashboard/fixed-charts

Returns curated chart groups for the Overview page. The default period is `90D`.

Optional params:

- `period`: one of `7D`, `30D`, `90D`, `180D`, `365D`
- `days`: legacy numeric override

Chart and instrument-like responses expose display metadata:

- `display_name`
- `base_currency`
- `quote_currency`
- `display_pair`
- `display_unit`
- `value_format`
- `chart_title`
- `chart_subtitle`
- `axis_label`
- `tooltip_label`

Examples: AAPL uses `AAPL/USD` with `USD`; FX uses the database convention `base=USD`, so BRL is displayed as `USD/BRL`; crypto uses `BTC/USD`; macro indicators expose explicit units such as `% a.d.`, `% a.m.`, `% a.a.`, or `index`.

Example response:

```json
{
  "fx": [
    {
      "id": "usd_brl_30d",
      "title": "USD/BRL - Last 30 Days",
      "asset_type": "FX",
      "base": "USD",
      "symbol": "BRL",
      "display_pair": "USD/BRL",
      "display_unit": "BRL per 1 USD",
      "value_format": "fx_rate",
      "axis_label": "BRL per USD",
      "points": [{"date": "2026-01-01", "value": 5.0}],
      "message": null
    },
    {
      "id": "usd_eur_30d",
      "title": "USD/EUR - Last 30 Days",
      "asset_type": "FX",
      "base": "USD",
      "symbol": "EUR",
      "points": [{"date": "2026-01-01", "value": 0.92}],
      "message": null
    }
  ],
  "crypto": [
    {
      "id": "btc_usd_30d",
      "title": "Bitcoin - Last 30 Days",
      "asset_type": "CRYPTO",
      "symbol": "BTC",
      "display_pair": "BTC/USD",
      "display_unit": "USD",
      "value_format": "currency_usd",
      "points": [{"date": "2026-01-01", "value": 65000.0}],
      "message": null
    }
  ],
  "macro": [
    {
      "id": "selic_30d",
      "title": "Selic - Last 30 Days",
      "asset_type": "MACRO",
      "symbol": "SELIC_DAILY",
      "display_unit": "% a.d.",
      "value_format": "percent",
      "points": [{"date": "2026-01-01", "value": 0.04}],
      "message": null
    }
  ]
}
```

History endpoints (`/api/stocks/history`, `/api/fx/history`, `/api/crypto/history`, `/api/macro/history`) also return `start_date`, `end_date`, `point_count`, `period`, and the same display metadata at the response root. Items include compatible metadata so JavaFX can render units and tooltips even when it reads only the `items` array.

## GET /api/dashboard/overview

Aggregated Overview payload for product dashboards.

```text
GET /api/dashboard/overview?period=90D
```

Returns summary, market cards, fixed charts, technical highlights, performance ranking, and data quality. Technical labels are deterministic display signals only, not financial advice.

## GET /api/dashboard/technical-highlights

```text
GET /api/dashboard/technical-highlights?period=90D
```

Returns grouped technical watch cards:

- `positive_momentum`
- `negative_momentum`
- `breakout_watch`
- `drawdown_risk`
- `stable`
- `volatile`

Items include `technical_label`, `technical_score`, `technical_tone`, and `technical_summary`.

## GET /api/dashboard/performance-ranking

```text
GET /api/dashboard/performance-ranking?period=90D&asset_type=ALL
```

Returns selected-period `top`, `bottom`, and full ranked `items`. `asset_type` can be `ALL`, `STOCK`, `FX`, `CRYPTO`, or `MACRO`.

When a fixed chart is not populated, the chart object still returns `points: []` plus a user-facing `message`, for example:

```json
{
  "points": [],
  "message": "No live data loaded. Run: python -m fx_rates dashboard build-live-db --days 365 --db-path .tmp/live-main-candidate.sqlite --external-test"
}
```

## GET /api/dashboard/top-stocks-30d

Returns the curated top company panel. Optional params:

- `symbols`: comma-separated override list
- `days`: defaults to `30`

Example:

```text
GET /api/dashboard/top-stocks-30d?symbols=AAPL,MSFT,NVDA
```

Example item:

```json
{
  "symbol": "AAPL",
  "name": "Apple Inc",
  "latest_price": 179.4,
  "start_price": 172.2,
  "change_30d": 4.18,
  "trend": "UP",
  "signal": "BREAKOUT",
  "points": [{"date": "2026-01-01", "value": 172.2}]
}
```

## Real-Time Behavior

The API exposes the latest stored quotes. The backend refreshes those rows through polling:

```powershell
python -m fx_rates quotes poll --symbols AAPL,MSFT,NVDA,TSLA --interval-seconds 30 --duration-minutes 5
```

The Java front-end should poll `/api/quotes/latest` on a UI-friendly cadence. Server-Sent Events are not part of v2.
