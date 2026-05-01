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

## GET /api/instruments

Query params:

- `asset_type`: optional, `STOCK` or `FX`
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
- `asset_type`: optional, `STOCK` or `FX`

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
- `asset_type`: optional, `STOCK` or `FX`

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
  "total_instruments": 100,
  "active_stocks": 100,
  "active_currencies": 0,
  "latest_quote_count": 4,
  "latest_analysis_count": 4,
  "last_successful_ingest_run": {
    "run_id": 12,
    "started_at": "2026-05-01T12:00:00+00:00",
    "finished_at": "2026-05-01T12:00:10+00:00",
    "mode": "stocks_backfill",
    "base": "STK",
    "symbols": "AAPL,MSFT",
    "start": "2026-01-01",
    "end": "2026-04-25",
    "row_count": 160,
    "status": "OK",
    "error": null
  },
  "failed_runs_count": 0
}
```

## Real-Time Behavior

The API exposes the latest stored quotes. The backend refreshes those rows through polling:

```powershell
python -m fx_rates quotes poll --symbols AAPL,MSFT,NVDA,TSLA --interval-seconds 30 --duration-minutes 5
```

The Java front-end should poll `/api/quotes/latest` on a UI-friendly cadence. Server-Sent Events are not part of v2.
