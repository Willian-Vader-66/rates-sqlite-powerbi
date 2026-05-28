# Frontend Productization Plan

## Product Target

Finance Monitor should feel like a local-first corporate financial dashboard: calm, dense enough for analysis, fast to scan, and honest about backend/data readiness. The UI should support portfolio demo storytelling without pretending to be a trading terminal.

## Navigation Model

Target sections:

- Overview
- Markets
- Stocks
- FX & Crypto
- Macro
- Watchlist
- Settings

This pass implements the navigation structure and fully usable Overview, Watchlist, and Settings pages. Markets, Stocks, FX & Crypto, and Macro are scaffolded as future pages so the app can grow without another shell rewrite.

## Visual Direction

- Dark corporate navy background.
- Card surfaces in restrained slate/navy.
- Clean blue accent for actions and selection.
- Green/red only for financial movement.
- Subtle borders and light shadow.
- No heavy glow, neon gradients, or cyberpunk treatment.
- Compact spacing and readable typography.
- Clear empty, loading, and error states.

## Overview Target

Above the fold:

- Finance Monitor identity.
- API status and last refresh.
- Refresh and Pause Auto controls.
- Summary metric cards for instruments, active stocks, FX, crypto, macro, latest quotes, and failed runs.
- Market overview cards for USD/BRL, USD/EUR, BTC/USD, ETH/USD, Selic, top performer, and worst performer.

Charts:

- Current pass keeps fixed 30-day mini charts.
- Selected instrument chart in Watchlist is now interactive and reusable.
- Next pass should let Overview cards promote a market into a main interactive chart.

Top 10 Companies:

- Structured table-like layout with Rank, Symbol, Company, Latest Price, 30D %, Trend, and Signal.
- Positive/negative formatting for movement.

## Watchlist Target

- Search, asset type, signal, trend, exchange, sector, and active-only filters.
- Showing X of Y counter.
- Clear Filters button.
- Numeric alignment for prices and percentages.
- Positive/negative movement styling.
- Selected row drives details and chart.
- Selected instrument panel shows latest quote, 30D change, bid/ask, trend, signal, SMA 20/50, volatility, last update, and history coverage.

## Settings Target

- API base URL.
- Timeout and refresh interval.
- Auto-refresh state.
- Backend connection/status.
- SQLite path from `/api/system/status`.
- DB size, instrument counts, historical rows, and coverage.
- Test Connection button.

## Chart Component Target

`ui/chart/InteractiveFinanceChart` is the reusable chart boundary.

Current capabilities:

- 30D, 90D, 180D, 1Y, and 365D ranges through `MarketDataService.HistoryRange`.
- Backend history requests use start/end dates.
- Hover tooltip with symbol, range, date, value, and previous-point percentage change.
- Vertical crosshair.
- Latest value marker.
- Sparse date and numeric axis labels.
- Client-side downsampling for long ranges.
- Empty, loading, and error states.

Future capabilities:

- Multi-series compare mode.
- Normalized percent performance view.
- Better year/month tick labeling.
- Optional export screenshot button.

## Backend Contract

The Java app keeps the existing endpoint set:

- Stock history: `/api/stocks/history`
- FX history: `/api/fx/history`
- Crypto history: `/api/crypto/history`
- Macro history: `/api/macro/history`

A unified `/api/assets/{symbol}/history` endpoint is not required yet because the service layer already isolates endpoint selection by asset type.

## Acceptance Focus For This Pass

- Backend endpoints remain compatible.
- Frontend tests and compile pass.
- Dashboard connects to `http://127.0.0.1:8000`.
- Watchlist and Overview remain populated after `prepare-demo`.
- Selected instrument chart supports 30D, 90D, 180D, 1Y, and 365D.
- Hover/crosshair behavior exists.
- Visual treatment is more corporate and less neon.
- Settings page exposes backend/DB diagnostics.
