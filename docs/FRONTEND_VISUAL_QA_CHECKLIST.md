# Frontend Visual QA Checklist

## Startup

- Start backend with `python -m fx_rates serve --host 127.0.0.1 --port 8000`.
- Start frontend with `cd frontend-java` then `mvn javafx:run`.
- Confirm the splash transitions into the dashboard.
- Confirm the header shows API connected and last refresh.

## Overview

- Summary cards show non-zero values after `dashboard prepare-demo`.
- Market cards show USD/BRL, USD/EUR, BTC/USD, ETH/USD, Selic, top performer, and worst performer where data exists.
- Positive/negative market movement uses green/red only.
- Fixed mini charts are visible and not empty in demo mode.
- Top 10 Companies table has aligned price and percent columns.
- Empty states mention `dashboard prepare-demo` if data is absent.

## Watchlist

- Search filters symbols and company names.
- Asset, Signal, and Trend chips visibly show the active filter.
- Exchange and Sector filters preserve selection across refresh.
- Showing X of Y counter updates.
- Clear Filters resets all filters.
- Table numbers align right.
- Percent changes are green/red.
- Long names do not make the table unreadable.
- Selected row remains visibly selected.

## Selected Instrument Details

- Details show symbol, name, asset type, price/rate, 30D change, bid/ask, trend, signal, SMA 20/50, volatility, last update, and history coverage.
- Changing rows updates the chart.
- Re-selecting the same row/range does not flicker or reload repeatedly.
- Range selector includes 30D, 90D, 180D, 1Y, and 365D.

## Interactive Chart

- Chart shows a line for selected instruments with available history.
- Hovering over the chart shows a tooltip with date, value, and change.
- Vertical crosshair follows the nearest point.
- Latest value marker appears on the right side of the chart.
- 365D range remains responsive.
- Empty, loading, and error states stay inside the chart area.

## Settings

- API base URL is visible.
- Timeout and refresh interval are visible.
- DB path from `/api/system/status` is visible.
- DB size, historical rows, and date coverage are visible.
- Test Connection refreshes status without closing the app.

## Screenshots

- Capture Overview with populated data.
- Capture Watchlist with filters and selected instrument.
- Capture Settings with DB path and readiness counts.
- Capture a hover tooltip if possible.
- Avoid exposing `.env`, API keys, personal tokens, or local secrets.
