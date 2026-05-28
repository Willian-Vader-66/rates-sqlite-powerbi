# Frontend Productization Audit

## Current Frontend Modules

- `MainApp`: JavaFX bootstrap, splash screen, scene and stylesheet loading.
- `api/ApiClient`: HTTP GET wrapper with JSON parsing and query-param encoding.
- `config/AppConfig`: API URL, refresh interval, timeout, and local override loading.
- `service/MarketDataService`: typed gateway for dashboard, instruments, quotes, analysis, and history endpoints.
- `model/*`: immutable API DTO records for instruments, quotes, analysis, summary, fixed charts, market overview, top stocks, price points, and system status.
- `ui/DashboardController`: application coordinator for Overview, Watchlist, Settings, refresh, selection, and history loading.
- `ui/InstrumentTableController`: watchlist filters, table, row composition, and selection events.
- `ui/ChartController`: thin wrapper around the reusable interactive chart component.
- `ui/chart/InteractiveFinanceChart`: canvas-based chart with downsampling, hover tooltip, crosshair, empty/loading/error state, and last-value marker.
- `ui/components/*`: reusable cards, error banner, and loading overlay.
- `util/FormatUtils`, `DateUtils`: deterministic US-style financial formatting and date display.

## API Endpoints Consumed

- `GET /api/system/status`
- `GET /api/dashboard/summary`
- `GET /api/dashboard/market-overview`
- `GET /api/dashboard/fixed-charts`
- `GET /api/dashboard/top-stocks-30d`
- `GET /api/instruments?active=true`
- `GET /api/quotes/latest`
- `GET /api/analysis/latest`
- `GET /api/stocks/history?symbol=&start=&end=`
- `GET /api/fx/history?base=&symbol=&start=&end=`
- `GET /api/crypto/history?symbol=&start=&end=`
- `GET /api/macro/history?indicator_code=&start=&end=`

The Java app remains HTTP-only and does not read SQLite directly.

## Current UI Gaps

- Dedicated Markets, Stocks, FX & Crypto, and Macro pages are scaffolded as navigation targets, but still route users back to Overview/Watchlist for the current pass.
- Overview still uses fixed backend charts for secondary cards; a full click-to-promote main market chart can be added next.
- Settings is read-only except for Test Connection; editable preferences should be persisted in a later pass.
- Compare mode is documented but not implemented.

## Current Data Gaps

- Demo data readiness is strong for portfolio validation: 68 instruments, latest quotes, latest analysis, and roughly four years of history.
- Current market overview cards expose latest value and 30D movement, but not a true 1D value for every asset.
- Macro signals are simplified and should remain intentionally different from stock/crypto trading signals.

## Performance Risks

- Full refresh still loads instruments, quotes, analysis, overview, fixed charts, and top stocks together. The refresh guard prevents overlap, and selected history is cached by symbol/range.
- 365D selected history can return more than 1,000 points. `InteractiveFinanceChart` downsamples to at most 420 rendered points to keep UI responsive.
- Console API logs are useful during development but should become configurable before packaging.

## Chart Limitations

- Canvas chart currently renders one series.
- Hover finds the nearest rendered point after downsampling, not necessarily the exact raw point.
- Axis labels are intentionally sparse; richer monthly/year ticks can be added without changing the component boundary.
- Compare mode should normalize series to percent performance before drawing multiple lines.

## Recommended Implementation Sequence

1. Add click-to-promote Overview market cards into the main interactive chart.
2. Split `DashboardController` into `layout/AppShell`, `pages/OverviewPage`, `pages/WatchlistPage`, and `pages/SettingsPage`.
3. Add dedicated Markets, Stocks, FX & Crypto, and Macro pages using the existing `MarketDataService`.
4. Add optional compare mode for up to three selected watchlist instruments.
5. Persist Settings changes in a local config file.
6. Add visual screenshot regression checks once the UI stabilizes.
