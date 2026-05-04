# Frontend Visual Finalization Plan

## Scope

This pass turns Finance Monitor into a more product-like local dashboard without changing the local-first architecture. JavaFX continues to consume FastAPI over HTTP, and SQLite remains the local source of truth.

## Defaults

- Overview default period: `90D`
- Supported chart periods: `30D`, `90D`, `6M`, `1Y`, `4Y`
- Demo data command: `python -m fx_rates dashboard prepare-demo --years 4 --demo`
- Audit command: `python -m fx_rates dashboard audit`

## Technical Signals

Technical labels are deterministic display signals derived from trend, signal, selected-period movement, and volatility. They are not financial advice and must not be shown as buy/sell recommendations.

Allowed display language:

- Positive Momentum
- Technical Buy Watch
- Breakout Watch
- Negative Momentum
- Technical Sell Watch
- Drawdown Risk
- Stable
- Neutral
- Volatile

Avoid:

- Buy now
- Sell now
- Guaranteed
- Recommendation
- Financial advice

## Backend Endpoints

- `/api/dashboard/overview?period=90D`
- `/api/dashboard/fixed-charts?period=90D`
- `/api/dashboard/technical-highlights?period=90D`
- `/api/dashboard/performance-ranking?period=90D&asset_type=ALL`

## Visual QA

- Overview opens with `90D` selected.
- Period selector chips are visible and never blank.
- Charts show display pair/unit in title, subtitle, tooltip, and last-value label.
- Y-axis padding prevents lines from sticking to top or bottom.
- Technical highlight cards separate positive, negative, watch, and stable states.
- Markets, Stocks, FX & Crypto, Macro, Watchlist, and Settings all show real data after demo preparation.
