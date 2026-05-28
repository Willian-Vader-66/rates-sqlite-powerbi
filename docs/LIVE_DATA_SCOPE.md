# Finance Monitor - Live Data Scope

## Current Release: LIVE 365D

Finance Monitor v1 uses real live data for the last 365 days.

This is the standard/free-provider scope used for the portfolio release:

- Local SQLite database.
- Incremental/live refresh controlled by the backend.
- Sample validation against external providers.
- `audit-live` before promotion.
- `api smoke-live` before frontend use.
- JavaFX frontend clearly shows the covered period.

## Future Expansion: Advanced History

Advanced History is reserved for a future mode with up to 10 years of data.

It requires a paid API plan or compatible provider configuration. It is not part of the current release and must never be enabled automatically without a provider that explicitly supports the requested historical range.

## Why 365D

CoinGecko Public/Demo returned `HTTP 401`, `error_code=10012`, because public historical crypto queries are limited to 365 days. Additional fallback attempts can also hit `HTTP 429` rate limits.

The product should be honest about provider limits. A stable dashboard with 365 days of real data is better than an unstable build that tries to force unsupported long history.

## V1 Scope

FX:

- BRL
- EUR
- GBP
- JPY
- CAD
- CHF

Crypto:

- BTC
- ETH
- BNB
- SOL
- XRP

Stocks:

- AAPL
- MSFT
- NVDA
- AMZN
- GOOGL
- META
- TSLA
- JPM
- KO
- AMD

Macro:

- SELIC_DAILY
- CDI_DAILY
- IPCA_MONTHLY

## UI Periods

Enabled in the standard release:

- 7D
- 30D
- 90D
- 180D
- 365D

Future periods:

- 3Y
- 5Y
- 10Y

Future periods must stay disabled or marked as `Requires paid provider / advanced history`.
