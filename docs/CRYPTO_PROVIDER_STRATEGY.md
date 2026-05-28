# Crypto Provider Strategy

## Current Release

The standard LIVE-FIRST release uses CoinGecko for crypto history and is limited to the last 365 days.

Supported symbols:

- BTC -> bitcoin
- ETH -> ethereum
- BNB -> binancecoin
- SOL -> solana
- XRP -> ripple

## Provider Limit

CoinGecko Public/Demo can reject long historical ranges with `HTTP 401`, `error_code=10012`, because public historical access is limited to 365 days. The project does not hide this limitation, does not invent data, and does not switch to another provider silently.

## Advanced History

Advanced History is planned for a future mode with up to 10 years of data. It requires `LIVE_HISTORY_MODE=advanced` and a paid provider/API plan that supports the requested historical range, such as a compatible CoinGecko Pro configuration.

The current release does not implement Binance Spot as an alternate crypto history provider.
