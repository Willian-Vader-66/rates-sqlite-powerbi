# Live Stock Ingestion Diagnosis

Date: 2026-05-21

## Problem

Live stock ingestion for AAPL, MSFT, and NVDA completed but produced large quote/history differences. The observed warnings were:

- AAPL latest quote differed from history by about 32%.
- MSFT latest quote differed from history by about 5%.
- NVDA latest quote differed from history by about 47%.

The dataset was allowed to remain mixed, but the affected stock symbols were not internally consistent.

## Root Cause

The live dashboard flow builds `latest_quote` and analysis from the staged historical series, not from the Twelve Data quote endpoint. That means the quote endpoint itself was not the direct source of the mismatch.

The issue was ordering. Twelve Data `time_series` responses can arrive in reverse chronological order. The provider returned rows in API order, and downstream code treated `history[-1]` as the latest point. For reverse-ordered provider payloads, `history[-1]` was actually the oldest point in the requested window.

That affected:

- `latest_quote`, because `_stock_quote_from_history` used the last list item.
- staged validation, because it compared the quote generated from the unsorted last item with history sorted by date.
- analysis snapshots, because staged live rows were analyzed in provider order.

A secondary safety issue was that `--allow-mixed` allowed staged validation failures to be committed as warnings. That is acceptable for unsupported symbols/providers, but not for quote/history inconsistency.

## Fix Strategy

The chosen strategy is option A from the task: after live staging succeeds, live ingestion replaces demo rows for the same `asset_type + symbol` inside the same SQLite transaction. Other demo symbols may remain, so the dataset can still be `mixed`, but an individual stock symbol should not silently combine demo and live data.

Additional defensive behavior was added:

- Twelve Data stock history is normalized to chronological order before it leaves the provider.
- Dashboard live staging sorts stock history before generating latest quotes.
- `_stock_quote_from_history` sorts by date before selecting latest and previous close.
- Staged quote/history validation failures now abort before DB mutation even when `--allow-mixed` is present.
- `commit_prepared_live_dataset` deletes conflicting demo rows for successfully staged live symbols inside the transaction.
- API history responses, latest quotes, ranking/chart helpers, and latest analysis prefer live rows for a symbol if live rows exist.
- Analysis snapshot generation filters to a single preferred data mode per symbol.
- Audits now detect conflicting data modes, quote/history divergence, quote older than history, stale quotes, future history/quote dates, and provider/data_mode mismatches.
- Quote/history thresholds are configurable with `LIVE_QUOTE_WARN_PCT`, `LIVE_QUOTE_FAIL_PCT`, and `LIVE_QUOTE_STALE_DAYS`; defaults are 1%, 5%, and 10 days.

## Expected Result

After running live ingestion for AAPL, MSFT, and NVDA:

- Those symbols should have live history, live latest quotes, and live analysis from the same provider/source.
- `latest_quote` should match the last historical close produced by the time series unless a future provider-specific quote strategy is explicitly implemented.
- The Twelve Data quote endpoint is not used as the canonical stock quote during `prepare-live`; the staged `time_series` close is canonical so history, latest quote, analysis, and API output stay on the same source.
- The overall dataset may remain `mixed` while other symbols are still demo.
- A staged quote/history inconsistency aborts before deleting or updating SQLite.

## Safe Validation Commands

```powershell
python -m fx_rates providers status --external-test
python -m fx_rates dashboard prepare-live --years 1 --asset-type STOCK --symbols AAPL,MSFT,NVDA --db-path .tmp/live-stock-test.sqlite
python -m fx_rates dashboard audit --db-path .tmp/live-stock-test.sqlite
python -m fx_rates dashboard audit-market --db-path .tmp/live-stock-test.sqlite
python -m fx_rates dashboard prepare-live --years 1 --asset-type STOCK --symbols AAPL,MSFT,NVDA --replace-demo
python -m fx_rates dashboard audit
python -m fx_rates dashboard audit-market
```

Do not store API keys in this repository. Use only `TWELVE_DATA_API_KEY` from the environment.
