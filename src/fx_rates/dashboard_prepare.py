from __future__ import annotations

import hashlib
import math
from dataclasses import replace
from datetime import date, timedelta
from typing import Iterable

from .analysis import build_analysis_snapshots
from .config import Settings
from .crypto_providers import CryptoAssetConfig, build_crypto_provider, load_crypto_reference
from .db_sqlite import (
    get_dashboard_summary,
    get_system_status,
    finish_ingest_run,
    insert_analysis_snapshots,
    deduplicate_dashboard_records,
    start_ingest_run,
    upsert_crypto_prices_daily,
    upsert_fx_rates,
    upsert_instruments,
    upsert_macro_indicators_daily,
    upsert_market_quotes_latest,
    upsert_stock_prices_daily,
)
from .macro_providers import MacroIndicatorConfig, build_macro_provider, load_macro_reference
from .market_providers import build_market_provider
from .models import (
    CryptoPriceDailyRow,
    FxRateRow,
    InstrumentRow,
    MacroIndicatorDailyRow,
    MarketQuoteRow,
    StockPriceDailyRow,
)
from .utils import utc_now_iso
from .watchlist import load_currency_reference, load_stock_watchlist

DEFAULT_DASHBOARD_STOCK_REFERENCE = "data/reference/top100_stocks.csv"
DEFAULT_CURRENCY_REFERENCE = "data/reference/currencies.csv"
DEFAULT_CRYPTO_REFERENCE = "data/reference/crypto_assets.csv"
DEFAULT_MACRO_REFERENCE = "data/reference/macro_indicators.csv"

DASHBOARD_FX_SYMBOLS = [
    "BRL",
    "USD",
    "EUR",
    "GBP",
    "JPY",
    "CHF",
    "CAD",
    "AUD",
    "NZD",
    "CNY",
    "MXN",
    "ARS",
    "CLP",
    "COP",
    "ZAR",
    "SEK",
    "NOK",
    "DKK",
    "PLN",
]
DASHBOARD_CRYPTO_SYMBOLS = ["BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "AVAX", "DOT", "LINK"]


def run_prepare_demo_dashboard(
    settings: Settings,
    years: int = 4,
    demo: bool = False,
    stock_reference: str = DEFAULT_DASHBOARD_STOCK_REFERENCE,
    currency_reference: str = DEFAULT_CURRENCY_REFERENCE,
    crypto_reference: str = DEFAULT_CRYPTO_REFERENCE,
    macro_reference: str = DEFAULT_MACRO_REFERENCE,
    stock_limit: int = 32,
) -> int:
    if years <= 0:
        raise ValueError("--years deve ser maior que zero")
    if stock_limit <= 0:
        raise ValueError("--stock-limit deve ser maior que zero")

    effective_settings = replace(settings, market_data_demo_mode=True, market_data_provider="mock") if demo else settings
    end_day = date.today()
    start_day = end_day - timedelta(days=years * 365)
    start = start_day.isoformat()
    end = end_day.isoformat()

    print("Preparing demo dashboard data")
    print(f"SQLite DB path: {get_system_status(settings.db_path)['db_path']}")

    run_id = start_ingest_run(
        db_path=settings.db_path,
        mode="dashboard_prepare_demo",
        base="DEM",
        symbols=["ALL"],
        start=start,
        end=end,
    )

    try:
        row_count = 0
        row_count += _prepare_stocks(effective_settings, stock_reference, start, end, stock_limit)
        row_count += _prepare_fx(effective_settings, currency_reference, start_day, end_day)
        row_count += _prepare_crypto(effective_settings, crypto_reference, start, end)
        row_count += _prepare_macro(effective_settings, macro_reference, start, end)
        deduplicate_dashboard_records(settings.db_path)

        snapshots = build_analysis_snapshots(settings.db_path)
        row_count += insert_analysis_snapshots(settings.db_path, snapshots)
        deduplicate_dashboard_records(settings.db_path)

        finish_ingest_run(settings.db_path, run_id, status="OK", row_count=row_count, error=None)
        _print_readiness_summary(settings.db_path)
        return 0
    except Exception as exc:
        finish_ingest_run(settings.db_path, run_id, status="FAIL", row_count=0, error=str(exc))
        raise


def _prepare_stocks(settings: Settings, reference: str, start: str, end: str, stock_limit: int) -> int:
    provider = build_market_provider(
        provider_name=settings.market_data_provider,
        api_key=settings.twelve_data_api_key,
        demo_mode=settings.market_data_demo_mode,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
    )
    instruments = [row for row in load_stock_watchlist(reference, provider=provider.name) if row.is_active][:stock_limit]
    rows_written = upsert_instruments(settings.db_path, instruments)
    quote_rows: list[MarketQuoteRow] = []

    for instrument in instruments:
        history = provider.fetch_stock_daily(
            symbol=instrument.provider_symbol or instrument.symbol,
            start=start,
            end=end,
            exchange=instrument.exchange,
        )
        rows_written += upsert_stock_prices_daily(settings.db_path, history)
        quote = _stock_quote_from_history(instrument.symbol, instrument.exchange, history)
        if quote is not None:
            quote_rows.append(quote)

    rows_written += upsert_market_quotes_latest(settings.db_path, quote_rows)
    return rows_written


def _stock_quote_from_history(
    symbol: str,
    exchange: str | None,
    history: list[StockPriceDailyRow],
) -> MarketQuoteRow | None:
    valid_rows = [row for row in history if row.close is not None]
    if not valid_rows:
        return None
    latest = valid_rows[-1]
    previous = valid_rows[-2] if len(valid_rows) > 1 else None
    price = float(latest.close)
    previous_close = float(previous.close) if previous and previous.close is not None else None
    change = price - previous_close if previous_close is not None else None
    percent_change = ((price / previous_close) - 1.0) * 100.0 if previous_close else None
    spread = max(0.01, price * 0.0005)
    return MarketQuoteRow(
        symbol=symbol.strip().upper(),
        asset_type="STOCK",
        exchange=exchange or latest.exchange,
        price=round(price, 4),
        bid=round(price - spread, 4),
        ask=round(price + spread, 4),
        open=latest.open,
        high=latest.high,
        low=latest.low,
        previous_close=round(previous_close, 4) if previous_close is not None else None,
        change=round(change, 4) if change is not None else None,
        percent_change=round(percent_change, 4) if percent_change is not None else None,
        volume=latest.volume,
        quote_time=latest.date,
        provider=latest.provider,
        fetched_at=utc_now_iso(),
    )


def _prepare_fx(settings: Settings, reference: str, start: date, end: date) -> int:
    currency_rows = [
        row
        for row in load_currency_reference(reference, provider="mock_fx" if settings.market_data_demo_mode else "frankfurter")
        if row.symbol in set(DASHBOARD_FX_SYMBOLS)
    ]
    rows_written = upsert_instruments(settings.db_path, currency_rows)

    fx_rows: list[FxRateRow] = []
    fetched_at = utc_now_iso()
    index = 0
    current = start
    while current <= end:
        for symbol in DASHBOARD_FX_SYMBOLS:
            fx_rows.append(
                FxRateRow(
                    date=current.isoformat(),
                    base="USD",
                    symbol=symbol,
                    rate=_mock_fx_rate(symbol, index),
                    source="mock_fx" if settings.market_data_demo_mode else "derived_fx",
                    fetched_at=fetched_at,
                )
            )
        index += 1
        current += timedelta(days=1)

    rows_written += upsert_fx_rates(settings.db_path, fx_rows)
    rows_written += upsert_market_quotes_latest(settings.db_path, _latest_fx_quotes(fx_rows))
    return rows_written


def _prepare_crypto(settings: Settings, reference: str, start: str, end: str) -> int:
    provider = build_crypto_provider(
        demo_mode=settings.market_data_demo_mode,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
    )
    assets = _select_crypto_assets(reference)
    rows_written = upsert_instruments(settings.db_path, _crypto_instruments(settings, assets))
    quote_rows: list[MarketQuoteRow] = []

    for asset in assets:
        history = provider.fetch_daily(asset, start, end)
        rows_written += upsert_crypto_prices_daily(settings.db_path, history)
        quote = _crypto_quote_from_history(asset, history)
        if quote is not None:
            quote_rows.append(quote)

    rows_written += upsert_market_quotes_latest(settings.db_path, quote_rows)
    return rows_written


def _crypto_quote_from_history(
    asset: CryptoAssetConfig,
    history: list[CryptoPriceDailyRow],
) -> MarketQuoteRow | None:
    valid_rows = [row for row in history if row.price_usd is not None]
    if not valid_rows:
        return None
    latest = valid_rows[-1]
    previous = valid_rows[-2] if len(valid_rows) > 1 else None
    price = float(latest.price_usd)
    previous_close = float(previous.price_usd) if previous and previous.price_usd is not None else None
    change = price - previous_close if previous_close is not None else None
    percent_change = ((price / previous_close) - 1.0) * 100.0 if previous_close else None
    spread = max(0.0001, price * 0.0008)
    return MarketQuoteRow(
        symbol=asset.symbol,
        asset_type="CRYPTO",
        exchange="CRYPTO",
        price=round(price, 4),
        bid=round(price - spread, 4),
        ask=round(price + spread, 4),
        open=round(previous_close, 4) if previous_close is not None else None,
        high=round(max(price, previous_close), 4) if previous_close is not None else round(price, 4),
        low=round(min(price, previous_close), 4) if previous_close is not None else round(price, 4),
        previous_close=round(previous_close, 4) if previous_close is not None else None,
        change=round(change, 4) if change is not None else None,
        percent_change=round(percent_change, 4) if percent_change is not None else None,
        volume=int(latest.volume_24h) if latest.volume_24h is not None else None,
        quote_time=latest.date,
        provider=latest.provider,
        fetched_at=utc_now_iso(),
    )


def _prepare_macro(settings: Settings, reference: str, start: str, end: str) -> int:
    provider = build_macro_provider(
        demo_mode=settings.market_data_demo_mode,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
    )
    indicators = load_macro_reference(reference)
    rows_written = upsert_instruments(settings.db_path, _macro_instruments(settings, indicators))
    quote_rows: list[MarketQuoteRow] = []

    for indicator in indicators:
        rows = provider.fetch_daily(indicator, start, end)
        rows_written += upsert_macro_indicators_daily(settings.db_path, rows)
        if rows:
            quote_rows.append(_macro_quote(indicator, rows[-2:]))

    rows_written += upsert_market_quotes_latest(settings.db_path, quote_rows)
    return rows_written


def _select_crypto_assets(reference: str) -> list[CryptoAssetConfig]:
    by_symbol = {asset.symbol: asset for asset in load_crypto_reference(reference) if asset.is_active}
    return [by_symbol[symbol] for symbol in DASHBOARD_CRYPTO_SYMBOLS if symbol in by_symbol]


def _crypto_instruments(settings: Settings, assets: Iterable[CryptoAssetConfig]) -> list[InstrumentRow]:
    now = utc_now_iso()
    return [
        InstrumentRow(
            symbol=asset.symbol,
            name=asset.name,
            asset_type="CRYPTO",
            exchange="CRYPTO",
            currency="USD",
            sector="Crypto",
            provider="mock" if settings.market_data_demo_mode else "coingecko",
            provider_symbol=asset.provider_code,
            is_active=1,
            priority=asset.priority,
            created_at=now,
            updated_at=now,
        )
        for asset in assets
    ]


def _macro_instruments(settings: Settings, indicators: Iterable[MacroIndicatorConfig]) -> list[InstrumentRow]:
    now = utc_now_iso()
    return [
        InstrumentRow(
            symbol=indicator.indicator_code,
            name=indicator.indicator_name,
            asset_type="MACRO",
            exchange="MACRO",
            currency=None,
            sector="Macro",
            provider="mock" if settings.market_data_demo_mode else indicator.source,
            provider_symbol=indicator.provider_code,
            is_active=1,
            priority=indicator.priority,
            created_at=now,
            updated_at=now,
        )
        for indicator in indicators
    ]


def _latest_fx_quotes(rows: list[FxRateRow]) -> list[MarketQuoteRow]:
    grouped: dict[str, list[FxRateRow]] = {}
    for row in rows:
        grouped.setdefault(row.symbol, []).append(row)

    quotes: list[MarketQuoteRow] = []
    fetched_at = utc_now_iso()
    for symbol, history in grouped.items():
        latest = history[-1]
        previous = history[-2] if len(history) > 1 else None
        change = latest.rate - previous.rate if previous else None
        percent_change = ((latest.rate / previous.rate) - 1.0) * 100.0 if previous and previous.rate else None
        spread = max(0.0001, latest.rate * 0.0004)
        quotes.append(
            MarketQuoteRow(
                symbol=symbol,
                asset_type="FX",
                exchange="USD",
                price=latest.rate,
                bid=round(latest.rate - spread, 6),
                ask=round(latest.rate + spread, 6),
                open=previous.rate if previous else latest.rate,
                high=max(latest.rate, previous.rate) if previous else latest.rate,
                low=min(latest.rate, previous.rate) if previous else latest.rate,
                previous_close=previous.rate if previous else None,
                change=change,
                percent_change=percent_change,
                volume=None,
                quote_time=latest.date,
                provider=latest.source,
                fetched_at=fetched_at,
            )
        )
    return quotes


def _macro_quote(indicator: MacroIndicatorConfig, rows: list[MacroIndicatorDailyRow]) -> MarketQuoteRow:
    latest = rows[-1]
    previous = rows[-2] if len(rows) > 1 else None
    latest_value = getattr(latest, "value", None)
    previous_value = getattr(previous, "value", None) if previous else None
    change = latest_value - previous_value if latest_value is not None and previous_value is not None else None
    percent_change = ((latest_value / previous_value) - 1.0) * 100.0 if latest_value is not None and previous_value else None
    return MarketQuoteRow(
        symbol=indicator.indicator_code,
        asset_type="MACRO",
        exchange="MACRO",
        price=latest_value,
        bid=None,
        ask=None,
        open=previous_value,
        high=max(latest_value, previous_value) if latest_value is not None and previous_value is not None else latest_value,
        low=min(latest_value, previous_value) if latest_value is not None and previous_value is not None else latest_value,
        previous_close=previous_value,
        change=change,
        percent_change=percent_change,
        volume=None,
        quote_time=getattr(latest, "date", None),
        provider="mock_macro",
        fetched_at=utc_now_iso(),
    )


def _mock_fx_rate(symbol: str, index: int) -> float:
    bases = {
        "BRL": 5.08,
        "USD": 1.0,
        "EUR": 0.92,
        "GBP": 0.79,
        "JPY": 156.0,
        "CAD": 1.36,
        "AUD": 1.52,
        "CHF": 0.90,
        "NZD": 1.67,
        "CNY": 7.22,
        "MXN": 17.1,
        "ARS": 875.0,
        "CLP": 930.0,
        "COP": 3900.0,
        "ZAR": 18.4,
        "SEK": 10.55,
        "NOK": 10.75,
        "DKK": 6.87,
        "PLN": 3.95,
    }
    normalized = symbol.strip().upper()
    base = bases[normalized]
    phase = _stable_unit(normalized, 0) * math.pi
    drift = index * base * 0.00003
    wave = math.sin(index / 11.0 + phase) * base * 0.009
    noise = (_stable_unit(normalized, index) - 0.5) * base * 0.002
    return round(max(0.0001, base + drift + wave + noise), 6)


def _stable_unit(value: str, salt: int) -> float:
    raw = f"{value.strip().upper()}:{salt}".encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def _print_readiness_summary(db_path: str) -> None:
    summary = get_dashboard_summary(db_path)
    status = get_system_status(db_path)
    print("Data readiness completed:")
    print(f"total_instruments: {summary['total_instruments']}")
    print(f"active_stocks: {summary['active_stocks']}")
    print(f"active_currencies: {summary['active_currencies']}")
    print(f"active_crypto: {summary['active_crypto']}")
    print(f"active_macro: {summary['active_macro']}")
    print(f"latest_quote_count: {summary['latest_quote_count']}")
    print(f"latest_analysis_count: {summary['latest_analysis_count']}")
    print(f"historical_rows: {status['historical_row_count']}")
    print(f"date_min: {status['date_min']}")
    print(f"date_max: {status['date_max']}")
