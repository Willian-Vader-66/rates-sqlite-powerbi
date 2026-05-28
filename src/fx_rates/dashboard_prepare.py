from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

from .api_frankfurter import FrankfurterClient, normalize_timeseries
from .analysis import build_analysis_snapshots, build_analysis_snapshots_from_live_rows
from .config import Settings
from .crypto_providers import CryptoAssetConfig, MockCryptoProvider, build_crypto_provider, load_crypto_reference
from .db_sqlite import (
    commit_prepared_live_dataset,
    get_data_mode_summary,
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
from .macro_providers import MacroIndicatorConfig, MockMacroProvider, build_macro_provider, load_macro_reference
from .market_providers import MockMarketDataProvider, build_market_provider
from .live_history import LiveHistoryPolicy, days_from_args, validate_requested_days
from .models import (
    CryptoPriceDailyRow,
    FxRateRow,
    InstrumentRow,
    MacroIndicatorDailyRow,
    MarketQuoteRow,
    StockPriceDailyRow,
)
from .provider_status import providers_status
from .redaction import redact_text
from .utils import normalize_symbol_list, utc_now_iso
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


@dataclass
class LiveStage:
    instruments: list[InstrumentRow]
    stock_rows: list[StockPriceDailyRow]
    fx_rows: list[FxRateRow]
    crypto_rows: list[CryptoPriceDailyRow]
    macro_rows: list[MacroIndicatorDailyRow]
    quote_rows: list[MarketQuoteRow]
    failures: list[str]

    @property
    def row_count(self) -> int:
        return (
            len(self.instruments)
            + len(self.stock_rows)
            + len(self.fx_rows)
            + len(self.crypto_rows)
            + len(self.macro_rows)
            + len(self.quote_rows)
        )

    def extend(self, other: "LiveStage") -> None:
        self.instruments.extend(other.instruments)
        self.stock_rows.extend(other.stock_rows)
        self.fx_rows.extend(other.fx_rows)
        self.crypto_rows.extend(other.crypto_rows)
        self.macro_rows.extend(other.macro_rows)
        self.quote_rows.extend(other.quote_rows)
        self.failures.extend(other.failures)


def run_prepare_demo_dashboard(
    settings: Settings,
    years: int | None = None,
    days: int | None = None,
    demo: bool = False,
    stock_reference: str = DEFAULT_DASHBOARD_STOCK_REFERENCE,
    currency_reference: str = DEFAULT_CURRENCY_REFERENCE,
    crypto_reference: str = DEFAULT_CRYPTO_REFERENCE,
    macro_reference: str = DEFAULT_MACRO_REFERENCE,
    stock_limit: int = 32,
    symbols: list[str] | None = None,
) -> int:
    if stock_limit <= 0:
        raise ValueError("--stock-limit deve ser maior que zero")

    requested_days = days_from_args(days=days, years=years, default_days=365)
    effective_settings = replace(settings, market_data_demo_mode=True, market_data_provider="mock") if demo else settings
    selected_symbols = normalize_symbol_list(symbols) if symbols else None
    end_day = date.today()
    start_day = end_day - timedelta(days=requested_days - 1)
    start = start_day.isoformat()
    end = end_day.isoformat()

    print("Preparing demo dashboard data")
    print(f"SQLite DB path: {get_system_status(settings.db_path)['db_path']}")

    run_id = start_ingest_run(
        db_path=settings.db_path,
        mode="dashboard_prepare_demo",
        base="DEM",
        symbols=selected_symbols or ["ALL"],
        start=start,
        end=end,
    )

    try:
        row_count = 0
        row_count += _prepare_stocks(effective_settings, stock_reference, start, end, stock_limit, selected_symbols)
        row_count += _prepare_fx(effective_settings, currency_reference, start_day, end_day, selected_symbols)
        row_count += _prepare_crypto(effective_settings, crypto_reference, start, end, selected_symbols)
        row_count += _prepare_macro(effective_settings, macro_reference, start, end, selected_symbols)
        deduplicate_dashboard_records(settings.db_path)

        snapshots = build_analysis_snapshots(settings.db_path, symbols=selected_symbols)
        row_count += insert_analysis_snapshots(settings.db_path, snapshots)
        deduplicate_dashboard_records(settings.db_path)

        finish_ingest_run(settings.db_path, run_id, status="OK", row_count=row_count, error=None)
        _print_readiness_summary(settings.db_path)
        return 0
    except Exception as exc:
        finish_ingest_run(settings.db_path, run_id, status="FAIL", row_count=0, error=redact_text(exc))
        raise


def run_prepare_live_dashboard(
    settings: Settings,
    years: int | None = None,
    days: int | None = None,
    allow_mixed: bool = False,
    replace_demo: bool = False,
    symbols: list[str] | None = None,
    asset_type: str | None = None,
    stock_reference: str = DEFAULT_DASHBOARD_STOCK_REFERENCE,
    currency_reference: str = DEFAULT_CURRENCY_REFERENCE,
    crypto_reference: str = DEFAULT_CRYPTO_REFERENCE,
    macro_reference: str = DEFAULT_MACRO_REFERENCE,
    stock_limit: int = 32,
) -> int:
    if stock_limit <= 0:
        raise ValueError("--stock-limit deve ser maior que zero")

    selected_asset_types = _selected_asset_types(asset_type)
    selected_symbols = normalize_symbol_list(symbols) if symbols else None
    requested_days = days_from_args(days=days, years=years, default_days=settings.live_default_days)
    validate_requested_days(
        LiveHistoryPolicy(
            default_days=settings.live_default_days,
            max_free_days=settings.live_max_free_days,
            mode=settings.live_history_mode,
            advanced_max_years=settings.live_advanced_max_years,
        ),
        requested_days,
        provider_plan=settings.coingecko_api_plan if "CRYPTO" in selected_asset_types else None,
    )
    provider_state = providers_status(settings, test_external=False)
    missing = [
        item for item in provider_state["providers"]
        if item["asset_type"] in selected_asset_types and (not item["configured"] or not item["available"])
    ]
    if missing:
        print(f"prepare-live aborted before DB mutation: provider validation failed for {','.join(selected_asset_types)}")
        for item in missing:
            missing_env = ",".join(item.get("missing_env") or []) or "-"
            print(
                f"{item['asset_type']}: provider={item['provider']} configured={item['configured']} "
                f"available={item['available']} key_present={item.get('key_present')} "
                f"key_valid_format={item.get('key_valid_format')} missing_env={missing_env} message={item.get('message')}"
        )
        print("Run: python -m fx_rates providers status")
        print("Live-first staging command: python -m fx_rates dashboard build-live-db --days 365 --db-path .tmp/live-main-candidate.sqlite --external-test")
        return 2

    data_mode = get_data_mode_summary(settings.db_path)
    current_mode = data_mode["data_mode"]
    if current_mode == "demo" and not (allow_mixed or replace_demo):
        print("prepare-live stopped: current SQLite dataset contains demo data.")
        print("Use --replace-demo to replace demo data, or --allow-mixed to explicitly allow a mixed dataset.")
        return 3
    if current_mode == "mixed" and not allow_mixed:
        print("prepare-live stopped: current SQLite dataset is already mixed.")
        print("Use --allow-mixed after reviewing: python -m fx_rates dashboard audit-market")
        return 3

    end_day = date.today()
    start_day = end_day - timedelta(days=requested_days - 1)
    start = start_day.isoformat()
    end = end_day.isoformat()
    print("Preparing LIVE dashboard data")
    print(f"SQLite DB path: {get_system_status(settings.db_path)['db_path']}")
    print(f"Asset types: {', '.join(selected_asset_types)}")
    print(f"Symbols: {', '.join(selected_symbols) if selected_symbols else 'reference defaults'}")
    print(f"Requested days: {requested_days}")
    print(f"History mode: {settings.live_history_mode}")

    run_id = start_ingest_run(
        db_path=settings.db_path,
        mode="dashboard_prepare_live",
        base="LIV",
        symbols=selected_symbols or selected_asset_types,
        start=start,
        end=end,
    )
    try:
        stage = LiveStage([], [], [], [], [], [], [])
        if "STOCK" in selected_asset_types:
            stage.extend(_stage_live_stocks(settings, stock_reference, start, end, stock_limit, selected_symbols))
        if "FX" in selected_asset_types:
            stage.extend(_stage_live_fx(settings, currency_reference, start_day, end_day, selected_symbols))
        if "CRYPTO" in selected_asset_types:
            stage.extend(_stage_live_crypto(settings, crypto_reference, start, end, selected_symbols))
        if "MACRO" in selected_asset_types:
            stage.extend(_stage_live_macro(settings, macro_reference, start, end, selected_symbols))

        fetch_failures = list(stage.failures)
        validation_failures = _validate_live_stage(stage, selected_asset_types, selected_symbols, settings=settings)
        if validation_failures:
            all_failures = fetch_failures + validation_failures
            message = "prepare-live aborted before DB mutation: staged live data is inconsistent; existing demo/live data preserved. " + "; ".join(all_failures)
            print(message)
            finish_ingest_run(settings.db_path, run_id, status="FAIL", row_count=0, error=message)
            return 4
        if fetch_failures and not allow_mixed:
            message = "prepare-live aborted before DB mutation: live fetch failed; existing demo/live data preserved. " + "; ".join(fetch_failures)
            print(message)
            finish_ingest_run(settings.db_path, run_id, status="FAIL", row_count=0, error=message)
            return 4
        if stage.row_count <= 0:
            message = "prepare-live aborted before DB mutation: no validated live rows; existing demo/live data preserved."
            print(message)
            finish_ingest_run(settings.db_path, run_id, status="FAIL", row_count=0, error=message)
            return 5

        snapshots = build_analysis_snapshots_from_live_rows(
            stock_rows=stage.stock_rows,
            fx_rows=stage.fx_rows,
            crypto_rows=stage.crypto_rows,
            macro_rows=stage.macro_rows,
        )
        row_count = commit_prepared_live_dataset(
            settings.db_path,
            instruments=stage.instruments,
            stock_rows=stage.stock_rows,
            fx_rows=stage.fx_rows,
            crypto_rows=stage.crypto_rows,
            macro_rows=stage.macro_rows,
            quote_rows=stage.quote_rows,
            analysis_rows=snapshots,
            replace_demo=replace_demo,
            asset_types=selected_asset_types,
            symbols=selected_symbols,
        )

        finish_ingest_run(
            settings.db_path,
            run_id,
            status="OK" if not fetch_failures else "WARN",
            row_count=row_count,
            error="; ".join(fetch_failures) if fetch_failures else None,
        )
        _print_readiness_summary(settings.db_path)
        if fetch_failures:
            print("Live ingest completed with explicit mixed/unsupported warnings:")
            for failure in fetch_failures:
                print(f"  {failure}")
        return 0
    except Exception as exc:
        message = f"prepare-live failed: {redact_text(exc)}"
        print(message)
        finish_ingest_run(settings.db_path, run_id, status="FAIL", row_count=0, error=message)
        return 6


def _selected_asset_types(asset_type: str | None) -> list[str]:
    if not asset_type or asset_type.strip().upper() == "ALL":
        return ["STOCK", "FX", "CRYPTO", "MACRO"]
    normalized = asset_type.strip().upper()
    if normalized not in {"STOCK", "FX", "CRYPTO", "MACRO"}:
        raise ValueError("--asset-type deve ser STOCK, FX, CRYPTO, MACRO ou ALL")
    return [normalized]



MIN_LIVE_HISTORY_ROWS = 2
MIN_LIVE_CRYPTO_ROWS_ADVANCED = 1300
LIVE_MACRO_UNITS = {"% a.a.", "% a.m.", "% a.d.", "index"}
DEMO_PROVIDER_MARKERS = ("mock", "demo")


def _validate_live_stage(stage: LiveStage, asset_types: list[str], symbols: list[str] | None, *, settings: Settings) -> list[str]:
    failures: list[str] = []
    quote_map = {(row.asset_type.upper(), row.symbol.upper()): row for row in stage.quote_rows}
    for instrument in stage.instruments:
        failures.extend(_live_origin_failures("INSTRUMENT", instrument.asset_type, instrument.symbol, instrument.data_mode, instrument.provider))

    if "STOCK" in asset_types:
        stock_groups = _group_by_symbol(stage.stock_rows, lambda row: row.symbol)
        expected = _expected_symbols(stage.instruments, "STOCK", symbols)
        failures.extend(_validate_symbol_groups("STOCK", expected, stock_groups, quote_map, lambda row: row.close, lambda row: row.date, fail_pct=settings.live_quote_fail_pct))
        for symbol, rows in stock_groups.items():
            for row in rows:
                failures.extend(_live_origin_failures("STOCK", "STOCK", symbol, row.data_mode, row.provider))
                if row.close is None or row.close <= 0:
                    failures.append(f"STOCK {symbol}: invalid non-positive close")
                if row.currency and row.currency.upper() != "USD":
                    failures.append(f"STOCK {symbol}: expected currency USD, got {row.currency}")
    if "FX" in asset_types:
        fx_groups = _group_by_symbol(stage.fx_rows, lambda row: row.symbol)
        expected = _expected_symbols(stage.instruments, "FX", symbols)
        failures.extend(_validate_symbol_groups("FX", expected, fx_groups, quote_map, lambda row: row.rate, lambda row: row.date, fail_pct=settings.live_quote_fail_pct))
        for symbol, rows in fx_groups.items():
            for row in rows:
                failures.extend(_live_origin_failures("FX", "FX", symbol, row.data_mode, row.source))
                if row.rate <= 0:
                    failures.append(f"FX USD/{symbol}: invalid non-positive rate")
    if "CRYPTO" in asset_types:
        crypto_groups = _group_by_symbol(stage.crypto_rows, lambda row: row.symbol)
        expected = _expected_symbols(stage.instruments, "CRYPTO", symbols)
        failures.extend(_validate_symbol_groups("CRYPTO", expected, crypto_groups, quote_map, lambda row: row.price_usd, lambda row: row.date, fail_pct=settings.live_quote_fail_pct))
        for symbol, rows in crypto_groups.items():
            for row in rows:
                failures.extend(_live_origin_failures("CRYPTO", "CRYPTO", symbol, row.data_mode, row.provider))
                if row.price_usd is None or row.price_usd <= 0:
                    failures.append(f"CRYPTO {symbol}: invalid non-positive USD price")
    if "MACRO" in asset_types:
        macro_groups = _group_by_symbol(stage.macro_rows, lambda row: row.indicator_code)
        expected = _expected_symbols(stage.instruments, "MACRO", symbols)
        failures.extend(_validate_symbol_groups("MACRO", expected, macro_groups, quote_map, lambda row: row.value, lambda row: row.date, allow_negative=True, fail_pct=settings.live_quote_fail_pct))
        for symbol, rows in macro_groups.items():
            for row in rows:
                failures.extend(_live_origin_failures("MACRO", "MACRO", symbol, row.data_mode, row.source))
                if row.value is None:
                    failures.append(f"MACRO {symbol}: missing value")
                if row.unit not in LIVE_MACRO_UNITS:
                    failures.append(f"MACRO {symbol}: unsupported unit {row.unit}")
    for quote in stage.quote_rows:
        failures.extend(_live_origin_failures("QUOTE", quote.asset_type, quote.symbol, quote.data_mode, quote.provider))
        if quote.asset_type.upper() in {"STOCK", "FX", "CRYPTO"} and (quote.price is None or quote.price <= 0):
            failures.append(f"{quote.asset_type.upper()} {quote.symbol}: invalid non-positive latest quote")
    return sorted(set(failures))


def _expected_symbols(instruments: list[InstrumentRow], asset_type: str, symbols: list[str] | None) -> set[str]:
    instrument_symbols = {row.symbol.upper() for row in instruments if row.asset_type.upper() == asset_type}
    requested = {symbol.upper() for symbol in symbols or []}
    return instrument_symbols or requested


def _group_by_symbol(rows: Iterable[object], key_func) -> dict[str, list[object]]:
    grouped: dict[str, list[object]] = {}
    for row in rows:
        grouped.setdefault(str(key_func(row)).upper(), []).append(row)
    return grouped


def _validate_symbol_groups(
    asset_type: str,
    expected: set[str],
    groups: dict[str, list[object]],
    quote_map: dict[tuple[str, str], MarketQuoteRow],
    value_func,
    date_func,
    *,
    allow_negative: bool = False,
    fail_pct: float = 5.0,
) -> list[str]:
    failures: list[str] = []
    for symbol in sorted(expected):
        rows = groups.get(symbol, [])
        if not rows:
            failures.append(f"{asset_type} {symbol}: no validated live history")
            continue
        if len(rows) < MIN_LIVE_HISTORY_ROWS:
            failures.append(f"{asset_type} {symbol}: insufficient live history rows ({len(rows)})")
        dates = []
        for row in rows:
            raw_date = str(date_func(row))
            try:
                dates.append(date.fromisoformat(raw_date[:10]))
            except ValueError:
                failures.append(f"{asset_type} {symbol}: invalid date {raw_date}")
            value = value_func(row)
            if value is None:
                failures.append(f"{asset_type} {symbol}: missing history value")
            elif not allow_negative and float(value) <= 0:
                failures.append(f"{asset_type} {symbol}: non-positive history value")
        if dates:
            if max(dates) < min(dates):
                failures.append(f"{asset_type} {symbol}: incoherent date range")
            if max(dates) > date.today() + timedelta(days=1):
                failures.append(f"{asset_type} {symbol}: future history date {max(dates).isoformat()}")
        quote = quote_map.get((asset_type, symbol))
        if quote is None:
            failures.append(f"{asset_type} {symbol}: missing latest quote")
        else:
            latest = sorted(rows, key=lambda row: str(date_func(row)))[-1]
            latest_value = value_func(latest)
            if latest_value not in (None, 0) and quote.price is not None:
                diff_pct = abs((float(quote.price) / float(latest_value)) - 1.0) * 100.0
                if diff_pct > fail_pct:
                    failures.append(f"{asset_type} {symbol}: latest quote differs from history by {diff_pct:.2f}%")
            if quote.quote_time and dates:
                try:
                    quote_date = date.fromisoformat(str(quote.quote_time)[:10])
                except ValueError:
                    failures.append(f"{asset_type} {symbol}: invalid latest quote date {quote.quote_time}")
                else:
                    if quote_date < max(dates):
                        failures.append(f"{asset_type} {symbol}: latest quote older than history")
                    if quote_date > date.today() + timedelta(days=1):
                        failures.append(f"{asset_type} {symbol}: future latest quote date {quote_date.isoformat()}")
    return failures


def _live_origin_failures(kind: str, asset_type: str, symbol: str, data_mode: str | None, provider: str | None) -> list[str]:
    failures: list[str] = []
    normalized_mode = (data_mode or "unknown").strip().lower()
    normalized_provider = (provider or "").strip().lower()
    label = f"{kind} {asset_type.upper()} {symbol.upper()}"
    if normalized_mode != "live":
        failures.append(f"{label}: expected data_mode=live, got {normalized_mode or 'unknown'}")
    if not normalized_provider:
        failures.append(f"{label}: missing provider")
    elif any(marker in normalized_provider for marker in DEMO_PROVIDER_MARKERS):
        failures.append(f"{label}: demo-like provider cannot be live ({provider})")
    return failures


def _stage_live_stocks(
    settings: Settings,
    reference: str,
    start: str,
    end: str,
    stock_limit: int,
    symbols: list[str] | None,
) -> LiveStage:
    provider = MockMarketDataProvider() if settings.stock_provider == "fake_live" else build_market_provider(
        provider_name=settings.stock_provider,
        api_key=settings.twelve_data_api_key,
        demo_mode=False,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
    )
    provider_label = "fake_live" if settings.stock_provider == "fake_live" else provider.name
    wanted = set(symbols or [])
    instruments = [
        replace(row, provider=provider_label, data_mode="live")
        for row in load_stock_watchlist(reference, provider=provider_label)
        if row.is_active and (not wanted or row.symbol in wanted)
    ][:stock_limit]
    failures: list[str] = []
    history_rows: list[StockPriceDailyRow] = []
    quote_rows: list[MarketQuoteRow] = []
    for instrument in instruments:
        try:
            history = provider.fetch_stock_daily(
                symbol=instrument.provider_symbol or instrument.symbol,
                start=start,
                end=end,
                exchange=instrument.exchange,
            )
            history = [
                replace(row, symbol=instrument.symbol, provider=provider_label, data_mode="live", source_updated_at=row.date)
                for row in history
            ]
        except Exception as exc:
            failures.append(f"STOCK {instrument.symbol}: provider={provider_label} error={redact_text(exc)}")
            continue
        if not history:
            failures.append(f"STOCK {instrument.symbol}: provider={provider_label} returned no history")
            continue
        history = sorted(history, key=lambda row: row.date)
        quote = _stock_quote_from_history(instrument.symbol, instrument.exchange, history)
        if quote is not None:
            quote_rows.append(replace(quote, provider=provider_label, data_mode="live", source_updated_at=quote.quote_time))
        history_rows.extend(history)

    return LiveStage(instruments, history_rows, [], [], [], quote_rows, failures)


def _stage_live_fx(
    settings: Settings,
    reference: str,
    start: date,
    end: date,
    symbols: list[str] | None,
) -> LiveStage:
    wanted = set(symbols or DASHBOARD_FX_SYMBOLS)
    currency_rows = [
        replace(row, provider=settings.fx_provider, data_mode="live")
        for row in load_currency_reference(reference, provider=settings.fx_provider)
        if row.symbol in wanted and row.symbol in set(DASHBOARD_FX_SYMBOLS)
    ]
    requested_symbols = [row.symbol for row in currency_rows]
    provider_label = "fake_live" if settings.fx_provider == "fake_live" else "frankfurter"
    failures: list[str] = []
    fx_rows: list[FxRateRow] = []
    if settings.fx_provider == "fake_live":
        index = 0
        current = start
        fetched_at = utc_now_iso()
        while current <= end:
            for symbol in requested_symbols:
                fx_rows.append(
                    FxRateRow(
                        date=current.isoformat(),
                        base="USD",
                        symbol=symbol,
                        rate=1.0 if symbol == "USD" else _mock_fx_rate(symbol, index),
                        source=provider_label,
                        fetched_at=fetched_at,
                        data_mode="live",
                        source_updated_at=current.isoformat(),
                    )
                )
            index += 1
            current += timedelta(days=1)
    else:
        external_symbols = [symbol for symbol in requested_symbols if symbol != "USD"]
        if external_symbols:
            try:
                client = FrankfurterClient(
                    base_url=settings.api_base_url,
                    cache_dir=settings.cache_dir,
                    timeout_seconds=settings.timeout_seconds,
                    use_cache=settings.use_cache,
                    max_retries=settings.max_retries,
                    use_cache_latest=settings.use_cache_latest,
                )
                payload = client.fetch_timeseries(start=start.isoformat(), end=end.isoformat(), base="USD", symbols=external_symbols)
                fx_rows = [
                    replace(row, source=provider_label, data_mode="live", source_updated_at=row.date)
                    for row in normalize_timeseries(payload, base="USD", source=provider_label)
                ]
            except Exception as exc:
                return LiveStage([], [], [], [], [], [], [f"FX USD/{','.join(external_symbols)}: provider={provider_label} error={redact_text(exc)}"])
        dates = sorted({row.date for row in fx_rows})
        if "USD" in requested_symbols:
            if not dates:
                dates = [(start + timedelta(days=offset)).isoformat() for offset in range((end - start).days + 1)]
            fetched_at = utc_now_iso()
            fx_rows.extend(
                FxRateRow(
                    date=raw_date,
                    base="USD",
                    symbol="USD",
                    rate=1.0,
                    source=provider_label,
                    fetched_at=fetched_at,
                    data_mode="live",
                    source_updated_at=raw_date,
                )
                for raw_date in dates
            )
    returned_symbols = {row.symbol for row in fx_rows}
    for symbol in requested_symbols:
        if symbol not in returned_symbols:
            failures.append(f"FX USD/{symbol}: provider={provider_label} returned no supported history")
    quote_rows = [replace(row, data_mode="live", source_updated_at=row.quote_time) for row in _latest_fx_quotes(fx_rows)]
    return LiveStage(currency_rows, [], fx_rows, [], [], quote_rows, failures)


def _stage_live_crypto(
    settings: Settings,
    reference: str,
    start: str,
    end: str,
    symbols: list[str] | None,
) -> LiveStage:
    provider = MockCryptoProvider() if settings.crypto_provider == "fake_live" else build_crypto_provider(
        demo_mode=False,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
        coingecko_api_plan=settings.coingecko_api_plan,
        coingecko_demo_api_key=settings.coingecko_demo_api_key,
        coingecko_pro_api_key=settings.coingecko_pro_api_key,
    )
    provider_label = "fake_live" if settings.crypto_provider == "fake_live" else provider.name
    wanted = set(symbols or DASHBOARD_CRYPTO_SYMBOLS)
    assets = [asset for asset in _select_crypto_assets(reference) if asset.symbol in wanted]
    instruments = [replace(row, provider=provider_label, data_mode="live") for row in _crypto_instruments(replace(settings, market_data_demo_mode=False), assets)]
    failures: list[str] = []
    history_rows: list[CryptoPriceDailyRow] = []
    quote_rows: list[MarketQuoteRow] = []
    for asset in assets:
        try:
            history = [
                replace(row, provider=provider_label, data_mode="live", source_updated_at=row.date)
                for row in provider.fetch_daily(asset, start, end)
            ]
        except Exception as exc:
            failures.append(f"CRYPTO {asset.symbol}: provider={provider_label} error={redact_text(exc)}")
            continue
        if not history:
            failures.append(f"CRYPTO {asset.symbol}: provider={provider_label} returned no history")
            continue
        min_rows = _min_crypto_rows_for_range(start, end)
        if len(history) < min_rows:
            failures.append(f"CRYPTO {asset.symbol}: insufficient live history rows ({len(history)} < {min_rows})")
        quote = _crypto_quote_from_history(asset, history)
        if quote is not None:
            quote_rows.append(replace(quote, provider=provider_label, data_mode="live", source_updated_at=quote.quote_time))
        history_rows.extend(history)
    return LiveStage(instruments, [], [], history_rows, [], quote_rows, failures)


def _min_crypto_rows_for_range(start: str, end: str) -> int:
    try:
        start_day = date.fromisoformat(str(start)[:10])
        end_day = min(date.fromisoformat(str(end)[:10]), datetime.now(timezone.utc).date())
    except ValueError:
        return MIN_LIVE_HISTORY_ROWS
    days = max(1, (end_day - start_day).days + 1)
    if days >= 365 * 3:
        return MIN_LIVE_CRYPTO_ROWS_ADVANCED
    return max(MIN_LIVE_HISTORY_ROWS, int(days * 0.90))


def _stage_live_macro(
    settings: Settings,
    reference: str,
    start: str,
    end: str,
    symbols: list[str] | None,
) -> LiveStage:
    provider = MockMacroProvider() if settings.macro_provider == "fake_live" else build_macro_provider(
        demo_mode=False,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
    )
    provider_label = "fake_live" if settings.macro_provider == "fake_live" else provider.name
    wanted = set(symbols or [])
    indicators = [
        indicator for indicator in load_macro_reference(reference)
        if (not wanted or indicator.indicator_code in wanted)
    ]
    supported = [
        indicator for indicator in indicators
        if settings.macro_provider == "fake_live" or indicator.source.lower().startswith("banco central")
    ]
    unsupported = [
        f"MACRO {indicator.indicator_code}: provider={provider_label} unsupported live source={indicator.source}"
        for indicator in indicators
        if indicator not in supported
    ]
    failures = list(unsupported)
    history_rows: list[MacroIndicatorDailyRow] = []
    quote_rows: list[MarketQuoteRow] = []
    for indicator in supported:
        try:
            rows = [
                replace(row, source=provider_label, data_mode="live", source_updated_at=row.date)
                for row in provider.fetch_daily(indicator, start, end)
            ]
        except Exception as exc:
            failures.append(f"MACRO {indicator.indicator_code}: provider={provider_label} error={redact_text(exc)}")
            continue
        if not rows:
            failures.append(f"MACRO {indicator.indicator_code}: provider={provider_label} returned no history")
            continue
        history_rows.extend(rows)
        quote_rows.append(replace(_macro_quote(indicator, rows[-2:]), provider=provider_label, data_mode="live", source_updated_at=rows[-1].date))
    instruments = [replace(row, provider=provider_label, data_mode="live") for row in _macro_instruments(replace(settings, market_data_demo_mode=False), supported)]
    return LiveStage(instruments, [], [], [], history_rows, quote_rows, failures)


def _prepare_stocks(settings: Settings, reference: str, start: str, end: str, stock_limit: int, symbols: list[str] | None = None) -> int:
    provider = build_market_provider(
        provider_name=settings.market_data_provider,
        api_key=settings.twelve_data_api_key,
        demo_mode=settings.market_data_demo_mode,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
    )
    wanted = set(symbols or [])
    instruments = [row for row in load_stock_watchlist(reference, provider=provider.name) if row.is_active and (not wanted or row.symbol in wanted)][:stock_limit]
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
        history = sorted(history, key=lambda row: row.date)
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
    valid_rows = sorted((row for row in history if row.close is not None), key=lambda row: row.date)
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


def _prepare_fx(settings: Settings, reference: str, start: date, end: date, symbols: list[str] | None = None) -> int:
    wanted = set(symbols or DASHBOARD_FX_SYMBOLS)
    currency_rows = [
        row
        for row in load_currency_reference(reference, provider="mock_fx" if settings.market_data_demo_mode else "frankfurter")
        if row.symbol in set(DASHBOARD_FX_SYMBOLS) and row.symbol in wanted
    ]
    rows_written = upsert_instruments(settings.db_path, currency_rows)

    fx_rows: list[FxRateRow] = []
    fetched_at = utc_now_iso()
    index = 0
    current = start
    while current <= end:
        for symbol in [row.symbol for row in currency_rows]:
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


def _prepare_crypto(settings: Settings, reference: str, start: str, end: str, symbols: list[str] | None = None) -> int:
    provider = build_crypto_provider(
        demo_mode=settings.market_data_demo_mode,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
        coingecko_api_plan=settings.coingecko_api_plan,
        coingecko_demo_api_key=settings.coingecko_demo_api_key,
        coingecko_pro_api_key=settings.coingecko_pro_api_key,
    )
    wanted = set(symbols or DASHBOARD_CRYPTO_SYMBOLS)
    assets = [asset for asset in _select_crypto_assets(reference) if asset.symbol in wanted]
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
    valid_rows = sorted((row for row in history if row.price_usd is not None), key=lambda row: row.date)
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


def _prepare_macro(settings: Settings, reference: str, start: str, end: str, symbols: list[str] | None = None) -> int:
    provider = build_macro_provider(
        demo_mode=settings.market_data_demo_mode,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
    )
    wanted = set(symbols or [])
    indicators = [indicator for indicator in load_macro_reference(reference) if not wanted or indicator.indicator_code in wanted]
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
            display_name=asset.name,
            unit_label="USD",
            value_label="Crypto Price",
            expected_frequency="daily",
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
            display_name=indicator.indicator_name,
            unit_label=indicator.unit,
            value_label=_macro_value_label(indicator.indicator_code),
            expected_frequency="monthly" if indicator.indicator_code.endswith("_MONTHLY") else "business_daily",
        )
        for indicator in indicators
    ]


def _macro_value_label(indicator_code: str) -> str:
    code = indicator_code.strip().upper()
    if code in {"SELIC_DAILY", "CDI_DAILY"}:
        return "Daily Rate"
    if code == "IPCA_MONTHLY":
        return "Monthly Inflation"
    if code.endswith("_MONTHLY"):
        return "Monthly Rate"
    return "Macro Value"


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
        provider=latest.source,
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
