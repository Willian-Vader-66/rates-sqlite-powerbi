from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

from .config import Settings
from .db_sqlite import (
    finish_ingest_run,
    start_ingest_run,
    upsert_instruments,
    upsert_market_quotes_latest,
    upsert_stock_prices_daily,
)
from .market_providers import MarketDataProvider, build_market_provider
from .utils import normalize_symbol_list, parse_yyyy_mm_dd
from .watchlist import load_stock_watchlist

logger = logging.getLogger(__name__)


def run_import_instruments(settings: Settings, file_path: str) -> int:
    rows = load_stock_watchlist(file_path, provider=settings.market_data_provider)
    count = upsert_instruments(settings.db_path, rows)
    logger.info("instruments_imported=%s file=%s", count, file_path, extra={"event": "instruments_import"})
    return 0


def run_stocks_daily(settings: Settings, watchlist: str) -> int:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=7)
    return run_stocks_backfill(
        settings=settings,
        start=start.isoformat(),
        end=end.isoformat(),
        watchlist=watchlist,
        mode="stocks_daily",
    )


def run_stocks_backfill(settings: Settings, start: str, end: str, watchlist: str, mode: str = "stocks_backfill") -> int:
    parse_yyyy_mm_dd(start)
    parse_yyyy_mm_dd(end)
    instruments = [row for row in load_stock_watchlist(watchlist, provider=settings.market_data_provider) if row.is_active]
    upsert_instruments(settings.db_path, instruments)
    symbols = [row.symbol for row in instruments]

    run_id = start_ingest_run(
        db_path=settings.db_path,
        mode=mode,
        base="STK",
        symbols=symbols or ["NONE"],
        start=start,
        end=end,
    )
    provider = _provider(settings)
    rows_written = 0
    errors: list[str] = []

    for instrument in instruments:
        try:
            rows = provider.fetch_stock_daily(
                symbol=instrument.provider_symbol or instrument.symbol,
                start=start,
                end=end,
                exchange=instrument.exchange,
            )
            rows_written += upsert_stock_prices_daily(settings.db_path, rows)
            logger.info(
                "stock_history_ingested symbol=%s rows=%s provider=%s",
                instrument.symbol,
                len(rows),
                provider.name,
                extra={"event": "stock_history_ingested"},
            )
        except Exception as exc:
            message = f"{instrument.symbol}: {exc}"
            errors.append(message)
            logger.warning("stock_history_failed %s", message, extra={"event": "stock_history_failed"})

    status = _run_status(rows_written, errors)
    finish_ingest_run(settings.db_path, run_id, status=status, row_count=rows_written, error="; ".join(errors) or None)
    logger.info(
        "stock_ingest_finished status=%s rows=%s errors=%s",
        status,
        rows_written,
        len(errors),
        extra={"event": "stock_ingest_finished"},
    )
    return 0 if status in {"OK", "PARTIAL"} else 1


def run_quotes_poll(
    settings: Settings,
    symbols: list[str],
    interval_seconds: int,
    duration_minutes: float,
    asset_type: str = "STOCK",
) -> int:
    normalized_symbols = normalize_symbol_list(symbols)
    if interval_seconds <= 0:
        raise ValueError("--interval-seconds deve ser maior que zero")
    if duration_minutes < 0:
        raise ValueError("--duration-minutes deve ser zero ou maior")

    run_id = start_ingest_run(
        db_path=settings.db_path,
        mode="quotes_poll",
        base="STK" if asset_type.strip().upper() == "STOCK" else "FXR",
        symbols=normalized_symbols,
        start=None,
        end=None,
    )
    provider = _provider(settings)
    deadline = time.monotonic() + (duration_minutes * 60)
    first_pass = True
    rows_written = 0
    errors: list[str] = []

    while first_pass or time.monotonic() < deadline:
        first_pass = False
        batch = []
        for symbol in normalized_symbols:
            try:
                batch.append(provider.fetch_quote(symbol=symbol, asset_type=asset_type))
            except Exception as exc:
                message = f"{symbol}: {exc}"
                errors.append(message)
                logger.warning("quote_failed %s", message, extra={"event": "quote_failed"})
        rows_written += upsert_market_quotes_latest(settings.db_path, batch)
        logger.info("quotes_polled count=%s provider=%s", len(batch), provider.name, extra={"event": "quotes_polled"})
        if time.monotonic() + interval_seconds > deadline:
            break
        time.sleep(interval_seconds)

    status = _run_status(rows_written, errors)
    finish_ingest_run(settings.db_path, run_id, status=status, row_count=rows_written, error="; ".join(errors) or None)
    return 0 if status in {"OK", "PARTIAL"} else 1


def _provider(settings: Settings) -> MarketDataProvider:
    return build_market_provider(
        provider_name=settings.market_data_provider,
        api_key=settings.twelve_data_api_key,
        demo_mode=settings.market_data_demo_mode,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
        request_logger=logger,
    )


def _run_status(rows_written: int, errors: list[str]) -> str:
    if errors and rows_written:
        return "PARTIAL"
    if errors:
        return "FAIL"
    return "OK"
