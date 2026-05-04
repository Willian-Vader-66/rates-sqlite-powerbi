from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from .config import Settings
from .crypto_providers import CryptoAssetConfig, build_crypto_provider, load_crypto_reference
from .db_sqlite import (
    finish_ingest_run,
    start_ingest_run,
    upsert_crypto_prices_daily,
    upsert_instruments,
    upsert_market_quotes_latest,
)
from .models import InstrumentRow
from .utils import normalize_symbol_list, parse_yyyy_mm_dd, utc_now_iso

logger = logging.getLogger(__name__)

DEFAULT_CRYPTO_REFERENCE = "data/reference/crypto_assets.csv"


def run_crypto_backfill(
    settings: Settings,
    start: str,
    end: str,
    reference: str = DEFAULT_CRYPTO_REFERENCE,
    symbols: list[str] | None = None,
    mode: str = "crypto_backfill",
) -> int:
    parse_yyyy_mm_dd(start)
    parse_yyyy_mm_dd(end)
    assets = _selected_assets(reference, symbols)
    _upsert_crypto_instruments(settings, assets)

    run_id = start_ingest_run(
        db_path=settings.db_path,
        mode=mode,
        base="CRY",
        symbols=[row.symbol for row in assets] or ["NONE"],
        start=start,
        end=end,
    )
    provider = build_crypto_provider(
        demo_mode=settings.market_data_demo_mode,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
    )
    rows_written = 0
    errors: list[str] = []
    for asset in assets:
        try:
            rows = provider.fetch_daily(asset, start, end)
            rows_written += upsert_crypto_prices_daily(settings.db_path, rows)
            logger.info(
                "crypto_history_ingested symbol=%s rows=%s provider=%s",
                asset.symbol,
                len(rows),
                provider.name,
                extra={"event": "crypto_history_ingested"},
            )
        except Exception as exc:
            message = f"{asset.symbol}: {exc}"
            errors.append(message)
            logger.warning("crypto_history_failed %s", message, extra={"event": "crypto_history_failed"})

    status = _run_status(rows_written, errors)
    finish_ingest_run(settings.db_path, run_id, status=status, row_count=rows_written, error="; ".join(errors) or None)
    return 0 if status in {"OK", "PARTIAL"} else 1


def run_crypto_daily(settings: Settings, reference: str = DEFAULT_CRYPTO_REFERENCE) -> int:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=10)
    return run_crypto_backfill(settings=settings, start=start.isoformat(), end=end.isoformat(), reference=reference, mode="crypto_daily")


def run_crypto_quotes(settings: Settings, symbols: list[str], reference: str = DEFAULT_CRYPTO_REFERENCE) -> int:
    selected = _selected_assets(reference, symbols)
    _upsert_crypto_instruments(settings, selected)
    run_id = start_ingest_run(
        db_path=settings.db_path,
        mode="crypto_quotes",
        base="CRY",
        symbols=[row.symbol for row in selected] or ["NONE"],
        start=None,
        end=None,
    )
    provider = build_crypto_provider(
        demo_mode=settings.market_data_demo_mode,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
    )
    rows = []
    errors: list[str] = []
    for asset in selected:
        try:
            rows.append(provider.fetch_quote(asset))
        except Exception as exc:
            message = f"{asset.symbol}: {exc}"
            errors.append(message)
            logger.warning("crypto_quote_failed %s", message, extra={"event": "crypto_quote_failed"})
    rows_written = upsert_market_quotes_latest(settings.db_path, rows)
    status = _run_status(rows_written, errors)
    finish_ingest_run(settings.db_path, run_id, status=status, row_count=rows_written, error="; ".join(errors) or None)
    return 0 if status in {"OK", "PARTIAL"} else 1


def _selected_assets(reference: str, symbols: list[str] | None) -> list[CryptoAssetConfig]:
    assets = [asset for asset in load_crypto_reference(reference) if asset.is_active]
    if not symbols:
        return assets
    wanted = set(normalize_symbol_list(symbols))
    return [asset for asset in assets if asset.symbol in wanted]


def _upsert_crypto_instruments(settings: Settings, assets: list[CryptoAssetConfig]) -> None:
    now = utc_now_iso()
    rows = [
        InstrumentRow(
            symbol=asset.symbol,
            name=asset.name,
            asset_type="CRYPTO",
            exchange="CRYPTO",
            currency="USD",
            sector="Crypto",
            provider="mock" if settings.market_data_demo_mode else "coingecko",
            provider_symbol=asset.provider_code,
            is_active=1 if asset.is_active else 0,
            priority=asset.priority,
            created_at=now,
            updated_at=now,
        )
        for asset in assets
    ]
    upsert_instruments(settings.db_path, rows)


def _run_status(rows_written: int, errors: list[str]) -> str:
    if errors and rows_written:
        return "PARTIAL"
    if errors:
        return "FAIL"
    return "OK"
