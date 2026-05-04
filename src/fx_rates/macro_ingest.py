from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from .config import Settings
from .db_sqlite import (
    finish_ingest_run,
    list_ingest_runs,
    start_ingest_run,
    upsert_instruments,
    upsert_macro_indicators_daily,
)
from .macro_providers import MacroIndicatorConfig, build_macro_provider, load_macro_reference
from .models import InstrumentRow
from .utils import parse_yyyy_mm_dd, utc_now_iso

logger = logging.getLogger(__name__)

DEFAULT_MACRO_REFERENCE = "data/reference/macro_indicators.csv"


def run_macro_backfill(
    settings: Settings,
    start: str,
    end: str,
    reference: str = DEFAULT_MACRO_REFERENCE,
    mode: str = "macro_backfill",
) -> int:
    parse_yyyy_mm_dd(start)
    parse_yyyy_mm_dd(end)
    indicators = [row for row in load_macro_reference(reference) if row.is_active]
    _upsert_macro_instruments(settings, indicators)

    run_id = start_ingest_run(
        db_path=settings.db_path,
        mode=mode,
        base="MAC",
        symbols=[row.indicator_code for row in indicators] or ["NONE"],
        start=start,
        end=end,
    )
    provider = build_macro_provider(
        demo_mode=settings.market_data_demo_mode,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
    )
    rows_written = 0
    errors: list[str] = []
    for indicator in indicators:
        try:
            rows = provider.fetch_daily(indicator, start, end)
            rows_written += upsert_macro_indicators_daily(settings.db_path, rows)
            logger.info(
                "macro_history_ingested code=%s rows=%s provider=%s",
                indicator.indicator_code,
                len(rows),
                provider.name,
                extra={"event": "macro_history_ingested"},
            )
        except Exception as exc:
            message = f"{indicator.indicator_code}: {exc}"
            errors.append(message)
            logger.warning("macro_history_failed %s", message, extra={"event": "macro_history_failed"})

    status = _run_status(rows_written, errors)
    finish_ingest_run(settings.db_path, run_id, status=status, row_count=rows_written, error="; ".join(errors) or None)
    return 0 if status in {"OK", "PARTIAL"} else 1


def run_macro_daily(settings: Settings, reference: str = DEFAULT_MACRO_REFERENCE) -> int:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=10)
    return run_macro_backfill(settings=settings, start=start.isoformat(), end=end.isoformat(), reference=reference, mode="macro_daily")


def run_macro_status(settings: Settings, last: int = 10) -> int:
    rows = [
        row
        for row in list_ingest_runs(settings.db_path, limit=max(last, 1) * 4)
        if str(row.get("mode", "")).startswith("macro")
    ][:last]
    if not rows:
        print("No macro ingest runs found.")
        return 0
    for row in rows:
        print(
            f"#{row['run_id']} {row['mode']} status={row['status']} rows={row['row_count']} "
            f"start={row['start']} end={row['end']} error={row['error'] or '-'}"
        )
    return 0


def _upsert_macro_instruments(settings: Settings, indicators: list[MacroIndicatorConfig]) -> None:
    now = utc_now_iso()
    rows = [
        InstrumentRow(
            symbol=indicator.indicator_code,
            name=indicator.indicator_name,
            asset_type="MACRO",
            exchange="MACRO",
            currency=None,
            sector="Macro",
            provider="mock" if settings.market_data_demo_mode else "bcb_sgs",
            provider_symbol=indicator.provider_code,
            is_active=1 if indicator.is_active else 0,
            priority=indicator.priority,
            created_at=now,
            updated_at=now,
        )
        for indicator in indicators
    ]
    upsert_instruments(settings.db_path, rows)


def _run_status(rows_written: int, errors: list[str]) -> str:
    if errors and rows_written:
        return "PARTIAL"
    if errors:
        return "FAIL"
    return "OK"
