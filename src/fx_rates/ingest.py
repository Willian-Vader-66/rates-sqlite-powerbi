from __future__ import annotations

import json
import logging
import time
from typing import Any

from .api_frankfurter import FrankfurterClient, _normalize_latest_with_stats, _normalize_timeseries_with_stats
from .config import Settings
from .db_sqlite import finish_ingest_run, list_ingest_runs, start_ingest_run, upsert_fx_rates
from .logging_setup import ContextLoggerAdapter
from .utils import normalize_base, normalize_symbol_list, parse_yyyy_mm_dd

logger = logging.getLogger(__name__)


def run_backfill(settings: Settings, start: str, end: str, base: str, symbols: list[str]) -> int:
    parse_yyyy_mm_dd(start)
    parse_yyyy_mm_dd(end)
    normalized_base = normalize_base(base)
    normalized_symbols = normalize_symbol_list(symbols)

    run_id = start_ingest_run(
        db_path=settings.db_path,
        mode="backfill",
        base=normalized_base,
        symbols=normalized_symbols,
        start=start,
        end=end,
    )
    run_logger = ContextLoggerAdapter(logger, {"run_id": run_id, "mode": "backfill"})

    run_logger.info(
        "starting ingest base=%s symbols=%s start=%s end=%s use_cache=%s timeout_seconds=%s max_retries=%s",
        normalized_base,
        ",".join(normalized_symbols),
        start,
        end,
        settings.use_cache,
        settings.timeout_seconds,
        settings.max_retries,
        extra={"event": "run_start"},
    )

    client = FrankfurterClient(
        base_url=settings.api_base_url,
        cache_dir=settings.cache_dir,
        timeout_seconds=settings.timeout_seconds,
        use_cache=settings.use_cache,
        max_retries=settings.max_retries,
        use_cache_latest=settings.use_cache_latest,
        request_logger=run_logger,
    )

    try:
        fetch_started = time.perf_counter()
        payload = client.fetch_timeseries(start=start, end=end, base=normalized_base, symbols=normalized_symbols)
        duration_fetch_ms = int((time.perf_counter() - fetch_started) * 1000)
        payload_bytes = _payload_bytes(payload)
        run_logger.info(
            "fetch_complete duration_fetch_ms=%s payload_bytes=%s",
            duration_fetch_ms,
            payload_bytes,
            extra={"event": "fetch_complete"},
        )

        rows, skipped_invalid = _normalize_timeseries_with_stats(payload=payload, base=normalized_base, row_logger=run_logger)
        run_logger.info(
            "rows_normalized=%s rows_skipped_invalid=%s",
            len(rows),
            skipped_invalid,
            extra={"event": "rows_normalized"},
        )

        db_started = time.perf_counter()
        row_count = upsert_fx_rates(settings.db_path, rows)
        duration_db_ms = int((time.perf_counter() - db_started) * 1000)
        run_logger.info(
            "rows_upserted=%s duration_db_ms=%s",
            row_count,
            duration_db_ms,
            extra={"event": "db_upsert"},
        )

        finish_ingest_run(settings.db_path, run_id, status="OK", row_count=row_count, error=None)
        run_logger.info(
            "status=OK rows_upserted=%s rows_skipped_invalid=%s duration_fetch_ms=%s duration_db_ms=%s payload_bytes=%s",
            row_count,
            skipped_invalid,
            duration_fetch_ms,
            duration_db_ms,
            payload_bytes,
            extra={"event": "run_finish"},
        )
        return 0
    except Exception as exc:
        finish_ingest_run(settings.db_path, run_id, status="FAIL", row_count=0, error=str(exc))
        run_logger.exception("ingest failed", extra={"event": "run_fail"})
        return 1


def run_daily(settings: Settings, base: str, symbols: list[str]) -> int:
    normalized_base = normalize_base(base)
    normalized_symbols = normalize_symbol_list(symbols)

    run_id = start_ingest_run(
        db_path=settings.db_path,
        mode="daily",
        base=normalized_base,
        symbols=normalized_symbols,
        start=None,
        end=None,
    )
    run_logger = ContextLoggerAdapter(logger, {"run_id": run_id, "mode": "daily"})

    run_logger.info(
        "starting ingest base=%s symbols=%s use_cache=%s use_cache_latest=%s timeout_seconds=%s max_retries=%s",
        normalized_base,
        ",".join(normalized_symbols),
        settings.use_cache,
        settings.use_cache_latest,
        settings.timeout_seconds,
        settings.max_retries,
        extra={"event": "run_start"},
    )

    client = FrankfurterClient(
        base_url=settings.api_base_url,
        cache_dir=settings.cache_dir,
        timeout_seconds=settings.timeout_seconds,
        use_cache=settings.use_cache,
        max_retries=settings.max_retries,
        use_cache_latest=settings.use_cache_latest,
        request_logger=run_logger,
    )

    try:
        fetch_started = time.perf_counter()
        payload = client.fetch_latest(base=normalized_base, symbols=normalized_symbols)
        duration_fetch_ms = int((time.perf_counter() - fetch_started) * 1000)
        payload_bytes = _payload_bytes(payload)
        run_logger.info(
            "fetch_complete duration_fetch_ms=%s payload_bytes=%s",
            duration_fetch_ms,
            payload_bytes,
            extra={"event": "fetch_complete"},
        )

        rows, skipped_invalid = _normalize_latest_with_stats(payload=payload, base=normalized_base, row_logger=run_logger)
        run_logger.info(
            "rows_normalized=%s rows_skipped_invalid=%s",
            len(rows),
            skipped_invalid,
            extra={"event": "rows_normalized"},
        )

        db_started = time.perf_counter()
        row_count = upsert_fx_rates(settings.db_path, rows)
        duration_db_ms = int((time.perf_counter() - db_started) * 1000)
        run_logger.info(
            "rows_upserted=%s duration_db_ms=%s",
            row_count,
            duration_db_ms,
            extra={"event": "db_upsert"},
        )

        finish_ingest_run(settings.db_path, run_id, status="OK", row_count=row_count, error=None)
        run_logger.info(
            "status=OK rows_upserted=%s rows_skipped_invalid=%s duration_fetch_ms=%s duration_db_ms=%s payload_bytes=%s",
            row_count,
            skipped_invalid,
            duration_fetch_ms,
            duration_db_ms,
            payload_bytes,
            extra={"event": "run_finish"},
        )
        return 0
    except Exception as exc:
        finish_ingest_run(settings.db_path, run_id, status="FAIL", row_count=0, error=str(exc))
        run_logger.exception("ingest failed", extra={"event": "run_fail"})
        return 1


def run_status(settings: Settings, last: int) -> int:
    records = list_ingest_runs(settings.db_path, limit=last)
    if not records:
        logger.info("nenhum run encontrado")
        return 0

    for record in records:
        logger.info(
            "run_id=%s mode=%s status=%s row_count=%s started_at=%s finished_at=%s base=%s symbols=%s start=%s end=%s error=%s",
            record["run_id"],
            record["mode"],
            record["status"],
            record["row_count"],
            record["started_at"],
            record["finished_at"],
            record["base"],
            record["symbols"],
            record["start"],
            record["end"],
            record["error"],
            extra={"event": "status"},
        )
    return 0


def _payload_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, ensure_ascii=True).encode("utf-8"))
