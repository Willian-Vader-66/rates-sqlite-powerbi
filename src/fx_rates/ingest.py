from __future__ import annotations

import logging

from .api_frankfurter import FrankfurterClient, normalize_latest, normalize_timeseries
from .config import Settings
from .db_sqlite import finish_ingest_run, latest_ingest_run, start_ingest_run, upsert_fx_rates
from .utils import parse_yyyy_mm_dd

logger = logging.getLogger(__name__)


def run_backfill(settings: Settings, start: str, end: str, base: str, symbols: list[str]) -> int:
    parse_yyyy_mm_dd(start)
    parse_yyyy_mm_dd(end)

    run_id = start_ingest_run(
        db_path=settings.db_path,
        mode="backfill",
        base=base,
        symbols=symbols,
        start=start,
        end=end,
    )

    logger.info("inicio backfill base=%s symbols=%s start=%s end=%s", base, ",".join(symbols), start, end)

    client = FrankfurterClient(
        base_url=settings.api_base_url,
        cache_dir=settings.cache_dir,
        timeout_seconds=settings.timeout_seconds,
        use_cache=settings.use_cache,
    )

    try:
        payload = client.fetch_timeseries(start=start, end=end, base=base, symbols=symbols)
        rows = normalize_timeseries(payload=payload, base=base)
        row_count = upsert_fx_rates(settings.db_path, rows)
        finish_ingest_run(settings.db_path, run_id, status="OK", row_count=row_count, error=None)
        logger.info("fim backfill row_count=%s", row_count)
        return 0
    except Exception as exc:
        finish_ingest_run(settings.db_path, run_id, status="FAIL", row_count=0, error=str(exc))
        logger.exception("falha backfill")
        return 1


def run_daily(settings: Settings, base: str, symbols: list[str]) -> int:
    run_id = start_ingest_run(
        db_path=settings.db_path,
        mode="daily",
        base=base,
        symbols=symbols,
        start=None,
        end=None,
    )

    logger.info("inicio daily base=%s symbols=%s", base, ",".join(symbols))

    client = FrankfurterClient(
        base_url=settings.api_base_url,
        cache_dir=settings.cache_dir,
        timeout_seconds=settings.timeout_seconds,
        use_cache=settings.use_cache,
    )

    try:
        payload = client.fetch_latest(base=base, symbols=symbols)
        rows = normalize_latest(payload=payload, base=base)
        row_count = upsert_fx_rates(settings.db_path, rows)
        finish_ingest_run(settings.db_path, run_id, status="OK", row_count=row_count, error=None)
        logger.info("fim daily row_count=%s", row_count)
        return 0
    except Exception as exc:
        finish_ingest_run(settings.db_path, run_id, status="FAIL", row_count=0, error=str(exc))
        logger.exception("falha daily")
        return 1


def run_status(settings: Settings) -> int:
    record = latest_ingest_run(settings.db_path)
    if not record:
        logger.info("nenhum run encontrado")
        return 0

    logger.info(
        "ultimo run_id=%s mode=%s status=%s row_count=%s started_at=%s finished_at=%s",
        record["run_id"],
        record["mode"],
        record["status"],
        record["row_count"],
        record["started_at"],
        record["finished_at"],
    )
    return 0
