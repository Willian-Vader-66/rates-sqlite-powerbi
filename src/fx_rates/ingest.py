from __future__ import annotations

import logging

from .api_frankfurter import FrankfurterClient, normalize_payload
from .config import Settings, ensure_runtime_paths
from .db_sqlite import finish_ingest_run, init_db, list_ingest_runs, start_ingest_run, upsert_rates
from .logging_setup import set_run_id
from .models import IngestRunRow
from .utils import join_symbols


def run_backfill(
    settings: Settings,
    start: str,
    end: str,
    base: str,
    symbols: list[str],
    use_cache: bool,
) -> int:
    return _run_ingest(
        settings=settings,
        mode="backfill",
        base=base,
        symbols=symbols,
        start=start,
        end=end,
        use_cache=use_cache,
    )


def run_daily(settings: Settings, base: str, symbols: list[str], use_cache: bool) -> int:
    return _run_ingest(
        settings=settings,
        mode="daily",
        base=base,
        symbols=symbols,
        start=None,
        end=None,
        use_cache=use_cache,
    )


def run_status(settings: Settings, last: int) -> list[IngestRunRow]:
    ensure_runtime_paths(settings)
    init_db(settings.db_path)
    return list_ingest_runs(settings.db_path, last)


def _run_ingest(
    settings: Settings,
    mode: str,
    base: str,
    symbols: list[str],
    start: str | None,
    end: str | None,
    use_cache: bool,
) -> int:
    ensure_runtime_paths(settings)
    init_db(settings.db_path)

    symbols_csv = join_symbols(symbols)
    run_id = start_ingest_run(
        db_path=settings.db_path,
        mode=mode,
        base=base,
        symbols=symbols_csv,
        start=start,
        end=end,
    )
    set_run_id(run_id)
    logger = logging.getLogger("fx_rates")

    try:
        client = FrankfurterClient(
            base_url=settings.api_base_url,
            cache_dir=settings.cache_dir,
            timeout=settings.timeout,
            use_cache=use_cache,
            logger=logger,
        )
        if mode == "daily":
            payload = client.fetch_latest(base=base, symbols=symbols)
        else:
            payload = client.fetch_timeseries(start=start or "", end=end or "", base=base, symbols=symbols)

        rows = normalize_payload(payload, logger=logger)
        row_count = upsert_rates(settings.db_path, rows)
        finish_ingest_run(settings.db_path, run_id, "OK", row_count)
        logger.info("Ingest finished successfully with %s rows.", row_count)
        return 0
    except Exception as exc:
        finish_ingest_run(settings.db_path, run_id, "FAIL", 0, str(exc))
        logger.exception("Ingest failed.")
        return 1
    finally:
        set_run_id(None)
