from __future__ import annotations

from typing import Any

from fx_rates.db_sqlite import (
    finish_ingest_run as _finish_ingest_run,
    initialize_schema,
    start_ingest_run as _start_ingest_run,
    upsert_fx_rates,
)
from fx_rates.models import FxRateRow


def init_db(db_path: str) -> None:
    initialize_schema(db_path)


def upsert_rates(db_path: str, rows: list[dict[str, Any]]) -> int:
    normalized = [
        FxRateRow(
            date=str(item["date"]),
            base=str(item["base"]).upper(),
            symbol=str(item["symbol"]).upper(),
            rate=float(item["rate"]),
            source=str(item.get("source", "frankfurter")),
            fetched_at=str(item["fetched_at"]),
        )
        for item in rows
    ]
    return upsert_fx_rates(db_path, normalized)


def start_ingest_run(db_path: str, command: str, args: dict[str, Any]) -> int:
    return _start_ingest_run(
        db_path=db_path,
        mode=command,
        base=str(args.get("base", "USD")),
        symbols=list(args.get("symbols", [])),
        start=args.get("start"),
        end=args.get("end"),
    )


def finish_ingest_run(
    db_path: str,
    run_id: int,
    status: str,
    rows_inserted: int,
    error_message: str | None = None,
) -> None:
    _finish_ingest_run(db_path=db_path, run_id=run_id, status=status, row_count=rows_inserted, error=error_message)


__all__ = ["init_db", "upsert_rates", "start_ingest_run", "finish_ingest_run"]
