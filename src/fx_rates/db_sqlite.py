from __future__ import annotations

import sqlite3
from pathlib import Path

from .models import FxRateRow, IngestRunRow
from .utils import utc_now_iso


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fx_rates (
    date TEXT NOT NULL,
    base TEXT NOT NULL,
    symbol TEXT NOT NULL,
    rate REAL NOT NULL,
    source TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (date, base, symbol)
);

CREATE INDEX IF NOT EXISTS idx_fx_symbol_date ON fx_rates (symbol, date);
CREATE INDEX IF NOT EXISTS idx_fx_date ON fx_rates (date);

CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    mode TEXT NOT NULL,
    base TEXT NOT NULL,
    symbols TEXT NOT NULL,
    start TEXT,
    end TEXT,
    row_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    error TEXT
);
"""


UPSERT_SQL = """
INSERT INTO fx_rates (date, base, symbol, rate, source, fetched_at)
VALUES (:date, :base, :symbol, :rate, :source, :fetched_at)
ON CONFLICT(date, base, symbol) DO UPDATE SET
    rate = excluded.rate,
    fetched_at = excluded.fetched_at,
    source = excluded.source;
"""


def init_db(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as connection:
        connection.executescript(SCHEMA_SQL)
        connection.commit()


def start_ingest_run(
    db_path: str,
    mode: str,
    base: str,
    symbols: str,
    start: str | None,
    end: str | None,
) -> int:
    with sqlite3.connect(db_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO ingest_runs (started_at, mode, base, symbols, start, end, row_count, status, error)
            VALUES (?, ?, ?, ?, ?, ?, 0, 'RUNNING', NULL)
            """,
            (utc_now_iso(), mode, base, symbols, start, end),
        )
        connection.commit()
        return int(cursor.lastrowid)


def finish_ingest_run(
    db_path: str,
    run_id: int,
    status: str,
    row_count: int,
    error: str | None = None,
) -> None:
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            UPDATE ingest_runs
            SET finished_at = ?, row_count = ?, status = ?, error = ?
            WHERE run_id = ?
            """,
            (utc_now_iso(), row_count, status, error, run_id),
        )
        connection.commit()


def upsert_rates(db_path: str, rows: list[FxRateRow]) -> int:
    if not rows:
        return 0

    with sqlite3.connect(db_path) as connection:
        connection.executemany(UPSERT_SQL, [row.as_db_params() for row in rows])
        connection.commit()
    return len(rows)


def list_ingest_runs(db_path: str, limit: int) -> list[IngestRunRow]:
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT run_id, started_at, finished_at, mode, base, symbols, start, end, row_count, status, error
            FROM ingest_runs
            ORDER BY run_id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [
        IngestRunRow(
            run_id=int(row["run_id"]),
            started_at=str(row["started_at"]),
            finished_at=str(row["finished_at"]) if row["finished_at"] is not None else None,
            mode=str(row["mode"]),
            base=str(row["base"]),
            symbols=str(row["symbols"]),
            start=str(row["start"]) if row["start"] is not None else None,
            end=str(row["end"]) if row["end"] is not None else None,
            row_count=int(row["row_count"]),
            status=str(row["status"]),
            error=str(row["error"]) if row["error"] is not None else None,
        )
        for row in rows
    ]
