from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

from .models import FxRateRow
from .utils import utc_now_iso

FX_RATES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fx_rates (
    date TEXT NOT NULL,
    base TEXT NOT NULL,
    symbol TEXT NOT NULL,
    rate REAL NOT NULL,
    source TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (date, base, symbol)
);

CREATE INDEX IF NOT EXISTS idx_fx_rates_symbol_date ON fx_rates(symbol, date);
CREATE INDEX IF NOT EXISTS idx_fx_rates_date ON fx_rates(date);
"""

INGEST_RUNS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    mode TEXT NOT NULL,
    base TEXT NOT NULL,
    symbols TEXT NOT NULL,
    start TEXT,
    "end" TEXT,
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
    source = excluded.source,
    fetched_at = excluded.fetched_at;
"""

EXPECTED_INGEST_COLS = {
    "run_id",
    "started_at",
    "finished_at",
    "mode",
    "base",
    "symbols",
    "start",
    "end",
    "row_count",
    "status",
    "error",
}


def initialize_schema(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        _migrate_legacy_ingest_runs(conn)
        conn.executescript(FX_RATES_TABLE_SQL)
        conn.executescript(INGEST_RUNS_TABLE_SQL)
        conn.commit()


def _migrate_legacy_ingest_runs(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "ingest_runs"):
        return

    cols = set(_table_columns(conn, "ingest_runs"))
    if cols == EXPECTED_INGEST_COLS:
        return

    suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    conn.execute(f'ALTER TABLE ingest_runs RENAME TO ingest_runs_legacy_{suffix}')


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    return [str(row[1]) for row in rows]


def upsert_fx_rates(db_path: str, rows: list[FxRateRow]) -> int:
    if not rows:
        return 0

    payload = [row.as_db_dict() for row in rows]
    with sqlite3.connect(db_path) as conn:
        conn.executemany(UPSERT_SQL, payload)
        conn.commit()
    return len(rows)


def start_ingest_run(
    db_path: str,
    mode: str,
    base: str,
    symbols: list[str],
    start: str | None,
    end: str | None,
) -> int:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO ingest_runs (started_at, mode, base, symbols, start, "end", status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (utc_now_iso(), mode, base.upper(), ",".join(symbols), start, end, "RUNNING"),
        )
        conn.commit()
        return int(cursor.lastrowid)


def finish_ingest_run(
    db_path: str,
    run_id: int,
    status: str,
    row_count: int,
    error: str | None = None,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE ingest_runs
            SET finished_at = ?, row_count = ?, status = ?, error = ?
            WHERE run_id = ?
            """,
            (utc_now_iso(), row_count, status, error, run_id),
        )
        conn.commit()


def latest_ingest_run(db_path: str) -> dict[str, str | int | None] | None:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT run_id, started_at, finished_at, mode, base, symbols, start, "end", row_count, status, error
            FROM ingest_runs
            ORDER BY run_id DESC
            LIMIT 1
            """
        ).fetchone()
    return dict(row) if row else None
