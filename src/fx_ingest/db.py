from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fx_rates (
    date TEXT NOT NULL,
    base TEXT NOT NULL,
    symbol TEXT NOT NULL,
    rate REAL NOT NULL,
    source TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
    updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
    PRIMARY KEY (date, base, symbol)
);

CREATE TABLE IF NOT EXISTS ingest_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    command TEXT NOT NULL,
    args TEXT,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);
"""


UPSERT_SQL = """
INSERT INTO fx_rates (date, base, symbol, rate, source, fetched_at)
VALUES (:date, :base, :symbol, :rate, :source, :fetched_at)
ON CONFLICT(date, base, symbol) DO UPDATE SET
    rate=excluded.rate,
    source=excluded.source,
    fetched_at=excluded.fetched_at,
    updated_at=CURRENT_TIMESTAMP;
"""


def init_db(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)


def upsert_rates(db_path: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    with sqlite3.connect(db_path) as conn:
        conn.executemany(UPSERT_SQL, rows)
        conn.commit()
    return len(rows)


def start_ingest_run(db_path: str, command: str, args: dict[str, Any]) -> int:
    started_at = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO ingest_runs (command, args, status, started_at)
            VALUES (?, ?, 'RUNNING', ?)
            """,
            (command, json.dumps(args, ensure_ascii=True, sort_keys=True), started_at),
        )
        conn.commit()
        return int(cursor.lastrowid)


def finish_ingest_run(
    db_path: str,
    run_id: int,
    status: str,
    rows_inserted: int,
    error_message: str | None = None,
) -> None:
    finished_at = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE ingest_runs
            SET status = ?, finished_at = ?, rows_inserted = ?, error_message = ?
            WHERE id = ?
            """,
            (status, finished_at, rows_inserted, error_message, run_id),
        )
        conn.commit()
