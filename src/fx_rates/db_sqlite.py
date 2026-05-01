from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import AnalysisSnapshotRow, FxRateRow, InstrumentRow, MarketQuoteRow, StockPriceDailyRow
from .utils import normalize_base, normalize_symbol_list, utc_now_iso

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

DROP INDEX IF EXISTS idx_fx_rates_symbol_date;
DROP INDEX IF EXISTS idx_fx_rates_date;
CREATE INDEX IF NOT EXISTS idx_fx_symbol_date ON fx_rates(symbol, date);
CREATE INDEX IF NOT EXISTS idx_fx_date ON fx_rates(date);
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

FX_VIEWS_SQL = """
DROP VIEW IF EXISTS v_fx_daily;
CREATE VIEW v_fx_daily AS
SELECT date, base, symbol, rate, source, fetched_at
FROM fx_rates;

DROP VIEW IF EXISTS v_fx_latest;
CREATE VIEW v_fx_latest AS
SELECT f.date, f.base, f.symbol, f.rate, f.source, f.fetched_at
FROM fx_rates AS f
INNER JOIN (
    SELECT base, symbol, MAX(date) AS latest_date
    FROM fx_rates
    GROUP BY base, symbol
) AS latest
    ON f.base = latest.base
   AND f.symbol = latest.symbol
   AND f.date = latest.latest_date;

DROP VIEW IF EXISTS v_fx_monthly_avg;
CREATE VIEW v_fx_monthly_avg AS
SELECT
    substr(date, 1, 7) AS year_month,
    base,
    symbol,
    AVG(rate) AS avg_rate,
    COUNT(*) AS day_count,
    MIN(date) AS first_date,
    MAX(date) AS last_date
FROM fx_rates
GROUP BY substr(date, 1, 7), base, symbol;
"""

MARKET_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS instruments (
    instrument_id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    name TEXT,
    asset_type TEXT NOT NULL,
    exchange TEXT,
    currency TEXT,
    sector TEXT,
    provider TEXT,
    provider_symbol TEXT,
    is_active INTEGER DEFAULT 1,
    priority INTEGER DEFAULT 100,
    created_at TEXT,
    updated_at TEXT,
    UNIQUE(asset_type, symbol, exchange)
);

CREATE TABLE IF NOT EXISTS stock_prices_daily (
    date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    adjusted_close REAL,
    volume INTEGER,
    currency TEXT,
    provider TEXT,
    fetched_at TEXT,
    PRIMARY KEY(date, symbol, exchange)
);

CREATE TABLE IF NOT EXISTS market_quotes_latest (
    symbol TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    exchange TEXT,
    price REAL,
    bid REAL,
    ask REAL,
    open REAL,
    high REAL,
    low REAL,
    previous_close REAL,
    change REAL,
    percent_change REAL,
    volume INTEGER,
    quote_time TEXT,
    provider TEXT,
    fetched_at TEXT,
    PRIMARY KEY(symbol, asset_type, exchange)
);

CREATE TABLE IF NOT EXISTS analysis_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    exchange TEXT,
    generated_at TEXT NOT NULL,
    last_price REAL,
    last_close REAL,
    daily_return REAL,
    sma_20 REAL,
    sma_50 REAL,
    volatility_20 REAL,
    min_30d REAL,
    max_30d REAL,
    trend TEXT,
    signal TEXT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date ON stock_prices_daily(symbol, date);
CREATE INDEX IF NOT EXISTS idx_market_quotes_asset_symbol ON market_quotes_latest(asset_type, symbol);
CREATE INDEX IF NOT EXISTS idx_analysis_symbol_generated ON analysis_snapshots(symbol, generated_at);
"""

UPSERT_SQL = """
INSERT INTO fx_rates (date, base, symbol, rate, source, fetched_at)
VALUES (:date, :base, :symbol, :rate, :source, :fetched_at)
ON CONFLICT(date, base, symbol) DO UPDATE SET
    rate = excluded.rate,
    source = excluded.source,
    fetched_at = excluded.fetched_at;
"""

UPSERT_INSTRUMENT_SQL = """
INSERT INTO instruments (
    symbol, name, asset_type, exchange, currency, sector, provider, provider_symbol,
    is_active, priority, created_at, updated_at
)
VALUES (
    :symbol, :name, :asset_type, :exchange, :currency, :sector, :provider, :provider_symbol,
    :is_active, :priority, :created_at, :updated_at
)
ON CONFLICT(asset_type, symbol, exchange) DO UPDATE SET
    name = excluded.name,
    currency = excluded.currency,
    sector = excluded.sector,
    provider = excluded.provider,
    provider_symbol = excluded.provider_symbol,
    is_active = excluded.is_active,
    priority = excluded.priority,
    updated_at = excluded.updated_at;
"""

UPSERT_STOCK_PRICE_SQL = """
INSERT INTO stock_prices_daily (
    date, symbol, exchange, open, high, low, close, adjusted_close, volume,
    currency, provider, fetched_at
)
VALUES (
    :date, :symbol, :exchange, :open, :high, :low, :close, :adjusted_close, :volume,
    :currency, :provider, :fetched_at
)
ON CONFLICT(date, symbol, exchange) DO UPDATE SET
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    adjusted_close = excluded.adjusted_close,
    volume = excluded.volume,
    currency = excluded.currency,
    provider = excluded.provider,
    fetched_at = excluded.fetched_at;
"""

UPSERT_MARKET_QUOTE_SQL = """
INSERT INTO market_quotes_latest (
    symbol, asset_type, exchange, price, bid, ask, open, high, low, previous_close,
    change, percent_change, volume, quote_time, provider, fetched_at
)
VALUES (
    :symbol, :asset_type, :exchange, :price, :bid, :ask, :open, :high, :low, :previous_close,
    :change, :percent_change, :volume, :quote_time, :provider, :fetched_at
)
ON CONFLICT(symbol, asset_type, exchange) DO UPDATE SET
    price = excluded.price,
    bid = excluded.bid,
    ask = excluded.ask,
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    previous_close = excluded.previous_close,
    change = excluded.change,
    percent_change = excluded.percent_change,
    volume = excluded.volume,
    quote_time = excluded.quote_time,
    provider = excluded.provider,
    fetched_at = excluded.fetched_at;
"""

INSERT_ANALYSIS_SNAPSHOT_SQL = """
INSERT INTO analysis_snapshots (
    symbol, asset_type, exchange, generated_at, last_price, last_close, daily_return,
    sma_20, sma_50, volatility_20, min_30d, max_30d, trend, signal, notes
)
VALUES (
    :symbol, :asset_type, :exchange, :generated_at, :last_price, :last_close, :daily_return,
    :sma_20, :sma_50, :volatility_20, :min_30d, :max_30d, :trend, :signal, :notes
);
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
        conn.executescript(FX_VIEWS_SQL)
        conn.executescript(MARKET_TABLES_SQL)
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


def upsert_instruments(db_path: str, rows: list[InstrumentRow]) -> int:
    if not rows:
        return 0

    with sqlite3.connect(db_path) as conn:
        conn.executemany(UPSERT_INSTRUMENT_SQL, [row.as_db_dict() for row in rows])
        conn.commit()
    return len(rows)


def list_instruments(
    db_path: str,
    asset_type: str | None = None,
    active: bool | None = None,
    search: str | None = None,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if asset_type:
        clauses.append("asset_type = ?")
        params.append(asset_type.strip().upper())
    if active is not None:
        clauses.append("is_active = ?")
        params.append(1 if active else 0)
    if search:
        clauses.append("(symbol LIKE ? OR name LIKE ?)")
        like = f"%{search.strip()}%"
        params.extend([like, like])

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT instrument_id, symbol, name, asset_type, exchange, currency, sector,
                   provider, provider_symbol, is_active, priority, created_at, updated_at
            FROM instruments
            {where}
            ORDER BY asset_type, priority, symbol
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def upsert_stock_prices_daily(db_path: str, rows: list[StockPriceDailyRow]) -> int:
    if not rows:
        return 0

    with sqlite3.connect(db_path) as conn:
        conn.executemany(UPSERT_STOCK_PRICE_SQL, [row.as_db_dict() for row in rows])
        conn.commit()
    return len(rows)


def upsert_market_quotes_latest(db_path: str, rows: list[MarketQuoteRow]) -> int:
    if not rows:
        return 0

    with sqlite3.connect(db_path) as conn:
        conn.executemany(UPSERT_MARKET_QUOTE_SQL, [row.as_db_dict() for row in rows])
        conn.commit()
    return len(rows)


def insert_analysis_snapshots(db_path: str, rows: list[AnalysisSnapshotRow]) -> int:
    if not rows:
        return 0

    with sqlite3.connect(db_path) as conn:
        conn.executemany(INSERT_ANALYSIS_SNAPSHOT_SQL, [row.as_db_dict() for row in rows])
        conn.commit()
    return len(rows)


def get_stock_history(db_path: str, symbol: str, start: str | None = None, end: str | None = None) -> list[dict[str, Any]]:
    clauses = ["symbol = ?"]
    params: list[Any] = [symbol.strip().upper()]
    if start:
        clauses.append("date >= ?")
        params.append(start)
    if end:
        clauses.append("date <= ?")
        params.append(end)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT date, symbol, exchange, open, high, low, close, adjusted_close,
                   volume, currency, provider, fetched_at
            FROM stock_prices_daily
            WHERE {' AND '.join(clauses)}
            ORDER BY date
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def get_fx_history(
    db_path: str,
    base: str,
    symbol: str,
    start: str | None = None,
    end: str | None = None,
) -> list[dict[str, Any]]:
    clauses = ["base = ?", "symbol = ?"]
    params: list[Any] = [base.strip().upper(), symbol.strip().upper()]
    if start:
        clauses.append("date >= ?")
        params.append(start)
    if end:
        clauses.append("date <= ?")
        params.append(end)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT date, base, symbol, rate, source, fetched_at
            FROM fx_rates
            WHERE {' AND '.join(clauses)}
            ORDER BY date
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def get_latest_quotes(
    db_path: str,
    symbols: list[str] | None = None,
    asset_type: str | None = None,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if asset_type:
        clauses.append("asset_type = ?")
        params.append(asset_type.strip().upper())
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        clauses.append(f"symbol IN ({placeholders})")
        params.extend([symbol.strip().upper() for symbol in symbols])

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT symbol, asset_type, exchange, price, bid, ask, open, high, low,
                   previous_close, change, percent_change, volume, quote_time,
                   provider, fetched_at
            FROM market_quotes_latest
            {where}
            ORDER BY asset_type, symbol
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def get_latest_analysis(
    db_path: str,
    symbols: list[str] | None = None,
    asset_type: str | None = None,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if asset_type:
        clauses.append("a.asset_type = ?")
        params.append(asset_type.strip().upper())
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        clauses.append(f"a.symbol IN ({placeholders})")
        params.extend([symbol.strip().upper() for symbol in symbols])

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT a.snapshot_id, a.symbol, a.asset_type, a.exchange, a.generated_at,
                   a.last_price, a.last_close, a.daily_return, a.sma_20, a.sma_50,
                   a.volatility_20, a.min_30d, a.max_30d, a.trend, a.signal, a.notes
            FROM analysis_snapshots AS a
            INNER JOIN (
                SELECT symbol, asset_type, COALESCE(exchange, '') AS exchange_key, MAX(generated_at) AS generated_at
                FROM analysis_snapshots
                GROUP BY symbol, asset_type, COALESCE(exchange, '')
            ) AS latest
                ON a.symbol = latest.symbol
               AND a.asset_type = latest.asset_type
               AND COALESCE(a.exchange, '') = latest.exchange_key
               AND a.generated_at = latest.generated_at
            {where}
            ORDER BY a.asset_type, a.symbol
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def get_dashboard_summary(db_path: str) -> dict[str, Any]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        total_instruments = conn.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
        active_stocks = conn.execute(
            "SELECT COUNT(*) FROM instruments WHERE asset_type='STOCK' AND is_active=1"
        ).fetchone()[0]
        active_currencies = conn.execute(
            "SELECT COUNT(*) FROM instruments WHERE asset_type='FX' AND is_active=1"
        ).fetchone()[0]
        latest_quote_count = conn.execute("SELECT COUNT(*) FROM market_quotes_latest").fetchone()[0]
        latest_analysis_count = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT symbol, asset_type, COALESCE(exchange, '') AS exchange_key, MAX(generated_at)
                FROM analysis_snapshots
                GROUP BY symbol, asset_type, COALESCE(exchange, '')
            )
            """
        ).fetchone()[0]
        last_successful = conn.execute(
            """
            SELECT run_id, started_at, finished_at, mode, base, symbols, start, "end", row_count, status, error
            FROM ingest_runs
            WHERE status='OK'
            ORDER BY run_id DESC
            LIMIT 1
            """
        ).fetchone()
        failed_runs_count = conn.execute("SELECT COUNT(*) FROM ingest_runs WHERE status='FAIL'").fetchone()[0]
        last_successful_dict = dict(last_successful) if last_successful else None

    return {
        "total_instruments": int(total_instruments),
        "active_stocks": int(active_stocks),
        "active_currencies": int(active_currencies),
        "latest_quote_count": int(latest_quote_count),
        "latest_analysis_count": int(latest_analysis_count),
        "last_successful_ingest_run": last_successful_dict,
        "failed_runs_count": int(failed_runs_count),
    }


def start_ingest_run(
    db_path: str,
    mode: str,
    base: str,
    symbols: list[str],
    start: str | None,
    end: str | None,
) -> int:
    normalized_base = normalize_base(base)
    normalized_symbols = normalize_symbol_list(symbols)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO ingest_runs (started_at, mode, base, symbols, start, "end", status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (utc_now_iso(), mode, normalized_base, ",".join(normalized_symbols), start, end, "RUNNING"),
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


def list_ingest_runs(db_path: str, limit: int) -> list[dict[str, Any]]:
    safe_limit = max(1, int(limit))
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT run_id, started_at, finished_at, mode, base, symbols, start, "end", row_count, status, error
            FROM ingest_runs
            ORDER BY run_id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
    return [dict(row) for row in rows]
