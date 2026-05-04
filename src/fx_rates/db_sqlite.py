from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import (
    AnalysisSnapshotRow,
    CryptoPriceDailyRow,
    FxRateRow,
    InstrumentRow,
    MacroIndicatorDailyRow,
    MarketQuoteRow,
    StockPriceDailyRow,
)
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
    change_30d REAL,
    change_90d REAL,
    change_1y REAL,
    sma_20 REAL,
    sma_50 REAL,
    volatility_20 REAL,
    min_30d REAL,
    max_30d REAL,
    trend TEXT,
    signal TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS macro_indicators_daily (
    date TEXT NOT NULL,
    indicator_code TEXT NOT NULL,
    indicator_name TEXT NOT NULL,
    value REAL,
    unit TEXT,
    source TEXT,
    fetched_at TEXT,
    PRIMARY KEY(date, indicator_code)
);

CREATE TABLE IF NOT EXISTS crypto_prices_daily (
    date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT,
    price_usd REAL,
    market_cap REAL,
    volume_24h REAL,
    change_24h REAL,
    provider TEXT,
    fetched_at TEXT,
    PRIMARY KEY(date, symbol)
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date ON stock_prices_daily(symbol, date);
CREATE INDEX IF NOT EXISTS idx_market_quotes_asset_symbol ON market_quotes_latest(asset_type, symbol);
CREATE INDEX IF NOT EXISTS idx_analysis_symbol_generated ON analysis_snapshots(symbol, generated_at);
CREATE INDEX IF NOT EXISTS idx_macro_indicator_date ON macro_indicators_daily(indicator_code, date);
CREATE INDEX IF NOT EXISTS idx_crypto_symbol_date ON crypto_prices_daily(symbol, date);
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
    change_30d, change_90d, change_1y, sma_20, sma_50, volatility_20, min_30d, max_30d, trend, signal, notes
)
VALUES (
    :symbol, :asset_type, :exchange, :generated_at, :last_price, :last_close, :daily_return,
    :change_30d, :change_90d, :change_1y, :sma_20, :sma_50, :volatility_20, :min_30d, :max_30d, :trend, :signal, :notes
);
"""

UPSERT_MACRO_INDICATOR_SQL = """
INSERT INTO macro_indicators_daily (
    date, indicator_code, indicator_name, value, unit, source, fetched_at
)
VALUES (
    :date, :indicator_code, :indicator_name, :value, :unit, :source, :fetched_at
)
ON CONFLICT(date, indicator_code) DO UPDATE SET
    indicator_name = excluded.indicator_name,
    value = excluded.value,
    unit = excluded.unit,
    source = excluded.source,
    fetched_at = excluded.fetched_at;
"""

UPSERT_CRYPTO_PRICE_SQL = """
INSERT INTO crypto_prices_daily (
    date, symbol, name, price_usd, market_cap, volume_24h, change_24h, provider, fetched_at
)
VALUES (
    :date, :symbol, :name, :price_usd, :market_cap, :volume_24h, :change_24h, :provider, :fetched_at
)
ON CONFLICT(date, symbol) DO UPDATE SET
    name = excluded.name,
    price_usd = excluded.price_usd,
    market_cap = excluded.market_cap,
    volume_24h = excluded.volume_24h,
    change_24h = excluded.change_24h,
    provider = excluded.provider,
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

DASHBOARD_EMPTY_MESSAGE = "No data loaded. Run: python -m fx_rates dashboard prepare-demo --years 4 --demo"
PREPARE_COMMAND = "python -m fx_rates dashboard prepare-demo --years 4 --demo"


def initialize_schema(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        _migrate_legacy_ingest_runs(conn)
        conn.executescript(FX_RATES_TABLE_SQL)
        conn.executescript(INGEST_RUNS_TABLE_SQL)
        conn.executescript(FX_VIEWS_SQL)
        conn.executescript(MARKET_TABLES_SQL)
        _ensure_analysis_columns(conn)
        conn.commit()


def _ensure_analysis_columns(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "analysis_snapshots"):
        return
    cols = set(_table_columns(conn, "analysis_snapshots"))
    for name in ("change_30d", "change_90d", "change_1y"):
        if name not in cols:
            conn.execute(f"ALTER TABLE analysis_snapshots ADD COLUMN {name} REAL")


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
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY asset_type, symbol
                           ORDER BY is_active DESC, priority ASC, instrument_id DESC
                       ) AS rn
                FROM instruments
                {where}
            )
            WHERE rn = 1
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


def upsert_macro_indicators_daily(db_path: str, rows: list[MacroIndicatorDailyRow]) -> int:
    if not rows:
        return 0

    with sqlite3.connect(db_path) as conn:
        conn.executemany(UPSERT_MACRO_INDICATOR_SQL, [row.as_db_dict() for row in rows])
        conn.commit()
    return len(rows)


def upsert_crypto_prices_daily(db_path: str, rows: list[CryptoPriceDailyRow]) -> int:
    if not rows:
        return 0

    with sqlite3.connect(db_path) as conn:
        conn.executemany(UPSERT_CRYPTO_PRICE_SQL, [row.as_db_dict() for row in rows])
        conn.commit()
    return len(rows)


def deduplicate_dashboard_records(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            DELETE FROM instruments
            WHERE instrument_id NOT IN (
                SELECT MAX(instrument_id)
                FROM instruments
                GROUP BY asset_type, symbol
            )
            """
        )
        conn.execute(
            """
            DELETE FROM market_quotes_latest
            WHERE rowid NOT IN (
                SELECT MAX(rowid)
                FROM market_quotes_latest
                GROUP BY asset_type, symbol
            )
            """
        )
        conn.commit()


def get_system_status(db_path: str) -> dict[str, Any]:
    path = Path(db_path).expanduser().resolve()
    db_exists = path.exists()
    db_size = path.stat().st_size if db_exists else 0
    if not db_exists:
        return {
            "db_path": str(path),
            "db_exists": False,
            "db_size_bytes": 0,
            "total_instruments": 0,
            "active_stocks": 0,
            "active_currencies": 0,
            "active_crypto": 0,
            "active_macro": 0,
            "latest_quote_count": 0,
            "latest_analysis_count": 0,
            "instruments_without_analysis": 0,
            "instruments_without_quotes": 0,
            "historical_row_count": 0,
            "date_min": None,
            "date_max": None,
            "is_empty": True,
            "message": "No data loaded. Run prepare-demo.",
            "recommended_prepare_command": PREPARE_COMMAND,
        }

    summary = get_dashboard_summary(str(path))
    with sqlite3.connect(str(path)) as conn:
        historical_row_count = sum(
            int(
                conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            )
            for table in ("stock_prices_daily", "fx_rates", "crypto_prices_daily", "macro_indicators_daily")
        )
        ranges = [
            conn.execute(f"SELECT MIN(date), MAX(date) FROM {table}").fetchone()
            for table in ("stock_prices_daily", "fx_rates", "crypto_prices_daily", "macro_indicators_daily")
        ]
    date_min_values = [row[0] for row in ranges if row[0] is not None]
    date_max_values = [row[1] for row in ranges if row[1] is not None]
    total_instruments = int(summary["total_instruments"])
    latest_quote_count = int(summary["latest_quote_count"])
    latest_analysis_count = int(summary["latest_analysis_count"])
    is_empty = total_instruments == 0 and historical_row_count == 0
    status = {
        "db_path": str(path),
        "db_exists": True,
        "db_size_bytes": db_size,
        "total_instruments": total_instruments,
        "active_stocks": int(summary["active_stocks"]),
        "active_currencies": int(summary["active_currencies"]),
        "active_crypto": int(summary["active_crypto"]),
        "active_macro": int(summary["active_macro"]),
        "latest_quote_count": latest_quote_count,
        "latest_analysis_count": latest_analysis_count,
        "instruments_without_analysis": int(summary["instruments_without_analysis"]),
        "instruments_without_quotes": int(summary["instruments_without_quotes"]),
        "historical_row_count": historical_row_count,
        "date_min": min(date_min_values) if date_min_values else None,
        "date_max": max(date_max_values) if date_max_values else None,
        "is_empty": is_empty,
        "recommended_prepare_command": PREPARE_COMMAND,
    }
    if is_empty:
        status["message"] = "No data loaded. Run prepare-demo."
    return status


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


def get_macro_history(
    db_path: str,
    indicator_code: str,
    start: str | None = None,
    end: str | None = None,
) -> list[dict[str, Any]]:
    clauses = ["indicator_code = ?"]
    params: list[Any] = [indicator_code.strip().upper()]
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
            SELECT date, indicator_code, indicator_name, value, unit, source, fetched_at
            FROM macro_indicators_daily
            WHERE {' AND '.join(clauses)}
            ORDER BY date
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def get_crypto_history(
    db_path: str,
    symbol: str,
    start: str | None = None,
    end: str | None = None,
) -> list[dict[str, Any]]:
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
            SELECT date, symbol, name, price_usd, market_cap, volume_24h, change_24h, provider, fetched_at
            FROM crypto_prices_daily
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
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY symbol, asset_type
                           ORDER BY fetched_at DESC, quote_time DESC
                       ) AS rn
                FROM market_quotes_latest
                {where}
            )
            WHERE rn = 1
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
    clauses: list[str] = ["a.rn = 1"]
    params: list[Any] = []
    if asset_type:
        clauses.append("a.asset_type = ?")
        params.append(asset_type.strip().upper())
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        clauses.append(f"a.symbol IN ({placeholders})")
        params.extend([symbol.strip().upper() for symbol in symbols])

    where = f"WHERE {' AND '.join(clauses)}"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT a.snapshot_id, a.symbol, a.asset_type, a.exchange, a.generated_at,
                   a.last_price, a.last_close, a.daily_return,
                   a.change_30d, a.change_90d, a.change_1y,
                   a.sma_20, a.sma_50,
                   a.volatility_20, a.min_30d, a.max_30d, a.trend, a.signal, a.notes
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY symbol, asset_type
                           ORDER BY generated_at DESC, snapshot_id DESC
                       ) AS rn
                FROM analysis_snapshots
            ) AS a
            {where}
            ORDER BY a.asset_type, a.symbol
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def get_dashboard_summary(db_path: str) -> dict[str, Any]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        total_instruments = conn.execute("SELECT COUNT(*) FROM (SELECT asset_type, symbol FROM instruments GROUP BY asset_type, symbol)").fetchone()[0]
        active_stocks = conn.execute(
            "SELECT COUNT(*) FROM (SELECT symbol FROM instruments WHERE asset_type='STOCK' AND is_active=1 GROUP BY symbol)"
        ).fetchone()[0]
        active_currencies = conn.execute(
            "SELECT COUNT(*) FROM (SELECT symbol FROM instruments WHERE asset_type='FX' AND is_active=1 GROUP BY symbol)"
        ).fetchone()[0]
        active_crypto = conn.execute(
            "SELECT COUNT(*) FROM (SELECT symbol FROM instruments WHERE asset_type='CRYPTO' AND is_active=1 GROUP BY symbol)"
        ).fetchone()[0]
        active_macro = conn.execute(
            "SELECT COUNT(*) FROM (SELECT symbol FROM instruments WHERE asset_type='MACRO' AND is_active=1 GROUP BY symbol)"
        ).fetchone()[0]
        latest_quote_count = conn.execute(
            "SELECT COUNT(*) FROM (SELECT symbol, asset_type FROM market_quotes_latest GROUP BY symbol, asset_type)"
        ).fetchone()[0]
        latest_analysis_count = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT symbol, asset_type, MAX(generated_at)
                FROM analysis_snapshots
                GROUP BY symbol, asset_type
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
        instruments_without_quotes = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT asset_type, symbol
                FROM instruments
                WHERE is_active=1
                GROUP BY asset_type, symbol
            ) AS i
            LEFT JOIN (
                SELECT asset_type, symbol
                FROM market_quotes_latest
                GROUP BY asset_type, symbol
            ) AS q
              ON i.asset_type=q.asset_type AND i.symbol=q.symbol
            WHERE q.symbol IS NULL
            """
        ).fetchone()[0]
        instruments_without_analysis = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT asset_type, symbol
                FROM instruments
                WHERE is_active=1
                GROUP BY asset_type, symbol
            ) AS i
            LEFT JOIN (
                SELECT asset_type, symbol
                FROM analysis_snapshots
                GROUP BY asset_type, symbol
            ) AS a
              ON i.asset_type=a.asset_type AND i.symbol=a.symbol
            WHERE a.symbol IS NULL
            """
        ).fetchone()[0]

    return {
        "total_instruments": int(total_instruments),
        "active_stocks": int(active_stocks),
        "active_currencies": int(active_currencies),
        "active_crypto": int(active_crypto),
        "active_macro": int(active_macro),
        "latest_quote_count": int(latest_quote_count),
        "latest_analysis_count": int(latest_analysis_count),
        "last_successful_ingest_run": last_successful_dict,
        "failed_runs_count": int(failed_runs_count),
        "instruments_without_analysis": int(instruments_without_analysis),
        "instruments_without_quotes": int(instruments_without_quotes),
        "message": DASHBOARD_EMPTY_MESSAGE if int(total_instruments) == 0 else None,
    }


def get_top_stocks_30d(db_path: str, symbols: list[str], days: int = 30) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for symbol in symbols:
        history = _latest_stock_points(db_path, symbol, days)
        if not history:
            continue
        start_price = history[0]["value"]
        latest_price = history[-1]["value"]
        change_30d = _percent_change(start_price, latest_price)
        meta = _instrument_meta(db_path, symbol, "STOCK")
        analysis = _latest_analysis_for_symbol(db_path, symbol, "STOCK")
        results.append(
            {
                "symbol": symbol,
                "name": meta.get("name") or symbol,
                "latest_price": latest_price,
                "start_price": start_price,
                "change_30d": change_30d,
                "trend": analysis.get("trend", "UNKNOWN"),
                "signal": analysis.get("signal", "UNKNOWN"),
                "points": history,
            }
        )
    return results


def get_fixed_dashboard_charts(db_path: str, days: int = 30) -> dict[str, list[dict[str, Any]]]:
    usd_brl_points = _fx_points(db_path, "USD", "BRL", days)
    usd_eur_points = _fx_points(db_path, "USD", "EUR", days)
    btc_points = _crypto_points(db_path, "BTC", days)
    eth_points = _crypto_points(db_path, "ETH", days)
    selic_points = _macro_points(db_path, "SELIC_DAILY", days)
    return {
        "fx": [
            {
                "id": "usd_brl_30d",
                "title": "USD/BRL - Last 30 Days",
                "asset_type": "FX",
                "base": "USD",
                "symbol": "BRL",
                "points": usd_brl_points,
                "message": None if usd_brl_points else "No USD/BRL history available. Run dashboard prepare-demo.",
            },
            {
                "id": "usd_eur_30d",
                "title": "USD/EUR - Last 30 Days",
                "asset_type": "FX",
                "base": "USD",
                "symbol": "EUR",
                "points": usd_eur_points,
                "message": None if usd_eur_points else "No USD/EUR history available. Run dashboard prepare-demo.",
            },
        ],
        "crypto": [
            {
                "id": "btc_usd_30d",
                "title": "Bitcoin - Last 30 Days",
                "asset_type": "CRYPTO",
                "symbol": "BTC",
                "points": btc_points,
                "message": None if btc_points else "No BTC history available. Run dashboard prepare-demo.",
            },
            {
                "id": "eth_usd_30d",
                "title": "Ethereum - Last 30 Days",
                "asset_type": "CRYPTO",
                "symbol": "ETH",
                "points": eth_points,
                "message": None if eth_points else "No ETH history available. Run dashboard prepare-demo.",
            },
        ],
        "macro": [
            {
                "id": "selic_30d",
                "title": "Selic - Last 30 Days",
                "asset_type": "MACRO",
                "symbol": "SELIC_DAILY",
                "points": selic_points,
                "message": None if selic_points else "No Selic history available. Run dashboard prepare-demo.",
            }
        ],
    }


def get_market_overview(db_path: str) -> dict[str, Any]:
    top_stock, worst_stock = _stock_performer_cards(db_path)
    cards = [
        _overview_card("USD/BRL", _fx_points(db_path, "USD", "BRL", 30), None),
        _overview_card("USD/EUR", _fx_points(db_path, "USD", "EUR", 30), None),
        _overview_card("BTC/USD", _crypto_points(db_path, "BTC", 30), "USD"),
        _overview_card("ETH/USD", _crypto_points(db_path, "ETH", 30), "USD"),
        _overview_card("Selic", _macro_points(db_path, "SELIC_DAILY", 30), "% a.d."),
        top_stock,
        worst_stock,
    ]
    cards = [card for card in cards if card is not None]
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        signals = conn.execute(
            """
            SELECT symbol, asset_type, trend, signal, generated_at
            FROM analysis_snapshots
            WHERE signal IN ('BREAKOUT', 'VOLATILE', 'DRAWDOWN')
            ORDER BY generated_at DESC
            LIMIT 10
            """
        ).fetchall()
    return {
        "generated_at": utc_now_iso(),
        "cards": cards,
        "signals": [dict(row) for row in signals],
        "message": None if cards else DASHBOARD_EMPTY_MESSAGE,
    }


def _latest_stock_points(db_path: str, symbol: str, limit: int) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT date, close AS value
            FROM stock_prices_daily
            WHERE symbol = ? AND close IS NOT NULL
            ORDER BY date DESC
            LIMIT ?
            """,
            (symbol.strip().upper(), limit),
        ).fetchall()
    return [dict(row) for row in reversed(rows)]


def _fx_points(db_path: str, base: str, symbol: str, limit: int) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT date, rate AS value
            FROM fx_rates
            WHERE base = ? AND symbol = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (base, symbol, limit),
        ).fetchall()
    return [dict(row) for row in reversed(rows)]


def _crypto_points(db_path: str, symbol: str, limit: int) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT date, price_usd AS value
            FROM crypto_prices_daily
            WHERE symbol = ? AND price_usd IS NOT NULL
            ORDER BY date DESC
            LIMIT ?
            """,
            (symbol.strip().upper(), limit),
        ).fetchall()
    return [dict(row) for row in reversed(rows)]


def _macro_points(db_path: str, indicator_code: str, limit: int) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT date, value
            FROM macro_indicators_daily
            WHERE indicator_code = ? AND value IS NOT NULL
            ORDER BY date DESC
            LIMIT ?
            """,
            (indicator_code.strip().upper(), limit),
        ).fetchall()
    return [dict(row) for row in reversed(rows)]


def _instrument_meta(db_path: str, symbol: str, asset_type: str) -> dict[str, Any]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT symbol, name, asset_type, exchange, currency, sector
            FROM instruments
            WHERE symbol = ? AND asset_type = ?
            ORDER BY priority
            LIMIT 1
            """,
            (symbol.strip().upper(), asset_type.strip().upper()),
        ).fetchone()
    return dict(row) if row else {}


def _latest_analysis_for_symbol(db_path: str, symbol: str, asset_type: str) -> dict[str, Any]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT symbol, asset_type, trend, signal, generated_at
            FROM analysis_snapshots
            WHERE symbol = ? AND asset_type = ?
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            (symbol.strip().upper(), asset_type.strip().upper()),
        ).fetchone()
    return dict(row) if row else {}


def _overview_card(label: str, points: list[dict[str, Any]], unit: str | None) -> dict[str, Any] | None:
    if not points:
        return None
    latest = points[-1]["value"]
    previous = points[-2]["value"] if len(points) > 1 else None
    change = _percent_change(previous, latest) if previous is not None else None
    status = "neutral"
    if change is not None and change > 0:
        status = "up"
    elif change is not None and change < 0:
        status = "down"
    return {"label": label, "value": latest, "change": change, "unit": unit, "status": status}


def _stock_performer_cards(db_path: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT DISTINCT symbol FROM stock_prices_daily ORDER BY symbol").fetchall()
    performers: list[tuple[str, float, float]] = []
    for row in rows:
        symbol = str(row[0])
        points = _latest_stock_points(db_path, symbol, 30)
        if len(points) < 2:
            continue
        change = _percent_change(points[0]["value"], points[-1]["value"])
        if change is not None:
            performers.append((symbol, points[-1]["value"], change))
    if not performers:
        return None, None
    best = max(performers, key=lambda item: item[2])
    worst = min(performers, key=lambda item: item[2])
    return (
        {"label": f"Top 30D {best[0]}", "value": best[1], "change": best[2], "unit": "USD", "status": "up"},
        {"label": f"Worst 30D {worst[0]}", "value": worst[1], "change": worst[2], "unit": "USD", "status": "down"},
    )


def _percent_change(start: float | None, end: float | None) -> float | None:
    if start is None or end is None or float(start) == 0:
        return None
    return ((float(end) / float(start)) - 1.0) * 100.0


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
