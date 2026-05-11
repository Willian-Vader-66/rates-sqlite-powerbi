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
from .data_origin import canonical_record_mode, mode_booleans, warning_for_mode
from .display_metadata import apply_display_metadata, build_display_metadata
from .utils import normalize_base, normalize_symbol_list, utc_now_iso

FX_RATES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fx_rates (
    date TEXT NOT NULL,
    base TEXT NOT NULL,
    symbol TEXT NOT NULL,
    rate REAL NOT NULL,
    source TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    data_mode TEXT NOT NULL DEFAULT 'unknown',
    source_updated_at TEXT,
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
SELECT date, base, symbol, rate, source, fetched_at, data_mode, source_updated_at
FROM fx_rates;

DROP VIEW IF EXISTS v_fx_latest;
CREATE VIEW v_fx_latest AS
SELECT f.date, f.base, f.symbol, f.rate, f.source, f.fetched_at, f.data_mode, f.source_updated_at
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
    data_mode TEXT NOT NULL DEFAULT 'unknown',
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
    data_mode TEXT NOT NULL DEFAULT 'unknown',
    source_updated_at TEXT,
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
    data_mode TEXT NOT NULL DEFAULT 'unknown',
    source_updated_at TEXT,
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
    notes TEXT,
    data_mode TEXT NOT NULL DEFAULT 'unknown',
    source_updated_at TEXT
);

CREATE TABLE IF NOT EXISTS macro_indicators_daily (
    date TEXT NOT NULL,
    indicator_code TEXT NOT NULL,
    indicator_name TEXT NOT NULL,
    value REAL,
    unit TEXT,
    source TEXT,
    fetched_at TEXT,
    data_mode TEXT NOT NULL DEFAULT 'unknown',
    source_updated_at TEXT,
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
    data_mode TEXT NOT NULL DEFAULT 'unknown',
    source_updated_at TEXT,
    PRIMARY KEY(date, symbol)
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date ON stock_prices_daily(symbol, date);
CREATE INDEX IF NOT EXISTS idx_market_quotes_asset_symbol ON market_quotes_latest(asset_type, symbol);
CREATE INDEX IF NOT EXISTS idx_analysis_symbol_generated ON analysis_snapshots(symbol, generated_at);
CREATE INDEX IF NOT EXISTS idx_macro_indicator_date ON macro_indicators_daily(indicator_code, date);
CREATE INDEX IF NOT EXISTS idx_crypto_symbol_date ON crypto_prices_daily(symbol, date);
"""

UPSERT_SQL = """
INSERT INTO fx_rates (date, base, symbol, rate, source, fetched_at, data_mode, source_updated_at)
VALUES (:date, :base, :symbol, :rate, :source, :fetched_at, :data_mode, :source_updated_at)
ON CONFLICT(date, base, symbol) DO UPDATE SET
    rate = excluded.rate,
    source = excluded.source,
    fetched_at = excluded.fetched_at,
    data_mode = excluded.data_mode,
    source_updated_at = excluded.source_updated_at;
"""

UPSERT_INSTRUMENT_SQL = """
INSERT INTO instruments (
    symbol, name, asset_type, exchange, currency, sector, provider, provider_symbol,
    data_mode, is_active, priority, created_at, updated_at
)
VALUES (
    :symbol, :name, :asset_type, :exchange, :currency, :sector, :provider, :provider_symbol,
    :data_mode, :is_active, :priority, :created_at, :updated_at
)
ON CONFLICT(asset_type, symbol, exchange) DO UPDATE SET
    name = excluded.name,
    currency = excluded.currency,
    sector = excluded.sector,
    provider = excluded.provider,
    provider_symbol = excluded.provider_symbol,
    data_mode = excluded.data_mode,
    is_active = excluded.is_active,
    priority = excluded.priority,
    updated_at = excluded.updated_at;
"""

UPSERT_STOCK_PRICE_SQL = """
INSERT INTO stock_prices_daily (
    date, symbol, exchange, open, high, low, close, adjusted_close, volume,
    currency, provider, fetched_at, data_mode, source_updated_at
)
VALUES (
    :date, :symbol, :exchange, :open, :high, :low, :close, :adjusted_close, :volume,
    :currency, :provider, :fetched_at, :data_mode, :source_updated_at
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
    fetched_at = excluded.fetched_at,
    data_mode = excluded.data_mode,
    source_updated_at = excluded.source_updated_at;
"""

UPSERT_MARKET_QUOTE_SQL = """
INSERT INTO market_quotes_latest (
    symbol, asset_type, exchange, price, bid, ask, open, high, low, previous_close,
    change, percent_change, volume, quote_time, provider, fetched_at, data_mode, source_updated_at
)
VALUES (
    :symbol, :asset_type, :exchange, :price, :bid, :ask, :open, :high, :low, :previous_close,
    :change, :percent_change, :volume, :quote_time, :provider, :fetched_at, :data_mode, :source_updated_at
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
    fetched_at = excluded.fetched_at,
    data_mode = excluded.data_mode,
    source_updated_at = excluded.source_updated_at;
"""

INSERT_ANALYSIS_SNAPSHOT_SQL = """
INSERT INTO analysis_snapshots (
    symbol, asset_type, exchange, generated_at, last_price, last_close, daily_return,
    change_30d, change_90d, change_1y, sma_20, sma_50, volatility_20, min_30d, max_30d, trend, signal, notes,
    data_mode, source_updated_at
)
VALUES (
    :symbol, :asset_type, :exchange, :generated_at, :last_price, :last_close, :daily_return,
    :change_30d, :change_90d, :change_1y, :sma_20, :sma_50, :volatility_20, :min_30d, :max_30d, :trend, :signal, :notes,
    :data_mode, :source_updated_at
);
"""

UPSERT_MACRO_INDICATOR_SQL = """
INSERT INTO macro_indicators_daily (
    date, indicator_code, indicator_name, value, unit, source, fetched_at, data_mode, source_updated_at
)
VALUES (
    :date, :indicator_code, :indicator_name, :value, :unit, :source, :fetched_at, :data_mode, :source_updated_at
)
ON CONFLICT(date, indicator_code) DO UPDATE SET
    indicator_name = excluded.indicator_name,
    value = excluded.value,
    unit = excluded.unit,
    source = excluded.source,
    fetched_at = excluded.fetched_at,
    data_mode = excluded.data_mode,
    source_updated_at = excluded.source_updated_at;
"""

UPSERT_CRYPTO_PRICE_SQL = """
INSERT INTO crypto_prices_daily (
    date, symbol, name, price_usd, market_cap, volume_24h, change_24h, provider, fetched_at, data_mode, source_updated_at
)
VALUES (
    :date, :symbol, :name, :price_usd, :market_cap, :volume_24h, :change_24h, :provider, :fetched_at, :data_mode, :source_updated_at
)
ON CONFLICT(date, symbol) DO UPDATE SET
    name = excluded.name,
    price_usd = excluded.price_usd,
    market_cap = excluded.market_cap,
    volume_24h = excluded.volume_24h,
    change_24h = excluded.change_24h,
    provider = excluded.provider,
    fetched_at = excluded.fetched_at,
    data_mode = excluded.data_mode,
    source_updated_at = excluded.source_updated_at;
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
        _ensure_data_origin_columns(conn)
        _ensure_analysis_columns(conn)
        _backfill_data_origin(conn)
        conn.commit()


def _ensure_analysis_columns(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "analysis_snapshots"):
        return
    cols = set(_table_columns(conn, "analysis_snapshots"))
    for name in ("change_30d", "change_90d", "change_1y"):
        if name not in cols:
            conn.execute(f"ALTER TABLE analysis_snapshots ADD COLUMN {name} REAL")


def _ensure_data_origin_columns(conn: sqlite3.Connection) -> None:
    columns = {
        "fx_rates": {"data_mode": "TEXT NOT NULL DEFAULT 'unknown'", "source_updated_at": "TEXT"},
        "instruments": {"data_mode": "TEXT NOT NULL DEFAULT 'unknown'"},
        "stock_prices_daily": {"data_mode": "TEXT NOT NULL DEFAULT 'unknown'", "source_updated_at": "TEXT"},
        "market_quotes_latest": {"data_mode": "TEXT NOT NULL DEFAULT 'unknown'", "source_updated_at": "TEXT"},
        "analysis_snapshots": {"data_mode": "TEXT NOT NULL DEFAULT 'unknown'", "source_updated_at": "TEXT"},
        "macro_indicators_daily": {"data_mode": "TEXT NOT NULL DEFAULT 'unknown'", "source_updated_at": "TEXT"},
        "crypto_prices_daily": {"data_mode": "TEXT NOT NULL DEFAULT 'unknown'", "source_updated_at": "TEXT"},
    }
    for table, additions in columns.items():
        if not _table_exists(conn, table):
            continue
        existing = set(_table_columns(conn, table))
        for name, ddl in additions.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def _backfill_data_origin(conn: sqlite3.Connection) -> None:
    updates = (
        ("instruments", "provider"),
        ("stock_prices_daily", "provider"),
        ("market_quotes_latest", "provider"),
        ("fx_rates", "source"),
        ("crypto_prices_daily", "provider"),
        ("macro_indicators_daily", "source"),
    )
    for table, marker_col in updates:
        if not _table_exists(conn, table):
            continue
        cols = set(_table_columns(conn, table))
        if "data_mode" not in cols or marker_col not in cols:
            continue
        rows = conn.execute(
            f"SELECT rowid, data_mode, {marker_col} AS marker FROM {table} WHERE data_mode IS NULL OR data_mode='' OR data_mode='unknown'"
        ).fetchall()
        for rowid, data_mode, marker in rows:
            conn.execute(
                f"UPDATE {table} SET data_mode=? WHERE rowid=?",
                (canonical_record_mode(data_mode, marker), rowid),
            )
    if _table_exists(conn, "analysis_snapshots") and "data_mode" in set(_table_columns(conn, "analysis_snapshots")):
        conn.execute(
            """
            UPDATE analysis_snapshots
               SET data_mode = COALESCE((
                   SELECT q.data_mode
                   FROM market_quotes_latest AS q
                   WHERE q.symbol = analysis_snapshots.symbol
                     AND q.asset_type = analysis_snapshots.asset_type
                   ORDER BY q.fetched_at DESC
                   LIMIT 1
               ), data_mode)
             WHERE data_mode IS NULL OR data_mode='' OR data_mode='unknown'
            """
        )


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
                   provider, provider_symbol, data_mode, is_active, priority, created_at, updated_at
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
    return _with_display_metadata(db_path, [dict(row) for row in rows])


def _with_display_metadata(db_path: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows

    symbols = {
        str(row.get("symbol") or row.get("indicator_code") or "").strip().upper()
        for row in rows
        if row.get("symbol") or row.get("indicator_code")
    }
    instrument_meta: dict[tuple[str, str], dict[str, Any]] = {}
    macro_units: dict[str, str] = {}
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            for row in conn.execute(
                f"""
                SELECT asset_type, symbol, name, exchange, currency, sector, provider, provider_symbol, data_mode
                FROM instruments
                WHERE symbol IN ({placeholders})
                ORDER BY is_active DESC, priority ASC
                """,
                sorted(symbols),
            ).fetchall():
                key = (str(row["asset_type"]).upper(), str(row["symbol"]).upper())
                instrument_meta.setdefault(key, dict(row))
            for row in conn.execute(
                f"""
                SELECT indicator_code, unit
                FROM macro_indicators_daily
                WHERE indicator_code IN ({placeholders}) AND unit IS NOT NULL AND unit <> ''
                GROUP BY indicator_code, unit
                """,
                sorted(symbols),
            ).fetchall():
                macro_units[str(row["indicator_code"]).upper()] = str(row["unit"])

    enriched: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        symbol = str(item.get("symbol") or item.get("indicator_code") or "").strip().upper()
        asset_type = str(item.get("asset_type") or _asset_type_from_history_row(item)).strip().upper()
        meta = instrument_meta.get((asset_type, symbol), {})
        item.setdefault("symbol", symbol)
        item.setdefault("asset_type", asset_type)
        item.setdefault("name", meta.get("name"))
        item.setdefault("exchange", meta.get("exchange") or item.get("base"))
        item.setdefault("currency", meta.get("currency"))
        item.setdefault("sector", meta.get("sector"))
        item.setdefault("provider", meta.get("provider"))
        item.setdefault("provider_symbol", meta.get("provider_symbol"))
        item["data_mode"] = canonical_record_mode(item.get("data_mode"), item.get("provider"), meta.get("data_mode"))
        if asset_type == "MACRO":
            item.setdefault("unit", macro_units.get(symbol))
        item = _apply_origin_metadata(item)
        enriched.append(apply_display_metadata(item))
    return enriched


def _apply_origin_metadata(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    marker = item.get("provider") or item.get("source")
    item["data_mode"] = canonical_record_mode(item.get("data_mode"), marker)
    item.update(mode_booleans(item["data_mode"]))
    item["data_warning"] = warning_for_mode(item["data_mode"])
    item.setdefault("source_updated_at", item.get("quote_time") or item.get("date"))
    return item


def _asset_type_from_history_row(row: dict[str, Any]) -> str:
    if "rate" in row or "base" in row:
        return "FX"
    if "price_usd" in row:
        return "CRYPTO"
    if "indicator_code" in row:
        return "MACRO"
    return "STOCK"


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


def delete_records_by_data_mode(
    db_path: str,
    data_mode: str,
    *,
    asset_types: list[str] | None = None,
    symbols: list[str] | None = None,
) -> int:
    normalized_mode = data_mode.strip().lower()
    normalized_types = {item.strip().upper() for item in asset_types or [] if item.strip()}
    normalized_symbols = {item.strip().upper() for item in symbols or [] if item.strip()}
    deleted = 0
    with sqlite3.connect(db_path) as conn:
        deleted += _delete_mode_rows(conn, "instruments", "data_mode = ?", [normalized_mode], asset_col="asset_type", symbol_col="symbol", asset_types=normalized_types, symbols=normalized_symbols)
        deleted += _delete_mode_rows(conn, "market_quotes_latest", "data_mode = ?", [normalized_mode], asset_col="asset_type", symbol_col="symbol", asset_types=normalized_types, symbols=normalized_symbols)
        deleted += _delete_mode_rows(conn, "analysis_snapshots", "data_mode = ?", [normalized_mode], asset_col="asset_type", symbol_col="symbol", asset_types=normalized_types, symbols=normalized_symbols)
        if not normalized_types or "STOCK" in normalized_types:
            deleted += _delete_mode_rows(conn, "stock_prices_daily", "data_mode = ?", [normalized_mode], symbol_col="symbol", symbols=normalized_symbols)
        if not normalized_types or "FX" in normalized_types:
            deleted += _delete_mode_rows(conn, "fx_rates", "data_mode = ?", [normalized_mode], symbol_col="symbol", symbols=normalized_symbols)
        if not normalized_types or "CRYPTO" in normalized_types:
            deleted += _delete_mode_rows(conn, "crypto_prices_daily", "data_mode = ?", [normalized_mode], symbol_col="symbol", symbols=normalized_symbols)
        if not normalized_types or "MACRO" in normalized_types:
            deleted += _delete_mode_rows(conn, "macro_indicators_daily", "data_mode = ?", [normalized_mode], symbol_col="indicator_code", symbols=normalized_symbols)
        conn.commit()
    return deleted


def _delete_mode_rows(
    conn: sqlite3.Connection,
    table: str,
    base_clause: str,
    params: list[Any],
    *,
    asset_col: str | None = None,
    symbol_col: str | None = None,
    asset_types: set[str] | None = None,
    symbols: set[str] | None = None,
) -> int:
    if not _table_exists(conn, table):
        return 0
    clauses = [base_clause]
    values: list[Any] = list(params)
    if asset_col and asset_types:
        placeholders = ",".join("?" for _ in asset_types)
        clauses.append(f"{asset_col} IN ({placeholders})")
        values.extend(sorted(asset_types))
    if symbol_col and symbols:
        placeholders = ",".join("?" for _ in symbols)
        clauses.append(f"{symbol_col} IN ({placeholders})")
        values.extend(sorted(symbols))
    cursor = conn.execute(f"DELETE FROM {table} WHERE {' AND '.join(clauses)}", values)
    return int(cursor.rowcount or 0)


def commit_prepared_live_dataset(
    db_path: str,
    *,
    instruments: list[InstrumentRow],
    stock_rows: list[StockPriceDailyRow],
    fx_rows: list[FxRateRow],
    crypto_rows: list[CryptoPriceDailyRow],
    macro_rows: list[MacroIndicatorDailyRow],
    quote_rows: list[MarketQuoteRow],
    analysis_rows: list[AnalysisSnapshotRow],
    replace_demo: bool,
    asset_types: list[str],
    symbols: list[str] | None,
    simulate_failure_after_delete: bool = False,
) -> int:
    normalized_symbols = sorted({item.strip().upper() for item in symbols or _symbols_from_live_payload(instruments, stock_rows, fx_rows, crypto_rows, macro_rows, quote_rows) if item.strip()})
    row_count = 0
    with sqlite3.connect(db_path) as conn:
        try:
            conn.execute("BEGIN")
            if replace_demo:
                _delete_mode_rows(conn, "instruments", "data_mode = ?", ["demo"], asset_col="asset_type", symbol_col="symbol", asset_types=set(asset_types), symbols=set(normalized_symbols))
                _delete_mode_rows(conn, "market_quotes_latest", "data_mode = ?", ["demo"], asset_col="asset_type", symbol_col="symbol", asset_types=set(asset_types), symbols=set(normalized_symbols))
                _delete_mode_rows(conn, "analysis_snapshots", "data_mode = ?", ["demo"], asset_col="asset_type", symbol_col="symbol", asset_types=set(asset_types), symbols=set(normalized_symbols))
                if "STOCK" in asset_types:
                    _delete_mode_rows(conn, "stock_prices_daily", "data_mode = ?", ["demo"], symbol_col="symbol", symbols=set(normalized_symbols))
                if "FX" in asset_types:
                    _delete_mode_rows(conn, "fx_rates", "data_mode = ?", ["demo"], symbol_col="symbol", symbols=set(normalized_symbols))
                if "CRYPTO" in asset_types:
                    _delete_mode_rows(conn, "crypto_prices_daily", "data_mode = ?", ["demo"], symbol_col="symbol", symbols=set(normalized_symbols))
                if "MACRO" in asset_types:
                    _delete_mode_rows(conn, "macro_indicators_daily", "data_mode = ?", ["demo"], symbol_col="indicator_code", symbols=set(normalized_symbols))
                if simulate_failure_after_delete:
                    raise RuntimeError("simulated failure after delete")
            if instruments:
                conn.executemany(UPSERT_INSTRUMENT_SQL, [row.as_db_dict() for row in instruments])
                row_count += len(instruments)
            if stock_rows:
                conn.executemany(UPSERT_STOCK_PRICE_SQL, [row.as_db_dict() for row in stock_rows])
                row_count += len(stock_rows)
            if fx_rows:
                conn.executemany(UPSERT_SQL, [row.as_db_dict() for row in fx_rows])
                row_count += len(fx_rows)
            if crypto_rows:
                conn.executemany(UPSERT_CRYPTO_PRICE_SQL, [row.as_db_dict() for row in crypto_rows])
                row_count += len(crypto_rows)
            if macro_rows:
                conn.executemany(UPSERT_MACRO_INDICATOR_SQL, [row.as_db_dict() for row in macro_rows])
                row_count += len(macro_rows)
            if quote_rows:
                conn.executemany(UPSERT_MARKET_QUOTE_SQL, [row.as_db_dict() for row in quote_rows])
                row_count += len(quote_rows)
            if analysis_rows:
                conn.executemany(INSERT_ANALYSIS_SNAPSHOT_SQL, [row.as_db_dict() for row in analysis_rows])
                row_count += len(analysis_rows)
            _deduplicate_dashboard_records_conn(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return row_count


def _deduplicate_dashboard_records_conn(conn: sqlite3.Connection) -> None:
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


def _symbols_from_live_payload(
    instruments: list[InstrumentRow],
    stock_rows: list[StockPriceDailyRow],
    fx_rows: list[FxRateRow],
    crypto_rows: list[CryptoPriceDailyRow],
    macro_rows: list[MacroIndicatorDailyRow],
    quote_rows: list[MarketQuoteRow],
) -> list[str]:
    symbols: set[str] = set()
    symbols.update(row.symbol for row in instruments)
    symbols.update(row.symbol for row in stock_rows)
    symbols.update(row.symbol for row in fx_rows)
    symbols.update(row.symbol for row in crypto_rows)
    symbols.update(row.indicator_code for row in macro_rows)
    symbols.update(row.symbol for row in quote_rows)
    return sorted(symbols)


IMPORTANT_MARKET_SYMBOLS = {
    "STOCK": {"AAPL", "MSFT", "NVDA"},
    "FX": {"BRL", "EUR"},
    "CRYPTO": {"BTC", "ETH"},
    "MACRO": {"SELIC_DAILY"},
}


def get_data_health(db_path: str) -> dict[str, Any]:
    path = Path(db_path).expanduser().resolve()
    if not path.exists():
        return {
            "status": "FAIL",
            "missing_important_symbols": ["DATABASE"],
            "symbols_without_history": [],
            "symbols_without_quote": [],
            "analysis_without_history": [],
            "live_without_provider": [],
            "demo_masked_as_live": [],
            "demo_count": 0,
            "live_count": 0,
            "mixed_count": 0,
            "unknown_count": 0,
            "repair_command": PREPARE_COMMAND,
        }
    with sqlite3.connect(str(path)) as conn:
        conn.row_factory = sqlite3.Row
        missing_important: list[str] = []
        without_history: list[str] = []
        without_quote: list[str] = []
        analysis_without_history: list[str] = []
        live_without_provider: list[str] = []
        demo_masked_as_live: list[str] = []
        for asset_type, symbols in IMPORTANT_MARKET_SYMBOLS.items():
            for symbol in sorted(symbols):
                history_count = _history_count_for_health(conn, asset_type, symbol)
                quote_count = _quote_count_for_health(conn, asset_type, symbol)
                key = f"{asset_type}:{symbol}"
                if history_count == 0:
                    missing_important.append(key)
                    without_history.append(key)
                if quote_count == 0:
                    without_quote.append(key)
        for row in conn.execute("SELECT asset_type, symbol, provider FROM market_quotes_latest WHERE data_mode='live'").fetchall():
            if not row["provider"]:
                live_without_provider.append(f"{row['asset_type']}:{row['symbol']}")
            if row["provider"] and _is_demo_marker(row["provider"]):
                demo_masked_as_live.append(f"{row['asset_type']}:{row['symbol']}")
        for row in conn.execute("SELECT asset_type, symbol FROM analysis_snapshots GROUP BY asset_type, symbol").fetchall():
            if _history_count_for_health(conn, str(row["asset_type"]), str(row["symbol"])) == 0:
                analysis_without_history.append(f"{row['asset_type']}:{row['symbol']}")
    data_mode = get_data_mode_summary(str(path))
    counts = data_mode.get("data_mode_counts", {})
    status = "OK"
    if missing_important or analysis_without_history or demo_masked_as_live:
        status = "FAIL"
    elif without_quote or live_without_provider or data_mode.get("data_mode") == "mixed":
        status = "WARN"
    return {
        "status": status,
        "missing_important_symbols": missing_important,
        "symbols_without_history": without_history,
        "symbols_without_quote": without_quote,
        "analysis_without_history": analysis_without_history,
        "live_without_provider": live_without_provider,
        "demo_masked_as_live": demo_masked_as_live,
        "demo_count": int(counts.get("demo", 0)),
        "live_count": int(counts.get("live", 0)),
        "mixed_count": int(counts.get("mixed", 0)),
        "unknown_count": int(counts.get("unknown", 0)),
        "repair_command": "python -m fx_rates dashboard prepare-demo --years 4 --demo --symbols AAPL,MSFT,NVDA",
    }


def _history_count_for_health(conn: sqlite3.Connection, asset_type: str, symbol: str) -> int:
    normalized = symbol.strip().upper()
    if asset_type == "STOCK":
        row = conn.execute("SELECT COUNT(*) FROM stock_prices_daily WHERE symbol=? AND close IS NOT NULL", (normalized,)).fetchone()
    elif asset_type == "FX":
        row = conn.execute("SELECT COUNT(*) FROM fx_rates WHERE base='USD' AND symbol=? AND rate IS NOT NULL", (normalized,)).fetchone()
    elif asset_type == "CRYPTO":
        row = conn.execute("SELECT COUNT(*) FROM crypto_prices_daily WHERE symbol=? AND price_usd IS NOT NULL", (normalized,)).fetchone()
    elif asset_type == "MACRO":
        row = conn.execute("SELECT COUNT(*) FROM macro_indicators_daily WHERE indicator_code=? AND value IS NOT NULL", (normalized,)).fetchone()
    else:
        return 0
    return int(row[0] or 0)


def _quote_count_for_health(conn: sqlite3.Connection, asset_type: str, symbol: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM market_quotes_latest WHERE asset_type=? AND symbol=? AND price IS NOT NULL",
        (asset_type, symbol.strip().upper()),
    ).fetchone()
    return int(row[0] or 0)



def get_data_mode_summary(db_path: str) -> dict[str, Any]:
    path = Path(db_path).expanduser().resolve()
    if not path.exists():
        return {
            "data_mode": "unknown",
            "providers": [],
            "provider_summary": [],
            "data_mode_counts": {"demo": 0, "live": 0, "mixed": 0, "unknown": 0},
            "coverage": {},
            "generated_at": None,
            "warning": "database not found",
        }

    providers: set[str] = set()
    modes: set[str] = set()
    timestamps: list[str] = []
    data_mode_counts = {"demo": 0, "live": 0, "mixed": 0, "unknown": 0}
    provider_summary: dict[str, set[str]] = {}
    coverage: dict[str, Any] = {}
    with sqlite3.connect(str(path)) as conn:
        conn.row_factory = sqlite3.Row
        source_queries = (
            ("instruments", "asset_type", "provider"),
            ("market_quotes_latest", "asset_type", "provider"),
            ("stock_prices_daily", "'STOCK'", "provider"),
            ("fx_rates", "'FX'", "source"),
            ("crypto_prices_daily", "'CRYPTO'", "provider"),
            ("macro_indicators_daily", "'MACRO'", "source"),
        )
        for table, asset_expr, marker_col in source_queries:
            try:
                for row in conn.execute(
                    f"""
                    SELECT {asset_expr} AS asset_type, {marker_col} AS provider,
                           COALESCE(data_mode, '') AS data_mode, COUNT(*) AS row_count
                    FROM {table}
                    GROUP BY {asset_expr}, {marker_col}, COALESCE(data_mode, '')
                    """
                ).fetchall():
                    provider = str(row["provider"] or "").strip()
                    mode = canonical_record_mode(row["data_mode"], provider)
                    data_mode_counts[mode] = data_mode_counts.get(mode, 0) + int(row["row_count"] or 0)
                    if provider:
                        providers.add(provider)
                        asset = str(row["asset_type"] or "UNKNOWN").upper()
                        provider_summary.setdefault(asset, set()).add(provider)
            except sqlite3.Error:
                continue
        try:
            for row in conn.execute("SELECT DISTINCT mode AS value FROM ingest_runs WHERE mode IS NOT NULL AND mode <> ''").fetchall():
                modes.add(str(row["value"]))
            for row in conn.execute("SELECT finished_at FROM ingest_runs WHERE finished_at IS NOT NULL AND finished_at <> '' ORDER BY run_id DESC LIMIT 5").fetchall():
                timestamps.append(str(row["finished_at"]))
        except sqlite3.Error:
            pass
        for sql in (
            "SELECT MAX(fetched_at) FROM market_quotes_latest",
            "SELECT MAX(fetched_at) FROM stock_prices_daily",
            "SELECT MAX(fetched_at) FROM fx_rates",
            "SELECT MAX(fetched_at) FROM crypto_prices_daily",
            "SELECT MAX(fetched_at) FROM macro_indicators_daily",
            "SELECT MAX(generated_at) FROM analysis_snapshots",
        ):
            try:
                value = conn.execute(sql).fetchone()[0]
                if value:
                    timestamps.append(str(value))
            except sqlite3.Error:
                continue
        try:
            row = conn.execute(
                """
                SELECT MIN(date_min) AS date_min, MAX(date_max) AS date_max, SUM(row_count) AS row_count
                FROM (
                    SELECT MIN(date) AS date_min, MAX(date) AS date_max, COUNT(*) AS row_count FROM stock_prices_daily
                    UNION ALL SELECT MIN(date), MAX(date), COUNT(*) FROM fx_rates
                    UNION ALL SELECT MIN(date), MAX(date), COUNT(*) FROM crypto_prices_daily
                    UNION ALL SELECT MIN(date), MAX(date), COUNT(*) FROM macro_indicators_daily
                )
                """
            ).fetchone()
            coverage = {
                "date_min": row["date_min"] if row else None,
                "date_max": row["date_max"] if row else None,
                "historical_rows": int(row["row_count"] or 0) if row else 0,
            }
        except sqlite3.Error:
            coverage = {}

    record_modes = {mode for mode, count in data_mode_counts.items() if count > 0}
    # Dataset mode reflects current records, not historical ingest run names.
    markers = providers
    has_demo = "demo" in record_modes or any(_is_demo_marker(value) for value in markers)
    has_live = "live" in record_modes or any(value and not _is_demo_marker(value) for value in markers)
    has_unknown = "unknown" in record_modes and not (has_demo or has_live)
    if has_demo and has_live:
        data_mode = "mixed"
    elif has_demo:
        data_mode = "demo"
    elif has_live:
        data_mode = "live"
    elif has_unknown:
        data_mode = "unknown"
    else:
        data_mode = "unknown"
    warning = None
    if data_mode == "demo":
        warning = "Values generated for UI testing. Do not compare with market."
    elif data_mode == "mixed":
        warning = "Dataset mixes demo and live/provider-sourced records."
    return {
        "data_mode": data_mode,
        "providers": sorted(providers),
        "provider_summary": [
            {"asset_type": asset_type, "providers": sorted(values)}
            for asset_type, values in sorted(provider_summary.items())
        ],
        "data_mode_counts": data_mode_counts,
        "coverage": coverage,
        "ingest_modes": sorted(modes),
        "generated_at": max(timestamps) if timestamps else None,
        "warning": warning,
    }


def _is_demo_marker(value: str | None) -> bool:
    from .data_origin import is_demo_marker

    return is_demo_marker(value)

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
            "data_mode": "unknown",
            "providers": [],
            "provider_summary": [],
            "data_mode_counts": {"demo": 0, "live": 0, "mixed": 0, "unknown": 0},
            "coverage": {},
            "data_health": get_data_health(str(path)),
            "data_generated_at": None,
            "data_warning": "database not found",
        }

    data_mode = get_data_mode_summary(str(path))
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
        "data_mode": data_mode["data_mode"],
        "providers": data_mode["providers"],
        "provider_summary": data_mode["provider_summary"],
        "data_mode_counts": data_mode["data_mode_counts"],
        "coverage": data_mode["coverage"],
        "data_health": get_data_health(str(path)),
        "data_generated_at": data_mode["generated_at"],
        "data_warning": data_mode["warning"],
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
                   volume, currency, provider, fetched_at, data_mode, source_updated_at
            FROM stock_prices_daily
            WHERE {' AND '.join(clauses)}
            ORDER BY date
            """,
            params,
        ).fetchall()
    return _with_display_metadata(db_path, [dict(row) for row in rows])


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
            SELECT date, base, symbol, rate, source, fetched_at, data_mode, source_updated_at
            FROM fx_rates
            WHERE {' AND '.join(clauses)}
            ORDER BY date
            """,
            params,
        ).fetchall()
    return _with_display_metadata(db_path, [dict(row) for row in rows])


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
            SELECT date, indicator_code, indicator_name, value, unit, source, fetched_at, data_mode, source_updated_at
            FROM macro_indicators_daily
            WHERE {' AND '.join(clauses)}
            ORDER BY date
            """,
            params,
        ).fetchall()
    return _with_display_metadata(db_path, [dict(row) for row in rows])


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
            SELECT date, symbol, name, price_usd, market_cap, volume_24h, change_24h, provider, fetched_at, data_mode, source_updated_at
            FROM crypto_prices_daily
            WHERE {' AND '.join(clauses)}
            ORDER BY date
            """,
            params,
        ).fetchall()
    return _with_display_metadata(db_path, [dict(row) for row in rows])


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
                   provider, fetched_at, data_mode, source_updated_at
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
    return _with_display_metadata(db_path, [dict(row) for row in rows])


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
                   a.volatility_20, a.min_30d, a.max_30d, a.trend, a.signal, a.notes,
                   a.data_mode, a.source_updated_at
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
    enriched = _with_display_metadata(db_path, [dict(row) for row in rows])
    return [_with_technical_summary(row) for row in enriched]


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
        **get_data_mode_summary(db_path),
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


def get_fixed_dashboard_charts(db_path: str, days: int = 30, period: str | None = None) -> dict[str, list[dict[str, Any]]]:
    period_label = (period or f"{days}D").strip().upper()
    usd_brl_points = _fx_points(db_path, "USD", "BRL", days)
    usd_eur_points = _fx_points(db_path, "USD", "EUR", days)
    btc_points = _crypto_points(db_path, "BTC", days)
    eth_points = _crypto_points(db_path, "ETH", days)
    selic_points = _macro_points(db_path, "SELIC_DAILY", days)
    return {
        "fx": [
            {
                "id": "usd_brl_30d",
                "asset_type": "FX",
                "base": "USD",
                "symbol": "BRL",
                **_chart_metadata("BRL", "FX", "Brazilian Real", exchange="USD", base_currency="USD", period=period_label),
                "period": period_label,
                "start_date": _points_start(usd_brl_points),
                "end_date": _points_end(usd_brl_points),
                "point_count": len(usd_brl_points),
                "points": usd_brl_points,
                "message": None if usd_brl_points else f"No USD/BRL history available for {period_label}. Run dashboard prepare-demo.",
            },
            {
                "id": "usd_eur_30d",
                "asset_type": "FX",
                "base": "USD",
                "symbol": "EUR",
                **_chart_metadata("EUR", "FX", "Euro", exchange="USD", base_currency="USD", period=period_label),
                "period": period_label,
                "start_date": _points_start(usd_eur_points),
                "end_date": _points_end(usd_eur_points),
                "point_count": len(usd_eur_points),
                "points": usd_eur_points,
                "message": None if usd_eur_points else f"No USD/EUR history available for {period_label}. Run dashboard prepare-demo.",
            },
        ],
        "crypto": [
            {
                "id": "btc_usd_30d",
                "asset_type": "CRYPTO",
                "symbol": "BTC",
                **_chart_metadata("BTC", "CRYPTO", "Bitcoin", exchange="CRYPTO", period=period_label),
                "period": period_label,
                "start_date": _points_start(btc_points),
                "end_date": _points_end(btc_points),
                "point_count": len(btc_points),
                "points": btc_points,
                "message": None if btc_points else f"No BTC/USD history available for {period_label}. Run dashboard prepare-demo.",
            },
            {
                "id": "eth_usd_30d",
                "asset_type": "CRYPTO",
                "symbol": "ETH",
                **_chart_metadata("ETH", "CRYPTO", "Ethereum", exchange="CRYPTO", period=period_label),
                "period": period_label,
                "start_date": _points_start(eth_points),
                "end_date": _points_end(eth_points),
                "point_count": len(eth_points),
                "points": eth_points,
                "message": None if eth_points else f"No ETH/USD history available for {period_label}. Run dashboard prepare-demo.",
            },
        ],
        "macro": [
            {
                "id": "selic_30d",
                "asset_type": "MACRO",
                "symbol": "SELIC_DAILY",
                **_chart_metadata("SELIC_DAILY", "MACRO", "Selic Daily Rate", exchange="MACRO", unit="% a.d.", period=period_label),
                "period": period_label,
                "start_date": _points_start(selic_points),
                "end_date": _points_end(selic_points),
                "point_count": len(selic_points),
                "points": selic_points,
                "message": None if selic_points else f"No Selic history available for {period_label}. Run dashboard prepare-demo.",
            }
        ],
    }


def get_market_overview(db_path: str, days: int = 30, period: str | None = None) -> dict[str, Any]:
    period_label = (period or f"{days}D").strip().upper()
    top_stock, worst_stock = _stock_performer_cards(db_path, days=days, period=period_label)
    cards = [
        _overview_card("USD/BRL", _fx_points(db_path, "USD", "BRL", days), "BRL per 1 USD"),
        _overview_card("USD/EUR", _fx_points(db_path, "USD", "EUR", days), "EUR per 1 USD"),
        _overview_card("BTC/USD", _crypto_points(db_path, "BTC", days), "USD"),
        _overview_card("ETH/USD", _crypto_points(db_path, "ETH", days), "USD"),
        _overview_card("Selic", _macro_points(db_path, "SELIC_DAILY", days), "% a.d."),
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
        "period": period_label,
        "cards": cards,
        "signals": [_with_technical_summary(apply_display_metadata(dict(row))) for row in signals],
        "message": None if cards else DASHBOARD_EMPTY_MESSAGE,
    }


def get_dashboard_overview(db_path: str, days: int = 90, period: str | None = "90D") -> dict[str, Any]:
    period_label = (period or f"{days}D").strip().upper()
    return {
        "generated_at": utc_now_iso(),
        "period": period_label,
        "summary": get_dashboard_summary(db_path),
        "market_overview_cards": get_market_overview(db_path, days=days, period=period_label)["cards"],
        "fixed_charts": get_fixed_dashboard_charts(db_path, days=days, period=period_label),
        "technical_highlights": get_technical_highlights(db_path, days=days, period=period_label),
        "performance_ranking": get_performance_ranking(db_path, days=days, period=period_label),
        "data_quality": get_system_status(db_path),
    }


def get_technical_highlights(db_path: str, days: int = 90, period: str | None = "90D") -> dict[str, Any]:
    rows = get_performance_ranking(db_path, days=days, period=period, asset_type=None, limit=68)["items"]
    positive = [row for row in rows if row["technical_score"] > 0]
    negative = [row for row in rows if row["technical_score"] < 0]
    volatile = [row for row in rows if row.get("signal") == "VOLATILE"]
    stable = [row for row in rows if row.get("technical_label") in {"Stable", "Neutral"}]
    breakout = [row for row in rows if row.get("signal") == "BREAKOUT"]
    drawdown = [row for row in rows if row.get("signal") == "DRAWDOWN"]
    return {
        "period": (period or f"{days}D").strip().upper(),
        "positive_momentum": positive[:5],
        "negative_momentum": negative[:5],
        "breakout_watch": breakout[:5],
        "drawdown_risk": drawdown[:5],
        "stable": stable[:5],
        "volatile": volatile[:5],
    }


def get_performance_ranking(
    db_path: str,
    days: int = 90,
    period: str | None = "90D",
    asset_type: str | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    normalized_type = asset_type.strip().upper() if asset_type else None
    rows: list[dict[str, Any]] = []
    for instrument in list_instruments(db_path, asset_type=normalized_type, active=True):
        points = _points_for_instrument(db_path, instrument, days)
        if len(points) < 2:
            continue
        start_price = points[0]["value"]
        latest_price = points[-1]["value"]
        change = _percent_change(start_price, latest_price)
        analysis = _latest_analysis_for_symbol(db_path, instrument["symbol"], instrument["asset_type"])
        item = {
            **instrument,
            "latest_price": latest_price,
            "start_price": start_price,
            "period_change": change,
            "change": change,
            "trend": analysis.get("trend", "UNKNOWN"),
            "signal": analysis.get("signal", "UNKNOWN"),
            "volatility_20": analysis.get("volatility_20"),
            "points": points,
            "period": (period or f"{days}D").strip().upper(),
            "point_count": len(points),
            "start_date": _points_start(points),
            "end_date": _points_end(points),
        }
        rows.append(_with_technical_summary(item))
    ranked = sorted(rows, key=lambda item: item.get("period_change") if item.get("period_change") is not None else -999, reverse=True)
    bottom = sorted(rows, key=lambda item: item.get("period_change") if item.get("period_change") is not None else 999)
    period_label = (period or f"{days}D").strip().upper()
    changes = [float(item["period_change"]) for item in rows if item.get("period_change") is not None]
    all_positive = bool(changes) and all(value >= 0 for value in changes)
    all_negative = bool(changes) and all(value < 0 for value in changes)
    top_label = f"Least negative {period_label}" if all_negative else f"Top {period_label}"
    bottom_label = f"Weakest {period_label}" if all_positive else f"Worst {period_label}"
    return {
        "period": period_label,
        "asset_type": normalized_type or "ALL",
        "count": len(rows),
        "top_label": top_label,
        "bottom_label": bottom_label,
        "ranking_context": "all_positive" if all_positive else "all_negative" if all_negative else "mixed",
        "top": ranked[:limit],
        "bottom": bottom[:limit],
        "items": ranked,
    }


def _chart_metadata(
    symbol: str,
    asset_type: str,
    display_name: str | None = None,
    exchange: str | None = None,
    currency: str | None = None,
    unit: str | None = None,
    base_currency: str | None = None,
    period: str = "30D",
) -> dict[str, Any]:
    metadata = build_display_metadata(
        symbol=symbol,
        asset_type=asset_type,
        display_name=display_name,
        exchange=exchange,
        currency=currency,
        unit=unit,
        base_currency=base_currency,
    )
    return {
        **metadata,
        "title": f"{metadata['chart_title']} - {period}",
    }


def period_to_days(period: str | None, default: int = 90) -> tuple[str, int]:
    normalized = (period or f"{default}D").strip().upper()
    mapping = {
        "30D": 30,
        "90D": 90,
        "6M": 183,
        "1Y": 365,
        "4Y": 1460,
    }
    return (normalized if normalized in mapping else f"{default}D", mapping.get(normalized, default))


def _latest_stock_points(db_path: str, symbol: str, limit: int) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT date, close AS value, provider, data_mode, source_updated_at
            FROM stock_prices_daily
            WHERE symbol = ? AND close IS NOT NULL
            ORDER BY date DESC
            LIMIT ?
            """,
            (symbol.strip().upper(), limit),
        ).fetchall()
    return [_apply_origin_metadata(dict(row)) for row in reversed(rows)]


def _points_for_instrument(db_path: str, instrument: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    asset_type = str(instrument.get("asset_type") or "").upper()
    symbol = str(instrument.get("symbol") or "").upper()
    if asset_type == "FX":
        return _fx_points(db_path, instrument.get("exchange") or "USD", symbol, limit)
    if asset_type == "CRYPTO":
        return _crypto_points(db_path, symbol, limit)
    if asset_type == "MACRO":
        return _macro_points(db_path, symbol, limit)
    return _latest_stock_points(db_path, symbol, limit)


def _fx_points(db_path: str, base: str, symbol: str, limit: int) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT date, rate AS value, source, data_mode, source_updated_at
            FROM fx_rates
            WHERE base = ? AND symbol = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (base, symbol, limit),
        ).fetchall()
    return [_apply_origin_metadata(dict(row)) for row in reversed(rows)]


def _crypto_points(db_path: str, symbol: str, limit: int) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT date, price_usd AS value, provider, data_mode, source_updated_at
            FROM crypto_prices_daily
            WHERE symbol = ? AND price_usd IS NOT NULL
            ORDER BY date DESC
            LIMIT ?
            """,
            (symbol.strip().upper(), limit),
        ).fetchall()
    return [_apply_origin_metadata(dict(row)) for row in reversed(rows)]


def _macro_points(db_path: str, indicator_code: str, limit: int) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT date, value, source, data_mode, source_updated_at
            FROM macro_indicators_daily
            WHERE indicator_code = ? AND value IS NOT NULL
            ORDER BY date DESC
            LIMIT ?
            """,
            (indicator_code.strip().upper(), limit),
        ).fetchall()
    return [_apply_origin_metadata(dict(row)) for row in reversed(rows)]


def _instrument_meta(db_path: str, symbol: str, asset_type: str) -> dict[str, Any]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT symbol, name, asset_type, exchange, currency, sector, provider, provider_symbol, data_mode
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
            SELECT symbol, asset_type, trend, signal, generated_at,
                   last_price, last_close, change_30d, change_90d, change_1y,
                   sma_20, sma_50, volatility_20, data_mode, source_updated_at
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
    return {
        "label": label,
        "value": latest,
        "change": change,
        "unit": unit,
        "status": status,
        "provider": points[-1].get("provider") or points[-1].get("source"),
        "data_mode": points[-1].get("data_mode", "unknown"),
        "data_warning": points[-1].get("data_warning"),
    }


def _stock_performer_cards(db_path: str, days: int = 30, period: str = "30D") -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT DISTINCT symbol FROM stock_prices_daily ORDER BY symbol").fetchall()
    performers: list[tuple[str, float, float, dict[str, Any]]] = []
    for row in rows:
        symbol = str(row[0])
        points = _latest_stock_points(db_path, symbol, days)
        if len(points) < 2:
            continue
        change = _percent_change(points[0]["value"], points[-1]["value"])
        if change is not None:
            performers.append((symbol, points[-1]["value"], change, points[-1]))
    if not performers:
        return None, None
    best = max(performers, key=lambda item: item[2])
    worst = min(performers, key=lambda item: item[2])
    return (
        {"label": f"Top {period} {best[0]}", "value": best[1], "change": best[2], "unit": "USD", "status": "up", "provider": best[3].get("provider"), "data_mode": best[3].get("data_mode", "unknown"), "data_warning": best[3].get("data_warning")},
        {"label": f"Worst {period} {worst[0]}", "value": worst[1], "change": worst[2], "unit": "USD", "status": "down", "provider": worst[3].get("provider"), "data_mode": worst[3].get("data_mode", "unknown"), "data_warning": worst[3].get("data_warning")},
    )


def _points_start(points: list[dict[str, Any]]) -> str | None:
    return points[0]["date"] if points else None


def _points_end(points: list[dict[str, Any]]) -> str | None:
    return points[-1]["date"] if points else None


def _with_technical_summary(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    score = _technical_score(result)
    result["technical_score"] = score
    result["technical_label"] = _technical_label(score)
    result["technical_tone"] = _technical_tone(score, result.get("signal"))
    result["technical_summary"] = _technical_summary_text(result, score)
    return result


def _technical_score(row: dict[str, Any]) -> int:
    trend = str(row.get("trend") or "").upper()
    signal = str(row.get("signal") or "").upper()
    change = _first_number(row.get("period_change"), row.get("change_90d"), row.get("change_30d"), row.get("change"))
    volatility = _first_number(row.get("volatility_20"))
    score = 0
    if trend == "UP":
        score += 2
    elif trend == "DOWN":
        score -= 2
    if signal == "BREAKOUT":
        score += 2
    elif signal == "DRAWDOWN":
        score -= 2
    elif signal == "VOLATILE":
        score -= 1
    elif signal == "STABLE":
        score += 1
    if change is not None:
        if change > 8:
            score += 2
        elif change > 1:
            score += 1
        elif change < -8:
            score -= 2
        elif change < -1:
            score -= 1
    if volatility is not None and volatility > 0.035:
        score -= 1
    return max(-6, min(6, score))


def _technical_label(score: int) -> str:
    if score >= 4:
        return "Strong Positive"
    if score >= 2:
        return "Positive"
    if score <= -4:
        return "Strong Negative"
    if score <= -2:
        return "Negative"
    if score == 0:
        return "Neutral"
    return "Watch"


def _technical_tone(score: int, signal: Any) -> str:
    if str(signal or "").upper() == "VOLATILE":
        return "watch"
    if score >= 2:
        return "positive"
    if score <= -2:
        return "negative"
    return "neutral"


def _technical_summary_text(row: dict[str, Any], score: int) -> str:
    symbol = row.get("display_pair") or row.get("symbol") or "Asset"
    change = _first_number(row.get("period_change"), row.get("change_90d"), row.get("change_30d"), row.get("change"))
    label = _technical_label(score)
    if score >= 2:
        return f"{symbol} shows positive technical momentum over the selected period."
    if score <= -2:
        return f"{symbol} is under negative technical pressure. Watch drawdown risk."
    if str(row.get("signal") or "").upper() == "VOLATILE":
        return f"{symbol} remains volatile; monitor risk before interpreting direction."
    if change is not None and abs(change) < 1:
        return f"{symbol} is broadly stable over the selected period."
    return f"{symbol} is classified as {label.lower()} from current trend and signal data."


def _first_number(*values: Any) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


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
