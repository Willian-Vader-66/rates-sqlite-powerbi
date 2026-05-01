from __future__ import annotations

import sqlite3
from pathlib import Path

from fx_rates.db_sqlite import initialize_schema


def test_market_tables_and_indexes_exist(tmp_path: Path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    initialize_schema(db_path)
    initialize_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        stock_indexes = {row[1] for row in conn.execute("PRAGMA index_list('stock_prices_daily')").fetchall()}
        quote_indexes = {row[1] for row in conn.execute("PRAGMA index_list('market_quotes_latest')").fetchall()}
        analysis_indexes = {row[1] for row in conn.execute("PRAGMA index_list('analysis_snapshots')").fetchall()}

    assert {"instruments", "stock_prices_daily", "market_quotes_latest", "analysis_snapshots"} <= tables
    assert "idx_stock_prices_symbol_date" in stock_indexes
    assert "idx_market_quotes_asset_symbol" in quote_indexes
    assert "idx_analysis_symbol_generated" in analysis_indexes
