from __future__ import annotations

import sqlite3
from pathlib import Path

from fx_rates.db_sqlite import initialize_schema, upsert_fx_rates
from fx_rates.models import FxRateRow


def test_upsert_updates_without_duplication(tmp_path: Path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    initialize_schema(db_path)

    first = [
        FxRateRow(
            date="2026-02-10",
            base="USD",
            symbol="BRL",
            rate=5.12,
            source="frankfurter",
            fetched_at="2026-02-10T00:00:00+00:00",
        )
    ]

    second = [
        FxRateRow(
            date="2026-02-10",
            base="USD",
            symbol="BRL",
            rate=5.22,
            source="frankfurter",
            fetched_at="2026-02-10T01:00:00+00:00",
        )
    ]

    assert upsert_fx_rates(db_path, first) == 1
    assert upsert_fx_rates(db_path, second) == 1

    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM fx_rates").fetchone()[0]
        rate = conn.execute(
            "SELECT rate FROM fx_rates WHERE date=? AND base=? AND symbol=?",
            ("2026-02-10", "USD", "BRL"),
        ).fetchone()[0]

    assert count == 1
    assert float(rate) == 5.22


def test_expected_indexes_exist(tmp_path: Path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    initialize_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("PRAGMA index_list('fx_rates')").fetchall()

    index_names = {row[1] for row in rows}
    assert "idx_fx_rates_symbol_date" in index_names
    assert "idx_fx_rates_date" in index_names
