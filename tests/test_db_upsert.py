import sqlite3

from fx_rates.db_sqlite import init_db, upsert_rates
from fx_rates.models import FxRateRow


def test_upsert_rates_is_idempotent_and_indexes_exist(tmp_path):
    db_path = tmp_path / "fx.sqlite"
    init_db(str(db_path))

    first = FxRateRow(
        date="2026-03-09",
        base="USD",
        symbol="BRL",
        rate=5.20,
        source="frankfurter",
        fetched_at="2026-03-09T12:00:00+00:00",
    )
    second = FxRateRow(
        date="2026-03-09",
        base="USD",
        symbol="BRL",
        rate=5.25,
        source="frankfurter",
        fetched_at="2026-03-09T12:05:00+00:00",
    )

    assert upsert_rates(str(db_path), [first]) == 1
    assert upsert_rates(str(db_path), [second]) == 1

    with sqlite3.connect(db_path) as connection:
        count = connection.execute("SELECT COUNT(*) FROM fx_rates").fetchone()[0]
        stored = connection.execute(
            "SELECT rate, fetched_at FROM fx_rates WHERE date = ? AND base = ? AND symbol = ?",
            ("2026-03-09", "USD", "BRL"),
        ).fetchone()
        indexes = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'fx_rates'"
            )
        }

    assert count == 1
    assert stored == (5.25, "2026-03-09T12:05:00+00:00")
    assert {"idx_fx_symbol_date", "idx_fx_date"} <= indexes
