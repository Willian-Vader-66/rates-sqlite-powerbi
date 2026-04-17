from __future__ import annotations

import sqlite3
from pathlib import Path

from fx_rates.db_sqlite import initialize_schema, list_ingest_runs, start_ingest_run, upsert_fx_rates
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
    assert "idx_fx_symbol_date" in index_names
    assert "idx_fx_date" in index_names


def test_views_exist_after_initialize_schema(tmp_path: Path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    initialize_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='view'").fetchall()

    view_names = {row[0] for row in rows}
    assert "v_fx_daily" in view_names
    assert "v_fx_latest" in view_names
    assert "v_fx_monthly_avg" in view_names


def test_ingest_runs_can_be_listed_with_normalized_symbols(tmp_path: Path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    initialize_schema(db_path)

    run_id = start_ingest_run(
        db_path=db_path,
        mode="daily",
        base="usd",
        symbols=["EUR", "brl", "EUR"],
        start=None,
        end=None,
    )

    rows = list_ingest_runs(db_path, limit=10)

    assert run_id > 0
    assert len(rows) == 1
    assert rows[0]["mode"] == "daily"
    assert rows[0]["status"] == "RUNNING"
    assert rows[0]["base"] == "USD"
    assert rows[0]["symbols"] == "BRL,EUR"
