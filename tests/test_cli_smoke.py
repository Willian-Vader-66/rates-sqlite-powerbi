import sqlite3

from fx_rates.cli import main


def test_daily_cli_creates_db_and_records_run(monkeypatch, tmp_path):
    db_path = tmp_path / "data" / "fx.sqlite"
    cache_dir = tmp_path / "cache"

    def fake_fetch_latest(self, base, symbols):
        return {
            "amount": 1.0,
            "base": base,
            "date": "2026-03-09",
            "rates": {symbol: 5.0 + index for index, symbol in enumerate(symbols)},
        }

    monkeypatch.setattr(
        "fx_rates.api_frankfurter.FrankfurterClient.fetch_latest",
        fake_fetch_latest,
    )

    exit_code = main(
        [
            "daily",
            "--base",
            "USD",
            "--symbols",
            "BRL,EUR",
            "--db-path",
            str(db_path),
            "--cache-dir",
            str(cache_dir),
            "--no-cache",
        ]
    )

    assert exit_code == 0
    assert db_path.exists()

    with sqlite3.connect(db_path) as connection:
        rate_count = connection.execute("SELECT COUNT(*) FROM fx_rates").fetchone()[0]
        run = connection.execute(
            "SELECT status, mode, base, symbols, row_count FROM ingest_runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()

    assert rate_count == 2
    assert run == ("OK", "daily", "USD", "BRL,EUR", 2)
