import sqlite3

import pytest

from fx_rates.api_frankfurter import FrankfurterHttpError
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


def test_backfill_cli_records_ok_run(monkeypatch, tmp_path):
    db_path = tmp_path / "data" / "fx.sqlite"
    cache_dir = tmp_path / "cache"

    def fake_fetch_timeseries(self, start, end, base, symbols):
        return {
            "amount": 1.0,
            "base": base,
            "rates": {
                "2026-03-06": {symbol: 5.0 + index for index, symbol in enumerate(symbols)},
                "2026-03-07": {symbol: 5.2 + index for index, symbol in enumerate(symbols)},
            },
        }

    monkeypatch.setattr(
        "fx_rates.api_frankfurter.FrankfurterClient.fetch_timeseries",
        fake_fetch_timeseries,
    )

    exit_code = main(
        [
            "backfill",
            "--start",
            "2026-03-06",
            "--end",
            "2026-03-07",
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

    with sqlite3.connect(db_path) as connection:
        rate_count = connection.execute("SELECT COUNT(*) FROM fx_rates").fetchone()[0]
        run = connection.execute(
            "SELECT status, mode, start, end, row_count FROM ingest_runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()

    assert rate_count == 4
    assert run == ("OK", "backfill", "2026-03-06", "2026-03-07", 4)


def test_daily_cli_records_fail_run(monkeypatch, tmp_path):
    db_path = tmp_path / "data" / "fx.sqlite"
    cache_dir = tmp_path / "cache"

    def fake_fetch_latest(self, base, symbols):
        raise FrankfurterHttpError("HTTP request failed: 503 Service Unavailable")

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

    assert exit_code == 1

    with sqlite3.connect(db_path) as connection:
        run = connection.execute(
            "SELECT status, row_count, error FROM ingest_runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()

    assert run[0] == "FAIL"
    assert run[1] == 0
    assert "HTTP request failed" in run[2]


def test_status_command_prints_recent_runs(monkeypatch, tmp_path, capsys):
    db_path = tmp_path / "data" / "fx.sqlite"
    cache_dir = tmp_path / "cache"

    def fake_fetch_latest(self, base, symbols):
        return {
            "amount": 1.0,
            "base": base,
            "date": "2026-03-09",
            "rates": {"BRL": 5.23},
        }

    monkeypatch.setattr(
        "fx_rates.api_frankfurter.FrankfurterClient.fetch_latest",
        fake_fetch_latest,
    )

    assert main(
        [
            "daily",
            "--base",
            "USD",
            "--symbols",
            "BRL",
            "--db-path",
            str(db_path),
            "--cache-dir",
            str(cache_dir),
            "--no-cache",
        ]
    ) == 0

    exit_code = main(["status", "--last", "1", "--db-path", str(db_path), "--cache-dir", str(cache_dir)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "run_id=" in captured.out
    assert "status=OK" in captured.out


def test_invalid_status_last_value_exits():
    with pytest.raises(SystemExit, match="Invalid value"):
        main(["status", "--last", "0"])
