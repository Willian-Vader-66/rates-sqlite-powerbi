from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from fx_rates.cli import build_parser, main


def test_parser_accepts_commands_and_new_flags() -> None:
    parser = build_parser()

    status_args = parser.parse_args(["status", "--last", "5", "--log-file", "logs/custom.log", "--retries", "2"])
    assert status_args.command == "status"
    assert status_args.last == 5
    assert status_args.log_file == "logs/custom.log"
    assert status_args.retries == 2

    daily_args = parser.parse_args(
        ["daily", "--base", "usd", "--symbols", "eur, BRL", "--use-cache-latest", "--retries", "4"]
    )
    assert daily_args.command == "daily"
    assert daily_args.base == "USD"
    assert daily_args.symbols == ["BRL", "EUR"]
    assert daily_args.use_cache_latest is True

    backfill_args = parser.parse_args(
        ["backfill", "--start", "2026-01-01", "--end", "2026-01-02", "--base", "USD", "--symbols", "BRL"]
    )
    assert backfill_args.command == "backfill"


@patch("fx_rates.cli.run_status", return_value=0)
def test_cli_status_smoke(mock_status, tmp_path: Path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    code = main(["status", "--db-path", db_path, "--last", "3"])
    assert code == 0
    assert mock_status.called


def test_cli_log_file_flag_overrides_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env_log_file = tmp_path / "env.log"
    cli_log_file = tmp_path / "cli.log"
    monkeypatch.setenv("LOG_FILE", str(env_log_file))

    with patch("fx_rates.cli.run_status", return_value=0):
        code = main(["status", "--db-path", str(tmp_path / "fx.sqlite"), "--log-file", str(cli_log_file)])

    assert code == 0
    assert cli_log_file.exists()
    assert not env_log_file.exists()


def test_cli_rejects_backfill_when_start_is_after_end() -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "backfill",
                "--start",
                "2026-01-03",
                "--end",
                "2026-01-02",
                "--base",
                "USD",
                "--symbols",
                "BRL",
            ]
        )

    assert excinfo.value.code == 2


def test_cli_backfill_smoke_creates_db_with_normalized_inputs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fx.sqlite"
    cache_dir = tmp_path / "cache"
    log_file = tmp_path / "app.log"
    monkeypatch.setenv("LOG_FILE", str(log_file))

    payload = {
        "base": "USD",
        "rates": {
            "2026-01-01": {"BRL": 5.10},
            "2026-01-02": {"BRL": 5.20, "EUR": 0.95},
        },
    }

    def _fake_fetch_timeseries(*, start: str, end: str, base: str, symbols: list[str]) -> dict:
        assert start == "2026-01-01"
        assert end == "2026-01-02"
        assert base == "USD"
        assert symbols == ["BRL", "EUR"]
        return payload

    with patch("fx_rates.ingest.FrankfurterClient.fetch_timeseries", side_effect=_fake_fetch_timeseries):
        code = main(
            [
                "backfill",
                "--start",
                "2026-01-01",
                "--end",
                "2026-01-02",
                "--base",
                "usd",
                "--symbols",
                "eur, BRL,eur ,brl",
                "--db-path",
                str(db_path),
                "--cache-dir",
                str(cache_dir),
                "--no-cache",
                "--timeout",
                "1",
            ]
        )

    assert code == 0
    assert db_path.exists()

    with sqlite3.connect(db_path) as conn:
        fx_count = conn.execute("SELECT COUNT(*) FROM fx_rates").fetchone()[0]
        row = conn.execute("SELECT status, base, symbols FROM ingest_runs ORDER BY run_id DESC LIMIT 1").fetchone()

    assert fx_count == 3
    assert row[0] == "OK"
    assert row[1] == "USD"
    assert row[2] == "BRL,EUR"


def test_cli_daily_failure_marks_run_fail(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fx.sqlite"
    cache_dir = tmp_path / "cache"
    log_file = tmp_path / "app.log"
    monkeypatch.setenv("LOG_FILE", str(log_file))

    with patch("fx_rates.ingest.FrankfurterClient.fetch_latest", side_effect=ValueError("boom")):
        code = main(
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
            ]
        )

    assert code == 1
    assert db_path.exists()

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT status, error FROM ingest_runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()

    assert row[0] == "FAIL"
    assert "boom" in row[1]
