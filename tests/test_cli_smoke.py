from __future__ import annotations

from unittest.mock import patch

from fx_rates.cli import build_parser, main


def test_parser_accepts_commands() -> None:
    parser = build_parser()

    status_args = parser.parse_args(["status"])
    assert status_args.command == "status"

    daily_args = parser.parse_args(["daily", "--base", "USD", "--symbols", "BRL,EUR"])
    assert daily_args.command == "daily"

    backfill_args = parser.parse_args(
        ["backfill", "--start", "2026-01-01", "--end", "2026-01-02", "--base", "USD", "--symbols", "BRL"]
    )
    assert backfill_args.command == "backfill"


@patch("fx_rates.cli.run_status", return_value=0)
def test_cli_status_smoke(mock_status, tmp_path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    code = main(["status", "--db-path", db_path])
    assert code == 0
    assert mock_status.called


@patch("fx_rates.cli.run_backfill", return_value=0)
def test_cli_backfill_smoke_no_network(mock_backfill, tmp_path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    code = main(
        [
            "backfill",
            "--start",
            "2026-01-01",
            "--end",
            "2026-01-02",
            "--base",
            "USD",
            "--symbols",
            "BRL,EUR",
            "--db-path",
            db_path,
            "--no-cache",
            "--timeout",
            "1",
        ]
    )
    assert code == 0
    assert mock_backfill.called
