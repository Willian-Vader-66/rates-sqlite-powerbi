from __future__ import annotations

import argparse
from typing import Sequence

from .config import Settings
from .ingest import run_backfill, run_daily, run_status
from .logging_setup import configure_logging
from .utils import parse_date, parse_symbols


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db-path", default=None, help="SQLite database path.")
    common.add_argument("--cache-dir", default=None, help="Directory for raw API cache files.")
    common.add_argument("--no-cache", action="store_true", help="Disable local API cache usage.")
    common.add_argument("--log-level", default=None, help="Logging level (INFO, DEBUG, ...).")
    common.add_argument("--timeout", default=None, type=int, help="HTTP timeout in seconds.")

    parser = argparse.ArgumentParser(
        prog="fx_rates",
        description="FX rates ingestion pipeline for SQLite and Power BI.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill = subparsers.add_parser("backfill", parents=[common], help="Fetch a historical range.")
    backfill.add_argument("--start", required=True, type=parse_date, help="Start date in YYYY-MM-DD.")
    backfill.add_argument("--end", required=True, type=parse_date, help="End date in YYYY-MM-DD.")
    backfill.add_argument("--base", required=True, type=str.upper, help="Base currency, e.g. USD.")
    backfill.add_argument(
        "--symbols",
        required=True,
        type=parse_symbols,
        help="Comma-separated target symbols, e.g. BRL,EUR.",
    )
    backfill.set_defaults(func=_run_backfill_command)

    daily = subparsers.add_parser("daily", parents=[common], help="Fetch the latest business day.")
    daily.add_argument("--base", required=True, type=str.upper, help="Base currency, e.g. USD.")
    daily.add_argument(
        "--symbols",
        required=True,
        type=parse_symbols,
        help="Comma-separated target symbols, e.g. BRL,EUR.",
    )
    daily.set_defaults(func=_run_daily_command)

    status = subparsers.add_parser("status", parents=[common], help="Show recent ingest runs.")
    status.add_argument("--last", default=10, type=int, help="Number of ingest runs to display.")
    status.set_defaults(func=_run_status_command)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit:
        raise
    settings = _settings_from_args(args)
    configure_logging(settings.log_file, settings.log_level)
    return int(args.func(args, settings))


def _settings_from_args(args: argparse.Namespace) -> Settings:
    defaults = Settings.from_env()
    return Settings(
        api_base_url=defaults.api_base_url,
        db_path=args.db_path or defaults.db_path,
        cache_dir=args.cache_dir or defaults.cache_dir,
        log_file=defaults.log_file,
        log_level=(args.log_level or defaults.log_level).upper(),
        timeout=args.timeout if args.timeout is not None else defaults.timeout,
    )


def _run_backfill_command(args: argparse.Namespace, settings: Settings) -> int:
    if args.start > args.end:
        raise SystemExit("Invalid date range: --start must be less than or equal to --end.")
    return run_backfill(
        settings=settings,
        start=args.start,
        end=args.end,
        base=args.base,
        symbols=args.symbols,
        use_cache=not args.no_cache,
    )


def _run_daily_command(args: argparse.Namespace, settings: Settings) -> int:
    return run_daily(
        settings=settings,
        base=args.base,
        symbols=args.symbols,
        use_cache=not args.no_cache,
    )


def _run_status_command(args: argparse.Namespace, settings: Settings) -> int:
    if args.last < 1:
        raise SystemExit("Invalid value: --last must be greater than zero.")
    rows = run_status(settings=settings, last=args.last)
    if not rows:
        print("No ingest runs found.")
        return 0

    for row in rows:
        print(
            f"run_id={row.run_id} status={row.status} mode={row.mode} base={row.base} "
            f"symbols={row.symbols} rows={row.row_count} started_at={row.started_at} "
            f"finished_at={row.finished_at or '-'}"
        )
        if row.error:
            print(f"error={row.error}")
    return 0
