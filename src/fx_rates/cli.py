from __future__ import annotations

import argparse

from .config import ensure_runtime_dirs, load_settings
from .db_sqlite import initialize_schema
from .ingest import run_backfill, run_daily, run_status
from .logging_setup import setup_logging
from .utils import normalize_base, parse_yyyy_mm_dd, split_symbols


def _date_arg(value: str) -> str:
    try:
        return parse_yyyy_mm_dd(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"data invalida: {value}; use YYYY-MM-DD") from exc


def _base_arg(value: str) -> str:
    try:
        return normalize_base(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _symbols_arg(value: str) -> list[str]:
    try:
        return split_symbols(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _add_common_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--cache-dir", default=None)
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--log-file", default=None)
    parser.add_argument("--log-level", default=None)
    parser.add_argument("--timeout", type=int, default=None)
    parser.add_argument("--retries", type=int, default=None)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="FX rates ingest CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill = subparsers.add_parser("backfill", help="Ingestao de intervalo")
    backfill.add_argument("--start", required=True, type=_date_arg)
    backfill.add_argument("--end", required=True, type=_date_arg)
    backfill.add_argument("--base", required=True, type=_base_arg)
    backfill.add_argument("--symbols", required=True, type=_symbols_arg)
    _add_common_flags(backfill)

    daily = subparsers.add_parser("daily", help="Ingestao do ultimo dia")
    daily.add_argument("--base", required=True, type=_base_arg)
    daily.add_argument("--symbols", required=True, type=_symbols_arg)
    daily.add_argument("--use-cache-latest", action="store_true", default=None)
    _add_common_flags(daily)

    status = subparsers.add_parser("status", help="Status do ultimo run")
    status.add_argument("--last", type=int, default=10)
    _add_common_flags(status)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "backfill" and args.start > args.end:
        parser.error("--start deve ser menor ou igual a --end")

    try:
        settings = load_settings(args)
    except ValueError as exc:
        parser.error(str(exc))

    ensure_runtime_dirs(settings)
    setup_logging(settings.log_level, settings.log_file)
    initialize_schema(settings.db_path)

    if args.command == "backfill":
        return run_backfill(settings=settings, start=args.start, end=args.end, base=args.base, symbols=args.symbols)
    if args.command == "daily":
        return run_daily(settings=settings, base=args.base, symbols=args.symbols)
    return run_status(settings=settings, last=args.last)
