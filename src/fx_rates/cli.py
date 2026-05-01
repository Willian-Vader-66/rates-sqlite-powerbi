from __future__ import annotations

import argparse

from .analysis import run_analyze_now
from .config import ensure_runtime_dirs, load_settings
from .db_sqlite import initialize_schema
from .ingest import run_backfill, run_daily, run_status
from .logging_setup import setup_logging
from .market_ingest import run_import_instruments, run_quotes_poll, run_stocks_backfill, run_stocks_daily
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


def _asset_type_arg(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in {"STOCK", "FX"}:
        raise argparse.ArgumentTypeError("--asset-type deve ser STOCK ou FX")
    return normalized


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

    instruments = subparsers.add_parser("instruments", help="Gerenciar instrumentos")
    instruments_sub = instruments.add_subparsers(dest="instruments_command", required=True)
    instruments_import = instruments_sub.add_parser("import", help="Importar watchlist de acoes")
    instruments_import.add_argument("--file", required=True)
    _add_common_flags(instruments_import)

    stocks = subparsers.add_parser("stocks", help="Ingestao de acoes")
    stocks_sub = stocks.add_subparsers(dest="stocks_command", required=True)
    stocks_daily = stocks_sub.add_parser("daily", help="Ingestao diaria de acoes")
    stocks_daily.add_argument("--watchlist", required=True)
    _add_common_flags(stocks_daily)

    stocks_backfill = stocks_sub.add_parser("backfill", help="Backfill historico de acoes")
    stocks_backfill.add_argument("--start", required=True, type=_date_arg)
    stocks_backfill.add_argument("--end", required=True, type=_date_arg)
    stocks_backfill.add_argument("--watchlist", required=True)
    _add_common_flags(stocks_backfill)

    quotes = subparsers.add_parser("quotes", help="Coleta de cotacoes recentes")
    quotes_sub = quotes.add_subparsers(dest="quotes_command", required=True)
    quotes_poll = quotes_sub.add_parser("poll", help="Polling rate-limited de cotacoes")
    quotes_poll.add_argument("--symbols", required=True, type=_symbols_arg)
    quotes_poll.add_argument("--interval-seconds", type=int, default=30)
    quotes_poll.add_argument("--duration-minutes", type=float, default=5)
    quotes_poll.add_argument("--asset-type", type=_asset_type_arg, default="STOCK")
    _add_common_flags(quotes_poll)

    analyze = subparsers.add_parser("analyze", help="Gerar snapshots analiticos")
    analyze_sub = analyze.add_subparsers(dest="analyze_command", required=True)
    analyze_now = analyze_sub.add_parser("now", help="Analise usando dados ja armazenados")
    analyze_now.add_argument("--symbols", type=_symbols_arg, default=None)
    analyze_now.add_argument("--asset-type", type=_asset_type_arg, default=None)
    _add_common_flags(analyze_now)

    serve = subparsers.add_parser("serve", help="Iniciar API HTTP local")
    serve.add_argument("--host", default=None)
    serve.add_argument("--port", type=int, default=None)
    _add_common_flags(serve)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "backfill" and args.start > args.end:
        parser.error("--start deve ser menor ou igual a --end")
    if args.command == "stocks" and args.stocks_command == "backfill" and args.start > args.end:
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
    if args.command == "status":
        return run_status(settings=settings, last=args.last)
    if args.command == "instruments" and args.instruments_command == "import":
        return run_import_instruments(settings=settings, file_path=args.file)
    if args.command == "stocks" and args.stocks_command == "daily":
        return run_stocks_daily(settings=settings, watchlist=args.watchlist)
    if args.command == "stocks" and args.stocks_command == "backfill":
        return run_stocks_backfill(settings=settings, start=args.start, end=args.end, watchlist=args.watchlist)
    if args.command == "quotes" and args.quotes_command == "poll":
        return run_quotes_poll(
            settings=settings,
            symbols=args.symbols,
            interval_seconds=args.interval_seconds,
            duration_minutes=args.duration_minutes,
            asset_type=args.asset_type,
        )
    if args.command == "analyze" and args.analyze_command == "now":
        return run_analyze_now(settings=settings, symbols=args.symbols, asset_type=args.asset_type)
    if args.command == "serve":
        from .api_server import create_app

        import uvicorn

        app = create_app(settings)
        uvicorn.run(app, host=settings.api_host, port=settings.api_port)
        return 0
    parser.error("comando nao suportado")
    return 2
