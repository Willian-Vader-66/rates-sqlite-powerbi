from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

from .api import FrankfurterClient, normalize_payload
from .config import Settings, ensure_runtime_paths
from .db import finish_ingest_run, init_db, start_ingest_run, upsert_rates


def _parse_symbols(raw: str) -> list[str]:
    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    if not symbols:
        raise argparse.ArgumentTypeError("Informe ao menos um symbol em --symbols")
    return symbols


def _valid_date(raw: str) -> str:
    try:
        datetime.strptime(raw, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Data invalida: {raw}. Use YYYY-MM-DD") from exc
    return raw


def _configure_logging(level: str, log_file: str) -> None:
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(level.upper())

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)


def _apply_common_settings(args: argparse.Namespace) -> Settings:
    base = Settings.from_env()
    return Settings(
        api_base_url=base.api_base_url,
        db_path=args.db_path or base.db_path,
        cache_dir=base.cache_dir,
        log_file=base.log_file,
        log_level=args.log_level or base.log_level,
    )


def run_backfill(args: argparse.Namespace) -> int:
    settings = _apply_common_settings(args)
    ensure_runtime_paths(settings)
    _configure_logging(settings.log_level, settings.log_file)
    init_db(settings.db_path)

    run_id = start_ingest_run(
        settings.db_path,
        command="backfill",
        args={
            "start": args.start,
            "end": args.end,
            "base": args.base,
            "symbols": args.symbols,
            "use_cache": not args.no_cache,
        },
    )

    try:
        client = FrankfurterClient(
            base_url=settings.api_base_url,
            cache_dir=settings.cache_dir,
            use_cache=not args.no_cache,
        )
        payload = client.fetch_timeseries(args.start, args.end, args.base, args.symbols)
        rows = normalize_payload(payload, base=args.base)
        inserted = upsert_rates(settings.db_path, rows)
        finish_ingest_run(settings.db_path, run_id, "OK", inserted)
        logging.info("Backfill finalizado com %s linhas.", inserted)
        return 0
    except Exception as exc:
        finish_ingest_run(settings.db_path, run_id, "FAIL", 0, str(exc))
        logging.exception("Backfill falhou")
        return 1


def run_daily(args: argparse.Namespace) -> int:
    settings = _apply_common_settings(args)
    ensure_runtime_paths(settings)
    _configure_logging(settings.log_level, settings.log_file)
    init_db(settings.db_path)

    run_id = start_ingest_run(
        settings.db_path,
        command="daily",
        args={
            "base": args.base,
            "symbols": args.symbols,
            "use_cache": not args.no_cache,
        },
    )

    try:
        client = FrankfurterClient(
            base_url=settings.api_base_url,
            cache_dir=settings.cache_dir,
            use_cache=not args.no_cache,
        )
        payload = client.fetch_latest(args.base, args.symbols)
        rows = normalize_payload(payload, base=args.base)
        inserted = upsert_rates(settings.db_path, rows)
        finish_ingest_run(settings.db_path, run_id, "OK", inserted)
        logging.info("Daily finalizado com %s linhas.", inserted)
        return 0
    except Exception as exc:
        finish_ingest_run(settings.db_path, run_id, "FAIL", 0, str(exc))
        logging.exception("Daily falhou")
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest de taxas FX para SQLite")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill = subparsers.add_parser("backfill", help="Carrega intervalo historico")
    backfill.add_argument("--start", required=True, type=_valid_date)
    backfill.add_argument("--end", required=True, type=_valid_date)
    backfill.add_argument("--base", required=True)
    backfill.add_argument("--symbols", required=True, type=_parse_symbols)
    backfill.add_argument("--db-path", default=None)
    backfill.add_argument("--log-level", default=None)
    backfill.add_argument("--no-cache", action="store_true")
    backfill.set_defaults(func=run_backfill)

    daily = subparsers.add_parser("daily", help="Carrega taxas do dia")
    daily.add_argument("--base", required=True)
    daily.add_argument("--symbols", required=True, type=_parse_symbols)
    daily.add_argument("--db-path", default=None)
    daily.add_argument("--log-level", default=None)
    daily.add_argument("--no-cache", action="store_true")
    daily.set_defaults(func=run_daily)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))
