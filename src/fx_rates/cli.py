from __future__ import annotations

import argparse

from .analysis import run_analyze_now
from .config import ensure_runtime_dirs, load_settings
from .crypto_ingest import run_crypto_backfill, run_crypto_daily, run_crypto_quotes, run_crypto_test_history
from .dashboard_audit import run_dashboard_audit
from .dashboard_market_audit import run_market_audit
from .dashboard_prepare import run_prepare_demo_dashboard, run_prepare_live_dashboard
from .live_first import DEFAULT_LIVE_CANDIDATE_DB, run_audit_live, run_build_live_db
from .live_full_test import DEFAULT_LIVE_TEST_DB, run_live_full_test
from .live_promotion import run_promote_live, run_restore_backup
from .live_refresh import run_refresh_live
from .live_samples import run_validate_samples
from .live_validation import run_validate_live
from .db_sqlite import initialize_schema
from .ingest import run_backfill, run_daily, run_status
from .logging_setup import setup_logging
from .macro_ingest import run_macro_backfill, run_macro_daily, run_macro_status
from .market_ingest import run_import_instruments, run_quotes_poll, run_stocks_backfill, run_stocks_daily
from .env_doctor import run_env_doctor
from .provider_status import print_providers_status
from .api_smoke import run_api_smoke_live
from .tls_support import maybe_inject_truststore
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
    if normalized not in {"STOCK", "FX", "CRYPTO", "MACRO", "ALL"}:
        raise argparse.ArgumentTypeError("--asset-type deve ser STOCK, FX, CRYPTO, MACRO ou ALL")
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

    env = subparsers.add_parser("env", help="Diagnosticar ambiente local, TLS e API keys")
    env_sub = env.add_subparsers(dest="env_command", required=True)
    env_doctor = env_sub.add_parser("doctor", help="Verificar Python, certificados, rede e chaves sem revelar secrets")
    _add_common_flags(env_doctor)

    providers = subparsers.add_parser("providers", help="Diagnosticar providers de dados de mercado")
    providers_sub = providers.add_subparsers(dest="providers_command", required=True)
    providers_status = providers_sub.add_parser("status", help="Mostrar providers configurados sem revelar chaves")
    providers_status.add_argument("--external-test", "--test-external", dest="external_test", action="store_true", help="Executar smoke test externo opt-in dos providers")
    _add_common_flags(providers_status)

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

    macro = subparsers.add_parser("macro", help="Ingestao de indicadores macroeconomicos")
    macro_sub = macro.add_subparsers(dest="macro_command", required=True)
    macro_backfill = macro_sub.add_parser("backfill", help="Backfill historico de indicadores macro")
    macro_backfill.add_argument("--start", required=True, type=_date_arg)
    macro_backfill.add_argument("--end", required=True, type=_date_arg)
    macro_backfill.add_argument("--reference", default="data/reference/macro_indicators.csv")
    _add_common_flags(macro_backfill)

    macro_daily = macro_sub.add_parser("daily", help="Ingestao diaria de indicadores macro")
    macro_daily.add_argument("--reference", default="data/reference/macro_indicators.csv")
    _add_common_flags(macro_daily)

    macro_status = macro_sub.add_parser("status", help="Status de runs macro")
    macro_status.add_argument("--last", type=int, default=10)
    _add_common_flags(macro_status)

    crypto = subparsers.add_parser("crypto", help="Ingestao de criptoativos")
    crypto_sub = crypto.add_subparsers(dest="crypto_command", required=True)
    crypto_backfill = crypto_sub.add_parser("backfill", help="Backfill historico de criptoativos")
    crypto_backfill.add_argument("--start", required=True, type=_date_arg)
    crypto_backfill.add_argument("--end", required=True, type=_date_arg)
    crypto_backfill.add_argument("--reference", default="data/reference/crypto_assets.csv")
    crypto_backfill.add_argument("--symbols", type=_symbols_arg, default=None)
    _add_common_flags(crypto_backfill)

    crypto_daily = crypto_sub.add_parser("daily", help="Ingestao diaria de criptoativos")
    crypto_daily.add_argument("--reference", default="data/reference/crypto_assets.csv")
    _add_common_flags(crypto_daily)

    crypto_quotes = crypto_sub.add_parser("quotes", help="Coleta de cotacoes recentes de criptoativos")
    crypto_quotes.add_argument("--symbols", required=True, type=_symbols_arg)
    crypto_quotes.add_argument("--reference", default="data/reference/crypto_assets.csv")
    _add_common_flags(crypto_quotes)
    crypto_test_history = crypto_sub.add_parser("test-history", help="Diagnosticar historico CoinGecko sem tocar no SQLite")
    crypto_test_history.add_argument("--symbols", required=True, type=_symbols_arg)
    crypto_test_history.add_argument("--days", type=int, default=None)
    crypto_test_history.add_argument("--years", type=int, default=None, help="Compatibilidade: --years 1 equivale a --days 365")
    crypto_test_history.add_argument("--reference", default="data/reference/crypto_assets.csv")
    _add_common_flags(crypto_test_history)

    analyze = subparsers.add_parser("analyze", help="Gerar snapshots analiticos")
    analyze_sub = analyze.add_subparsers(dest="analyze_command", required=True)
    analyze_now = analyze_sub.add_parser("now", help="Analise usando dados ja armazenados")
    analyze_now.add_argument("--symbols", type=_symbols_arg, default=None)
    analyze_now.add_argument("--asset-type", type=_asset_type_arg, default=None)
    _add_common_flags(analyze_now)

    dashboard = subparsers.add_parser("dashboard", help="Preparar dados do dashboard")
    dashboard_sub = dashboard.add_subparsers(dest="dashboard_command", required=True)
    dashboard_prepare = dashboard_sub.add_parser("prepare-demo", help="Preparar SQLite local somente para demo/dev/testes")
    dashboard_prepare.add_argument("--days", type=int, default=None)
    dashboard_prepare.add_argument("--years", type=int, default=None, help="Compatibilidade: --years 1 equivale a --days 365")
    dashboard_prepare.add_argument("--demo", action="store_true", help="Usar dados mock deterministas sem API key")
    dashboard_prepare.add_argument("--stock-reference", default="data/reference/top100_stocks.csv")
    dashboard_prepare.add_argument("--currency-reference", default="data/reference/currencies.csv")
    dashboard_prepare.add_argument("--crypto-reference", default="data/reference/crypto_assets.csv")
    dashboard_prepare.add_argument("--macro-reference", default="data/reference/macro_indicators.csv")
    dashboard_prepare.add_argument("--stock-limit", type=int, default=32)
    dashboard_prepare.add_argument("--symbols", type=_symbols_arg, default=None, help="Preparar/reparar somente os simbolos informados")
    _add_common_flags(dashboard_prepare)
    dashboard_prepare_live = dashboard_sub.add_parser("prepare-live", help="Validar preparo de dados live sem misturar silenciosamente")
    dashboard_prepare_live.add_argument("--days", type=int, default=None)
    dashboard_prepare_live.add_argument("--years", type=int, default=None)
    dashboard_prepare_live.add_argument("--symbols", type=_symbols_arg, default=None)
    dashboard_prepare_live.add_argument("--asset-type", type=_asset_type_arg, default=None)
    dashboard_prepare_live.add_argument("--allow-mixed", action="store_true", help="Permitir dataset misto explicitamente")
    dashboard_prepare_live.add_argument("--replace-demo", action="store_true", help="Solicitar substituicao explicita de demo por live")
    dashboard_prepare_live.add_argument("--stock-reference", default="data/reference/top100_stocks.csv")
    dashboard_prepare_live.add_argument("--currency-reference", default="data/reference/currencies.csv")
    dashboard_prepare_live.add_argument("--crypto-reference", default="data/reference/crypto_assets.csv")
    dashboard_prepare_live.add_argument("--macro-reference", default="data/reference/macro_indicators.csv")
    dashboard_prepare_live.add_argument("--stock-limit", type=int, default=32)
    _add_common_flags(dashboard_prepare_live)

    dashboard_live_full_test = dashboard_sub.add_parser("live-full-test", help="Executar teste live completo em banco temporario")
    dashboard_live_full_test.add_argument("--days", type=int, default=None)
    dashboard_live_full_test.add_argument("--years", type=int, default=None)
    dashboard_live_full_test.add_argument("--symbols", type=_symbols_arg, default=None)
    dashboard_live_full_test.add_argument("--asset-type", type=_asset_type_arg, default="ALL")
    dashboard_live_full_test.add_argument("--top", type=int, default=10, help="Quantidade maxima de acoes ao usar referencia de stocks")
    dashboard_live_full_test.add_argument("--allow-partial", action="store_true", help="Continuar quando algum provider estiver indisponivel")
    dashboard_live_full_test.add_argument("--external-test", action="store_true", help="Validar providers externos antes da ingestao")
    dashboard_live_full_test.add_argument("--max-retries", dest="retries", type=int, default=None)
    dashboard_live_full_test.add_argument("--rate-limit-delay", type=float, default=0.0)
    _add_common_flags(dashboard_live_full_test)

    dashboard_build_live = dashboard_sub.add_parser("build-live-db", help="Criar banco principal live-first em staging")
    dashboard_build_live.add_argument("--days", type=int, default=None)
    dashboard_build_live.add_argument("--years", type=int, default=None)
    dashboard_build_live.add_argument("--symbols", type=_symbols_arg, default=None)
    dashboard_build_live.add_argument("--asset-type", type=_asset_type_arg, default="ALL")
    dashboard_build_live.add_argument("--top", type=int, default=10)
    dashboard_build_live.add_argument("--allow-partial", action="store_true")
    dashboard_build_live.add_argument("--external-test", action="store_true")
    dashboard_build_live.add_argument("--max-retries", dest="retries", type=int, default=None)
    dashboard_build_live.add_argument("--rate-limit-delay", type=float, default=0.0)
    _add_common_flags(dashboard_build_live)

    dashboard_refresh_live = dashboard_sub.add_parser("refresh-live", help="Atualizar incrementalmente um banco live existente")
    dashboard_refresh_live.add_argument("--symbols", type=_symbols_arg, default=None)
    dashboard_refresh_live.add_argument("--asset-type", type=_asset_type_arg, default="ALL")
    dashboard_refresh_live.add_argument("--since", type=_date_arg, default=None)
    dashboard_refresh_live.add_argument("--external-test", action="store_true")
    dashboard_refresh_live.add_argument("--dry-run", action="store_true")
    dashboard_refresh_live.add_argument("--max-retries", dest="retries", type=int, default=None)
    dashboard_refresh_live.add_argument("--rate-limit-delay", type=float, default=0.0)
    _add_common_flags(dashboard_refresh_live)

    dashboard_validate_live = dashboard_sub.add_parser("validate-live", help="Validar banco SQLite live temporario")
    dashboard_validate_live.add_argument("--expected-days", type=int, default=365)
    dashboard_validate_live.add_argument("--expected-years", type=int, default=None)
    _add_common_flags(dashboard_validate_live)

    dashboard_validate_samples = dashboard_sub.add_parser("validate-samples", help="Validar amostras do banco live contra providers")
    dashboard_validate_samples.add_argument("--samples-per-symbol", type=int, default=5)
    dashboard_validate_samples.add_argument("--external-test", action="store_true")
    dashboard_validate_samples.add_argument("--rate-limit-delay", type=float, default=0.0)
    _add_common_flags(dashboard_validate_samples)

    dashboard_audit_live = dashboard_sub.add_parser("audit-live", help="Auditar banco live-first completo")
    dashboard_audit_live.add_argument("--expected-days", type=int, default=365)
    dashboard_audit_live.add_argument("--expected-years", type=int, default=None)
    _add_common_flags(dashboard_audit_live)

    dashboard_promote_live = dashboard_sub.add_parser("promote-live", help="Promover DB live validado para o banco principal com backup opcional")
    dashboard_promote_live.add_argument("--from-db", default=None)
    dashboard_promote_live.add_argument("--candidate-db", default=None)
    dashboard_promote_live.add_argument("--to-db", default="data/fx.sqlite")
    dashboard_promote_live.add_argument("--backup", action="store_true")
    dashboard_promote_live.add_argument("--dry-run", action="store_true")
    dashboard_promote_live.add_argument("--skip-samples", action="store_true", help="Pular validate-samples explicitamente")
    dashboard_promote_live.add_argument("--skip-api-smoke", action="store_true", help="Pular smoke API explicitamente")
    dashboard_promote_live.add_argument("--smoke-port", type=int, default=8001)
    dashboard_promote_live.add_argument("--samples-per-symbol", type=int, default=5)
    dashboard_promote_live.add_argument("--expected-days", type=int, default=365)
    dashboard_promote_live.add_argument("--expected-years", type=int, default=None)
    _add_common_flags(dashboard_promote_live)

    dashboard_restore_backup = dashboard_sub.add_parser("restore-backup", help="Restaurar backup SQLite para o banco principal")
    dashboard_restore_backup.add_argument("--backup", required=True)
    dashboard_restore_backup.add_argument("--to-db", default="data/fx.sqlite")
    _add_common_flags(dashboard_restore_backup)
    dashboard_audit = dashboard_sub.add_parser("audit", help="Auditar prontidao dos dados do dashboard")
    dashboard_audit.add_argument("--expected-years", type=int, default=1)
    _add_common_flags(dashboard_audit)
    dashboard_market_audit = dashboard_sub.add_parser("audit-market", help="Auditar consistencia semantica dos dados de mercado")
    dashboard_market_audit.add_argument("--with-live-sample", action="store_true", help="Comparar amostras com fontes publicas quando disponiveis")
    dashboard_market_audit.add_argument("--json", action="store_true", help="Emitir resultado em JSON")
    _add_common_flags(dashboard_market_audit)

    api = subparsers.add_parser("api", help="Utilitarios de validacao da API local")
    api_sub = api.add_subparsers(dest="api_command", required=True)
    api_smoke_live = api_sub.add_parser("smoke-live", help="Subir API temporaria e validar endpoints contra DB live")
    api_smoke_live.add_argument("--host", default="127.0.0.1")
    api_smoke_live.add_argument("--port", type=int, default=8001)
    _add_common_flags(api_smoke_live)

    serve = subparsers.add_parser("serve", help="Iniciar API HTTP local")
    serve.add_argument("--host", default=None)
    serve.add_argument("--port", type=int, default=None)
    _add_common_flags(serve)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    truststore_status = maybe_inject_truststore()

    if args.command == "backfill" and args.start > args.end:
        parser.error("--start deve ser menor ou igual a --end")
    if args.command == "stocks" and args.stocks_command == "backfill" and args.start > args.end:
        parser.error("--start deve ser menor ou igual a --end")
    if args.command == "macro" and args.macro_command == "backfill" and args.start > args.end:
        parser.error("--start deve ser menor ou igual a --end")
    if args.command == "crypto" and args.crypto_command == "backfill" and args.start > args.end:
        parser.error("--start deve ser menor ou igual a --end")

    if args.command == "dashboard" and getattr(args, "dashboard_command", None) in {"live-full-test", "validate-live"} and args.db_path is None:
        args.db_path = DEFAULT_LIVE_TEST_DB
    if args.command == "dashboard" and getattr(args, "dashboard_command", None) in {"build-live-db", "validate-samples", "audit-live"} and args.db_path is None:
        args.db_path = DEFAULT_LIVE_CANDIDATE_DB
    if args.command == "api" and getattr(args, "api_command", None) == "smoke-live" and args.db_path is None:
        args.db_path = DEFAULT_LIVE_TEST_DB

    try:
        settings = load_settings(args)
    except ValueError as exc:
        parser.error(str(exc))

    ensure_runtime_dirs(settings)
    setup_logging(settings.log_level, settings.log_file)
    manages_sqlite_lifecycle = (
        args.command == "dashboard"
        and getattr(args, "dashboard_command", None) in {"build-live-db", "promote-live", "restore-backup"}
    ) or (args.command == "api" and getattr(args, "api_command", None) == "smoke-live")
    if args.command != "env" and not manages_sqlite_lifecycle:
        initialize_schema(settings.db_path)

    if args.command == "backfill":
        return run_backfill(settings=settings, start=args.start, end=args.end, base=args.base, symbols=args.symbols)
    if args.command == "daily":
        return run_daily(settings=settings, base=args.base, symbols=args.symbols)
    if args.command == "status":
        return run_status(settings=settings, last=args.last)
    if args.command == "env" and args.env_command == "doctor":
        return run_env_doctor(timeout_seconds=settings.timeout_seconds, truststore_status=truststore_status)
    if args.command == "providers" and args.providers_command == "status":
        return print_providers_status(settings=settings, test_external=args.external_test)
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
    if args.command == "macro" and args.macro_command == "backfill":
        return run_macro_backfill(settings=settings, start=args.start, end=args.end, reference=args.reference)
    if args.command == "macro" and args.macro_command == "daily":
        return run_macro_daily(settings=settings, reference=args.reference)
    if args.command == "macro" and args.macro_command == "status":
        return run_macro_status(settings=settings, last=args.last)
    if args.command == "crypto" and args.crypto_command == "backfill":
        return run_crypto_backfill(settings=settings, start=args.start, end=args.end, reference=args.reference, symbols=args.symbols)
    if args.command == "crypto" and args.crypto_command == "daily":
        return run_crypto_daily(settings=settings, reference=args.reference)
    if args.command == "crypto" and args.crypto_command == "quotes":
        return run_crypto_quotes(settings=settings, symbols=args.symbols, reference=args.reference)
    if args.command == "crypto" and args.crypto_command == "test-history":
        return run_crypto_test_history(settings=settings, symbols=args.symbols, days=args.days, years=args.years, reference=args.reference)
    if args.command == "analyze" and args.analyze_command == "now":
        return run_analyze_now(settings=settings, symbols=args.symbols, asset_type=args.asset_type)
    if args.command == "dashboard" and args.dashboard_command == "prepare-demo":
        return run_prepare_demo_dashboard(
            settings=settings,
            years=args.years,
            days=args.days,
            demo=True,
            stock_reference=args.stock_reference,
            currency_reference=args.currency_reference,
            crypto_reference=args.crypto_reference,
            macro_reference=args.macro_reference,
            stock_limit=args.stock_limit,
            symbols=args.symbols,
        )
    if args.command == "dashboard" and args.dashboard_command == "prepare-live":
        return run_prepare_live_dashboard(
            settings=settings,
            years=args.years,
            days=args.days,
            allow_mixed=args.allow_mixed,
            replace_demo=args.replace_demo,
            symbols=args.symbols,
            asset_type=args.asset_type,
            stock_reference=args.stock_reference,
            currency_reference=args.currency_reference,
            crypto_reference=args.crypto_reference,
            macro_reference=args.macro_reference,
            stock_limit=args.stock_limit,
        )
    if args.command == "dashboard" and args.dashboard_command == "live-full-test":
        return run_live_full_test(
            settings=settings,
            years=args.years,
            days=args.days,
            db_path=args.db_path,
            asset_type=args.asset_type,
            symbols=args.symbols,
            top=args.top,
            allow_partial=args.allow_partial,
            external_test=args.external_test,
            rate_limit_delay=args.rate_limit_delay,
        )
    if args.command == "dashboard" and args.dashboard_command == "build-live-db":
        return run_build_live_db(
            settings=settings,
            years=args.years,
            days=args.days,
            db_path=args.db_path,
            asset_type=args.asset_type,
            symbols=args.symbols,
            top=args.top,
            allow_partial=args.allow_partial,
            external_test=args.external_test,
            rate_limit_delay=args.rate_limit_delay,
        )
    if args.command == "dashboard" and args.dashboard_command == "refresh-live":
        return run_refresh_live(
            settings=settings,
            asset_type=args.asset_type,
            symbols=args.symbols,
            since=args.since,
            external_test=args.external_test,
            dry_run=args.dry_run,
            rate_limit_delay=args.rate_limit_delay,
        )
    if args.command == "dashboard" and args.dashboard_command == "validate-live":
        return run_validate_live(settings.db_path, expected_years=args.expected_years, expected_days=args.expected_days)
    if args.command == "dashboard" and args.dashboard_command == "validate-samples":
        return run_validate_samples(
            settings=settings,
            db_path=settings.db_path,
            samples_per_symbol=args.samples_per_symbol,
            external_test=args.external_test,
            rate_limit_delay=args.rate_limit_delay,
        )
    if args.command == "dashboard" and args.dashboard_command == "audit-live":
        return run_audit_live(settings.db_path, expected_years=args.expected_years, expected_days=args.expected_days)
    if args.command == "dashboard" and args.dashboard_command == "promote-live":
        return run_promote_live(
            settings=settings,
            from_db=args.from_db,
            candidate_db=args.candidate_db,
            to_db=args.to_db,
            backup=args.backup,
            dry_run=args.dry_run,
            skip_samples=args.skip_samples,
            skip_api_smoke=args.skip_api_smoke,
            smoke_port=args.smoke_port,
            samples_per_symbol=args.samples_per_symbol,
            expected_years=args.expected_years,
            expected_days=args.expected_days,
        )
    if args.command == "dashboard" and args.dashboard_command == "restore-backup":
        return run_restore_backup(backup=args.backup, to_db=args.to_db)
    if args.command == "dashboard" and args.dashboard_command == "audit":
        return run_dashboard_audit(settings.db_path, expected_years=args.expected_years)
    if args.command == "dashboard" and args.dashboard_command == "audit-market":
        return run_market_audit(settings.db_path, with_live_sample=args.with_live_sample, output_json=args.json)
    if args.command == "api" and args.api_command == "smoke-live":
        return run_api_smoke_live(settings=settings, db_path=settings.db_path, host=args.host, port=args.port)
    if args.command == "serve":
        from .api_server import create_app
        from .db_sqlite import get_system_status

        import uvicorn

        status = get_system_status(settings.db_path)
        print("Finance Monitor API starting")
        print(f"SQLite DB path: {status['db_path']}")
        print(f"DB exists: {str(status['db_exists']).lower()}")
        print(f"DB size: {_format_bytes(status['db_size_bytes'])}")
        print(f"Instruments: {status['total_instruments']}")
        print(f"Quotes: {status['latest_quote_count']}")
        print(f"Analysis snapshots: {status['latest_analysis_count']}")
        if status["is_empty"]:
            print("WARNING: SQLite database is empty.")
            print(f"Run: {status['recommended_prepare_command']}")

        app = create_app(settings)
        uvicorn.run(app, host=settings.api_host, port=settings.api_port)
        return 0
    parser.error("comando nao suportado")
    return 2


def _format_bytes(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes / (1024 * 1024):.1f} MB"
