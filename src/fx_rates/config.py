from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

from .live_history import normalize_history_mode
from .utils import ensure_dir, ensure_parent_dir, parse_bool, validate_log_level


DOTENV_PATH = find_dotenv(usecwd=True)
PROJECT_ROOT = Path(DOTENV_PATH).resolve().parent if DOTENV_PATH else Path.cwd().resolve()
load_dotenv(DOTENV_PATH or None)


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    db_path: str
    cache_dir: str
    log_file: str
    log_level: str
    timeout_seconds: int
    max_retries: int
    use_cache: bool
    use_cache_latest: bool
    twelve_data_api_key: str
    market_data_provider: str
    market_data_demo_mode: bool
    api_host: str
    api_port: int
    fx_api_key: str = ""
    crypto_api_key: str = ""
    macro_api_key: str = ""
    fred_api_key: str = ""
    coingecko_demo_api_key: str = ""
    coingecko_pro_api_key: str = ""
    coingecko_api_plan: str = "public"
    fx_provider: str = "frankfurter"
    crypto_provider: str = "coingecko"
    stock_provider: str = "twelvedata"
    macro_provider: str = "bcb_sgs"
    live_quote_warn_pct: float = 1.0
    live_quote_fail_pct: float = 5.0
    live_quote_stale_days: int = 10
    live_default_days: int = 365
    live_max_free_days: int = 365
    live_history_mode: str = "standard"
    live_advanced_max_years: int = 10


DEFAULTS = Settings(
    api_base_url="https://api.frankfurter.dev/v1",
    db_path="data/fx.sqlite",
    cache_dir="cache",
    log_file="logs/app.log",
    log_level="INFO",
    timeout_seconds=20,
    max_retries=3,
    use_cache=True,
    use_cache_latest=False,
    twelve_data_api_key="",
    fred_api_key="",
    fx_provider="frankfurter",
    crypto_provider="coingecko",
    stock_provider="twelvedata",
    macro_provider="bcb_sgs",
    market_data_provider="twelvedata",
    market_data_demo_mode=False,
    api_host="127.0.0.1",
    api_port=8000,
)


def load_settings(args: object | None = None) -> Settings:
    db_path_arg = getattr(args, "db_path", None)
    cache_dir_arg = getattr(args, "cache_dir", None)
    no_cache_arg = bool(getattr(args, "no_cache", False))
    log_file_arg = getattr(args, "log_file", None)
    log_level_arg = getattr(args, "log_level", None)
    timeout_arg = getattr(args, "timeout", None)
    retries_arg = getattr(args, "retries", None)
    use_cache_latest_arg = getattr(args, "use_cache_latest", None)
    api_host_arg = getattr(args, "host", None)
    api_port_arg = getattr(args, "port", None)

    api_base_url = os.getenv("API_BASE_URL", DEFAULTS.api_base_url).rstrip("/")
    db_path = _resolve_path(db_path_arg, os.getenv("DB_PATH", DEFAULTS.db_path), base_dir=PROJECT_ROOT)
    cache_dir = _resolve_path(cache_dir_arg, os.getenv("CACHE_DIR", DEFAULTS.cache_dir), base_dir=PROJECT_ROOT)
    log_file = _resolve_path(log_file_arg, os.getenv("LOG_FILE", DEFAULTS.log_file), base_dir=PROJECT_ROOT)
    log_level = validate_log_level(log_level_arg or os.getenv("LOG_LEVEL", DEFAULTS.log_level))

    timeout_raw = timeout_arg if timeout_arg is not None else os.getenv("TIMEOUT_SECONDS", str(DEFAULTS.timeout_seconds))
    timeout_seconds = int(timeout_raw)
    if timeout_seconds <= 0:
        raise ValueError("timeout deve ser maior que zero")

    retries_raw = retries_arg if retries_arg is not None else os.getenv("MAX_RETRIES", str(DEFAULTS.max_retries))
    max_retries = int(retries_raw)
    if max_retries < 0:
        raise ValueError("retries deve ser zero ou maior")

    if use_cache_latest_arg is not None:
        use_cache_latest = bool(use_cache_latest_arg)
    else:
        use_cache_latest = parse_bool(
            os.getenv("USE_CACHE_LATEST", str(DEFAULTS.use_cache_latest)),
            name="USE_CACHE_LATEST",
        )

    api_port_raw = api_port_arg if api_port_arg is not None else os.getenv("API_PORT", str(DEFAULTS.api_port))
    api_port = int(api_port_raw)

    settings = Settings(
        api_base_url=api_base_url,
        db_path=db_path,
        cache_dir=cache_dir,
        log_file=log_file,
        log_level=log_level,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        use_cache=not no_cache_arg,
        use_cache_latest=use_cache_latest and not no_cache_arg,
        twelve_data_api_key=os.getenv("TWELVE_DATA_API_KEY", DEFAULTS.twelve_data_api_key),
        fx_api_key=os.getenv("FX_API_KEY", DEFAULTS.fx_api_key),
        crypto_api_key=os.getenv("CRYPTO_API_KEY", DEFAULTS.crypto_api_key),
        macro_api_key=os.getenv("MACRO_API_KEY", DEFAULTS.macro_api_key),
        fred_api_key=os.getenv("FRED_API_KEY", DEFAULTS.fred_api_key),
        coingecko_demo_api_key=os.getenv(
            "COINGECKO_DEMO_API_KEY",
            os.getenv("CRYPTO_API_KEY", DEFAULTS.coingecko_demo_api_key),
        ),
        coingecko_pro_api_key=os.getenv("COINGECKO_PRO_API_KEY", DEFAULTS.coingecko_pro_api_key),
        coingecko_api_plan=os.getenv("COINGECKO_API_PLAN", DEFAULTS.coingecko_api_plan).strip().lower(),
        fx_provider=os.getenv("FX_PROVIDER", DEFAULTS.fx_provider).strip().lower(),
        crypto_provider=os.getenv("CRYPTO_PROVIDER", DEFAULTS.crypto_provider).strip().lower(),
        stock_provider=os.getenv("STOCK_PROVIDER", os.getenv("MARKET_DATA_PROVIDER", DEFAULTS.stock_provider)).strip().lower(),
        macro_provider=os.getenv("MACRO_PROVIDER", DEFAULTS.macro_provider).strip().lower(),
        live_quote_warn_pct=float(os.getenv("LIVE_QUOTE_WARN_PCT", str(DEFAULTS.live_quote_warn_pct))),
        live_quote_fail_pct=float(os.getenv("LIVE_QUOTE_FAIL_PCT", str(DEFAULTS.live_quote_fail_pct))),
        live_quote_stale_days=int(os.getenv("LIVE_QUOTE_STALE_DAYS", str(DEFAULTS.live_quote_stale_days))),
        live_default_days=int(os.getenv("LIVE_DEFAULT_DAYS", str(DEFAULTS.live_default_days))),
        live_max_free_days=int(os.getenv("LIVE_MAX_FREE_DAYS", str(DEFAULTS.live_max_free_days))),
        live_history_mode=normalize_history_mode(os.getenv("LIVE_HISTORY_MODE", DEFAULTS.live_history_mode)),
        live_advanced_max_years=int(os.getenv("LIVE_ADVANCED_MAX_YEARS", str(DEFAULTS.live_advanced_max_years))),
        market_data_provider=os.getenv("MARKET_DATA_PROVIDER", DEFAULTS.market_data_provider).strip().lower(),
        market_data_demo_mode=parse_bool(
            os.getenv("MARKET_DATA_DEMO_MODE", str(DEFAULTS.market_data_demo_mode)),
            name="MARKET_DATA_DEMO_MODE",
        ),
        api_host=api_host_arg or os.getenv("API_HOST", DEFAULTS.api_host),
        api_port=api_port,
    )
    validate_settings(settings)
    return settings


def validate_settings(settings: Settings) -> None:
    if not settings.api_base_url.startswith("http"):
        raise ValueError("API_BASE_URL invalida")
    if not settings.db_path:
        raise ValueError("DB_PATH nao pode ser vazio")
    if not settings.cache_dir:
        raise ValueError("CACHE_DIR nao pode ser vazio")
    if not settings.log_file:
        raise ValueError("LOG_FILE nao pode ser vazio")
    if settings.market_data_provider not in {"twelvedata", "mock"}:
        raise ValueError("MARKET_DATA_PROVIDER invalido: use twelvedata ou mock")
    if settings.fx_provider not in {"frankfurter", "fake_live", "none", "disabled", "off"}:
        raise ValueError("FX_PROVIDER invalido: use frankfurter, fake_live ou none")
    if settings.crypto_provider not in {"coingecko", "fake_live", "none", "disabled", "off"}:
        raise ValueError("CRYPTO_PROVIDER invalido: use coingecko, fake_live ou none")
    if settings.coingecko_api_plan not in {"public", "demo", "pro"}:
        raise ValueError("COINGECKO_API_PLAN invalido: use public, demo ou pro")
    if settings.stock_provider not in {"twelvedata", "fake_live", "none", "disabled", "off"}:
        raise ValueError("STOCK_PROVIDER invalido: use twelvedata, fake_live ou none")
    if settings.macro_provider not in {"bcb_sgs", "fred", "fake_live", "none", "disabled", "off"}:
        raise ValueError("MACRO_PROVIDER invalido: use bcb_sgs, fred, fake_live ou none")
    if settings.api_port <= 0 or settings.api_port > 65535:
        raise ValueError("API_PORT invalida")
    if settings.live_quote_warn_pct < 0:
        raise ValueError("LIVE_QUOTE_WARN_PCT deve ser zero ou maior")
    if settings.live_quote_fail_pct <= settings.live_quote_warn_pct:
        raise ValueError("LIVE_QUOTE_FAIL_PCT deve ser maior que LIVE_QUOTE_WARN_PCT")
    if settings.live_quote_stale_days <= 0:
        raise ValueError("LIVE_QUOTE_STALE_DAYS deve ser maior que zero")
    if settings.live_default_days <= 0:
        raise ValueError("LIVE_DEFAULT_DAYS deve ser maior que zero")
    if settings.live_max_free_days <= 0:
        raise ValueError("LIVE_MAX_FREE_DAYS deve ser maior que zero")
    if settings.live_advanced_max_years <= 0:
        raise ValueError("LIVE_ADVANCED_MAX_YEARS deve ser maior que zero")


def ensure_runtime_dirs(settings: Settings) -> None:
    ensure_parent_dir(settings.db_path)
    ensure_dir(settings.cache_dir)
    ensure_parent_dir(settings.log_file)


def _resolve_path(cli_value: str | None, configured_value: str, *, base_dir: Path) -> str:
    raw = cli_value if cli_value is not None else configured_value
    path = Path(raw).expanduser()
    if path.is_absolute():
        return str(path)
    anchor = Path.cwd().resolve() if cli_value is not None else base_dir
    return str((anchor / path).resolve())
