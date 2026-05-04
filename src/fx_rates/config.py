from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

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
    if settings.api_port <= 0 or settings.api_port > 65535:
        raise ValueError("API_PORT invalida")


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
