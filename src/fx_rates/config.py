from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

from .utils import ensure_dir, ensure_parent_dir, parse_bool, validate_log_level


load_dotenv()


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

    api_base_url = os.getenv("API_BASE_URL", DEFAULTS.api_base_url).rstrip("/")
    db_path = db_path_arg or os.getenv("DB_PATH", DEFAULTS.db_path)
    cache_dir = cache_dir_arg or os.getenv("CACHE_DIR", DEFAULTS.cache_dir)
    log_file = log_file_arg or os.getenv("LOG_FILE", DEFAULTS.log_file)
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


def ensure_runtime_dirs(settings: Settings) -> None:
    ensure_parent_dir(settings.db_path)
    ensure_dir(settings.cache_dir)
    ensure_parent_dir(settings.log_file)
