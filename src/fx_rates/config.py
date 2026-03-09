from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from .utils import ensure_directory


load_dotenv()


@dataclass(frozen=True)
class Settings:
    api_base_url: str = "https://api.frankfurter.dev"
    db_path: str = "data/fx.sqlite"
    cache_dir: str = "cache"
    log_file: str = "logs/app.log"
    log_level: str = "INFO"
    timeout: int = 20

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            api_base_url=os.getenv("API_BASE_URL", cls.api_base_url).rstrip("/"),
            db_path=os.getenv("DB_PATH", cls.db_path),
            cache_dir=os.getenv("CACHE_DIR", cls.cache_dir),
            log_file=os.getenv("LOG_FILE", cls.log_file),
            log_level=os.getenv("LOG_LEVEL", cls.log_level).upper(),
            timeout=int(os.getenv("TIMEOUT", str(cls.timeout))),
        )


def ensure_runtime_paths(settings: Settings) -> None:
    ensure_directory(Path(settings.db_path).parent)
    ensure_directory(settings.cache_dir)
    ensure_directory(Path(settings.log_file).parent)
