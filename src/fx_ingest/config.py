from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    api_base_url: str = "https://api.frankfurter.dev"
    db_path: str = "data/fx.sqlite"
    cache_dir: str = ".cache/http"
    log_file: str = "logs/app.log"
    log_level: str = "INFO"

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            api_base_url=os.getenv("API_BASE_URL", "https://api.frankfurter.dev").rstrip("/"),
            db_path=os.getenv("DB_PATH", "data/fx.sqlite"),
            cache_dir=os.getenv("CACHE_DIR", ".cache/http"),
            log_file=os.getenv("LOG_FILE", "logs/app.log"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )


def ensure_runtime_paths(settings: Settings) -> None:
    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)
    Path(settings.cache_dir).mkdir(parents=True, exist_ok=True)
    Path(settings.log_file).parent.mkdir(parents=True, exist_ok=True)
