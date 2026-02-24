from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    api_base_url: str = "https://api.frankfurter.dev/v1"
    db_path: str = "data/fx.sqlite"
    cache_dir: str = "cache"
    log_file: str = "logs/app.log"
    log_level: str = "INFO"
    timeout_seconds: int = 20
    use_cache: bool = True
