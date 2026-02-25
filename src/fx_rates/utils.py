from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


def parse_yyyy_mm_dd(value: str) -> str:
    datetime.strptime(value, "%Y-%m-%d")
    return value


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_parent_dir(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def cache_key(url: str, params: dict[str, str]) -> str:
    raw = json.dumps({"url": url, "params": params}, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def split_symbols(raw: str) -> list[str]:
    symbols = [item.strip().upper() for item in raw.split(",") if item.strip()]
    if not symbols:
        raise ValueError("--symbols precisa conter ao menos um valor")
    return symbols


def validate_log_level(level: str) -> str:
    normalized = level.upper()
    if normalized not in VALID_LOG_LEVELS:
        raise ValueError(f"log level invalido: {level}")
    return normalized
