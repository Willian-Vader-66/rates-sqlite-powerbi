from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
BASE_CODE_RE = re.compile(r"^[A-Z]{3}$")
TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}


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


def normalize_base(value: str) -> str:
    normalized = value.strip().upper()
    if not BASE_CODE_RE.fullmatch(normalized):
        raise ValueError("base invalida: use codigo de 3 letras, ex: USD")
    return normalized


def normalize_symbol_list(symbols: Iterable[str]) -> list[str]:
    normalized = sorted({item.strip().upper() for item in symbols if item.strip()})
    if not normalized:
        raise ValueError("--symbols precisa conter ao menos um valor")
    return normalized


def split_symbols(raw: str) -> list[str]:
    return normalize_symbol_list(raw.split(","))


def parse_bool(value: str, *, name: str) -> bool:
    normalized = value.strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    raise ValueError(f"{name} invalido: use true/false")


def validate_log_level(level: str) -> str:
    normalized = level.upper()
    if normalized not in VALID_LOG_LEVELS:
        raise ValueError(f"log level invalido: {level}")
    return normalized
