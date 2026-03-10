from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def parse_date(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc
    return value


def parse_symbols(raw: str) -> list[str]:
    symbols = [item.strip().upper() for item in raw.split(",") if item.strip()]
    if not symbols:
        raise argparse.ArgumentTypeError("At least one symbol is required.")
    return symbols


def join_symbols(symbols: list[str]) -> str:
    return ",".join(symbols)


def ensure_directory(path: str | Path) -> Path:
    resolved = Path(path)
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def stable_hash(url: str, params: dict[str, str]) -> str:
    payload = json.dumps({"url": url, "params": params}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
