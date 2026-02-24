from __future__ import annotations


def split_symbols(raw: str) -> list[str]:
    return [item.strip().upper() for item in raw.split(",") if item.strip()]
