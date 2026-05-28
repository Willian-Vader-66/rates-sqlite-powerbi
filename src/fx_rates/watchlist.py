from __future__ import annotations

import csv
from pathlib import Path

from .models import InstrumentRow
from .utils import utc_now_iso

REQUIRED_WATCHLIST_COLUMNS = {"symbol", "name", "exchange", "currency", "sector", "is_active", "priority"}


def load_stock_watchlist(path: str, *, provider: str = "twelvedata") -> list[InstrumentRow]:
    rows: list[InstrumentRow] = []
    now = utc_now_iso()
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = set(reader.fieldnames or [])
        missing = REQUIRED_WATCHLIST_COLUMNS - columns
        if missing:
            raise ValueError(f"watchlist sem colunas obrigatorias: {', '.join(sorted(missing))}")

        for item in reader:
            symbol = (item.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            exchange = _blank_to_none(item.get("exchange"))
            rows.append(
                InstrumentRow(
                    symbol=symbol,
                    name=_blank_to_none(item.get("name")),
                    asset_type="STOCK",
                    exchange=exchange,
                    currency=(_blank_to_none(item.get("currency")) or "USD").upper(),
                    sector=_blank_to_none(item.get("sector")),
                    provider=provider,
                    provider_symbol=symbol,
                    is_active=1 if _parse_active(item.get("is_active")) else 0,
                    priority=_parse_priority(item.get("priority")),
                    created_at=now,
                    updated_at=now,
                    display_name=_blank_to_none(item.get("name")) or symbol,
                    unit_label=(_blank_to_none(item.get("currency")) or "USD").upper(),
                    value_label="Stock Price",
                    expected_frequency="business_daily",
                )
            )

    return rows


def load_currency_reference(path: str, *, provider: str = "frankfurter") -> list[InstrumentRow]:
    rows: list[InstrumentRow] = []
    now = utc_now_iso()
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if "symbol" not in set(reader.fieldnames or []):
            raise ValueError("currencies.csv precisa conter a coluna symbol")

        for index, item in enumerate(reader, start=1):
            symbol = (item.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            rows.append(
                InstrumentRow(
                    symbol=symbol,
                    name=_blank_to_none(item.get("name")) or symbol,
                    asset_type="FX",
                    exchange="USD",
                    currency=symbol,
                    sector="Currency",
                    provider=provider,
                    provider_symbol=symbol,
                    is_active=1,
                    priority=index,
                    created_at=now,
                    updated_at=now,
                    display_name=_blank_to_none(item.get("name")) or f"USD/{symbol}",
                    unit_label=f"{symbol} per 1 USD",
                    value_label="Exchange Rate",
                    expected_frequency="business_daily",
                )
            )
    return rows


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _parse_active(value: str | None) -> bool:
    if value is None:
        return True
    return value.strip().lower() not in {"0", "false", "no", "n"}


def _parse_priority(value: str | None) -> int:
    if value is None or not value.strip():
        return 100
    return int(value)
