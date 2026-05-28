from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

DEFAULT_LIVE_RELEASE_SCOPE = "data/reference/live_release_scope.csv"
ASSET_TYPES = ["FX", "CRYPTO", "STOCK", "MACRO"]


@dataclass(frozen=True)
class LiveScopeItem:
    asset_type: str
    symbol: str
    display_name: str
    provider_symbol: str
    currency: str | None
    unit_label: str | None
    value_label: str | None
    expected_frequency: str
    required: bool
    min_rows_365: int


def load_live_scope(path: str | Path = DEFAULT_LIVE_RELEASE_SCOPE) -> list[LiveScopeItem]:
    source = Path(path)
    rows: list[LiveScopeItem] = []
    with source.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {
            "asset_type",
            "symbol",
            "display_name",
            "provider_symbol",
            "currency",
            "unit_label",
            "value_label",
            "expected_frequency",
            "required",
            "min_rows_365",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"live release scope missing columns: {', '.join(sorted(missing))}")
        for row in reader:
            asset_type = (row.get("asset_type") or "").strip().upper()
            symbol = (row.get("symbol") or "").strip().upper()
            if asset_type not in ASSET_TYPES or not symbol:
                continue
            rows.append(
                LiveScopeItem(
                    asset_type=asset_type,
                    symbol=symbol,
                    display_name=(row.get("display_name") or symbol).strip(),
                    provider_symbol=(row.get("provider_symbol") or symbol).strip(),
                    currency=(row.get("currency") or "").strip().upper() or None,
                    unit_label=(row.get("unit_label") or "").strip() or None,
                    value_label=(row.get("value_label") or "").strip() or None,
                    expected_frequency=(row.get("expected_frequency") or "").strip().lower() or "daily",
                    required=_parse_bool(row.get("required")),
                    min_rows_365=int((row.get("min_rows_365") or "0").strip() or "0"),
                )
            )
    return rows


def release_scope_by_asset(path: str | Path = DEFAULT_LIVE_RELEASE_SCOPE) -> dict[str, list[str]]:
    scoped: dict[str, list[str]] = {asset_type: [] for asset_type in ASSET_TYPES}
    for item in load_live_scope(path):
        if item.required:
            scoped[item.asset_type].append(item.symbol)
    return scoped


def required_scope_items(path: str | Path = DEFAULT_LIVE_RELEASE_SCOPE) -> list[LiveScopeItem]:
    return [item for item in load_live_scope(path) if item.required]


def min_rows_for_years(item: LiveScopeItem, expected_years: int) -> int:
    if expected_years <= 0:
        return 1
    return max(1, int(item.min_rows_365 * expected_years))


def min_rows_for_days(item: LiveScopeItem, expected_days: int) -> int:
    if expected_days <= 0:
        return 1
    return max(1, int(item.min_rows_365 * (expected_days / 365.0)))


def _parse_bool(value: str | None) -> bool:
    return str(value or "").strip().lower() not in {"", "0", "false", "no", "n"}
