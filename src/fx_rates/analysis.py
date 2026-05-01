from __future__ import annotations

import sqlite3
from statistics import pstdev
from typing import Any

from .config import Settings
from .db_sqlite import insert_analysis_snapshots
from .models import AnalysisSnapshotRow
from .utils import normalize_symbol_list, utc_now_iso

VOLATILITY_THRESHOLD = 0.035
NEAR_HIGH_RATIO = 0.98
NEAR_LOW_RATIO = 1.02


def run_analyze_now(settings: Settings, symbols: list[str] | None = None, asset_type: str | None = None) -> int:
    snapshots = build_analysis_snapshots(settings.db_path, symbols=symbols, asset_type=asset_type)
    count = insert_analysis_snapshots(settings.db_path, snapshots)
    return 0 if count or snapshots == [] else 1


def build_analysis_snapshots(
    db_path: str,
    symbols: list[str] | None = None,
    asset_type: str | None = None,
) -> list[AnalysisSnapshotRow]:
    normalized_type = asset_type.strip().upper() if asset_type else None
    if normalized_type not in {None, "STOCK", "FX"}:
        raise ValueError("--asset-type deve ser STOCK ou FX")

    snapshots: list[AnalysisSnapshotRow] = []
    if normalized_type in {None, "STOCK"}:
        snapshots.extend(_build_stock_snapshots(db_path, symbols))
    if normalized_type in {None, "FX"}:
        snapshots.extend(_build_fx_snapshots(db_path, symbols))
    return snapshots


def _build_stock_snapshots(db_path: str, symbols: list[str] | None) -> list[AnalysisSnapshotRow]:
    target_symbols = normalize_symbol_list(symbols) if symbols else _stock_symbols(db_path)
    snapshots: list[AnalysisSnapshotRow] = []
    for symbol in target_symbols:
        rows = _history_rows(
            db_path,
            """
            SELECT date, symbol, exchange, close AS value
            FROM stock_prices_daily
            WHERE symbol = ? AND close IS NOT NULL
            ORDER BY date
            """,
            [symbol],
        )
        if rows:
            snapshots.append(_snapshot_from_series(symbol=symbol, asset_type="STOCK", exchange=rows[-1]["exchange"], rows=rows))
    return snapshots


def _build_fx_snapshots(db_path: str, symbols: list[str] | None) -> list[AnalysisSnapshotRow]:
    requested = normalize_symbol_list(symbols) if symbols else None
    pairs = _fx_pairs(db_path, requested)
    snapshots: list[AnalysisSnapshotRow] = []
    for base, symbol in pairs:
        rows = _history_rows(
            db_path,
            """
            SELECT date, symbol, base AS exchange, rate AS value
            FROM fx_rates
            WHERE base = ? AND symbol = ?
            ORDER BY date
            """,
            [base, symbol],
        )
        if rows:
            snapshots.append(_snapshot_from_series(symbol=symbol, asset_type="FX", exchange=base, rows=rows))
    return snapshots


def _snapshot_from_series(symbol: str, asset_type: str, exchange: str | None, rows: list[dict[str, Any]]) -> AnalysisSnapshotRow:
    values = [float(row["value"]) for row in rows if row["value"] is not None]
    generated_at = utc_now_iso()
    if len(values) < 2:
        return AnalysisSnapshotRow(
            symbol=symbol,
            asset_type=asset_type,
            exchange=exchange,
            generated_at=generated_at,
            last_price=values[-1] if values else None,
            last_close=values[-1] if values else None,
            daily_return=None,
            sma_20=None,
            sma_50=None,
            volatility_20=None,
            min_30d=None,
            max_30d=None,
            trend="UNKNOWN",
            signal="UNKNOWN",
            notes="insufficient data",
        )

    last = values[-1]
    previous = values[-2]
    daily_return = (last / previous) - 1 if previous else None
    sma_20 = _mean(values[-20:]) if len(values) >= 20 else None
    sma_50 = _mean(values[-50:]) if len(values) >= 50 else None
    returns = _returns(values[-21:])
    volatility_20 = pstdev(returns) if len(returns) >= 20 else None
    min_30d = min(values[-30:]) if len(values) >= 30 else None
    max_30d = max(values[-30:]) if len(values) >= 30 else None
    trend = _trend(last, sma_20, sma_50)
    signal = _signal(last, volatility_20, min_30d, max_30d)
    notes = None if signal != "UNKNOWN" else "insufficient data"

    return AnalysisSnapshotRow(
        symbol=symbol,
        asset_type=asset_type,
        exchange=exchange,
        generated_at=generated_at,
        last_price=last,
        last_close=last,
        daily_return=daily_return,
        sma_20=sma_20,
        sma_50=sma_50,
        volatility_20=volatility_20,
        min_30d=min_30d,
        max_30d=max_30d,
        trend=trend,
        signal=signal,
        notes=notes,
    )


def _stock_symbols(db_path: str) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT DISTINCT symbol FROM stock_prices_daily ORDER BY symbol").fetchall()
    return [str(row[0]) for row in rows]


def _fx_pairs(db_path: str, symbols: list[str] | None) -> list[tuple[str, str]]:
    with sqlite3.connect(db_path) as conn:
        if symbols:
            placeholders = ",".join("?" for _ in symbols)
            rows = conn.execute(
                f"""
                SELECT DISTINCT base, symbol
                FROM fx_rates
                WHERE symbol IN ({placeholders})
                ORDER BY base, symbol
                """,
                symbols,
            ).fetchall()
        else:
            rows = conn.execute("SELECT DISTINCT base, symbol FROM fx_rates ORDER BY base, symbol").fetchall()
    return [(str(row[0]), str(row[1])) for row in rows]


def _history_rows(db_path: str, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def _returns(values: list[float]) -> list[float]:
    result: list[float] = []
    for previous, current in zip(values, values[1:]):
        if previous:
            result.append((current / previous) - 1)
    return result


def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _trend(last: float, sma_20: float | None, sma_50: float | None) -> str:
    if sma_20 is None or sma_50 is None:
        return "UNKNOWN"
    if sma_20 > sma_50 and last > sma_20:
        return "UP"
    if sma_20 < sma_50 and last < sma_20:
        return "DOWN"
    return "SIDEWAYS"


def _signal(last: float, volatility_20: float | None, min_30d: float | None, max_30d: float | None) -> str:
    if volatility_20 is None or min_30d is None or max_30d is None:
        return "UNKNOWN"
    if volatility_20 > VOLATILITY_THRESHOLD:
        return "VOLATILE"
    if max_30d and last >= max_30d * NEAR_HIGH_RATIO:
        return "BREAKOUT"
    if min_30d and last <= min_30d * NEAR_LOW_RATIO:
        return "DRAWDOWN"
    return "STABLE"
