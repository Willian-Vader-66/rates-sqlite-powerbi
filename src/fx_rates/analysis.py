from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from statistics import pstdev
from typing import Any

from .config import Settings
from .db_sqlite import insert_analysis_snapshots
from .data_origin import canonical_record_mode
from .models import AnalysisSnapshotRow
from .models import CryptoPriceDailyRow, FxRateRow, MacroIndicatorDailyRow, StockPriceDailyRow
from .utils import normalize_symbol_list, utc_now_iso

VOLATILITY_THRESHOLD = 0.035
BREAKOUT_THRESHOLD = 0.05
DRAWDOWN_THRESHOLD = -0.05
STABLE_THRESHOLD = 0.01


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
    if normalized_type not in {None, "STOCK", "FX", "CRYPTO", "MACRO"}:
        raise ValueError("--asset-type deve ser STOCK, FX, CRYPTO ou MACRO")

    snapshots: list[AnalysisSnapshotRow] = []
    if normalized_type in {None, "STOCK"}:
        snapshots.extend(_build_stock_snapshots(db_path, symbols))
    if normalized_type in {None, "FX"}:
        snapshots.extend(_build_fx_snapshots(db_path, symbols))
    if normalized_type in {None, "CRYPTO"}:
        snapshots.extend(_build_crypto_snapshots(db_path, symbols))
    if normalized_type in {None, "MACRO"}:
        snapshots.extend(_build_macro_snapshots(db_path, symbols))
    return snapshots


def build_analysis_snapshots_from_live_rows(
    *,
    stock_rows: list[StockPriceDailyRow],
    fx_rows: list[FxRateRow],
    crypto_rows: list[CryptoPriceDailyRow],
    macro_rows: list[MacroIndicatorDailyRow],
) -> list[AnalysisSnapshotRow]:
    snapshots: list[AnalysisSnapshotRow] = []
    for symbol, rows in _group_rows(stock_rows, lambda row: row.symbol).items():
        points = [
            {
                "date": row.date,
                "symbol": row.symbol,
                "exchange": row.exchange,
                "value": row.close,
                "provider": row.provider,
                "data_mode": row.data_mode,
                "source_updated_at": row.source_updated_at,
            }
            for row in rows
            if row.close is not None
        ]
        if points:
            snapshots.append(_snapshot_from_series(symbol=symbol, asset_type="STOCK", exchange=points[-1]["exchange"], rows=points))
    for key, rows in _group_rows(fx_rows, lambda row: f"{row.base}|{row.symbol}").items():
        base, symbol = key.split("|", 1)
        points = [
            {
                "date": row.date,
                "symbol": row.symbol,
                "exchange": row.base,
                "value": row.rate,
                "provider": row.source,
                "data_mode": row.data_mode,
                "source_updated_at": row.source_updated_at,
            }
            for row in rows
        ]
        if points:
            snapshots.append(_snapshot_from_series(symbol=symbol, asset_type="FX", exchange=base, rows=points))
    for symbol, rows in _group_rows(crypto_rows, lambda row: row.symbol).items():
        points = [
            {
                "date": row.date,
                "symbol": row.symbol,
                "exchange": "CRYPTO",
                "value": row.price_usd,
                "provider": row.provider,
                "data_mode": row.data_mode,
                "source_updated_at": row.source_updated_at,
            }
            for row in rows
            if row.price_usd is not None
        ]
        if points:
            snapshots.append(_snapshot_from_series(symbol=symbol, asset_type="CRYPTO", exchange="CRYPTO", rows=points))
    for symbol, rows in _group_rows(macro_rows, lambda row: row.indicator_code).items():
        points = [
            {
                "date": row.date,
                "symbol": row.indicator_code,
                "exchange": "MACRO",
                "value": row.value,
                "provider": row.source,
                "data_mode": row.data_mode,
                "source_updated_at": row.source_updated_at,
            }
            for row in rows
            if row.value is not None
        ]
        if points:
            snapshots.append(_snapshot_from_series(symbol=symbol, asset_type="MACRO", exchange="MACRO", rows=points, macro=True))
    return snapshots


def _group_rows(rows: list[Any], key_func: Any) -> dict[str, list[Any]]:
    grouped: dict[str, list[Any]] = {}
    for row in rows:
        grouped.setdefault(str(key_func(row)), []).append(row)
    return grouped


def _build_stock_snapshots(db_path: str, symbols: list[str] | None) -> list[AnalysisSnapshotRow]:
    target_symbols = normalize_symbol_list(symbols) if symbols else _symbols_from_table(db_path, "stock_prices_daily", "symbol")
    snapshots: list[AnalysisSnapshotRow] = []
    for symbol in target_symbols:
        rows = _history_rows(
            db_path,
            """
            SELECT date, symbol, exchange, close AS value, provider, data_mode, source_updated_at
            FROM stock_prices_daily
            WHERE symbol = ? AND close IS NOT NULL
              AND data_mode = COALESCE((SELECT CASE WHEN EXISTS (SELECT 1 FROM stock_prices_daily WHERE symbol = ? AND data_mode = 'live') THEN 'live' WHEN EXISTS (SELECT 1 FROM stock_prices_daily WHERE symbol = ? AND data_mode = 'demo') THEN 'demo' END), data_mode)
            ORDER BY date
            """,
            [symbol, symbol, symbol],
        )
        if rows:
            snapshots.append(_snapshot_from_series(symbol=symbol, asset_type="STOCK", exchange=rows[-1]["exchange"], rows=rows))
    return snapshots


def _build_fx_snapshots(db_path: str, symbols: list[str] | None) -> list[AnalysisSnapshotRow]:
    requested = normalize_symbol_list(symbols) if symbols else None
    snapshots: list[AnalysisSnapshotRow] = []
    for base, symbol in _fx_pairs(db_path, requested):
        rows = _history_rows(
            db_path,
            """
            SELECT date, symbol, base AS exchange, rate AS value, source AS provider, data_mode, source_updated_at
            FROM fx_rates
            WHERE base = ? AND symbol = ? AND rate IS NOT NULL
              AND data_mode = COALESCE((SELECT CASE WHEN EXISTS (SELECT 1 FROM fx_rates WHERE base = ? AND symbol = ? AND data_mode = 'live') THEN 'live' WHEN EXISTS (SELECT 1 FROM fx_rates WHERE base = ? AND symbol = ? AND data_mode = 'demo') THEN 'demo' END), data_mode)
            ORDER BY date
            """,
            [base, symbol, base, symbol, base, symbol],
        )
        if rows:
            snapshots.append(_snapshot_from_series(symbol=symbol, asset_type="FX", exchange=base, rows=rows))
    return snapshots


def _build_crypto_snapshots(db_path: str, symbols: list[str] | None) -> list[AnalysisSnapshotRow]:
    target_symbols = normalize_symbol_list(symbols) if symbols else _symbols_from_table(db_path, "crypto_prices_daily", "symbol")
    snapshots: list[AnalysisSnapshotRow] = []
    for symbol in target_symbols:
        rows = _history_rows(
            db_path,
            """
            SELECT date, symbol, 'CRYPTO' AS exchange, price_usd AS value, provider, data_mode, source_updated_at
            FROM crypto_prices_daily
            WHERE symbol = ? AND price_usd IS NOT NULL
              AND data_mode = COALESCE((SELECT CASE WHEN EXISTS (SELECT 1 FROM crypto_prices_daily WHERE symbol = ? AND data_mode = 'live') THEN 'live' WHEN EXISTS (SELECT 1 FROM crypto_prices_daily WHERE symbol = ? AND data_mode = 'demo') THEN 'demo' END), data_mode)
            ORDER BY date
            """,
            [symbol, symbol, symbol],
        )
        if rows:
            snapshots.append(_snapshot_from_series(symbol=symbol, asset_type="CRYPTO", exchange="CRYPTO", rows=rows))
    return snapshots


def _build_macro_snapshots(db_path: str, symbols: list[str] | None) -> list[AnalysisSnapshotRow]:
    target_symbols = normalize_symbol_list(symbols) if symbols else _symbols_from_table(
        db_path, "macro_indicators_daily", "indicator_code"
    )
    snapshots: list[AnalysisSnapshotRow] = []
    for symbol in target_symbols:
        rows = _history_rows(
            db_path,
            """
            SELECT date, indicator_code AS symbol, 'MACRO' AS exchange, value, source AS provider, data_mode, source_updated_at
            FROM macro_indicators_daily
            WHERE indicator_code = ? AND value IS NOT NULL
              AND data_mode = COALESCE((SELECT CASE WHEN EXISTS (SELECT 1 FROM macro_indicators_daily WHERE indicator_code = ? AND data_mode = 'live') THEN 'live' WHEN EXISTS (SELECT 1 FROM macro_indicators_daily WHERE indicator_code = ? AND data_mode = 'demo') THEN 'demo' END), data_mode)
            ORDER BY date
            """,
            [symbol, symbol, symbol],
        )
        if rows:
            snapshots.append(_snapshot_from_series(symbol=symbol, asset_type="MACRO", exchange="MACRO", rows=rows, macro=True))
    return snapshots


def _snapshot_from_series(
    symbol: str,
    asset_type: str,
    exchange: str | None,
    rows: list[dict[str, Any]],
    macro: bool = False,
) -> AnalysisSnapshotRow:
    dated_values = [(row["date"], float(row["value"])) for row in rows if row["value"] is not None]
    values = [value for _, value in dated_values]
    generated_at = utc_now_iso()
    data_mode = _series_data_mode(rows)
    source_updated_at = rows[-1].get("source_updated_at") or rows[-1].get("date") if rows else None
    if not values:
        return _empty_snapshot(symbol, asset_type, exchange, generated_at, "no historical data")

    last = values[-1]
    previous = values[-2] if len(values) >= 2 else None
    daily_return = _ratio_change(previous, last)
    change_30d = _period_change(dated_values, 30)
    change_90d = _period_change(dated_values, 90)
    change_1y = _period_change(dated_values, 365)
    sma_20 = _mean(values[-20:]) if len(values) >= 20 else None
    sma_50 = _mean(values[-50:]) if len(values) >= 50 else None
    returns = _returns(values[-21:])
    volatility_20 = pstdev(returns) if len(returns) >= 2 else None
    min_30d = min(values[-30:]) if len(values) >= 2 else None
    max_30d = max(values[-30:]) if len(values) >= 2 else None

    if macro:
        trend = _macro_trend(change_90d if change_90d is not None else change_30d, volatility_20)
        signal = trend
    else:
        trend = _trend(last, sma_20, sma_50, change_30d)
        signal = _signal(change_30d, volatility_20, min_30d, max_30d, last)

    notes = _coverage_note(dated_values[0][0], dated_values[-1][0], len(values), asset_type)
    return AnalysisSnapshotRow(
        symbol=symbol,
        asset_type=asset_type,
        exchange=exchange,
        generated_at=generated_at,
        last_price=last,
        last_close=last,
        daily_return=daily_return,
        change_30d=change_30d,
        change_90d=change_90d,
        change_1y=change_1y,
        sma_20=sma_20,
        sma_50=sma_50,
        volatility_20=volatility_20,
        min_30d=min_30d,
        max_30d=max_30d,
        trend=trend,
        signal=signal,
        notes=notes,
        data_mode=data_mode,
        source_updated_at=source_updated_at,
    )


def _empty_snapshot(
    symbol: str,
    asset_type: str,
    exchange: str | None,
    generated_at: str,
    notes: str,
) -> AnalysisSnapshotRow:
    return AnalysisSnapshotRow(
        symbol=symbol,
        asset_type=asset_type,
        exchange=exchange,
        generated_at=generated_at,
        last_price=None,
        last_close=None,
        daily_return=None,
        change_30d=None,
        change_90d=None,
        change_1y=None,
        sma_20=None,
        sma_50=None,
        volatility_20=None,
        min_30d=None,
        max_30d=None,
        trend="UNKNOWN",
        signal="UNKNOWN",
        notes=notes,
    )


def _series_data_mode(rows: list[dict[str, Any]]) -> str:
    modes = {
        canonical_record_mode(row.get("data_mode"), row.get("provider"))
        for row in rows
    }
    modes.discard("unknown")
    if len(modes) > 1:
        return "mixed"
    if len(modes) == 1:
        return modes.pop()
    return "unknown"


def _symbols_from_table(db_path: str, table: str, column: str) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(f"SELECT DISTINCT {column} FROM {table} ORDER BY {column}").fetchall()
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


def _period_change(dated_values: list[tuple[str, float]], days: int) -> float | None:
    valid = [(raw_date, value) for raw_date, value in dated_values if value not in (None, 0)]
    if len(valid) < 2:
        return None
    try:
        end_date = date.fromisoformat(valid[-1][0])
        cutoff = end_date - timedelta(days=days)
        window = [(raw_date, value) for raw_date, value in valid if date.fromisoformat(raw_date) >= cutoff]
    except ValueError:
        window = []
    if len(window) < 2:
        window = valid
    return _ratio_change(window[0][1], window[-1][1])


def _ratio_change(start: float | None, end: float | None) -> float | None:
    if start is None or end is None or float(start) == 0:
        return None
    return (float(end) / float(start)) - 1.0


def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _trend(last: float, sma_20: float | None, sma_50: float | None, change_30d: float | None) -> str:
    if sma_20 is None:
        return "UNKNOWN" if change_30d is None else "SIDEWAYS"
    if sma_50 is None:
        if last > sma_20:
            return "UP"
        if last < sma_20:
            return "DOWN"
        return "SIDEWAYS"
    if last > sma_20 and sma_20 >= sma_50:
        return "UP"
    if last < sma_20 and sma_20 <= sma_50:
        return "DOWN"
    if change_30d is not None and abs(change_30d) < STABLE_THRESHOLD:
        return "SIDEWAYS"
    return "SIDEWAYS"


def _signal(
    change_30d: float | None,
    volatility_20: float | None,
    min_30d: float | None,
    max_30d: float | None,
    last: float,
) -> str:
    if change_30d is None:
        return "UNKNOWN"
    if volatility_20 is not None and volatility_20 > VOLATILITY_THRESHOLD:
        return "VOLATILE"
    if change_30d >= BREAKOUT_THRESHOLD or (max_30d is not None and last >= max_30d * 0.995 and change_30d > 0):
        return "BREAKOUT"
    if change_30d <= DRAWDOWN_THRESHOLD or (min_30d is not None and last <= min_30d * 1.005 and change_30d < 0):
        return "DRAWDOWN"
    if abs(change_30d) <= STABLE_THRESHOLD and (volatility_20 is None or volatility_20 <= VOLATILITY_THRESHOLD):
        return "STABLE"
    return "WATCH"


def _macro_trend(change: float | None, volatility_20: float | None) -> str:
    if change is None:
        return "UNKNOWN"
    if abs(change) <= STABLE_THRESHOLD or (volatility_20 is not None and volatility_20 <= 0.001):
        return "STABLE"
    if change > 0:
        return "UP"
    if change < 0:
        return "DOWN"
    return "STABLE"


def _coverage_note(start: str, end: str, count: int, asset_type: str) -> str:
    return f"coverage {start} to {end}; points={count}; asset_type={asset_type}"
