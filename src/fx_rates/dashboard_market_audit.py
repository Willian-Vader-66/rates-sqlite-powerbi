from __future__ import annotations

import json
import os
import sqlite3
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .db_sqlite import get_data_health, get_data_mode_summary, get_performance_ranking
from .display_metadata import build_display_metadata

FX_RANGES = {
    ("USD", "BRL"): (2.0, 10.0),
    ("USD", "EUR"): (0.5, 1.5),
    ("USD", "JPY"): (50.0, 250.0),
}
STOCK_RANGES = {symbol: (1.0, 5000.0) for symbol in ("AAPL", "AMZN", "MSFT", "NVDA", "GOOGL", "KO", "BRK.B", "AMD", "JPM", "COST")}
CRYPTO_RANGES = {
    "BTC": (1000.0, 1_000_000.0),
    "ETH": (100.0, 100_000.0),
    "BNB": (1.0, 50_000.0),
    "SOL": (1.0, 50_000.0),
    "XRP": (0.01, 100.0),
    "ADA": (0.01, 100.0),
    "DOGE": (0.001, 100.0),
    "LINK": (0.01, 10_000.0),
    "AVAX": (0.01, 10_000.0),
    "DOT": (0.01, 10_000.0),
}
MACRO_RANGES = {
    "SELIC_DAILY": (0.0, 1.0, "% a.d."),
    "CDI_DAILY": (0.0, 1.0, "% a.d."),
    "SELIC_MONTHLY": (0.0, 5.0, "% a.m."),
    "IPCA_MONTHLY": (-5.0, 10.0, "% a.m."),
    "SELIC_ANNUALIZED_MONTHLY": (0.0, 50.0, "% a.a."),
    "SELIC_TARGET": (0.0, 50.0, "% a.a."),
    "FED_FUNDS_DAILY": (0.0, 20.0, "% a.a."),
}
SAMPLE_SYMBOLS = [
    ("FX", "BRL"), ("FX", "EUR"),
    ("CRYPTO", "BTC"), ("CRYPTO", "ETH"),
    ("STOCK", "AAPL"), ("STOCK", "AMZN"), ("STOCK", "MSFT"), ("STOCK", "NVDA"), ("STOCK", "BRK.B"),
    ("MACRO", "SELIC_DAILY"), ("MACRO", "SELIC_ANNUALIZED_MONTHLY"), ("MACRO", "IPCA_MONTHLY"), ("MACRO", "FED_FUNDS_DAILY"),
]
QUOTE_HISTORY_WARN_PCT = float(os.getenv("LIVE_QUOTE_WARN_PCT", "1.0"))
QUOTE_HISTORY_FAIL_PCT = float(os.getenv("LIVE_QUOTE_FAIL_PCT", "5.0"))
QUOTE_STALE_DAYS = int(os.getenv("LIVE_QUOTE_STALE_DAYS", "10"))
MACRO_MONTHLY_STALE_DAYS = int(os.getenv("MACRO_MONTHLY_STALE_DAYS", "75"))


def run_market_audit(db_path: str, *, with_live_sample: bool = False, output_json: bool = False) -> int:
    audit = audit_market(db_path, with_live_sample=with_live_sample)
    print(json.dumps(audit, indent=2, ensure_ascii=False) if output_json else format_market_audit(audit))
    hard_fail_flags = {"NO_HISTORY", "NO_LATEST_QUOTE", "FX_SUSPICIOUS_RANGE", "STOCK_SUSPICIOUS_RANGE", "STOCK_UNIT_SUSPICIOUS", "CRYPTO_SUSPICIOUS_RANGE", "CRYPTO_UNIT_SUSPICIOUS", "MACRO_UNIT_SUSPICIOUS", "INCONSISTENT_30D_CHANGE", "INCONSISTENT_90D_CHANGE", "TOP_WORST_RANKING_BUG", "QUOTE_HISTORY_DIVERGENCE_FAIL", "QUOTE_OLDER_THAN_HISTORY", "FUTURE_HISTORY_DATE", "LIVE_WITH_DEMO_PROVIDER", "DEMO_WITH_LIVE_PROVIDER", "CONFLICTING_DATA_MODES", "QUOTE_MODE_MISMATCH", "ANALYSIS_MODE_MISMATCH"}
    has_item_failure = any(hard_fail_flags.intersection(item["flags"]) for item in audit.get("items", []))
    has_health_failure = audit.get("data_health", {}).get("status") == "FAIL"
    return 1 if has_item_failure or has_health_failure else 0


def audit_market(db_path: str, *, with_live_sample: bool = False) -> dict[str, Any]:
    path = Path(db_path).expanduser().resolve()
    if not path.exists():
        return {"db_path": str(path), "db_exists": False, "items": [], "alerts": ["DB_NOT_FOUND"]}
    with sqlite3.connect(str(path)) as conn:
        conn.row_factory = sqlite3.Row
        instruments = [dict(row) for row in conn.execute("""
            SELECT instrument_id, symbol, name, asset_type, exchange, currency, sector, provider, provider_symbol, data_mode, is_active
            FROM instruments
            WHERE is_active=1
            ORDER BY asset_type, priority, symbol
        """).fetchall()]
        items = [_audit_instrument(conn, instrument) for instrument in instruments]
        totals = _totals(conn)
        date_range = _overall_range(conn)
    data_mode = get_data_mode_summary(str(path))
    data_health = get_data_health(str(path))
    if data_mode["data_mode"] == "demo":
        for item in items:
            item["flags"].append("DEMO_DATA_PRESENT")
    elif data_mode["data_mode"] == "mixed":
        for item in items:
            if item.get("is_demo"):
                item["flags"].append("DEMO_DATA_PRESENT")
            item["flags"].append("MIXED_DATASET_REVIEW_REQUIRED")
    ranking = _ranking_flags(str(path), items)
    live_samples = _live_validation(items) if with_live_sample else {"status": "SKIPPED", "reason": "run with --with-live-sample"}
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(path),
        "db_exists": True,
        "data_mode": data_mode,
        "totals": totals,
        "date_range": date_range,
        "items": items,
        "ranking": ranking,
        "live_validation": live_samples,
        "data_health": data_health,
        "data_mode_breakdown": _data_mode_breakdown(items),
        "summary": _summary(items),
    }


def _audit_instrument(conn: sqlite3.Connection, instrument: dict[str, Any]) -> dict[str, Any]:
    asset_type = str(instrument["asset_type"]).upper()
    symbol = str(instrument["symbol"]).upper()
    exchange = instrument.get("exchange")
    history_modes = _history_modes(conn, asset_type, symbol, exchange)
    preferred_history_mode = _preferred_mode_for_asset(conn, asset_type, symbol, exchange)
    latest = _latest_quote(conn, asset_type, symbol, exchange, preferred_history_mode)
    history = _history(conn, asset_type, symbol, exchange)
    analysis = _latest_analysis(conn, asset_type, symbol, preferred_history_mode)
    quote_modes = _quote_modes(conn, asset_type, symbol, exchange)
    analysis_modes = _analysis_modes(conn, asset_type, symbol)
    metadata = build_display_metadata(
        symbol=symbol,
        asset_type=asset_type,
        display_name=instrument.get("name"),
        exchange=exchange,
        currency=instrument.get("currency"),
        unit=history.get("unit"),
        base_currency=exchange if asset_type == "FX" else None,
    )
    latest_value = latest.get("price") if latest else history.get("latest_value")
    latest_date = latest.get("quote_time") if latest else history.get("history_end")
    change_30d = _period_change(history["points"], 30)
    change_90d = _period_change(history["points"], 90)
    change_1y = _period_change(history["points"], 365)
    flags = []
    if not history["historical_row_count"]:
        flags.append("NO_HISTORY")
    if not latest:
        flags.append("NO_LATEST_QUOTE")
    if not analysis:
        flags.append("NO_ANALYSIS")
    if _is_stale(asset_type, symbol, latest_date):
        flags.append("STALE_DATA")
    flags.extend(_range_flags(asset_type, symbol, exchange, latest_value, metadata.get("display_unit")))
    flags.extend(_source_unit_flags(asset_type, history.get("unit")))
    if len(history_modes) > 1 or len(quote_modes) > 1 or len(analysis_modes) > 1:
        flags.append("CONFLICTING_DATA_MODES")
    if latest and history.get("latest_value") not in (None, 0) and latest.get("price") is not None:
        diff_pct = abs((float(latest["price"]) / float(history["latest_value"])) - 1.0) * 100.0
        if diff_pct > QUOTE_HISTORY_FAIL_PCT:
            flags.append("QUOTE_HISTORY_DIVERGENCE_FAIL")
        elif diff_pct > QUOTE_HISTORY_WARN_PCT:
            flags.append("QUOTE_HISTORY_DIVERGENCE_WARN")
        if latest.get("quote_time") and history.get("history_end") and str(latest["quote_time"])[:10] < str(history["history_end"])[:10]:
            flags.append("QUOTE_OLDER_THAN_HISTORY")
    if _has_future_date(history.get("history_end")):
        flags.append("FUTURE_HISTORY_DATE")
    if preferred_history_mode and latest and str(latest.get("data_mode") or "").lower() != preferred_history_mode:
        flags.append("QUOTE_MODE_MISMATCH")
    if preferred_history_mode and analysis_modes and preferred_history_mode not in analysis_modes:
        flags.append("ANALYSIS_MODE_MISMATCH")
    if _differs(change_30d, analysis.get("change_30d") if analysis else None):
        flags.append("INCONSISTENT_30D_CHANGE")
    if _differs(change_90d, analysis.get("change_90d") if analysis else None):
        flags.append("INCONSISTENT_90D_CHANGE")
    provider = (latest.get("provider") if latest else None) or history.get("provider") or instrument.get("provider")
    row_mode = (latest.get("data_mode") if latest else None) or history.get("data_mode") or instrument.get("data_mode")
    if not row_mode:
        row_mode = "demo" if _is_demo_provider(provider) else "unknown"
    if row_mode == "live" and _is_demo_provider(provider):
        flags.append("LIVE_WITH_DEMO_PROVIDER")
    if row_mode == "demo" and provider and "twelvedata" in str(provider).lower():
        flags.append("DEMO_WITH_LIVE_PROVIDER")
    return {
        "asset_type": asset_type,
        "symbol": symbol,
        "name": instrument.get("name"),
        "exchange": exchange,
        "display_pair": metadata.get("display_pair"),
        "display_unit": metadata.get("display_unit"),
        "value_label": metadata.get("value_label"),
        "latest_value": latest_value,
        "latest_date": latest_date,
        "history_start": history.get("history_start"),
        "history_end": history.get("history_end"),
        "historical_row_count": history.get("historical_row_count"),
        "change_30d_pct": change_30d,
        "change_90d_pct": change_90d,
        "change_1y_pct": change_1y,
        "stored_change_30d_pct": (analysis.get("change_30d") * 100.0) if analysis and analysis.get("change_30d") is not None else None,
        "stored_change_90d_pct": (analysis.get("change_90d") * 100.0) if analysis and analysis.get("change_90d") is not None else None,
        "provider": provider,
        "data_mode": row_mode,
        "is_demo": row_mode == "demo" or _is_demo_provider(provider),
        "is_live": row_mode == "live",
        "flags": sorted(set(flags)),
    }


def _preferred_mode(conn: sqlite3.Connection, table: str, where: str, params: list[Any]) -> str | None:
    rows = conn.execute(f"SELECT DISTINCT COALESCE(data_mode, 'unknown') FROM {table} WHERE {where}", params).fetchall()
    modes = {str(row[0] or "unknown").lower() for row in rows}
    if "live" in modes:
        return "live"
    if "demo" in modes:
        return "demo"
    return None


def _history_modes(conn: sqlite3.Connection, asset_type: str, symbol: str, exchange: str | None) -> set[str]:
    if asset_type == "FX":
        rows = conn.execute("SELECT DISTINCT COALESCE(data_mode, 'unknown') FROM fx_rates WHERE base=? AND symbol=?", (exchange or "USD", symbol)).fetchall()
    elif asset_type == "CRYPTO":
        rows = conn.execute("SELECT DISTINCT COALESCE(data_mode, 'unknown') FROM crypto_prices_daily WHERE symbol=?", (symbol,)).fetchall()
    elif asset_type == "MACRO":
        rows = conn.execute("SELECT DISTINCT COALESCE(data_mode, 'unknown') FROM macro_indicators_daily WHERE indicator_code=?", (symbol,)).fetchall()
    else:
        rows = conn.execute("SELECT DISTINCT COALESCE(data_mode, 'unknown') FROM stock_prices_daily WHERE symbol=?", (symbol,)).fetchall()
    return {str(row[0] or "unknown").lower() for row in rows}


def _quote_modes(conn: sqlite3.Connection, asset_type: str, symbol: str, exchange: str | None) -> set[str]:
    params: list[Any] = [asset_type, symbol]
    clause = "asset_type=? AND symbol=?"
    if asset_type == "FX" and exchange:
        clause += " AND exchange=?"
        params.append(exchange)
    rows = conn.execute(f"SELECT DISTINCT COALESCE(data_mode, 'unknown') FROM market_quotes_latest WHERE {clause}", params).fetchall()
    return {str(row[0] or "unknown").lower() for row in rows}


def _has_future_date(raw_date: str | None) -> bool:
    if not raw_date:
        return False
    try:
        return date.fromisoformat(str(raw_date)[:10]) > date.today() + timedelta(days=1)
    except ValueError:
        return False


def _preferred_mode_for_asset(conn: sqlite3.Connection, asset_type: str, symbol: str, exchange: str | None) -> str | None:
    if asset_type == "FX":
        return _preferred_mode(conn, "fx_rates", "symbol=? AND base=?", [symbol, exchange or "USD"])
    if asset_type == "CRYPTO":
        return _preferred_mode(conn, "crypto_prices_daily", "symbol=?", [symbol])
    if asset_type == "MACRO":
        return _preferred_mode(conn, "macro_indicators_daily", "indicator_code=?", [symbol])
    return _preferred_mode(conn, "stock_prices_daily", "symbol=?", [symbol])


def _analysis_modes(conn: sqlite3.Connection, asset_type: str, symbol: str) -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT COALESCE(data_mode, 'unknown') FROM analysis_snapshots WHERE asset_type=? AND symbol=?",
        (asset_type, symbol),
    ).fetchall()
    return {str(row[0] or "unknown").lower() for row in rows}


def _latest_quote(conn: sqlite3.Connection, asset_type: str, symbol: str, exchange: str | None, preferred_mode: str | None = None) -> dict[str, Any] | None:
    params: list[Any] = [asset_type, symbol]
    clause = "asset_type=? AND symbol=?"
    if asset_type == "FX" and exchange:
        clause += " AND exchange=?"
        params.append(exchange)
    if preferred_mode:
        clause += " AND data_mode=?"
        params.append(preferred_mode)
    row = conn.execute(f"""
        SELECT price, quote_time, provider, fetched_at, percent_change, data_mode, source_updated_at
        FROM market_quotes_latest
        WHERE {clause}
        ORDER BY CASE data_mode WHEN 'live' THEN 0 WHEN 'demo' THEN 1 ELSE 2 END, fetched_at DESC, quote_time DESC
        LIMIT 1
    """, params).fetchone()
    return dict(row) if row else None


def _latest_analysis(conn: sqlite3.Connection, asset_type: str, symbol: str, preferred_mode: str | None = None) -> dict[str, Any] | None:
    params: list[Any] = [asset_type, symbol]
    mode_clause = ""
    if preferred_mode:
        mode_clause = " AND data_mode=?"
        params.append(preferred_mode)
    row = conn.execute(f"""
        SELECT change_30d, change_90d, change_1y, generated_at, trend, signal, data_mode
        FROM analysis_snapshots
        WHERE asset_type=? AND symbol=?{mode_clause}
        ORDER BY generated_at DESC, snapshot_id DESC
        LIMIT 1
    """, params).fetchone()
    return dict(row) if row else None


def _history(conn: sqlite3.Connection, asset_type: str, symbol: str, exchange: str | None) -> dict[str, Any]:
    if asset_type == "FX":
        preferred = _preferred_mode(conn, "fx_rates", "symbol=? AND base=?", [symbol, exchange or "USD"])
        mode_clause = " AND data_mode=?" if preferred else ""
        params = [exchange or "USD", symbol] + ([preferred] if preferred else [])
        rows = conn.execute(f"SELECT date, rate AS value, source AS provider, NULL AS unit, data_mode FROM fx_rates WHERE base=? AND symbol=? AND rate IS NOT NULL{mode_clause} ORDER BY date", params).fetchall()
    elif asset_type == "CRYPTO":
        preferred = _preferred_mode(conn, "crypto_prices_daily", "symbol=?", [symbol])
        mode_clause = " AND data_mode=?" if preferred else ""
        rows = conn.execute(f"SELECT date, price_usd AS value, provider, NULL AS unit, data_mode FROM crypto_prices_daily WHERE symbol=? AND price_usd IS NOT NULL{mode_clause} ORDER BY date", [symbol] + ([preferred] if preferred else [])).fetchall()
    elif asset_type == "MACRO":
        preferred = _preferred_mode(conn, "macro_indicators_daily", "indicator_code=?", [symbol])
        mode_clause = " AND data_mode=?" if preferred else ""
        rows = conn.execute(f"SELECT date, value, source AS provider, unit, data_mode FROM macro_indicators_daily WHERE indicator_code=? AND value IS NOT NULL{mode_clause} ORDER BY date", [symbol] + ([preferred] if preferred else [])).fetchall()
    else:
        preferred = _preferred_mode(conn, "stock_prices_daily", "symbol=?", [symbol])
        mode_clause = " AND data_mode=?" if preferred else ""
        rows = conn.execute(f"SELECT date, close AS value, provider, currency AS unit, data_mode FROM stock_prices_daily WHERE symbol=? AND close IS NOT NULL{mode_clause} ORDER BY date", [symbol] + ([preferred] if preferred else [])).fetchall()
    points = [(str(row["date"]), float(row["value"])) for row in rows if row["value"] is not None]
    return {
        "points": points,
        "history_start": points[0][0] if points else None,
        "history_end": points[-1][0] if points else None,
        "historical_row_count": len(points),
        "latest_value": points[-1][1] if points else None,
        "provider": rows[-1]["provider"] if rows else None,
        "data_mode": rows[-1]["data_mode"] if rows and "data_mode" in rows[-1].keys() else None,
        "unit": rows[-1]["unit"] if rows and "unit" in rows[-1].keys() else None,
    }


def _period_change(points: list[tuple[str, float]], days: int) -> float | None:
    if len(points) < 2:
        return None
    end_date = date.fromisoformat(points[-1][0])
    cutoff = end_date - timedelta(days=days)
    candidates = [(d, v) for d, v in points if date.fromisoformat(d) >= cutoff and v not in (None, 0)]
    if len(candidates) < 2:
        candidates = [(d, v) for d, v in points if v not in (None, 0)]
    if len(candidates) < 2 or candidates[0][1] == 0:
        return None
    return ((candidates[-1][1] / candidates[0][1]) - 1.0) * 100.0


def _range_flags(asset_type: str, symbol: str, exchange: str | None, value: float | None, unit: str | None) -> list[str]:
    if value is None:
        return []
    flags = []
    if asset_type == "FX":
        low, high = FX_RANGES.get((exchange or "USD", symbol), (0.0001, 100000.0))
        if not (low <= float(value) <= high):
            flags.append("FX_SUSPICIOUS_RANGE")
    elif asset_type == "STOCK" and symbol in STOCK_RANGES:
        if unit and unit.upper() != "USD":
            flags.append("STOCK_UNIT_SUSPICIOUS")
        low, high = STOCK_RANGES[symbol]
        if not (low <= float(value) <= high):
            flags.append("STOCK_SUSPICIOUS_RANGE")
    elif asset_type == "STOCK" and unit and unit.upper() != "USD":
        flags.append("STOCK_UNIT_SUSPICIOUS")
    elif asset_type == "CRYPTO" and symbol in CRYPTO_RANGES:
        if unit and unit.upper() != "USD":
            flags.append("CRYPTO_UNIT_SUSPICIOUS")
        low, high = CRYPTO_RANGES[symbol]
        if not (low <= float(value) <= high):
            flags.append("CRYPTO_SUSPICIOUS_RANGE")
    elif asset_type == "CRYPTO" and unit and unit.upper() != "USD":
        flags.append("CRYPTO_UNIT_SUSPICIOUS")
    elif asset_type == "MACRO":
        rule = MACRO_RANGES.get(symbol)
        if not unit or unit == "index" and "CPI" not in symbol:
            flags.append("MACRO_UNIT_SUSPICIOUS")
        if rule:
            low, high, expected_unit = rule
            if unit != expected_unit or not (low <= float(value) <= high):
                flags.append("MACRO_UNIT_SUSPICIOUS")
    return flags


def _source_unit_flags(asset_type: str, unit: str | None) -> list[str]:
    normalized = str(unit or "").strip().upper()
    if asset_type == "STOCK" and normalized and normalized != "USD":
        return ["STOCK_UNIT_SUSPICIOUS"]
    if asset_type == "CRYPTO" and normalized and normalized != "USD":
        return ["CRYPTO_UNIT_SUSPICIOUS"]
    return []


def _differs(calculated_pct: float | None, stored_ratio: float | None) -> bool:
    if calculated_pct is None or stored_ratio is None:
        return False
    return abs(calculated_pct - (float(stored_ratio) * 100.0)) > 0.75


def _is_stale(asset_type: str, symbol: str, raw_date: str | None) -> bool:
    if not raw_date:
        return True
    try:
        parsed = date.fromisoformat(raw_date[:10])
    except ValueError:
        return False
    return (date.today() - parsed).days > _allowed_stale_days(asset_type, symbol)


def _allowed_stale_days(asset_type: str, symbol: str) -> int:
    if asset_type == "MACRO" and _is_monthly_macro(symbol):
        return MACRO_MONTHLY_STALE_DAYS
    return QUOTE_STALE_DAYS


def _is_monthly_macro(symbol: str) -> bool:
    return symbol.upper().endswith("_MONTHLY") or symbol.upper() in {"IPCA_MONTHLY", "US_CPI_MONTHLY"}


def _is_demo_provider(provider: str | None) -> bool:
    return bool(provider and ("mock" in provider.lower() or "demo" in provider.lower()))


def _totals(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute("SELECT asset_type, COUNT(*) FROM instruments WHERE is_active=1 GROUP BY asset_type").fetchall()
    return {str(row[0]): int(row[1]) for row in rows}


def _overall_range(conn: sqlite3.Connection) -> dict[str, Any]:
    ranges = []
    for table in ("stock_prices_daily", "fx_rates", "crypto_prices_daily", "macro_indicators_daily"):
        row = conn.execute(f"SELECT MIN(date), MAX(date), COUNT(*) FROM {table}").fetchone()
        ranges.append(row)
    starts = [row[0] for row in ranges if row[0]]
    ends = [row[1] for row in ranges if row[1]]
    return {"date_min": min(starts) if starts else None, "date_max": max(ends) if ends else None, "historical_rows": sum(int(row[2] or 0) for row in ranges)}


def _summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    flag_counts: dict[str, int] = {}
    for item in items:
        for flag in item["flags"]:
            flag_counts[flag] = flag_counts.get(flag, 0) + 1
    return {
        "total_instruments": len(items),
        "without_quote": flag_counts.get("NO_LATEST_QUOTE", 0),
        "without_history": flag_counts.get("NO_HISTORY", 0),
        "stale": flag_counts.get("STALE_DATA", 0),
        "demo": sum(1 for item in items if item.get("is_demo")),
        "live": sum(1 for item in items if item.get("is_live")),
        "unknown": sum(1 for item in items if item.get("data_mode") == "unknown"),
        "demo_symbols": [item["symbol"] for item in items if item.get("is_demo")],
        "live_symbols": [item["symbol"] for item in items if item.get("is_live")],
        "unknown_symbols": [item["symbol"] for item in items if item.get("data_mode") == "unknown"],
        "flag_counts": flag_counts,
    }


def _data_mode_breakdown(items: list[dict[str, Any]]) -> dict[str, Any]:
    by_mode: dict[str, int] = {}
    by_asset_type: dict[str, dict[str, int]] = {}
    providers_by_asset_type: dict[str, set[str]] = {}
    for item in items:
        mode = str(item.get("data_mode") or "unknown")
        asset_type = str(item.get("asset_type") or "UNKNOWN")
        by_mode[mode] = by_mode.get(mode, 0) + 1
        by_asset_type.setdefault(asset_type, {})
        by_asset_type[asset_type][mode] = by_asset_type[asset_type].get(mode, 0) + 1
        if item.get("provider"):
            providers_by_asset_type.setdefault(asset_type, set()).add(str(item["provider"]))
    return {
        "by_mode": by_mode,
        "by_asset_type": by_asset_type,
        "providers_by_asset_type": {asset_type: sorted(values) for asset_type, values in sorted(providers_by_asset_type.items())},
        "demo_symbols": [item["symbol"] for item in items if item.get("is_demo")],
        "live_symbols": [item["symbol"] for item in items if item.get("is_live")],
        "unknown_symbols": [item["symbol"] for item in items if item.get("data_mode") == "unknown"],
    }


def _ranking_flags(db_path: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    ranking = get_performance_ranking(db_path, days=30, period="30D", asset_type=None, limit=10)
    bottom = ranking.get("bottom") or []
    flags = []
    if bottom and bottom[0].get("period_change") is not None and bottom[0]["period_change"] > 10:
        flags.append("TOP_WORST_RANKING_BUG")
    if flags:
        for item in items:
            item["flags"].extend(flags)
    return {"status": "FAIL" if flags else "OK", "flags": flags, "bottom_label": ranking.get("bottom_label")}


def _live_validation(items: list[dict[str, Any]]) -> dict[str, Any]:
    samples = []
    live_fx = _fetch_frankfurter()
    live_crypto = _fetch_coingecko()
    for asset_type, symbol in SAMPLE_SYMBOLS:
        item = next((row for row in items if row["asset_type"] == asset_type and row["symbol"] == symbol), None)
        if item is None:
            samples.append({"symbol": symbol, "asset_type": asset_type, "status": "SKIPPED", "notes": "not active in DB"})
            continue
        external = None
        source = None
        tolerance = None
        if asset_type == "FX" and live_fx.get(symbol) is not None:
            external = live_fx[symbol]
            source = "Frankfurter/ECB"
            tolerance = 1.5
        elif asset_type == "CRYPTO" and live_crypto.get(symbol) is not None:
            external = live_crypto[symbol]
            source = "CoinGecko"
            tolerance = 3.0
        status = "SKIPPED"
        notes = "LIVE_VALIDATION_SKIPPED: no internet/provider/API key or unsupported sample"
        diff_pct = None
        if external is not None and item.get("latest_value") not in (None, 0):
            diff_pct = abs((float(item["latest_value"]) / float(external)) - 1.0) * 100.0
            status = "OK" if diff_pct <= float(tolerance) else "WARN"
            notes = f"source={source}; tolerance={tolerance}%"
        samples.append({"asset_type": asset_type, "symbol": symbol, "app_value": item.get("latest_value"), "external_value": external, "diff_pct": diff_pct, "status": status, "notes": notes})
    statuses = {sample.get("status") for sample in samples}
    if statuses == {"SKIPPED"}:
        overall = "SKIPPED"
    elif "WARN" in statuses:
        overall = "WARN"
    else:
        overall = "OK"
    return {"status": overall, "samples": samples}


def _fetch_json(url: str) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=8) as response:
            return json.loads(response.read().decode("utf-8"))
    except (OSError, urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return {}


def _fetch_frankfurter() -> dict[str, float]:
    payload = _fetch_json("https://api.frankfurter.app/latest?from=USD&to=EUR,BRL")
    rates = payload.get("rates") if isinstance(payload.get("rates"), dict) else {}
    return {key: float(value) for key, value in rates.items() if isinstance(value, (int, float))}


def _fetch_coingecko() -> dict[str, float]:
    ids = "bitcoin,ethereum,binancecoin,solana,ripple,cardano,dogecoin,chainlink,avalanche-2,polkadot"
    payload = _fetch_json(f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd")
    mapping = {"bitcoin": "BTC", "ethereum": "ETH", "binancecoin": "BNB", "solana": "SOL", "ripple": "XRP", "cardano": "ADA", "dogecoin": "DOGE", "chainlink": "LINK", "avalanche-2": "AVAX", "polkadot": "DOT"}
    result = {}
    for provider_id, symbol in mapping.items():
        value = payload.get(provider_id, {}).get("usd") if isinstance(payload.get(provider_id), dict) else None
        if isinstance(value, (int, float)):
            result[symbol] = float(value)
    return result


def format_market_audit(audit: dict[str, Any]) -> str:
    if not audit.get("db_exists"):
        return f"DB: {audit.get('db_path')}\nExists: no\nAlerts: DB_NOT_FOUND"
    lines = [
        "MARKET DATA AUDIT",
        f"DB: {audit['db_path']}",
        f"Data mode: {audit['data_mode']['data_mode'].upper()} providers={', '.join(audit['data_mode'].get('providers', [])) or '-'}",
        f"Data mode counts: {audit.get('data_mode_breakdown', {}).get('by_mode', {})}",
        f"Historical range: {audit['date_range']['date_min']} to {audit['date_range']['date_max']} ({audit['date_range']['historical_rows']} rows)",
        f"Data health: {audit.get('data_health', {}).get('status', 'UNKNOWN')} missing_important={audit.get('data_health', {}).get('missing_important_symbols', [])}",
        f"Total instruments: {audit['summary']['total_instruments']}",
        "Flags: " + (", ".join(f"{k}={v}" for k, v in sorted(audit['summary']['flag_counts'].items())) or "none"),
        "",
        "asset symbol display latest latest_date rows chg30 chg90 chg1y provider flags",
    ]
    for item in audit["items"]:
        lines.append(
            "{asset_type} {symbol} {display_pair} {latest_value} {latest_date} {historical_row_count} {change_30d_pct} {change_90d_pct} {change_1y_pct} {provider} {flags}".format(
                **{**item, "flags": ",".join(item["flags"]) or "OK"}
            )
        )
    health = audit.get("data_health", {})
    if health.get("status") in {"WARN", "FAIL"} and health.get("repair_command"):
        lines.append("")
        lines.append(f"Repair action: Run: {health['repair_command']}")
    live = audit.get("live_validation", {})
    lines.append("")
    lines.append(f"Live validation: {live.get('status')}")
    for sample in live.get("samples", [])[:20]:
        lines.append(f"  {sample['asset_type']} {sample['symbol']}: app={sample.get('app_value')} external={sample.get('external_value')} diff={sample.get('diff_pct')} status={sample.get('status')} notes={sample.get('notes')}")
    return "\n".join(lines)
