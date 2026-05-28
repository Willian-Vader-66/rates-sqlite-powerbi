from __future__ import annotations

import sqlite3
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from .db_sqlite import PREPARE_COMMAND, get_system_status

IMPORTANT_SERIES = [
    ("FX", "USD/BRL"),
    ("FX", "USD/EUR"),
    ("CRYPTO", "BTC"),
    ("CRYPTO", "ETH"),
    ("MACRO", "SELIC_MONTHLY"),
    ("MACRO", "SELIC_ANNUALIZED_MONTHLY"),
    ("STOCK", "AAPL"),
    ("STOCK", "MSFT"),
    ("STOCK", "NVDA"),
    ("STOCK", "GOOGL"),
    ("STOCK", "AMZN"),
]

QUOTE_HISTORY_WARN_PCT = float(os.getenv("LIVE_QUOTE_WARN_PCT", "1.0"))
QUOTE_HISTORY_FAIL_PCT = float(os.getenv("LIVE_QUOTE_FAIL_PCT", "5.0"))
QUOTE_STALE_DAYS = int(os.getenv("LIVE_QUOTE_STALE_DAYS", "10"))


def run_dashboard_audit(db_path: str, expected_years: int = 1) -> int:
    audit = audit_dashboard(db_path, expected_years=expected_years)
    print(format_dashboard_audit(audit))
    return 1 if audit["alerts"] else 0


def audit_dashboard(db_path: str, expected_years: int = 1) -> dict[str, Any]:
    path = Path(db_path).expanduser().resolve()
    if not path.exists():
        return {
            "db_path": str(path),
            "db_exists": False,
            "db_size_bytes": 0,
            "historical_row_count": 0,
            "date_min": None,
            "date_max": None,
            "is_empty": True,
            "recommended_prepare_command": PREPARE_COMMAND,
            "alerts": [f"SQLite database not found: {db_path}"],
        }

    status = get_system_status(str(path))
    with sqlite3.connect(str(path)) as conn:
        conn.row_factory = sqlite3.Row
        instruments_by_type = _count_by_type(conn, "instruments", "asset_type", "WHERE is_active=1")
        quotes_by_type = _count_by_type(conn, "market_quotes_latest", "asset_type")
        analysis_by_type = _count_by_type(conn, "analysis_snapshots", "asset_type")
        ranges_by_type = {
            "STOCK": _range(conn, "stock_prices_daily", "date"),
            "FX": _range(conn, "fx_rates", "date"),
            "CRYPTO": _range(conn, "crypto_prices_daily", "date"),
            "MACRO": _range(conn, "macro_indicators_daily", "date"),
        }
        important = {label: _important_range(conn, asset_type, label) for asset_type, label in IMPORTANT_SERIES}
        missing_quotes = _missing(conn, "market_quotes_latest")
        missing_analysis = _missing(conn, "analysis_snapshots")
        duplicate_instruments = _duplicates(conn, "instruments", "asset_type, symbol")
        duplicate_quotes = _duplicates(conn, "market_quotes_latest", "asset_type, symbol")
        quote_consistency = [_quote_consistency(conn, asset_type, label) for asset_type, label in IMPORTANT_SERIES]
        suspicious_values = _suspicious_values(conn, expected_years=expected_years)
        conflicting_data_modes = _conflicting_data_modes(conn)

    total_instruments = sum(instruments_by_type.values())
    alerts = _alerts(
        expected_years=expected_years,
        ranges_by_type=ranges_by_type,
        important=important,
        missing_quotes=missing_quotes,
        missing_analysis=missing_analysis,
        duplicate_instruments=duplicate_instruments,
        duplicate_quotes=duplicate_quotes,
        quote_consistency=quote_consistency,
        suspicious_values=suspicious_values,
        conflicting_data_modes=conflicting_data_modes,
    )
    if status["is_empty"]:
        alerts.append("SQLite database is empty")
    return {
        "db_path": str(path),
        "db_exists": True,
        "db_size_bytes": status["db_size_bytes"],
        "historical_row_count": status["historical_row_count"],
        "date_min": status["date_min"],
        "date_max": status["date_max"],
        "is_empty": status["is_empty"],
        "recommended_prepare_command": status["recommended_prepare_command"],
        "total_instruments": total_instruments,
        "instruments_by_type": instruments_by_type,
        "quotes_by_type": quotes_by_type,
        "analysis_by_type": analysis_by_type,
        "ranges_by_type": ranges_by_type,
        "important_ranges": important,
        "missing_quotes": missing_quotes,
        "missing_analysis": missing_analysis,
        "duplicate_instruments": duplicate_instruments,
        "duplicate_quotes": duplicate_quotes,
        "quote_consistency": quote_consistency,
        "suspicious_values": suspicious_values,
        "conflicting_data_modes": conflicting_data_modes,
        "alerts": alerts,
    }


def format_dashboard_audit(audit: dict[str, Any]) -> str:
    if not audit.get("db_exists"):
        return "\n".join(
            [
                f"DB: {audit['db_path']}",
                "Exists: no",
                f"Run: {audit['recommended_prepare_command']}",
                *[f"ALERT: {item}" for item in audit["alerts"]],
            ]
        )

    lines = [
        f"DB: {audit['db_path']}",
        "Exists: yes",
        f"DB size: {_format_bytes(audit['db_size_bytes'])}",
        f"Total instruments: {audit['total_instruments']}",
        f"Historical rows: {audit['historical_row_count']}",
        f"Overall date range: {audit.get('date_min') or '-'} to {audit.get('date_max') or '-'}",
        "Instruments by asset_type: " + _format_counts(audit["instruments_by_type"]),
        "Latest quotes by asset_type: " + _format_counts(audit["quotes_by_type"]),
        "Analysis snapshots by asset_type: " + _format_counts(audit["analysis_by_type"]),
        "Historical ranges by asset_type:",
    ]
    for asset_type, coverage in audit["ranges_by_type"].items():
        lines.append(f"  {asset_type}: {_format_range(coverage)}")
    lines.append("Important series:")
    for label, coverage in audit["important_ranges"].items():
        lines.append(f"  {label}: {_format_range(coverage)}")
    lines.append("Quote consistency examples:")
    lines.append("  symbol asset_type latest_quote last_history ratio rows date_min date_max quote_time status flags")
    for item in audit["quote_consistency"]:
        lines.append(
            "  {symbol} {asset_type} {latest_quote} {last_history} {ratio} {rows} {date_min} {date_max} {quote_time} {status} {flags}".format(
                symbol=item["symbol"],
                asset_type=item["asset_type"],
                latest_quote=_format_number(item["latest_quote"]),
                last_history=_format_number(item["last_history"]),
                ratio=_format_number(item["ratio"]),
                rows=item["historical_rows"],
                date_min=item["date_min"] or "-",
                date_max=item["date_max"] or "-",
                quote_time=item.get("quote_time") or "-",
                status=item["status"],
                flags=",".join(item.get("flags") or []) or "-",
            )
        )
    lines.append(f"Instruments without quote: {len(audit['missing_quotes'])}")
    lines.append(f"Instruments without analysis: {len(audit['missing_analysis'])}")
    lines.append(f"Duplicate instruments: {len(audit['duplicate_instruments'])}")
    lines.append(f"Duplicate quotes: {len(audit['duplicate_quotes'])}")
    lines.append(f"Suspicious values: {len(audit['suspicious_values'])}")
    lines.append(f"Conflicting data modes: {len(audit.get('conflicting_data_modes', []))}")
    for item in audit["suspicious_values"][:12]:
        lines.append(f"  WARN: {item}")
    if audit["alerts"]:
        lines.append("Alerts:")
        lines.extend(f"  ALERT: {alert}" for alert in audit["alerts"])
        if audit.get("is_empty"):
            lines.append(f"  Run: {audit['recommended_prepare_command']}")
    else:
        lines.append("Alerts: none")
    lines.append("API path check: compare this DB with GET /api/system/status if the running dashboard is empty.")
    return "\n".join(lines)


def _count_by_type(conn: sqlite3.Connection, table: str, column: str, where: str = "") -> dict[str, int]:
    rows = conn.execute(
        f"""
        SELECT {column}, COUNT(*) AS count
        FROM (
            SELECT {column}, symbol
            FROM {table}
            {where}
            GROUP BY {column}, symbol
        )
        GROUP BY {column}
        """
    ).fetchall()
    return {str(row[0]): int(row[1]) for row in rows}


def _range(conn: sqlite3.Connection, table: str, column: str, where: str = "") -> dict[str, Any]:
    row = conn.execute(f"SELECT MIN({column}), MAX({column}), COUNT(*) FROM {table} {where}").fetchone()
    return {"start": row[0], "end": row[1], "count": int(row[2] or 0)}


def _important_range(conn: sqlite3.Connection, asset_type: str, label: str) -> dict[str, Any]:
    if asset_type == "FX":
        base, symbol = label.split("/")
        mode = _preferred_mode(conn, "fx_rates", "base=? AND symbol=?", [base, symbol])
        return _range_params(conn, "fx_rates", "date", "WHERE base=? AND symbol=?" + (" AND data_mode=?" if mode else ""), [base, symbol] + ([mode] if mode else []))
    if asset_type == "CRYPTO":
        mode = _preferred_mode(conn, "crypto_prices_daily", "symbol=?", [label])
        return _range_params(conn, "crypto_prices_daily", "date", "WHERE symbol=?" + (" AND data_mode=?" if mode else ""), [label] + ([mode] if mode else []))
    if asset_type == "MACRO":
        mode = _preferred_mode(conn, "macro_indicators_daily", "indicator_code=?", [label])
        return _range_params(conn, "macro_indicators_daily", "date", "WHERE indicator_code=?" + (" AND data_mode=?" if mode else ""), [label] + ([mode] if mode else []))
    mode = _preferred_mode(conn, "stock_prices_daily", "symbol=?", [label])
    return _range_params(conn, "stock_prices_daily", "date", "WHERE symbol=?" + (" AND data_mode=?" if mode else ""), [label] + ([mode] if mode else []))


def _quote_consistency(conn: sqlite3.Connection, asset_type: str, label: str) -> dict[str, Any]:
    symbol = label
    quote_exchange: str | None = None
    mode: str | None
    if asset_type == "FX":
        base, symbol = label.split("/")
        quote_exchange = base
        mode = _preferred_mode(conn, "fx_rates", "base=? AND symbol=?", [base, symbol])
        history = _last_history_value(
            conn,
            "fx_rates",
            "rate",
            "WHERE base=? AND symbol=?" + (" AND data_mode=?" if mode else ""),
            [base, symbol] + ([mode] if mode else []),
        )
    elif asset_type == "CRYPTO":
        mode = _preferred_mode(conn, "crypto_prices_daily", "symbol=?", [symbol])
        history = _last_history_value(
            conn,
            "crypto_prices_daily",
            "price_usd",
            "WHERE symbol=?" + (" AND data_mode=?" if mode else ""),
            [symbol] + ([mode] if mode else []),
        )
    elif asset_type == "MACRO":
        mode = _preferred_mode(conn, "macro_indicators_daily", "indicator_code=?", [symbol])
        history = _last_history_value(
            conn,
            "macro_indicators_daily",
            "value",
            "WHERE indicator_code=?" + (" AND data_mode=?" if mode else ""),
            [symbol] + ([mode] if mode else []),
        )
    else:
        mode = _preferred_mode(conn, "stock_prices_daily", "symbol=?", [symbol])
        history = _last_history_value(
            conn,
            "stock_prices_daily",
            "close",
            "WHERE symbol=?" + (" AND data_mode=?" if mode else ""),
            [symbol] + ([mode] if mode else []),
        )

    quote = _latest_quote_value(conn, asset_type, symbol, quote_exchange, mode)
    latest_quote = quote["price"]
    last_history = history["value"]
    ratio = latest_quote / last_history if latest_quote is not None and last_history not in {None, 0} else None
    diff_pct = abs(ratio - 1.0) * 100.0 if ratio is not None else None
    flags: list[str] = []
    status = "OK"
    if latest_quote is None or last_history is None:
        status = "WARN"
        flags.append("MISSING_QUOTE_OR_HISTORY")
    elif diff_pct is None:
        status = "FAIL"
        flags.append("QUOTE_HISTORY_RATIO_INVALID")
    elif diff_pct > QUOTE_HISTORY_FAIL_PCT:
        status = "FAIL"
        flags.append("QUOTE_HISTORY_DIVERGENCE_FAIL")
    elif diff_pct > QUOTE_HISTORY_WARN_PCT:
        status = "WARN"
        flags.append("QUOTE_HISTORY_DIVERGENCE_WARN")
    if quote.get("quote_time") and history.get("date_max") and str(quote["quote_time"])[:10] < str(history["date_max"])[:10]:
        status = "FAIL"
        flags.append("QUOTE_OLDER_THAN_HISTORY")
    if _is_future_date(history.get("date_max")) or _is_future_date(quote.get("quote_time")):
        status = "FAIL"
        flags.append("FUTURE_HISTORY_OR_QUOTE_DATE")
    if _is_stale_date(quote.get("quote_time") or history.get("date_max")) and status != "FAIL":
        status = "WARN"
        flags.append("STALE_LATEST_QUOTE")
    return {
        "label": label,
        "symbol": symbol,
        "asset_type": asset_type,
        "latest_quote": latest_quote,
        "last_history": last_history,
        "ratio": ratio,
        "diff_pct": diff_pct,
        "historical_rows": history["count"],
        "date_min": history["date_min"],
        "date_max": history["date_max"],
        "quote_time": quote.get("quote_time"),
        "data_mode": mode,
        "flags": sorted(set(flags)),
        "status": status,
    }


def _last_history_value(
    conn: sqlite3.Connection,
    table: str,
    value_column: str,
    where: str,
    params: list[str],
) -> dict[str, Any]:
    row = conn.execute(
        f"""
        SELECT {value_column}, date,
               (SELECT MIN(date) FROM {table} {where}) AS date_min,
               (SELECT COUNT(*) FROM {table} {where}) AS row_count
        FROM {table}
        {where}
        ORDER BY date DESC
        LIMIT 1
        """,
        params + params + params,
    ).fetchone()
    if row is None:
        return {"value": None, "date_min": None, "date_max": None, "count": 0}
    return {"value": row[0], "date_min": row[2], "date_max": row[1], "count": int(row[3] or 0)}


def _latest_quote_value(
    conn: sqlite3.Connection,
    asset_type: str,
    symbol: str,
    exchange: str | None,
    preferred_mode: str | None = None,
) -> dict[str, Any]:
    params: list[Any] = [asset_type, symbol]
    exchange_clause = ""
    if exchange is not None:
        exchange_clause = "AND exchange = ?"
        params.append(exchange)
    mode_clause = ""
    if preferred_mode:
        mode_clause = "AND data_mode = ?"
        params.append(preferred_mode)
    row = conn.execute(
        f"""
        SELECT price, bid, ask, quote_time, data_mode, provider
        FROM market_quotes_latest
        WHERE asset_type=? AND symbol=? {exchange_clause} {mode_clause}
        ORDER BY CASE data_mode WHEN 'live' THEN 0 WHEN 'demo' THEN 1 ELSE 2 END, fetched_at DESC, quote_time DESC
        LIMIT 1
        """,
        params,
    ).fetchone()
    if row is None:
        return {"price": None, "bid": None, "ask": None, "quote_time": None, "data_mode": None, "provider": None}
    return {"price": row[0], "bid": row[1], "ask": row[2], "quote_time": row[3], "data_mode": row[4], "provider": row[5]}


def _is_future_date(raw_date: str | None) -> bool:
    if not raw_date:
        return False
    try:
        return date.fromisoformat(str(raw_date)[:10]) > date.today() + timedelta(days=1)
    except ValueError:
        return False


def _is_stale_date(raw_date: str | None) -> bool:
    if not raw_date:
        return False
    try:
        return (date.today() - date.fromisoformat(str(raw_date)[:10])).days > QUOTE_STALE_DAYS
    except ValueError:
        return True


def _suspicious_values(conn: sqlite3.Connection, *, expected_years: int) -> list[str]:
    checks: list[str] = []
    checks.extend(
        _format_rows(
            conn,
            """
            SELECT symbol, price
            FROM market_quotes_latest
            WHERE asset_type='STOCK' AND (price > 10000 OR price <= 0)
            ORDER BY price DESC
            """,
            "STOCK latest quote suspicious",
        )
    )
    checks.extend(
        _format_rows(
            conn,
            """
            SELECT symbol, close AS price
            FROM stock_prices_daily
            WHERE close > 10000 OR close <= 0
            GROUP BY symbol
            ORDER BY close DESC
            """,
            "STOCK history suspicious",
        )
    )
    checks.extend(
        _format_rows(
            conn,
            """
            SELECT symbol, price_usd AS price
            FROM crypto_prices_daily
            WHERE price_usd <= 0
            GROUP BY symbol
            """,
            "CRYPTO history non-positive",
        )
    )
    checks.extend(
        _format_rows(
            conn,
            """
            SELECT base || '/' || symbol AS symbol, rate AS price
            FROM fx_rates
            WHERE rate <= 0
            GROUP BY base, symbol
            """,
            "FX history non-positive",
        )
    )
    checks.extend(
        _format_rows(
            conn,
            """
            SELECT indicator_code AS symbol, value AS price
            FROM macro_indicators_daily
            WHERE unit IS NULL OR unit = ''
            GROUP BY indicator_code
            """,
            "MACRO unit missing",
        )
    )
    years = max(1, expected_years)
    stock_min = max(2, int(180 * years * 0.70))
    crypto_min = max(2, int(330 * years * 0.70))
    fx_min = max(2, int(240 * years * 0.70))
    checks.extend(_short_series(conn, "stock_prices_daily", "symbol", None, stock_min, f"STOCK series under {stock_min} points"))
    checks.extend(_short_series(conn, "crypto_prices_daily", "symbol", None, crypto_min, f"CRYPTO series under {crypto_min} points"))
    checks.extend(_short_series(conn, "fx_rates", "symbol", "base", fx_min, f"FX series under {fx_min} points"))
    return checks


def _format_rows(conn: sqlite3.Connection, sql: str, prefix: str) -> list[str]:
    rows = conn.execute(sql).fetchall()
    return [f"{prefix}: {row[0]}={_format_number(row[1])}" for row in rows]


def _short_series(
    conn: sqlite3.Connection,
    table: str,
    symbol_column: str,
    base_column: str | None,
    min_count: int,
    prefix: str,
) -> list[str]:
    if base_column:
        rows = conn.execute(
            f"""
            SELECT {base_column} || '/' || {symbol_column} AS label, COUNT(*) AS count
            FROM {table}
            GROUP BY {base_column}, {symbol_column}
            HAVING COUNT(*) < ?
            ORDER BY count
            """,
            (min_count,),
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT {symbol_column} AS label, COUNT(*) AS count
            FROM {table}
            GROUP BY {symbol_column}
            HAVING COUNT(*) < ?
            ORDER BY count
            """,
            (min_count,),
        ).fetchall()
    return [f"{prefix}: {row[0]} has {row[1]} points" for row in rows]


def _preferred_mode(conn: sqlite3.Connection, table: str, where: str, params: list[Any]) -> str | None:
    rows = conn.execute(f"SELECT DISTINCT COALESCE(data_mode, 'unknown') FROM {table} WHERE {where}", params).fetchall()
    modes = {str(row[0] or "unknown").lower() for row in rows}
    if "live" in modes:
        return "live"
    if "demo" in modes:
        return "demo"
    return None


def _conflicting_data_modes(conn: sqlite3.Connection) -> list[str]:
    checks = []
    specs = [
        ("stock_prices_daily", "STOCK", "symbol"),
        ("crypto_prices_daily", "CRYPTO", "symbol"),
        ("macro_indicators_daily", "MACRO", "indicator_code"),
        ("fx_rates", "FX", "base || '/' || symbol"),
        ("market_quotes_latest", "QUOTE", "asset_type || ':' || symbol"),
        ("analysis_snapshots", "ANALYSIS", "asset_type || ':' || symbol"),
    ]
    for table, label, expr in specs:
        rows = conn.execute(
            f"""
            SELECT {expr} AS symbol, COUNT(DISTINCT COALESCE(data_mode, 'unknown')) AS modes
            FROM {table}
            GROUP BY {expr}
            HAVING modes > 1
            ORDER BY symbol
            """
        ).fetchall()
        checks.extend(f"{label} conflicting data_mode: {row[0]}" for row in rows)
    return checks


def _range_params(conn: sqlite3.Connection, table: str, column: str, where: str, params: list[str]) -> dict[str, Any]:
    row = conn.execute(f"SELECT MIN({column}), MAX({column}), COUNT(*) FROM {table} {where}", params).fetchone()
    return {"start": row[0], "end": row[1], "count": int(row[2] or 0)}


def _missing(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(
        f"""
        SELECT i.asset_type, i.symbol
        FROM (
            SELECT asset_type, symbol
            FROM instruments
            WHERE is_active=1
            GROUP BY asset_type, symbol
        ) AS i
        LEFT JOIN (
            SELECT asset_type, symbol
            FROM {table}
            GROUP BY asset_type, symbol
        ) AS other
          ON i.asset_type=other.asset_type AND i.symbol=other.symbol
        WHERE other.symbol IS NULL
        ORDER BY i.asset_type, i.symbol
        """
    ).fetchall()
    return [f"{row[0]}:{row[1]}" for row in rows]


def _duplicates(conn: sqlite3.Connection, table: str, columns: str) -> list[str]:
    rows = conn.execute(
        f"""
        SELECT {columns}, COUNT(*) AS count
        FROM {table}
        GROUP BY {columns}
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        """
    ).fetchall()
    return [":".join(str(part) for part in row[:-1]) + f" x{row[-1]}" for row in rows]


def _alerts(
    expected_years: int,
    ranges_by_type: dict[str, dict[str, Any]],
    important: dict[str, dict[str, Any]],
    missing_quotes: list[str],
    missing_analysis: list[str],
    duplicate_instruments: list[str],
    duplicate_quotes: list[str],
    quote_consistency: list[dict[str, Any]],
    suspicious_values: list[str],
    conflicting_data_modes: list[str],
) -> list[str]:
    alerts: list[str] = []
    if missing_quotes:
        alerts.append(f"{len(missing_quotes)} active instruments do not have latest quotes")
    if missing_analysis:
        alerts.append(f"{len(missing_analysis)} active instruments do not have analysis snapshots")
    if duplicate_instruments:
        alerts.append(f"{len(duplicate_instruments)} duplicate instrument keys found")
    if duplicate_quotes:
        alerts.append(f"{len(duplicate_quotes)} duplicate latest quote keys found")
    for label, coverage in {**ranges_by_type, **important}.items():
        if coverage["count"] and _coverage_years(coverage) < expected_years - 0.25:
            alerts.append(f"{label} history is shorter than {expected_years} years: {_format_range(coverage)}")
    failed_consistency = [item for item in quote_consistency if item["status"] == "FAIL"]
    if failed_consistency:
        labels = ", ".join(item["label"] for item in failed_consistency)
        alerts.append(f"quote/history ratio failed for: {labels}")
    if suspicious_values:
        alerts.append(f"{len(suspicious_values)} suspicious data/display values found")
    if conflicting_data_modes:
        alerts.append(f"{len(conflicting_data_modes)} symbols have conflicting demo/live modes")
    return alerts


def _coverage_years(coverage: dict[str, Any]) -> float:
    if not coverage.get("start") or not coverage.get("end"):
        return 0.0
    start = date.fromisoformat(str(coverage["start"]))
    end = date.fromisoformat(str(coverage["end"]))
    return (end - start).days / 365.0


def _format_counts(counts: dict[str, int]) -> str:
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items())) or "-"


def _format_range(coverage: dict[str, Any]) -> str:
    return f"{coverage.get('start') or '-'} to {coverage.get('end') or '-'} ({coverage.get('count', 0)} rows)"


def _format_number(value: float | int | None) -> str:
    if value is None:
        return "-"
    return f"{float(value):.6g}"


def _format_bytes(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes / (1024 * 1024):.1f} MB"
