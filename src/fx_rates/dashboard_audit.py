from __future__ import annotations

import sqlite3
from datetime import date
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


def run_dashboard_audit(db_path: str, expected_years: int = 4) -> int:
    audit = audit_dashboard(db_path, expected_years=expected_years)
    print(format_dashboard_audit(audit))
    return 1 if audit["alerts"] else 0


def audit_dashboard(db_path: str, expected_years: int = 4) -> dict[str, Any]:
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

    total_instruments = sum(instruments_by_type.values())
    alerts = _alerts(
        expected_years=expected_years,
        ranges_by_type=ranges_by_type,
        important=important,
        missing_quotes=missing_quotes,
        missing_analysis=missing_analysis,
        duplicate_instruments=duplicate_instruments,
        duplicate_quotes=duplicate_quotes,
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
    lines.append(f"Instruments without quote: {len(audit['missing_quotes'])}")
    lines.append(f"Instruments without analysis: {len(audit['missing_analysis'])}")
    lines.append(f"Duplicate instruments: {len(audit['duplicate_instruments'])}")
    lines.append(f"Duplicate quotes: {len(audit['duplicate_quotes'])}")
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
        return _range_params(conn, "fx_rates", "date", "WHERE base=? AND symbol=?", [base, symbol])
    if asset_type == "CRYPTO":
        return _range_params(conn, "crypto_prices_daily", "date", "WHERE symbol=?", [label])
    if asset_type == "MACRO":
        return _range_params(conn, "macro_indicators_daily", "date", "WHERE indicator_code=?", [label])
    return _range_params(conn, "stock_prices_daily", "date", "WHERE symbol=?", [label])


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


def _format_bytes(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes / (1024 * 1024):.1f} MB"
