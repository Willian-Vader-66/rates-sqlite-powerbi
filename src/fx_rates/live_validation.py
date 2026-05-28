from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .dashboard_audit import audit_dashboard
from .dashboard_market_audit import audit_market
from .db_sqlite import get_data_health, get_data_mode_summary, get_system_status, initialize_schema
from .live_scope import LiveScopeItem, min_rows_for_days, required_scope_items

REPORT_PATH = Path("docs/LIVE_FULL_TEST_REPORT.md")

CRITICAL_AUDIT_FLAGS = {
    "NO_HISTORY",
    "NO_LATEST_QUOTE",
    "FX_SUSPICIOUS_RANGE",
    "STOCK_SUSPICIOUS_RANGE",
    "STOCK_UNIT_SUSPICIOUS",
    "CRYPTO_SUSPICIOUS_RANGE",
    "CRYPTO_UNIT_SUSPICIOUS",
    "MACRO_UNIT_SUSPICIOUS",
    "QUOTE_HISTORY_DIVERGENCE_FAIL",
    "QUOTE_OLDER_THAN_HISTORY",
    "FUTURE_HISTORY_DATE",
    "LIVE_WITH_DEMO_PROVIDER",
    "DEMO_WITH_LIVE_PROVIDER",
    "CONFLICTING_DATA_MODES",
}

WARN_AUDIT_FLAGS = {
    "STALE_DATA",
    "QUOTE_HISTORY_DIVERGENCE_WARN",
    "NO_ANALYSIS",
    "INCONSISTENT_30D_CHANGE",
    "INCONSISTENT_90D_CHANGE",
    "MIXED_DATASET_REVIEW_REQUIRED",
}

MIN_ROWS_PER_YEAR = {
    "STOCK": 180,
    "FX": 240,
    "CRYPTO": 330,
    "MACRO": 6,
}

STALE_DAYS = {
    "STOCK": 10,
    "FX": 10,
    "CRYPTO": 3,
    "MACRO": 10,
}
MACRO_MONTHLY_STALE_DAYS = int(os.getenv("MACRO_MONTHLY_STALE_DAYS", "75"))


@dataclass(frozen=True)
class LiveValidationResult:
    status: str
    db_path: str
    summary: dict[str, Any]
    symbols: list[dict[str, Any]]
    critical_failures: list[str]
    warnings: list[str]
    dashboard_audit: dict[str, Any]
    market_audit: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "db_path": self.db_path,
            "summary": self.summary,
            "symbols": self.symbols,
            "critical_failures": self.critical_failures,
            "warnings": self.warnings,
            "dashboard_audit": self.dashboard_audit,
            "market_audit": self.market_audit,
        }


def run_validate_live(db_path: str, *, expected_years: int | None = None, expected_days: int = 365, report_path: str | Path = REPORT_PATH) -> int:
    result = validate_live_database(db_path, expected_years=expected_years, expected_days=expected_days, report_path=report_path)
    print(format_live_validation(result))
    return 1 if result.status == "FAIL" else 0


def validate_live_database(
    db_path: str,
    *,
    expected_years: int | None = None,
    expected_days: int = 365,
    report_path: str | Path | None = REPORT_PATH,
) -> LiveValidationResult:
    if expected_years is not None:
        expected_days = max(1, expected_years * 365)
    audit_expected_years = max(1, round(expected_days / 365))
    path = Path(db_path).expanduser().resolve()
    if not path.exists():
        result = LiveValidationResult(
            status="FAIL",
            db_path=str(path),
            summary={"db_exists": False},
            symbols=[],
            critical_failures=[f"SQLite database not found: {path}"],
            warnings=[],
            dashboard_audit={},
            market_audit={},
        )
        if report_path:
            write_live_validation_report(result, report_path)
        return result

    initialize_schema(str(path))
    system = get_system_status(str(path))
    mode_summary = get_data_mode_summary(str(path))
    health = get_data_health(str(path))
    dashboard = audit_dashboard(str(path), expected_years=audit_expected_years)
    market = audit_market(str(path), with_live_sample=False)
    symbol_rows = _symbol_table(str(path), expected_days=expected_days)
    active_keys = {(row["asset_type"], row["symbol"]) for row in symbol_rows}
    required_items = required_scope_items()
    required_keys = {(item.asset_type, item.symbol) for item in required_items}

    critical: list[str] = []
    warnings: list[str] = []

    if system.get("is_empty"):
        critical.append("Live validation database is empty")
    if mode_summary.get("data_mode") != "live":
        critical.append(f"Temporary live DB must be data_mode=live, got {mode_summary.get('data_mode')}")
    if health.get("status") == "FAIL":
        critical.append("Data health is FAIL: " + ", ".join(health.get("missing_important_symbols") or []))
    elif health.get("status") == "WARN":
        warnings.append("Data health is WARN")

    for item in dashboard.get("quote_consistency", []):
        item_key = (str(item.get("asset_type") or "").upper(), str(item.get("symbol") or "").upper())
        if item_key not in active_keys and "MISSING_QUOTE_OR_HISTORY" in set(item.get("flags") or []):
            continue
        if item.get("status") == "FAIL":
            critical.append(f"Quote/history ratio failed for {item.get('label')}")
        elif item.get("status") == "WARN":
            warnings.append(f"Quote/history ratio warning for {item.get('label')}")
    for alert in dashboard.get("alerts", []):
        if "quote/history ratio failed" in alert or "duplicate" in alert.lower() or "conflicting" in alert.lower():
            critical.append(str(alert))
        else:
            warnings.append(str(alert))

    for item in market.get("items", []):
        flags = set(item.get("flags") or [])
        hard = sorted(flags.intersection(CRITICAL_AUDIT_FLAGS))
        warn = sorted(flags.intersection(WARN_AUDIT_FLAGS))
        if hard:
            critical.append(f"{item.get('asset_type')} {item.get('symbol')}: " + ", ".join(hard))
        if warn:
            warnings.append(f"{item.get('asset_type')} {item.get('symbol')}: " + ", ".join(warn))

    for row in symbol_rows:
        if row["status"] == "FAIL":
            critical.append(f"{row['asset_type']} {row['symbol']}: {row['notes']}")
        elif row["status"] == "WARN":
            warnings.append(f"{row['asset_type']} {row['symbol']}: {row['notes']}")

    for item in required_items:
        if (item.asset_type, item.symbol) not in active_keys:
            critical.append(f"{item.asset_type} {item.symbol}: required release symbol missing")

    duplicate_failures = _duplicate_checks(str(path))
    critical.extend(duplicate_failures)

    status = "FAIL" if critical else "WARN" if warnings else "OK"
    result = LiveValidationResult(
        status=status,
        db_path=str(path),
        summary={
            "db_exists": True,
            "data_mode": mode_summary.get("data_mode"),
            "providers": mode_summary.get("providers", []),
            "historical_rows": system.get("historical_row_count"),
            "date_min": system.get("date_min"),
            "date_max": system.get("date_max"),
            "instruments": system.get("total_instruments"),
            "data_health": health,
            "history_mode": "standard" if expected_days <= 365 else "advanced",
            "requested_days": expected_days,
            "advanced_history_enabled": expected_days > 365,
            "advanced_history_max_years": 10,
        },
        symbols=symbol_rows,
        critical_failures=sorted(set(critical)),
        warnings=sorted(set(warnings)),
        dashboard_audit=dashboard,
        market_audit=market,
    )
    if report_path:
        write_live_validation_report(result, report_path)
    return result


def format_live_validation(result: LiveValidationResult) -> str:
    lines = [
        "LIVE DATA VALIDATION",
        f"DB: {result.db_path}",
        f"Status: {result.status}",
        f"Data mode: {result.summary.get('data_mode', '-')}",
        f"Providers: {', '.join(result.summary.get('providers') or []) or '-'}",
        f"Historical rows: {result.summary.get('historical_rows', 0)}",
        f"Date range: {result.summary.get('date_min') or '-'} to {result.summary.get('date_max') or '-'}",
        f"History mode: {result.summary.get('history_mode', '-')}",
        f"Requested days: {result.summary.get('requested_days', '-')}",
        f"Symbols checked: {len(result.symbols)}",
        f"Critical failures: {len(result.critical_failures)}",
        f"Warnings: {len(result.warnings)}",
    ]
    for item in result.critical_failures[:20]:
        lines.append(f"FAIL: {item}")
    for item in result.warnings[:20]:
        lines.append(f"WARN: {item}")
    return "\n".join(lines)


def write_live_validation_report(result: LiveValidationResult, report_path: str | Path = REPORT_PATH) -> None:
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    generated = datetime.now(timezone.utc).isoformat()
    title = "Live Audit Report" if path.name == "LIVE_AUDIT_REPORT.md" else "Live Full Test Report"
    lines = [
        f"# {title}",
        "",
        f"Generated: {generated}",
        f"DB: `{result.db_path}`",
        f"Overall status: **{result.status}**",
        "",
        "## Summary",
        "",
        f"- data_mode: `{result.summary.get('data_mode', '-')}`",
        f"- providers: `{', '.join(result.summary.get('providers') or []) or '-'}`",
        f"- instruments: `{result.summary.get('instruments', 0)}`",
        f"- historical rows: `{result.summary.get('historical_rows', 0)}`",
        f"- date_min: `{result.summary.get('date_min') or '-'}`",
        f"- date_max: `{result.summary.get('date_max') or '-'}`",
        f"- history_mode: `{result.summary.get('history_mode', '-')}`",
        f"- requested_days: `{result.summary.get('requested_days', '-')}`",
        f"- advanced_history_enabled: `{str(bool(result.summary.get('advanced_history_enabled'))).lower()}`",
        f"- advanced_history_max_years: `{result.summary.get('advanced_history_max_years', 10)}`",
        "",
        "## Symbol Validation",
        "",
        "| symbol | asset_type | provider | rows | date_min | date_max | latest_value | unit_label | expected_frequency | stale_days | allowed_stale_days | stale_status | data_mode | status | notes |",
        "|---|---|---:|---:|---|---|---:|---|---|---:|---:|---|---|---|---|",
    ]
    for row in result.symbols:
        lines.append(
            "| {symbol} | {asset_type} | {provider} | {rows} | {date_min} | {date_max} | {latest_value} | {unit_label} | {expected_frequency} | {stale_days} | {allowed_stale_days} | {stale_status} | {data_mode} | {status} | {notes} |".format(
                **{
                    key: _md(row.get(key))
                    for key in (
                        "symbol",
                        "asset_type",
                        "provider",
                        "rows",
                        "date_min",
                        "date_max",
                        "latest_value",
                        "unit_label",
                        "expected_frequency",
                        "stale_days",
                        "allowed_stale_days",
                        "stale_status",
                        "data_mode",
                        "status",
                        "notes",
                    )
                }
            )
        )
    lines.extend(["", "## Critical Failures", ""])
    lines.extend([f"- {item}" for item in result.critical_failures] or ["- none"])
    lines.extend(["", "## Warnings", ""])
    lines.extend([f"- {item}" for item in result.warnings] or ["- none"])
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This report is generated from a temporary live-test SQLite database.",
            "- Demo rows are not accepted in this validation path.",
            "- API keys are never written to this report.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _symbol_table(db_path: str, *, expected_days: int) -> list[dict[str, Any]]:
    scope = {(item.asset_type, item.symbol): item for item in required_scope_items()}
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        instruments = [
            dict(row)
            for row in conn.execute(
                """
                SELECT symbol, asset_type, exchange, currency, provider, data_mode
                FROM instruments
                WHERE is_active=1
                ORDER BY asset_type, priority, symbol
                """
            ).fetchall()
        ]
        rows = [
            _validate_symbol(
                conn,
                item,
                expected_days=expected_days,
                scope_item=scope.get((str(item.get("asset_type") or "").upper(), str(item.get("symbol") or "").upper())),
            )
            for item in instruments
        ]
    return rows


def _validate_symbol(
    conn: sqlite3.Connection,
    instrument: dict[str, Any],
    *,
    expected_days: int,
    scope_item: LiveScopeItem | None,
) -> dict[str, Any]:
    asset_type = str(instrument.get("asset_type") or "").upper()
    symbol = str(instrument.get("symbol") or "").upper()
    exchange = instrument.get("exchange")
    history = _history_summary(conn, asset_type, symbol, exchange)
    quote = _quote_summary(conn, asset_type, symbol, exchange)
    notes: list[str] = []
    status = "OK"

    modes = set(history.get("modes") or []) | ({str(instrument.get("data_mode") or "unknown").lower()} if instrument.get("data_mode") else set())
    providers = set(history.get("providers") or []) | ({str(instrument.get("provider"))} if instrument.get("provider") else set())
    quote_provider = quote.get("provider")
    if quote_provider:
        providers.add(str(quote_provider))
    provider_label = ",".join(sorted(providers)) or "-"

    if not history["rows"]:
        status = "FAIL"
        notes.append("no live history")
    if any(mode != "live" for mode in modes):
        status = "FAIL"
        notes.append("non-live data_mode present: " + ",".join(sorted(modes)))
    if any(_is_forbidden_live_provider(provider) for provider in providers):
        status = "FAIL"
        notes.append("mock/demo provider cannot be live")
    if not quote:
        status = "FAIL"
        notes.append("missing latest quote")
    elif quote.get("data_mode") != "live":
        status = "FAIL"
        notes.append(f"latest quote data_mode={quote.get('data_mode')}")

    expected_years_float = expected_days / 365.0
    required_rows = min_rows_for_days(scope_item, expected_days) if scope_item else max(2, int(MIN_ROWS_PER_YEAR.get(asset_type, 2) * max(expected_years_float, 1.0) * 0.70))
    if history["rows"] and history["rows"] < required_rows:
        if scope_item:
            status = "FAIL"
        elif status != "FAIL":
            status = "WARN"
        notes.append(f"history shorter than expected ({history['rows']} < {required_rows})")
    if scope_item and history.get("date_min") and history.get("date_max"):
        try:
            coverage_days = (date.fromisoformat(str(history["date_max"])[:10]) - date.fromisoformat(str(history["date_min"])[:10])).days
        except ValueError:
            status = "FAIL"
            notes.append("invalid history coverage dates")
        else:
            required_days = int(max(expected_days, 1) * 0.85)
            if coverage_days < required_days:
                status = "FAIL"
                notes.append(f"history range shorter than expected ({coverage_days}d < {required_days}d)")

    latest_value = quote.get("price") if quote else history.get("latest_value")
    if latest_value is not None and history.get("latest_value") not in (None, 0):
        diff_pct = abs((float(latest_value) / float(history["latest_value"])) - 1.0) * 100.0
        if diff_pct > 5.0:
            status = "FAIL"
            notes.append(f"latest quote differs from history by {diff_pct:.2f}%")
        elif diff_pct > 1.0 and status != "FAIL":
            status = "WARN"
            notes.append(f"latest quote differs from history by {diff_pct:.2f}%")

    if quote and quote.get("quote_time") and history.get("date_max") and str(quote["quote_time"])[:10] < str(history["date_max"])[:10]:
        status = "FAIL"
        notes.append("latest quote older than history")

    stale = _stale_status(asset_type, symbol, history.get("date_max"), scope_item)
    if stale["status"] == "FAIL":
        status = "FAIL"
        notes.append(str(stale["note"]))
    elif stale["status"] == "WARN" and status != "FAIL":
        status = "WARN"
        notes.append(str(stale["note"]))

    unit_note = _unit_note(asset_type, symbol, history.get("unit") or instrument.get("currency"))
    if unit_note:
        status = "FAIL"
        notes.append(unit_note)

    return {
        "symbol": symbol,
        "asset_type": asset_type,
        "provider": provider_label,
        "rows": history.get("rows", 0),
        "date_min": history.get("date_min"),
        "date_max": history.get("date_max"),
        "latest_value": latest_value,
        "unit_label": history.get("unit") or instrument.get("currency") or "-",
        "expected_frequency": scope_item.expected_frequency if scope_item else "-",
        "stale_days": stale["stale_days"],
        "allowed_stale_days": stale["allowed_stale_days"],
        "stale_status": stale["status"],
        "data_mode": ",".join(sorted(modes)) or "unknown",
        "status": status,
        "notes": "; ".join(notes) if notes else "OK",
    }


def _history_summary(conn: sqlite3.Connection, asset_type: str, symbol: str, exchange: str | None) -> dict[str, Any]:
    if asset_type == "FX":
        rows = conn.execute(
            """
            SELECT date, rate AS value, NULL AS unit, source AS provider, data_mode
            FROM fx_rates
            WHERE base=? AND symbol=? AND rate IS NOT NULL
            ORDER BY date
            """,
            (exchange or "USD", symbol),
        ).fetchall()
    elif asset_type == "CRYPTO":
        rows = conn.execute(
            """
            SELECT date, price_usd AS value, 'USD' AS unit, provider, data_mode
            FROM crypto_prices_daily
            WHERE symbol=? AND price_usd IS NOT NULL
            ORDER BY date
            """,
            (symbol,),
        ).fetchall()
    elif asset_type == "MACRO":
        rows = conn.execute(
            """
            SELECT date, value, unit, source AS provider, data_mode
            FROM macro_indicators_daily
            WHERE indicator_code=? AND value IS NOT NULL
            ORDER BY date
            """,
            (symbol,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT date, close AS value, currency AS unit, provider, data_mode
            FROM stock_prices_daily
            WHERE symbol=? AND close IS NOT NULL
            ORDER BY date
            """,
            (symbol,),
        ).fetchall()
    if not rows:
        return {"rows": 0, "date_min": None, "date_max": None, "latest_value": None, "unit": None, "providers": [], "modes": []}
    return {
        "rows": len(rows),
        "date_min": rows[0]["date"],
        "date_max": rows[-1]["date"],
        "latest_value": rows[-1]["value"],
        "unit": rows[-1]["unit"],
        "providers": sorted({str(row["provider"]) for row in rows if row["provider"]}),
        "modes": sorted({str(row["data_mode"] or "unknown").lower() for row in rows}),
    }


def _quote_summary(conn: sqlite3.Connection, asset_type: str, symbol: str, exchange: str | None) -> dict[str, Any]:
    params: list[Any] = [asset_type, symbol]
    clause = "asset_type=? AND symbol=?"
    if asset_type == "FX" and exchange:
        clause += " AND exchange=?"
        params.append(exchange)
    row = conn.execute(
        f"""
        SELECT price, quote_time, provider, data_mode
        FROM market_quotes_latest
        WHERE {clause}
        ORDER BY CASE data_mode WHEN 'live' THEN 0 WHEN 'demo' THEN 1 ELSE 2 END, fetched_at DESC, quote_time DESC
        LIMIT 1
        """,
        params,
    ).fetchone()
    return dict(row) if row else {}


def _duplicate_checks(db_path: str) -> list[str]:
    checks: list[str] = []
    specs = [
        ("stock_prices_daily", "STOCK", "symbol, date, COALESCE(provider,''), COALESCE(data_mode,'unknown')"),
        ("crypto_prices_daily", "CRYPTO", "symbol, date, COALESCE(provider,''), COALESCE(data_mode,'unknown')"),
        ("macro_indicators_daily", "MACRO", "indicator_code, date, COALESCE(source,''), COALESCE(data_mode,'unknown')"),
        ("fx_rates", "FX", "base, symbol, date, COALESCE(source,''), COALESCE(data_mode,'unknown')"),
        ("market_quotes_latest", "QUOTE", "asset_type, symbol, COALESCE(exchange,''), COALESCE(provider,''), COALESCE(data_mode,'unknown')"),
    ]
    with sqlite3.connect(db_path) as conn:
        for table, label, columns in specs:
            rows = conn.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM {table}
                GROUP BY {columns}
                HAVING count > 1
                """
            ).fetchall()
            if rows:
                checks.append(f"{label}: duplicate rows by symbol/date/provider/data_mode ({len(rows)} groups)")
    return checks


def _stale_status(
    asset_type: str,
    symbol: str,
    raw_date: str | None,
    scope_item: LiveScopeItem | None,
) -> dict[str, Any]:
    frequency = _expected_frequency(asset_type, symbol, scope_item)
    allowed = _allowed_stale_days(asset_type, frequency)
    if not raw_date:
        return {
            "frequency": frequency,
            "latest_date": None,
            "stale_days": None,
            "allowed_stale_days": allowed,
            "status": "FAIL",
            "note": "missing latest history date",
        }
    try:
        parsed = date.fromisoformat(str(raw_date)[:10])
    except ValueError:
        return {
            "frequency": frequency,
            "latest_date": raw_date,
            "stale_days": None,
            "allowed_stale_days": allowed,
            "status": "FAIL",
            "note": f"invalid latest history date {raw_date}",
        }
    days = (date.today() - parsed).days
    status = "OK"
    note = None
    if days > allowed:
        status = "WARN"
        note = f"latest history appears stale ({days} days old; allowed {allowed})"
        if asset_type == "MACRO" and frequency == "monthly" and days > allowed * 2:
            status = "FAIL"
            note = f"latest monthly history is too stale ({days} days old; allowed {allowed})"
    return {
        "frequency": frequency,
        "latest_date": parsed.isoformat(),
        "stale_days": days,
        "allowed_stale_days": allowed,
        "status": status,
        "note": note,
    }


def _expected_frequency(asset_type: str, symbol: str, scope_item: LiveScopeItem | None) -> str:
    if scope_item and scope_item.expected_frequency:
        frequency = scope_item.expected_frequency.lower()
        return "monthly" if frequency == "monthly" else frequency
    if asset_type == "MACRO" and symbol.upper().endswith("_MONTHLY"):
        return "monthly"
    if asset_type in {"STOCK", "FX"}:
        return "business_daily"
    return "daily"


def _allowed_stale_days(asset_type: str, frequency: str) -> int:
    if asset_type == "MACRO" and frequency == "monthly":
        return MACRO_MONTHLY_STALE_DAYS
    return STALE_DAYS.get(asset_type, 10)


def _unit_note(asset_type: str, symbol: str, unit: str | None) -> str | None:
    normalized = str(unit or "").strip()
    upper = normalized.upper()
    if asset_type == "FX" and upper != symbol.upper():
        return f"expected FX quote unit {symbol.upper()}, got {normalized or 'missing'}"
    if asset_type in {"STOCK", "CRYPTO"} and upper != "USD":
        return f"expected unit USD, got {normalized or 'missing'}"
    if asset_type == "MACRO" and normalized not in {"% a.a.", "% a.m.", "% a.d.", "index"}:
        return f"unsupported macro unit {normalized or 'missing'}"
    return None


def _is_forbidden_live_provider(provider: str | None) -> bool:
    normalized = str(provider or "").strip().lower()
    if normalized == "fake_live":
        return False
    return normalized in {"mock", "demo"} or "mock" in normalized


def _md(value: Any) -> str:
    if value is None:
        return "-"
    text = str(value).replace("|", "\\|").replace("\n", " ")
    return text if text else "-"
