from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .api_frankfurter import FrankfurterClient, normalize_timeseries
from .config import Settings
from .crypto_providers import MockCryptoProvider, build_crypto_provider, load_crypto_reference
from .dashboard_prepare import (
    DEFAULT_CRYPTO_REFERENCE,
    DEFAULT_CURRENCY_REFERENCE,
    DEFAULT_DASHBOARD_STOCK_REFERENCE,
    DEFAULT_MACRO_REFERENCE,
    _stage_live_fx,
)
from .macro_providers import MockMacroProvider, build_macro_provider, load_macro_reference
from .market_providers import MockMarketDataProvider, build_market_provider
from .provider_status import key_status, providers_status
from .watchlist import load_stock_watchlist

REPORT_PATH = Path("docs/LIVE_SAMPLE_VALIDATION_REPORT.md")
TOLERANCE_PCT = {
    "FX": 0.50,
    "CRYPTO": 3.00,
    "STOCK": 2.00,
    "MACRO": 1.00,
}


@dataclass(frozen=True)
class SampleValidationResult:
    status: str
    db_path: str
    generated_at: str
    requested_period_days: int
    history_mode: str
    advanced_history_available: bool
    samples: list[dict[str, Any]]
    provider_checks: list[dict[str, Any]]
    reason: str | None = None
    action: str | None = None


def run_validate_samples(
    settings: Settings,
    *,
    db_path: str,
    samples_per_symbol: int = 5,
    external_test: bool = False,
    rate_limit_delay: float = 0.0,
    report_path: str | Path = REPORT_PATH,
) -> int:
    result = validate_samples(
        settings,
        db_path=db_path,
        samples_per_symbol=samples_per_symbol,
        external_test=external_test,
        rate_limit_delay=rate_limit_delay,
        report_path=report_path,
    )
    print(format_sample_validation(result))
    return 1 if result.status in {"FAIL", "NOT READY"} else 0


def validate_samples(
    settings: Settings,
    *,
    db_path: str,
    samples_per_symbol: int = 5,
    external_test: bool = False,
    rate_limit_delay: float = 0.0,
    report_path: str | Path | None = REPORT_PATH,
) -> SampleValidationResult:
    target = str(Path(db_path).expanduser().resolve())
    instruments = _live_instruments(target)
    readiness = stock_sample_validation_readiness(settings, instruments)
    if not readiness["ready"]:
        result = SampleValidationResult(
            status="NOT READY",
            db_path=target,
            generated_at=datetime.now(timezone.utc).isoformat(),
            requested_period_days=settings.live_default_days,
            history_mode=settings.live_history_mode,
            advanced_history_available=settings.live_history_mode == "advanced",
            samples=[],
            provider_checks=[
                {
                    "asset_type": "STOCK",
                    "provider": "twelvedata",
                    "external_test": "not_run",
                    "status": "not_ready",
                    "message": readiness["reason"],
                }
            ],
            reason=readiness["reason"],
            action=readiness["action"],
        )
        if report_path:
            write_sample_validation_report(result, report_path)
        return result

    provider_checks: list[dict[str, Any]] = []
    if external_test:
        provider_state = providers_status(settings, test_external=True)
        provider_checks = list(provider_state.get("providers") or [])

    samples: list[dict[str, Any]] = []
    if not instruments:
        samples.append(
            _sample_row(
                {"symbol": "NO_INSTRUMENTS", "asset_type": "ALL", "provider": "-"},
                None,
                None,
                None,
                None,
                None,
                "FAIL",
                "no live instruments in DB",
            )
        )
    for instrument in instruments:
        history = _history(target, instrument)
        selected = _sample_points(history, samples_per_symbol)
        if not selected:
            samples.append(_sample_row(instrument, None, None, None, None, None, "FAIL", "no DB history"))
            continue
        provider_values: dict[str, float] = {}
        provider_note = ""
        try:
            provider_values = _provider_values(settings, instrument, history[0]["date"], history[-1]["date"])
        except Exception as exc:
            provider_note = f"provider error: {exc}"
        for point in selected:
            db_value = _value(point)
            provider_date, provider_value = _nearest_value(provider_values, point["date"])
            tolerance = TOLERANCE_PCT.get(instrument["asset_type"], 2.0)
            if provider_value is None:
                samples.append(_sample_row(instrument, point["date"], db_value, None, None, tolerance, "FAIL", provider_note or "provider value missing"))
                continue
            delta_pct = abs((float(db_value) / float(provider_value)) - 1.0) * 100.0 if provider_value not in (None, 0) else None
            status = "OK"
            note = "OK"
            if provider_date != point["date"]:
                status = "WARN"
                note = f"provider returned nearest date {provider_date}"
            if delta_pct is None or delta_pct > tolerance:
                status = "FAIL"
                note = f"delta above tolerance; {note}"
            samples.append(_sample_row(instrument, point["date"], db_value, provider_value, delta_pct, tolerance, status, note, provider_date=provider_date))
        if rate_limit_delay:
            time.sleep(rate_limit_delay)

    statuses = {row["status"] for row in samples}
    status = "FAIL" if "FAIL" in statuses else "WARN" if "WARN" in statuses else "OK"
    result = SampleValidationResult(
        status=status,
        db_path=target,
        generated_at=datetime.now(timezone.utc).isoformat(),
        requested_period_days=settings.live_default_days,
        history_mode=settings.live_history_mode,
        advanced_history_available=settings.live_history_mode == "advanced",
        samples=samples,
        provider_checks=provider_checks,
    )
    if report_path:
        write_sample_validation_report(result, report_path)
    return result


def stock_sample_validation_readiness(
    settings: Settings,
    instruments: list[dict[str, Any]] | None = None,
    *,
    db_path: str | None = None,
) -> dict[str, Any]:
    if instruments is None:
        if db_path is None:
            raise ValueError("provide instruments or db_path")
        instruments = _live_instruments(str(Path(db_path).expanduser().resolve()))

    requires_twelve = any(
        str(item.get("asset_type") or "").upper() == "STOCK"
        and str(item.get("data_mode") or "").lower() == "live"
        and str(item.get("provider") or settings.stock_provider).strip().lower() == "twelvedata"
        for item in instruments
    )
    if not requires_twelve:
        return {"ready": True, "reason": None, "action": None}

    status = key_status(settings.twelve_data_api_key)
    action = "Run inside run_live_pipeline.ps1 or set TWELVE_DATA_API_KEY in the current PowerShell session."
    if not status["present"]:
        return {
            "ready": False,
            "reason": "TWELVE_DATA_API_KEY missing for stock sample validation.",
            "action": action,
        }
    if not status["valid_format"]:
        return {
            "ready": False,
            "reason": "TWELVE_DATA_API_KEY invalid for stock sample validation.",
            "action": "Paste only the Twelve Data API key in the current PowerShell session.",
        }
    return {"ready": True, "reason": None, "action": None}


def format_sample_validation(result: SampleValidationResult) -> str:
    counts = _counts(result.samples)
    lines = [
        "LIVE SAMPLE VALIDATION",
        f"DB: {result.db_path}",
        f"LIVE SAMPLE VALIDATION STATUS: {result.status}",
        f"Requested period days: {result.requested_period_days}",
        f"History mode: {result.history_mode}",
        f"Samples: OK={counts.get('OK', 0)} WARN={counts.get('WARN', 0)} FAIL={counts.get('FAIL', 0)}",
        f"Report: {REPORT_PATH}",
    ]
    if result.reason:
        lines.append(f"Reason: {result.reason}")
    if result.action:
        lines.append(f"Action: {result.action}")
    for item in result.samples[:30]:
        if item["status"] != "OK":
            lines.append(f"{item['status']}: {item['asset_type']} {item['symbol']} {item['sample_date']} {item['note']}")
    return "\n".join(lines)


def write_sample_validation_report(result: SampleValidationResult, report_path: str | Path = REPORT_PATH) -> None:
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    counts = _counts(result.samples)
    lines = [
        "# Live Sample Validation Report",
        "",
        f"Generated: {result.generated_at}",
        f"DB: `{result.db_path}`",
        f"Overall status: **{result.status}**",
        f"requested_period_days: `{result.requested_period_days}`",
        f"history_mode: `{result.history_mode}`",
        f"advanced_history_available: `{str(result.advanced_history_available).lower()}`",
        f"Samples OK/WARN/FAIL: `{counts.get('OK', 0)}/{counts.get('WARN', 0)}/{counts.get('FAIL', 0)}`",
    ]
    if result.reason:
        lines.extend(["", "## Readiness", "", f"- reason: `{_md(result.reason)}`", f"- action: `{_md(result.action)}`"])
    lines.extend(
        [
            "",
            "| symbol | asset_type | provider | endpoint | sample_date | provider_date | db_value | provider_value | delta_pct | tolerance_pct | status | note |",
            "|---|---|---|---|---|---|---:|---:|---:|---:|---|---|",
        ]
    )
    for row in result.samples:
        lines.append(
            "| {symbol} | {asset_type} | {provider} | {endpoint} | {sample_date} | {provider_date} | {db_value} | {provider_value} | {delta_pct} | {tolerance_pct} | {status} | {note} |".format(
                **{key: _md(row.get(key)) for key in ("symbol", "asset_type", "provider", "endpoint", "sample_date", "provider_date", "db_value", "provider_value", "delta_pct", "tolerance_pct", "status", "note")}
            )
        )
    if result.provider_checks:
        lines.extend(["", "## Provider External Tests", "", "| asset_type | provider | external_test | status | message |", "|---|---|---|---|---|"])
        for item in result.provider_checks:
            lines.append(
                "| {asset_type} | {provider} | {external_test} | {status} | {message} |".format(
                    **{key: _md(item.get(key)) for key in ("asset_type", "provider", "external_test", "status", "message")}
                )
            )
    lines.extend(["", "## Notes", "", "- Samples are deterministic: first, last, middle, and evenly spaced interior dates.", "- Provider nearest-date matches are WARN unless the value diverges beyond tolerance.", "- API keys are never written to this report."])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _live_instruments(db_path: str) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT symbol, asset_type, exchange, provider, provider_symbol, currency, data_mode
            FROM instruments
            WHERE is_active=1
            ORDER BY asset_type, priority, symbol
            """
        ).fetchall()
    return [dict(row) for row in rows]


def _history(db_path: str, instrument: dict[str, Any]) -> list[dict[str, Any]]:
    asset_type = instrument["asset_type"]
    symbol = instrument["symbol"]
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if asset_type == "FX":
            rows = conn.execute("SELECT date, rate AS value FROM fx_rates WHERE base=? AND symbol=? AND data_mode='live' ORDER BY date", (instrument.get("exchange") or "USD", symbol)).fetchall()
        elif asset_type == "CRYPTO":
            rows = conn.execute("SELECT date, price_usd AS value FROM crypto_prices_daily WHERE symbol=? AND data_mode='live' ORDER BY date", (symbol,)).fetchall()
        elif asset_type == "MACRO":
            rows = conn.execute("SELECT date, value FROM macro_indicators_daily WHERE indicator_code=? AND data_mode='live' ORDER BY date", (symbol,)).fetchall()
        else:
            rows = conn.execute("SELECT date, close AS value FROM stock_prices_daily WHERE symbol=? AND data_mode='live' ORDER BY date", (symbol,)).fetchall()
    return [dict(row) for row in rows if row["value"] is not None]


def _sample_points(history: list[dict[str, Any]], count: int) -> list[dict[str, Any]]:
    if not history:
        return []
    if len(history) <= count:
        return history
    positions = {0, len(history) - 1, round((len(history) - 1) * 0.25), round((len(history) - 1) * 0.50), round((len(history) - 1) * 0.75)}
    if count > len(positions):
        step = max(1, len(history) // count)
        positions.update(range(0, len(history), step))
    return [history[index] for index in sorted(positions)[:count]]


def _provider_values(settings: Settings, instrument: dict[str, Any], start: str, end: str) -> dict[str, float]:
    asset_type = instrument["asset_type"]
    symbol = instrument["symbol"]
    if asset_type == "FX":
        if settings.fx_provider == "fake_live":
            stage = _stage_live_fx(settings, DEFAULT_CURRENCY_REFERENCE, date.fromisoformat(start), date.fromisoformat(end), [symbol])
            return {row.date: float(row.rate) for row in stage.fx_rows if row.symbol == symbol}
        client = FrankfurterClient(settings.api_base_url, settings.cache_dir, settings.timeout_seconds, settings.use_cache, settings.max_retries, settings.use_cache_latest)
        payload = client.fetch_timeseries(start=start, end=end, base=instrument.get("exchange") or "USD", symbols=[symbol])
        return {row.date: float(row.rate) for row in normalize_timeseries(payload, base=instrument.get("exchange") or "USD", source="frankfurter") if row.symbol == symbol}
    if asset_type == "STOCK":
        provider = MockMarketDataProvider() if settings.stock_provider == "fake_live" else build_market_provider(settings.stock_provider, settings.twelve_data_api_key, False, settings.timeout_seconds, settings.max_retries)
        rows = provider.fetch_stock_daily(symbol=instrument.get("provider_symbol") or symbol, start=start, end=end, exchange=instrument.get("exchange"))
        return {row.date: float(row.close) for row in rows if row.close is not None}
    if asset_type == "CRYPTO":
        assets = {item.symbol: item for item in load_crypto_reference(DEFAULT_CRYPTO_REFERENCE)}
        asset = assets[symbol]
        provider = MockCryptoProvider() if settings.crypto_provider == "fake_live" else build_crypto_provider(
            False,
            settings.timeout_seconds,
            settings.max_retries,
            coingecko_api_plan=settings.coingecko_api_plan,
            coingecko_demo_api_key=settings.coingecko_demo_api_key,
            coingecko_pro_api_key=settings.coingecko_pro_api_key,
        )
        rows = provider.fetch_daily(asset, start, end)
        return {row.date: float(row.price_usd) for row in rows if row.price_usd is not None}
    indicators = {item.indicator_code: item for item in load_macro_reference(DEFAULT_MACRO_REFERENCE)}
    indicator = indicators[symbol]
    provider = MockMacroProvider() if settings.macro_provider == "fake_live" else build_macro_provider(False, settings.timeout_seconds, settings.max_retries)
    rows = provider.fetch_daily(indicator, start, end)
    return {row.date: float(row.value) for row in rows if row.value is not None}


def _nearest_value(values: dict[str, float], sample_date: str) -> tuple[str | None, float | None]:
    if sample_date in values:
        return sample_date, values[sample_date]
    if not values:
        return None, None
    target = date.fromisoformat(sample_date)
    candidates = sorted(values, key=lambda raw: abs((date.fromisoformat(raw) - target).days))
    nearest = candidates[0]
    if abs((date.fromisoformat(nearest) - target).days) > 7:
        return None, None
    return nearest, values[nearest]


def _value(row: dict[str, Any]) -> float:
    return float(row["value"])


def _sample_row(
    instrument: dict[str, Any],
    sample_date: str | None,
    db_value: float | None,
    provider_value: float | None,
    delta_pct: float | None,
    tolerance_pct: float | None,
    status: str,
    note: str,
    *,
    provider_date: str | None = None,
) -> dict[str, Any]:
    return {
        "symbol": instrument.get("symbol"),
        "asset_type": instrument.get("asset_type"),
        "provider": instrument.get("provider"),
        "endpoint": _endpoint_for_sample(instrument),
        "sample_date": sample_date,
        "provider_date": provider_date or sample_date,
        "db_value": db_value,
        "provider_value": provider_value,
        "delta_pct": round(delta_pct, 6) if delta_pct is not None else None,
        "tolerance_pct": tolerance_pct,
        "status": status,
        "note": note,
    }


def _endpoint_for_sample(instrument: dict[str, Any]) -> str:
    asset_type = str(instrument.get("asset_type") or "").upper()
    if asset_type == "FX":
        return "frankfurter timeseries"
    if asset_type == "CRYPTO":
        return f"coins/{instrument.get('provider_symbol') or instrument.get('symbol')}/market_chart/range"
    if asset_type == "STOCK":
        return "twelvedata time_series"
    if asset_type == "MACRO":
        return f"bcb_sgs {instrument.get('provider_symbol') or instrument.get('symbol')}"
    return "-"


def _counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status") or "UNKNOWN")
        counts[status] = counts.get(status, 0) + 1
    return counts


def _md(value: Any) -> str:
    if value is None:
        return "-"
    return str(value).replace("|", "\\|").replace("\n", " ") or "-"
