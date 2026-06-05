from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .api_frankfurter import FrankfurterClient, normalize_latest, normalize_timeseries
from .config import Settings
from .crypto_providers import CoinGeckoProviderError, MockCryptoProvider, build_crypto_provider, load_crypto_reference
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
from .env_doctor import classify_external_error
from .watchlist import load_stock_watchlist

REPORT_PATH = Path("docs/LIVE_SAMPLE_VALIDATION_REPORT.md")
TOLERANCE_PCT = {
    "FX": 0.50,
    "CRYPTO": 3.00,
    "STOCK": 2.00,
    "MACRO": 1.00,
}
REASON_VALIDATION_OK = "VALIDATION_OK"
REASON_EXTERNAL_RATE_LIMIT = "EXTERNAL_RATE_LIMIT"
REASON_EXTERNAL_PROVIDER_UNAVAILABLE = "EXTERNAL_PROVIDER_UNAVAILABLE"
REASON_CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST = "CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST"
REASON_HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE = "HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE"
REASON_HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE = "HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE"
REASON_LATEST_QUOTE_DIVERGENCE_WARN = "LATEST_QUOTE_DIVERGENCE_WARN"
REASON_LATEST_QUOTE_DIVERGENCE_FAIL = "LATEST_QUOTE_DIVERGENCE_FAIL"
REASON_INSUFFICIENT_HISTORY_POINTS = "INSUFFICIENT_HISTORY_POINTS"
REASON_MISSING_LIVE_HISTORY = "MISSING_LIVE_HISTORY"
REASON_DUPLICATED_HISTORY_DATE = "DUPLICATED_HISTORY_DATE"
REASON_NON_POSITIVE_PRICE = "NON_POSITIVE_PRICE"
REASON_STALE_LATEST_QUOTE = "STALE_LATEST_QUOTE"
REASON_PROVIDER_KEY_MISSING = "PROVIDER_KEY_MISSING"
REASON_PROVIDER_TLS_ERROR = "PROVIDER_TLS_ERROR"
REASON_DEMO_DATA_IN_LIVE_VALIDATION = "DEMO_DATA_IN_LIVE_VALIDATION"
REASON_HISTORY_NOT_ORDERED = "HISTORY_NOT_ORDERED"
REASON_FUTURE_HISTORY_DATE = "FUTURE_HISTORY_DATE"
REASON_HISTORICAL_SAMPLE_DIVERGENCE_FAIL = "HISTORICAL_SAMPLE_DIVERGENCE_FAIL"
REASON_HISTORICAL_SAMPLE_NEAREST_DATE_WARN = "HISTORICAL_SAMPLE_NEAREST_DATE_WARN"
REASON_EXTERNAL_VALIDATION_LIMITED = "EXTERNAL_VALIDATION_LIMITED"

TRANSIENT_EXTERNAL_REASON_CODES = {
    REASON_EXTERNAL_RATE_LIMIT,
    REASON_EXTERNAL_PROVIDER_UNAVAILABLE,
    REASON_PROVIDER_TLS_ERROR,
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
    release_gate: str = "BLOCKED"
    promotion_allowed: bool = False
    internal_summary: dict[str, Any] = field(default_factory=dict)
    external_summary: dict[str, Any] = field(default_factory=dict)
    reason_codes: list[str] = field(default_factory=list)


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
    return 1 if result.status in {"FAIL", "NOT_READY"} else 0


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
    provider_checks: list[dict[str, Any]] = []
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
                reason_code=REASON_MISSING_LIVE_HISTORY,
                endpoint="internal sqlite",
            )
        )
    for instrument in instruments:
        history = _history(target, instrument)
        samples.extend(_internal_sample_rows(settings, target, instrument, history, samples_per_symbol))

    readiness = stock_sample_validation_readiness(settings, instruments)
    if external_test:
        provider_checks.extend(_configured_provider_checks(settings, instruments))
        if not readiness["ready"]:
            provider_checks.append(
                {
                    "asset_type": "STOCK",
                    "provider": "twelvedata",
                    "external_test": "not_run",
                    "status": "not_ready",
                    "reason_code": REASON_PROVIDER_KEY_MISSING,
                    "message": readiness["reason"],
                }
            )
        else:
            for instrument in instruments:
                if _external_validation_blocked(settings, instrument):
                    continue
                history = _history(target, instrument)
                if not history:
                    continue
                samples.extend(_external_history_sample_rows(settings, instrument, history, samples_per_symbol))
                latest_row = _external_latest_sample_row(settings, target, instrument, history)
                if latest_row is not None:
                    samples.append(latest_row)
                if rate_limit_delay:
                    time.sleep(rate_limit_delay)

    internal_summary = _internal_validation_summary(samples)
    external_summary = _external_validation_summary(samples, provider_checks, external_test=external_test)
    status, reason, action, release_gate, promotion_allowed, reason_codes = _overall_result(
        samples,
        provider_checks,
        readiness if external_test else {"ready": True},
        internal_summary,
        external_summary,
    )
    result = SampleValidationResult(
        status=status,
        db_path=target,
        generated_at=datetime.now(timezone.utc).isoformat(),
        requested_period_days=settings.live_default_days,
        history_mode=settings.live_history_mode,
        advanced_history_available=settings.live_history_mode == "advanced",
        samples=samples,
        provider_checks=provider_checks,
        reason=reason,
        action=action,
        release_gate=release_gate,
        promotion_allowed=promotion_allowed,
        internal_summary=internal_summary,
        external_summary=external_summary,
        reason_codes=reason_codes,
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


def _configured_provider_checks(settings: Settings, instruments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    wanted = {str(item.get("asset_type") or "").upper() for item in instruments}
    provider_state = providers_status(settings, test_external=False)
    checks: list[dict[str, Any]] = []
    for item in provider_state.get("providers", []):
        asset_type = str(item.get("asset_type") or "").upper()
        if asset_type not in wanted:
            continue
        reason_code = REASON_VALIDATION_OK if item.get("configured") else REASON_PROVIDER_KEY_MISSING
        checks.append(
            {
                "asset_type": asset_type,
                "provider": item.get("provider"),
                "external_test": "configuration_only",
                "status": "configured" if item.get("configured") else "not_ready",
                "reason_code": reason_code,
                "message": item.get("message") or "Provider configuration available for sample validation.",
            }
        )
    return checks


def _external_validation_blocked(settings: Settings, instrument: dict[str, Any]) -> bool:
    asset_type = str(instrument.get("asset_type") or "").upper()
    if asset_type != "STOCK":
        return False
    provider = str(instrument.get("provider") or settings.stock_provider).strip().lower()
    if provider in {"fake_live", "mock"}:
        return False
    return not stock_sample_validation_readiness(settings, [instrument])["ready"]


def _internal_sample_rows(
    settings: Settings,
    db_path: str,
    instrument: dict[str, Any],
    history: list[dict[str, Any]],
    samples_per_symbol: int,
) -> list[dict[str, Any]]:
    failures = _history_failures(settings, instrument, history)
    failures.extend(_latest_quote_failures(settings, db_path, instrument, history))
    if failures:
        return [
            _sample_row(
                instrument,
                failure.get("sample_date"),
                failure.get("db_value"),
                None,
                None,
                failure.get("tolerance_pct"),
                failure["status"],
                failure["note"],
                reason_code=failure["reason_code"],
                endpoint="internal sqlite",
            )
            for failure in failures
        ]
    return [
        _sample_row(
            instrument,
            point["date"],
            _value(point),
            None,
            None,
            None,
            "OK",
            "internal DB history sample valid; old historical point not compared to current price",
            reason_code=REASON_HISTORICAL_SAMPLE_NOT_COMPARED_TO_CURRENT_PRICE,
            endpoint="internal sqlite",
        )
        for point in _sample_points(history, samples_per_symbol)
    ]


def _history_failures(settings: Settings, instrument: dict[str, Any], history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    asset_type = str(instrument.get("asset_type") or "").upper()
    symbol = str(instrument.get("symbol") or "-").upper()
    if str(instrument.get("data_mode") or "").lower() != "live":
        return [{
            "status": "FAIL",
            "reason_code": REASON_DEMO_DATA_IN_LIVE_VALIDATION,
            "note": f"{asset_type} {symbol}: instrument is not live data",
        }]
    if not history:
        return [{
            "status": "FAIL",
            "reason_code": REASON_MISSING_LIVE_HISTORY,
            "note": f"{asset_type} {symbol}: no live history rows",
        }]
    failures: list[dict[str, Any]] = []
    dates: list[date] = []
    raw_dates: list[str] = []
    today = date.today()
    for row in history:
        raw_date = str(row.get("date") or "")
        raw_dates.append(raw_date)
        try:
            parsed_date = date.fromisoformat(raw_date[:10])
            dates.append(parsed_date)
            if parsed_date > today:
                failures.append({"status": "FAIL", "reason_code": REASON_FUTURE_HISTORY_DATE, "sample_date": raw_date, "note": f"{asset_type} {symbol}: future history date {raw_date}"})
        except ValueError:
            failures.append({"status": "FAIL", "reason_code": REASON_HISTORY_NOT_ORDERED, "sample_date": raw_date, "note": f"{asset_type} {symbol}: invalid history date {raw_date}"})
        value = row.get("value")
        if value is None:
            failures.append({"status": "FAIL", "reason_code": REASON_MISSING_LIVE_HISTORY, "sample_date": raw_date, "note": f"{asset_type} {symbol}: missing history value"})
        elif asset_type != "MACRO" and float(value) <= 0:
            failures.append({"status": "FAIL", "reason_code": REASON_NON_POSITIVE_PRICE, "sample_date": raw_date, "db_value": float(value), "note": f"{asset_type} {symbol}: non-positive history value"})
    if raw_dates != sorted(raw_dates):
        failures.append({"status": "FAIL", "reason_code": REASON_HISTORY_NOT_ORDERED, "note": f"{asset_type} {symbol}: history dates are not chronological"})
    duplicates = sorted({item for item in raw_dates if raw_dates.count(item) > 1})
    for duplicated in duplicates[:5]:
        failures.append({"status": "FAIL", "reason_code": REASON_DUPLICATED_HISTORY_DATE, "sample_date": duplicated, "note": f"{asset_type} {symbol}: duplicated history date {duplicated}"})
    minimum = _minimum_history_points(settings, instrument)
    if len(history) < minimum:
        failures.append({"status": "FAIL", "reason_code": REASON_INSUFFICIENT_HISTORY_POINTS, "note": f"{asset_type} {symbol}: insufficient history points ({len(history)} < {minimum})"})
    if dates:
        max_gap = _max_gap_days(dates)
        allowed_gap = _allowed_gap_days(instrument)
        if max_gap > allowed_gap:
            failures.append({"status": "FAIL", "reason_code": REASON_MISSING_LIVE_HISTORY, "note": f"{asset_type} {symbol}: absurd history gap {max_gap} days > {allowed_gap}"})
    return failures


def _latest_quote_failures(settings: Settings, db_path: str, instrument: dict[str, Any], history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not history:
        return []
    asset_type = str(instrument.get("asset_type") or "").upper()
    if asset_type == "MACRO":
        return []
    symbol = str(instrument.get("symbol") or "-").upper()
    quote = _latest_quote(db_path, instrument)
    if quote is None:
        return [{"status": "FAIL", "reason_code": REASON_STALE_LATEST_QUOTE, "note": f"{asset_type} {symbol}: missing latest quote"}]
    latest = history[-1]
    latest_value = _value(latest)
    quote_price = quote.get("price")
    if quote_price is None:
        return [{"status": "FAIL", "reason_code": REASON_STALE_LATEST_QUOTE, "sample_date": latest["date"], "db_value": latest_value, "note": f"{asset_type} {symbol}: latest quote has no price"}]
    delta_pct = _delta_pct(latest_value, float(quote_price))
    if delta_pct is not None and delta_pct > settings.live_quote_fail_pct:
        return [{
            "status": "FAIL",
            "reason_code": REASON_LATEST_QUOTE_DIVERGENCE_FAIL,
            "sample_date": latest["date"],
            "db_value": latest_value,
            "tolerance_pct": settings.live_quote_fail_pct,
            "note": f"{asset_type} {symbol}: latest quote differs from latest history by {delta_pct:.2f}%",
        }]
    if delta_pct is not None and delta_pct > settings.live_quote_warn_pct:
        return [{
            "status": "WARN",
            "reason_code": REASON_LATEST_QUOTE_DIVERGENCE_WARN,
            "sample_date": latest["date"],
            "db_value": latest_value,
            "tolerance_pct": settings.live_quote_warn_pct,
            "note": f"{asset_type} {symbol}: latest quote differs from latest history by {delta_pct:.2f}%",
        }]
    quote_time = quote.get("quote_time")
    if quote_time:
        try:
            quote_day = date.fromisoformat(str(quote_time)[:10])
            latest_day = date.fromisoformat(str(latest["date"])[:10])
            if (latest_day - quote_day).days > settings.live_quote_stale_days:
                return [{
                    "status": "WARN",
                    "reason_code": REASON_STALE_LATEST_QUOTE,
                    "sample_date": latest["date"],
                    "db_value": latest_value,
                    "note": f"{asset_type} {symbol}: latest quote is stale ({quote_day.isoformat()})",
                }]
        except ValueError:
            return [{"status": "WARN", "reason_code": REASON_STALE_LATEST_QUOTE, "sample_date": latest["date"], "db_value": latest_value, "note": f"{asset_type} {symbol}: invalid latest quote date {quote_time}"}]
    return []


def _external_history_sample_rows(
    settings: Settings,
    instrument: dict[str, Any],
    history: list[dict[str, Any]],
    samples_per_symbol: int,
) -> list[dict[str, Any]]:
    selected = _sample_points(history, samples_per_symbol)
    if not selected:
        return []
    try:
        provider_values = _provider_values(settings, instrument, history[0]["date"], history[-1]["date"])
    except Exception as exc:
        check = _provider_exception_check(instrument, exc, "historical_range")
        return [
            _sample_row(
                instrument,
                selected[-1]["date"],
                _value(selected[-1]),
                None,
                None,
                TOLERANCE_PCT.get(str(instrument.get("asset_type") or "").upper(), 2.0),
                "WARN",
                check["message"],
                reason_code=check["reason_code"],
                endpoint=_endpoint_for_sample(instrument),
            )
        ]
    rows: list[dict[str, Any]] = []
    for point in selected:
        db_value = _value(point)
        provider_date, provider_value = _nearest_value(provider_values, point["date"])
        tolerance = TOLERANCE_PCT.get(str(instrument.get("asset_type") or "").upper(), 2.0)
        if provider_value is None:
            rows.append(_sample_row(instrument, point["date"], db_value, None, None, tolerance, "WARN", "provider historical range returned no comparable value", reason_code=REASON_EXTERNAL_PROVIDER_UNAVAILABLE, endpoint=_endpoint_for_sample(instrument)))
            continue
        delta_pct = _delta_pct(db_value, provider_value)
        status = "OK"
        note = "historical sample validated from provider range"
        reason_code = REASON_HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE
        if provider_date != point["date"]:
            status = "WARN"
            note = f"provider returned nearest date {provider_date}"
            reason_code = REASON_HISTORICAL_SAMPLE_NEAREST_DATE_WARN
        if delta_pct is None or delta_pct > tolerance:
            if provider_date != point["date"]:
                status = "WARN"
                note = f"historical sample not failed because provider returned nearest date {provider_date}; external confirmation is partial"
                reason_code = REASON_HISTORICAL_SAMPLE_NEAREST_DATE_WARN
            else:
                status = "FAIL"
                note = f"historical sample delta above tolerance; {note}"
                reason_code = REASON_HISTORICAL_SAMPLE_DIVERGENCE_FAIL
        rows.append(_sample_row(instrument, point["date"], db_value, provider_value, delta_pct, tolerance, status, note, provider_date=provider_date, reason_code=reason_code, endpoint=_endpoint_for_sample(instrument)))
    return rows


def _external_latest_sample_row(settings: Settings, db_path: str, instrument: dict[str, Any], history: list[dict[str, Any]]) -> dict[str, Any] | None:
    asset_type = str(instrument.get("asset_type") or "").upper()
    if asset_type == "MACRO" or not history:
        return None
    latest = history[-1]
    latest_quote = _latest_quote(db_path, instrument)
    if latest_quote is None:
        return None
    try:
        provider_value, provider_date, endpoint = _provider_latest_value(settings, instrument)
    except Exception as exc:
        check = _provider_exception_check(instrument, exc, "latest")
        return _sample_row(instrument, latest["date"], float(latest_quote.get("price") or _value(latest)), None, None, settings.live_quote_fail_pct, "WARN", check["message"], reason_code=check["reason_code"], endpoint=_latest_endpoint_for_sample(instrument))
    db_latest_quote = float(latest_quote["price"]) if latest_quote.get("price") is not None else _value(latest)
    delta_pct = _delta_pct(db_latest_quote, provider_value)
    status = "OK"
    note = "current price endpoint used only for latest quote validation"
    if delta_pct is not None and delta_pct > settings.live_quote_fail_pct:
        status = "WARN"
        note = f"current price differs from stored latest quote by {delta_pct:.2f}%; timestamp/cache may differ"
    return _sample_row(
        instrument,
        latest["date"],
        db_latest_quote,
        provider_value,
        delta_pct,
        settings.live_quote_fail_pct,
        status,
        note,
        provider_date=provider_date or latest["date"],
        reason_code=REASON_CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST,
        endpoint=endpoint,
    )


def _overall_result(
    samples: list[dict[str, Any]],
    provider_checks: list[dict[str, Any]],
    readiness: dict[str, Any],
    internal_summary: dict[str, Any],
    external_summary: dict[str, Any],
) -> tuple[str, str | None, str | None, str, bool, list[str]]:
    reason_codes = _reason_codes(samples, provider_checks)
    internal_failures = [row for row in samples if _is_internal_row(row) and row.get("status") == "FAIL"]
    external_failures = [
        row
        for row in samples
        if _is_external_row(row)
        and row.get("status") == "FAIL"
        and str(row.get("reason_code") or "") not in TRANSIENT_EXTERNAL_REASON_CODES
    ]
    provider_blockers = [
        item
        for item in provider_checks
        if str(item.get("status") or "").lower() in {"not_ready", "fail", "failed"}
        and str(item.get("reason_code") or "") not in TRANSIENT_EXTERNAL_REASON_CODES
    ]
    if internal_failures:
        first = internal_failures[0]
        return "NOT_READY", str(first.get("reason_code") or first.get("note") or "internal sample validation failed"), None, "BLOCKED", False, reason_codes
    internal_warnings = [row for row in samples if _is_internal_row(row) and row.get("status") == "WARN"]
    if internal_warnings:
        first = internal_warnings[0]
        return "NOT_READY", str(first.get("reason_code") or first.get("note") or "internal sample validation warning"), None, "BLOCKED", False, reason_codes
    if not readiness.get("ready", True):
        return "NOT_READY", readiness.get("reason") or REASON_PROVIDER_KEY_MISSING, readiness.get("action"), "BLOCKED", False, reason_codes
    if provider_blockers:
        first_provider = provider_blockers[0]
        return "NOT_READY", str(first_provider.get("reason_code") or first_provider.get("message") or "provider validation blocked"), None, "BLOCKED", False, reason_codes
    if external_failures:
        first = external_failures[0]
        return "NOT_READY", str(first.get("reason_code") or first.get("note") or "external sample validation failed"), None, "BLOCKED", False, reason_codes

    has_warning = (
        int(internal_summary.get("WARN", 0)) > 0
        or int(external_summary.get("WARN", 0)) > 0
        or int(external_summary.get("RATE_LIMIT", 0)) > 0
        or int(external_summary.get("transient_failures", 0)) > 0
        or int(external_summary.get("SKIPPED", 0)) > 0
    )
    if has_warning:
        reason = REASON_EXTERNAL_RATE_LIMIT if int(external_summary.get("RATE_LIMIT", 0)) > 0 else _first_warning_reason(samples, provider_checks)
        action = (
            "Provider rate limit detected; internal candidate remains valid but external confirmation is incomplete."
            if reason == REASON_EXTERNAL_RATE_LIMIT
            else "Review warnings before manual promotion."
        )
        return "READY_WITH_WARNINGS", reason or REASON_EXTERNAL_VALIDATION_LIMITED, action, "PASS_WITH_WARNINGS", True, reason_codes
    return "READY", None, None, "PASS", True, reason_codes or [REASON_VALIDATION_OK]


def _internal_validation_summary(samples: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for row in samples if _is_internal_row(row)]
    counts = _counts(rows)
    return {
        "total_samples": len(rows),
        "OK": counts.get("OK", 0),
        "WARN": counts.get("WARN", 0),
        "FAIL": counts.get("FAIL", 0),
        "duplicate_count": _reason_count(rows, REASON_DUPLICATED_HISTORY_DATE),
        "invalid_price_count": _reason_count(rows, REASON_NON_POSITIVE_PRICE),
        "future_date_count": _reason_count(rows, REASON_FUTURE_HISTORY_DATE),
        "stale_count": _reason_count(rows, REASON_STALE_LATEST_QUOTE),
        "insufficient_history_count": _reason_count(rows, REASON_INSUFFICIENT_HISTORY_POINTS),
    }


def _external_validation_summary(samples: list[dict[str, Any]], provider_checks: list[dict[str, Any]], *, external_test: bool) -> dict[str, Any]:
    rows = [row for row in samples if _is_external_row(row)]
    counts = _counts(rows)
    provider_call_keys = {
        (
            str(row.get("asset_type") or ""),
            str(row.get("symbol") or ""),
            str(row.get("endpoint") or ""),
        )
        for row in rows
        if row.get("endpoint")
    }
    provider_failures = [
        item
        for item in provider_checks
        if str(item.get("status") or "").lower() not in {"configured", "ok", "pass"}
        and str(item.get("reason_code") or "") not in TRANSIENT_EXTERNAL_REASON_CODES
    ]
    skipped = len([item for item in provider_checks if str(item.get("external_test") or "") == "not_run"])
    return {
        "provider_calls_attempted": len(provider_call_keys),
        "OK": counts.get("OK", 0),
        "WARN": counts.get("WARN", 0),
        "FAIL": counts.get("FAIL", 0),
        "RATE_LIMIT": _reason_count(rows, REASON_EXTERNAL_RATE_LIMIT),
        "SKIPPED": skipped if external_test else 0,
        "provider_failures": len(provider_failures),
        "transient_failures": sum(_reason_count(rows, reason) for reason in TRANSIENT_EXTERNAL_REASON_CODES),
    }


def _reason_count(rows: list[dict[str, Any]], reason_code: str) -> int:
    return sum(1 for row in rows if row.get("reason_code") == reason_code)


def _reason_codes(samples: list[dict[str, Any]], provider_checks: list[dict[str, Any]]) -> list[str]:
    values: list[str] = []
    for item in [*samples, *provider_checks]:
        reason = str(item.get("reason_code") or "").strip()
        if reason and reason not in values:
            values.append(reason)
    return values


def _first_warning_reason(samples: list[dict[str, Any]], provider_checks: list[dict[str, Any]]) -> str | None:
    warning = next((row for row in samples if row.get("status") == "WARN"), None)
    if warning and warning.get("reason_code"):
        return str(warning["reason_code"])
    provider_warning = next((item for item in provider_checks if str(item.get("status") or "").lower() in {"warn", "not_ready"}), None)
    if provider_warning and provider_warning.get("reason_code"):
        return str(provider_warning["reason_code"])
    return None


def format_sample_validation(result: SampleValidationResult) -> str:
    counts = _counts(result.samples)
    internal = result.internal_summary or _internal_validation_summary(result.samples)
    external = result.external_summary or _external_validation_summary(result.samples, result.provider_checks, external_test=bool(result.provider_checks))
    provider_failures = int(external.get("provider_failures", 0))
    rate_limited = int(external.get("RATE_LIMIT", 0)) > 0 or any(item.get("reason_code") == REASON_EXTERNAL_RATE_LIMIT for item in result.provider_checks)
    lines = [
        "LIVE SAMPLE VALIDATION",
        f"DB: {result.db_path}",
        f"LIVE SAMPLE VALIDATION STATUS: {result.status}",
        f"Requested period days: {result.requested_period_days}",
        f"History mode: {result.history_mode}",
        f"Samples: OK={counts.get('OK', 0)} WARN={counts.get('WARN', 0)} FAIL={counts.get('FAIL', 0)}",
        f"Provider failures: {provider_failures}",
        f"Rate limit detected: {str(rate_limited).lower()}",
        f"Report: {REPORT_PATH}",
        "INTERNAL SAMPLE VALIDATION:",
        f"total samples: {internal.get('total_samples', 0)}",
        f"OK: {internal.get('OK', 0)}",
        f"WARN: {internal.get('WARN', 0)}",
        f"FAIL: {internal.get('FAIL', 0)}",
        f"duplicate count: {internal.get('duplicate_count', 0)}",
        f"invalid price count: {internal.get('invalid_price_count', 0)}",
        f"future date count: {internal.get('future_date_count', 0)}",
        f"stale count: {internal.get('stale_count', 0)}",
        f"insufficient history count: {internal.get('insufficient_history_count', 0)}",
        "EXTERNAL PROVIDER SAMPLE VALIDATION:",
        f"provider calls attempted: {external.get('provider_calls_attempted', 0)}",
        f"OK: {external.get('OK', 0)}",
        f"WARN: {external.get('WARN', 0)}",
        f"FAIL: {external.get('FAIL', 0)}",
        f"RATE_LIMIT: {external.get('RATE_LIMIT', 0)}",
        f"SKIPPED: {external.get('SKIPPED', 0)}",
        f"provider failures: {external.get('provider_failures', 0)}",
        f"transient failures: {external.get('transient_failures', 0)}",
        "DATA DECISION:",
        f"release_gate: {result.release_gate}",
        f"promotion_allowed: {str(result.promotion_allowed).lower()}",
        "reason_codes: " + (", ".join(result.reason_codes) if result.reason_codes else REASON_VALIDATION_OK),
    ]
    if rate_limited:
        lines.append("Provider rate limit detected; internal candidate remains valid but external confirmation is incomplete.")
    if result.reason:
        lines.append(f"Reason: {result.reason}")
    if result.action:
        lines.append(f"Action: {result.action}")
    for item in result.samples[:30]:
        if item["status"] != "OK":
            lines.append(f"{item['status']}: {item['asset_type']} {item['symbol']} {item['sample_date']} {item.get('reason_code', '-')} {item['note']}")
    return "\n".join(lines)


def write_sample_validation_report(result: SampleValidationResult, report_path: str | Path = REPORT_PATH) -> None:
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    counts = _counts(result.samples)
    by_asset = _counts_by_asset(result.samples)
    warnings = [row for row in result.samples if row.get("status") == "WARN"]
    failures = [row for row in result.samples if row.get("status") == "FAIL"]
    internal = result.internal_summary or _internal_validation_summary(result.samples)
    external = result.external_summary or _external_validation_summary(result.samples, result.provider_checks, external_test=bool(result.provider_checks))
    recommendation = "READY" if result.status == "READY" else "READY_WITH_WARNINGS" if result.status == "READY_WITH_WARNINGS" else "NOT READY"
    lines = [
        "# Live Sample Validation Report",
        "",
        f"Generated: {result.generated_at}",
        f"DB: `{result.db_path}`",
        f"Overall status: **{result.status}**",
        f"Recommendation: **{recommendation}**",
        f"requested_period_days: `{result.requested_period_days}`",
        f"history_mode: `{result.history_mode}`",
        f"advanced_history_available: `{str(result.advanced_history_available).lower()}`",
        f"Samples OK/WARN/FAIL: `{counts.get('OK', 0)}/{counts.get('WARN', 0)}/{counts.get('FAIL', 0)}`",
        f"release_gate: `{result.release_gate}`",
        f"promotion_allowed: `{str(result.promotion_allowed).lower()}`",
        f"reason_codes: `{', '.join(result.reason_codes) if result.reason_codes else REASON_VALIDATION_OK}`",
    ]
    if result.reason:
        lines.extend(["", "## Readiness", "", f"- reason: `{_md(result.reason)}`", f"- action: `{_md(result.action)}`"])
    lines.extend(
        [
            "",
            "## Internal Sample Validation",
            "",
            f"- total samples: `{internal.get('total_samples', 0)}`",
            f"- OK: `{internal.get('OK', 0)}`",
            f"- WARN: `{internal.get('WARN', 0)}`",
            f"- FAIL: `{internal.get('FAIL', 0)}`",
            f"- duplicate count: `{internal.get('duplicate_count', 0)}`",
            f"- invalid price count: `{internal.get('invalid_price_count', 0)}`",
            f"- future date count: `{internal.get('future_date_count', 0)}`",
            f"- stale count: `{internal.get('stale_count', 0)}`",
            f"- insufficient history count: `{internal.get('insufficient_history_count', 0)}`",
            "",
            "## External Provider Sample Validation",
            "",
            f"- provider calls attempted: `{external.get('provider_calls_attempted', 0)}`",
            f"- OK: `{external.get('OK', 0)}`",
            f"- WARN: `{external.get('WARN', 0)}`",
            f"- FAIL: `{external.get('FAIL', 0)}`",
            f"- RATE_LIMIT: `{external.get('RATE_LIMIT', 0)}`",
            f"- SKIPPED: `{external.get('SKIPPED', 0)}`",
            f"- provider failures: `{external.get('provider_failures', 0)}`",
            f"- transient failures: `{external.get('transient_failures', 0)}`",
            "",
            "## Data Decision",
            "",
            f"- release_gate: `{result.release_gate}`",
            f"- promotion_allowed: `{str(result.promotion_allowed).lower()}`",
            f"- recommendation: `{recommendation}`",
        ]
    )
    lines.extend(["", "## Result By Asset Type", "", "| asset_type | OK | WARN | FAIL |", "|---|---:|---:|---:|"])
    for asset_type, asset_counts in by_asset.items():
        lines.append(f"| {asset_type} | {asset_counts.get('OK', 0)} | {asset_counts.get('WARN', 0)} | {asset_counts.get('FAIL', 0)} |")
    lines.extend(
        [
            "",
            "## Samples",
            "",
            "| scope | symbol | asset_type | provider | endpoint | sample_date | provider_date | db_value | provider_value | delta_pct | tolerance_pct | status | reason_code | note |",
            "|---|---|---|---|---|---|---|---:|---:|---:|---:|---|---|---|",
        ]
    )
    for row in result.samples:
        lines.append(
            "| {validation_scope} | {symbol} | {asset_type} | {provider} | {endpoint} | {sample_date} | {provider_date} | {db_value} | {provider_value} | {delta_pct} | {tolerance_pct} | {status} | {reason_code} | {note} |".format(
                **{key: _md(row.get(key)) for key in ("validation_scope", "symbol", "asset_type", "provider", "endpoint", "sample_date", "provider_date", "db_value", "provider_value", "delta_pct", "tolerance_pct", "status", "reason_code", "note")}
            )
        )
    if result.provider_checks:
        lines.extend(["", "## Provider External Tests", "", "| asset_type | provider | external_test | status | reason_code | message |", "|---|---|---|---|---|---|"])
        for item in result.provider_checks:
            lines.append(
                "| {asset_type} | {provider} | {external_test} | {status} | {reason_code} | {message} |".format(
                    **{key: _md(item.get(key)) for key in ("asset_type", "provider", "external_test", "status", "reason_code", "message")}
                )
            )
    lines.extend(["", "## Failures", ""])
    lines.extend([f"- `{_md(row.get('reason_code'))}` {row.get('asset_type')} {row.get('symbol')} {row.get('sample_date')}: {_md(row.get('note'))}" for row in failures] or ["- none"])
    lines.extend(["", "## Warnings", ""])
    lines.extend([f"- `{_md(row.get('reason_code'))}` {row.get('asset_type')} {row.get('symbol')} {row.get('sample_date')}: {_md(row.get('note'))}" for row in warnings] or ["- none"])
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Samples are deterministic: first, last, middle, and evenly spaced interior dates.",
            "- Historical samples use historical provider ranges such as `market_chart/range`, Frankfurter timeseries, or Twelve Data `time_series`.",
            "- Current price endpoints such as CoinGecko `simple/price` are used only for latest quote validation.",
            "- Provider nearest-date matches are WARN and do not block promotion when internal validation is clean.",
            "- Provider rate limit or transient provider failures produce `READY_WITH_WARNINGS` when the internal candidate is coherent.",
            "- API keys are never written to this report.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _live_instruments(db_path: str) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT symbol, asset_type, exchange, provider, provider_symbol, currency, data_mode, expected_frequency
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
            rows = conn.execute("SELECT date, close AS value FROM stock_prices_daily WHERE symbol=? AND COALESCE(exchange, '')=COALESCE(?, '') AND data_mode='live' ORDER BY date", (symbol, instrument.get("exchange"))).fetchall()
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


def _provider_latest_value(settings: Settings, instrument: dict[str, Any]) -> tuple[float, str | None, str]:
    asset_type = str(instrument["asset_type"]).upper()
    symbol = str(instrument["symbol"]).upper()
    if asset_type == "FX":
        if settings.fx_provider == "fake_live":
            values = _provider_values(settings, instrument, date.today().isoformat(), date.today().isoformat())
            provider_date, provider_value = _nearest_value(values, date.today().isoformat())
            if provider_value is None:
                raise ValueError("fake_live FX latest unavailable")
            return provider_value, provider_date, "internal fake_live latest"
        client = FrankfurterClient(settings.api_base_url, settings.cache_dir, settings.timeout_seconds, settings.use_cache, settings.max_retries, settings.use_cache_latest)
        payload = client.fetch_latest(base=instrument.get("exchange") or "USD", symbols=[symbol])
        rows = normalize_latest(payload, base=instrument.get("exchange") or "USD", source="frankfurter")
        row = next((item for item in rows if item.symbol == symbol), None)
        if row is None:
            raise ValueError("latest FX provider value missing")
        return float(row.rate), row.date, "frankfurter latest"
    if asset_type == "STOCK":
        provider = MockMarketDataProvider() if settings.stock_provider == "fake_live" else build_market_provider(settings.stock_provider, settings.twelve_data_api_key, False, settings.timeout_seconds, settings.max_retries)
        quote = provider.fetch_quote(symbol=instrument.get("provider_symbol") or symbol, asset_type="STOCK", exchange=instrument.get("exchange"))
        if quote.price is None:
            raise ValueError("latest stock provider value missing")
        return float(quote.price), quote.quote_time, "twelvedata quote" if settings.stock_provider != "fake_live" else "internal fake_live quote"
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
        quote = provider.fetch_quote(asset)
        if quote.price is None:
            raise ValueError("latest crypto provider value missing")
        return float(quote.price), quote.quote_time, "coingecko simple/price" if settings.crypto_provider != "fake_live" else "internal fake_live quote"
    raise ValueError(f"latest external validation unsupported for {asset_type}")


def _latest_quote(db_path: str, instrument: dict[str, Any]) -> dict[str, Any] | None:
    asset_type = str(instrument.get("asset_type") or "").upper()
    symbol = str(instrument.get("symbol") or "").upper()
    exchange = instrument.get("exchange")
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT symbol, asset_type, exchange, price, quote_time, provider, fetched_at, data_mode, source_updated_at
            FROM market_quotes_latest
            WHERE symbol=? AND asset_type=? AND COALESCE(exchange, '')=COALESCE(?, '') AND data_mode='live'
            LIMIT 1
            """,
            (symbol, asset_type, exchange),
        ).fetchone()
    return dict(row) if row else None


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


def _delta_pct(left: float | None, right: float | None) -> float | None:
    if left is None or right is None or float(right) == 0:
        return None
    return abs((float(left) / float(right)) - 1.0) * 100.0


def _minimum_history_points(settings: Settings, instrument: dict[str, Any]) -> int:
    asset_type = str(instrument.get("asset_type") or "").upper()
    requested_days = max(1, settings.live_default_days)
    expected_frequency = str(instrument.get("expected_frequency") or "").lower()
    if asset_type == "CRYPTO":
        return max(2, int(requested_days * 0.85))
    if asset_type in {"STOCK", "FX"}:
        return max(2, int(requested_days * 0.45))
    if asset_type == "MACRO" and "monthly" in expected_frequency:
        return 8
    if asset_type == "MACRO":
        return max(2, int(requested_days * 0.35))
    return 2


def _allowed_gap_days(instrument: dict[str, Any]) -> int:
    asset_type = str(instrument.get("asset_type") or "").upper()
    expected_frequency = str(instrument.get("expected_frequency") or "").lower()
    if asset_type == "CRYPTO":
        return 7
    if asset_type in {"STOCK", "FX"}:
        return 10
    if asset_type == "MACRO" and "monthly" in expected_frequency:
        return 75
    return 14


def _max_gap_days(dates: list[date]) -> int:
    ordered = sorted(dates)
    if len(ordered) < 2:
        return 0
    return max((right - left).days for left, right in zip(ordered, ordered[1:]))


def _provider_exception_check(instrument: dict[str, Any], exc: Exception, external_test: str) -> dict[str, Any]:
    reason_code = _reason_code_for_exception(exc)
    return {
        "asset_type": instrument.get("asset_type"),
        "provider": instrument.get("provider"),
        "external_test": external_test,
        "status": "warn" if reason_code in {REASON_EXTERNAL_RATE_LIMIT, REASON_PROVIDER_TLS_ERROR, REASON_EXTERNAL_PROVIDER_UNAVAILABLE} else "not_ready",
        "reason_code": reason_code,
        "message": f"{reason_code}: {_safe_exception_message(exc)}",
    }


def _reason_code_for_exception(exc: Exception) -> str:
    message = str(exc).lower()
    status_code = getattr(exc, "status_code", None)
    if status_code == 429 or "rate limit" in message or "too many requests" in message or "HTTP 429".lower() in message:
        return REASON_EXTERNAL_RATE_LIMIT
    if isinstance(exc, CoinGeckoProviderError) and getattr(exc, "status_code", None) in {408, 500, 502, 503, 504}:
        return REASON_EXTERNAL_PROVIDER_UNAVAILABLE
    classified = classify_external_error(exc)
    if classified.error_type == "SSL_ERROR":
        return REASON_PROVIDER_TLS_ERROR
    if classified.error_type in {"DNS_ERROR", "TIMEOUT", "HTTP_ERROR", "UNKNOWN"}:
        return REASON_EXTERNAL_PROVIDER_UNAVAILABLE
    if classified.error_type == "AUTH_ERROR":
        return REASON_PROVIDER_KEY_MISSING
    return REASON_EXTERNAL_PROVIDER_UNAVAILABLE


def _safe_exception_message(exc: Exception) -> str:
    classified = classify_external_error(exc)
    return classified.message


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
    reason_code: str = REASON_VALIDATION_OK,
    endpoint: str | None = None,
    validation_scope: str | None = None,
) -> dict[str, Any]:
    resolved_endpoint = endpoint or _endpoint_for_sample(instrument)
    resolved_scope = validation_scope or ("internal" if resolved_endpoint == "internal sqlite" else "external")
    return {
        "symbol": instrument.get("symbol"),
        "asset_type": instrument.get("asset_type"),
        "provider": instrument.get("provider"),
        "endpoint": resolved_endpoint,
        "validation_scope": resolved_scope,
        "sample_date": sample_date,
        "provider_date": provider_date or sample_date,
        "db_value": db_value,
        "provider_value": provider_value,
        "delta_pct": round(delta_pct, 6) if delta_pct is not None else None,
        "tolerance_pct": tolerance_pct,
        "status": status,
        "reason_code": reason_code,
        "note": note,
    }


def _is_internal_row(row: dict[str, Any]) -> bool:
    return str(row.get("validation_scope") or "").lower() == "internal" or row.get("endpoint") == "internal sqlite"


def _is_external_row(row: dict[str, Any]) -> bool:
    return not _is_internal_row(row)


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


def _latest_endpoint_for_sample(instrument: dict[str, Any]) -> str:
    asset_type = str(instrument.get("asset_type") or "").upper()
    if asset_type == "FX":
        return "frankfurter latest"
    if asset_type == "CRYPTO":
        return "coingecko simple/price"
    if asset_type == "STOCK":
        return "twelvedata quote"
    return "-"


def _counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status") or "UNKNOWN")
        counts[status] = counts.get(status, 0) + 1
    return counts


def _counts_by_asset(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for row in rows:
        asset_type = str(row.get("asset_type") or "UNKNOWN")
        status = str(row.get("status") or "UNKNOWN")
        result.setdefault(asset_type, {})
        result[asset_type][status] = result[asset_type].get(status, 0) + 1
    return dict(sorted(result.items()))


def _md(value: Any) -> str:
    if value is None:
        return "-"
    return str(value).replace("|", "\\|").replace("\n", " ") or "-"
