from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import Settings
from .dashboard_prepare import run_prepare_live_dashboard
from .db_sqlite import get_system_status, initialize_schema
from .live_history import LiveHistoryPolicy, days_from_args, validate_requested_days
from .live_scope import release_scope_by_asset
from .live_validation import REPORT_PATH, LiveValidationResult, validate_live_database, write_live_validation_report
from .provider_status import providers_status

DEFAULT_LIVE_TEST_DB = ".tmp/live-full-test.sqlite"
ASSET_TYPES = ["FX", "CRYPTO", "STOCK", "MACRO"]


def run_live_full_test(
    settings: Settings,
    *,
    years: int | None = None,
    days: int | None = None,
    db_path: str | None = None,
    asset_type: str | None = "ALL",
    symbols: list[str] | None = None,
    top: int = 10,
    allow_partial: bool = False,
    external_test: bool = False,
    rate_limit_delay: float = 0.0,
    report_path: str | Path = REPORT_PATH,
) -> int:
    target_db = str(Path(db_path or settings.db_path or DEFAULT_LIVE_TEST_DB).expanduser().resolve())
    effective = replace(settings, db_path=target_db, market_data_demo_mode=False)
    selected = _selected_asset_types(asset_type)
    requested_days = days_from_args(days=days, years=years, default_days=settings.live_default_days)
    try:
        validate_requested_days(
            LiveHistoryPolicy(
                default_days=settings.live_default_days,
                max_free_days=settings.live_max_free_days,
                mode=settings.live_history_mode,
                advanced_max_years=settings.live_advanced_max_years,
            ),
            requested_days,
            provider_plan=settings.coingecko_api_plan if "CRYPTO" in selected else None,
        )
    except ValueError as exc:
        print(f"live-full-test aborted: {exc}")
        return 2

    print("LIVE FULL TEST")
    print(f"SQLite temp DB path: {target_db}")
    print(f"Asset types: {', '.join(selected)}")
    print(f"Requested days: {requested_days}")
    print(f"History mode: {settings.live_history_mode}")
    print(f"Symbols: {', '.join(symbols) if symbols else 'reference defaults'}")
    print(f"Top stocks: {top}")
    print(f"External provider test: {str(external_test).lower()}")
    print(f"Allow partial: {str(allow_partial).lower()}")
    if rate_limit_delay:
        print(f"Rate limit delay requested: {rate_limit_delay}s (provider-native rate limits still apply)")

    try:
        _reset_live_test_db(target_db)
    except ValueError as exc:
        print(f"live-full-test aborted: {exc}")
        return 2
    initialize_schema(target_db)

    provider_state = providers_status(effective, test_external=external_test)
    missing = [
        item
        for item in provider_state["providers"]
        if item["asset_type"] in selected and (not item["configured"] or not item["available"] or item.get("external_test") == "fail")
    ]
    if missing and not allow_partial:
        print("live-full-test aborted before ingest: provider validation failed")
        for item in missing:
            print(_provider_line(item))
        result = _empty_result(target_db, "NOT READY", provider_state, missing)
        _write_live_full_test_summary(result, None, provider_state, attempted=[], report_path=report_path)
        return 2

    attempted: list[dict[str, Any]] = []
    for current_type in selected:
        item = next((row for row in provider_state["providers"] if row["asset_type"] == current_type), None)
        if item and (not item["configured"] or not item["available"] or item.get("external_test") == "fail"):
            attempted.append({"asset_type": current_type, "status": "UNSUPPORTED", "message": item.get("message") or "provider not configured/available"})
            print(f"Skipping {current_type}: provider not configured/available (--allow-partial enabled)")
            continue

        asset_symbols = symbols if symbols and len(selected) == 1 else release_scope_by_asset().get(current_type)
        code = run_prepare_live_dashboard(
            effective,
            days=requested_days,
            allow_mixed=True,
            replace_demo=True,
            symbols=asset_symbols,
            asset_type=current_type,
            stock_limit=top,
        )
        attempted.append({"asset_type": current_type, "status": "OK" if code == 0 else "FAIL", "exit_code": code})
        if code != 0 and not allow_partial:
            print(f"live-full-test stopped: {current_type} ingest failed with exit_code={code}")
            validation = validate_live_database(target_db, expected_days=requested_days, report_path=report_path)
            _write_live_full_test_summary("NOT READY", validation, provider_state, attempted, report_path=report_path)
            return code or 1

    validation = validate_live_database(target_db, expected_days=requested_days, report_path=report_path)
    final_status = _final_status(validation, attempted, allow_partial=allow_partial)
    _write_live_full_test_summary(final_status, validation, provider_state, attempted, report_path=report_path)
    print(f"LIVE FULL TEST RESULT: {final_status}")
    if validation.status == "FAIL" or final_status != "READY":
        return 1
    return 0


def _selected_asset_types(asset_type: str | None) -> list[str]:
    if not asset_type or asset_type.strip().upper() == "ALL":
        return list(ASSET_TYPES)
    normalized = asset_type.strip().upper()
    if normalized not in ASSET_TYPES:
        raise ValueError("--asset-type deve ser STOCK, FX, CRYPTO, MACRO ou ALL")
    return [normalized]


def _reset_live_test_db(db_path: str) -> None:
    path = Path(db_path).expanduser().resolve()
    data_fx = (Path.cwd() / "data" / "fx.sqlite").resolve()
    if path == data_fx:
        raise ValueError("refusing to run live-full-test against data/fx.sqlite; use a temporary --db-path")
    path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(path) + suffix)
        if candidate.exists():
            candidate.unlink()


def _final_status(validation: LiveValidationResult, attempted: list[dict[str, Any]], *, allow_partial: bool) -> str:
    failed_attempts = [item for item in attempted if item.get("status") in {"FAIL", "UNSUPPORTED"}]
    if validation.status == "OK" and not failed_attempts:
        return "READY"
    if allow_partial and attempted and any(item.get("status") == "OK" for item in attempted):
        return "PARTIALLY FUNCTIONAL"
    return "NOT READY"


def _empty_result(db_path: str, status: str, provider_state: dict[str, Any], missing: list[dict[str, Any]]) -> str:
    return status


def _provider_line(item: dict[str, Any]) -> str:
    missing_env = ",".join(item.get("missing_env") or []) or "-"
    message = item.get("message") or "-"
    return (
        f"{item['asset_type']}: provider={item['provider']} configured={item['configured']} "
        f"available={item['available']} key_present={item.get('key_present')} "
        f"key_valid_format={item.get('key_valid_format')} external_test={item.get('external_test')} "
        f"missing_env={missing_env} message={message}"
    )


def _write_live_full_test_summary(
    final_status: str,
    validation: LiveValidationResult | None,
    provider_state: dict[str, Any],
    attempted: list[dict[str, Any]],
    *,
    report_path: str | Path,
) -> None:
    if validation is not None:
        write_live_validation_report(validation, report_path)
        path = Path(report_path)
        existing = path.read_text(encoding="utf-8") if path.exists() else ""
    else:
        path = Path(report_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        existing = "# Live Full Test Report\n\n"

    system = {}
    if validation is not None:
        try:
            system = get_system_status(validation.db_path)
        except Exception:
            system = {}
    generated = datetime.now(timezone.utc).isoformat()
    lines = [
        "",
        "## Live Full Test Run",
        "",
        f"Generated: {generated}",
        f"Final status: **{final_status}**",
        "",
        "### Providers",
        "",
        "| asset_type | provider | configured | available | key_present | key_valid_format | external_test | status | message |",
        "|---|---|---:|---:|---:|---:|---|---|---|",
    ]
    for item in provider_state.get("providers", []):
        lines.append(
            "| {asset_type} | {provider} | {configured} | {available} | {key_present} | {key_valid_format} | {external_test} | {status} | {message} |".format(
                asset_type=_md(item.get("asset_type")),
                provider=_md(item.get("provider")),
                configured=_md(item.get("configured")),
                available=_md(item.get("available")),
                key_present=_md(item.get("key_present")),
                key_valid_format=_md(item.get("key_valid_format")),
                external_test=_md(item.get("external_test")),
                status=_md(item.get("status")),
                message=_md(item.get("message")),
            )
        )
    lines.extend(["", "### Asset Attempts", "", "| asset_type | status | exit_code | message |", "|---|---|---:|---|"])
    for item in attempted:
        lines.append(
            "| {asset_type} | {status} | {exit_code} | {message} |".format(
                asset_type=_md(item.get("asset_type")),
                status=_md(item.get("status")),
                exit_code=_md(item.get("exit_code")),
                message=_md(item.get("message")),
            )
        )
    if system:
        lines.extend(
            [
                "",
                "### Temporary DB Totals",
                "",
                f"- instruments: `{system.get('total_instruments', 0)}`",
                f"- historical rows: `{system.get('historical_row_count', 0)}`",
                f"- date_min: `{system.get('date_min') or '-'}`",
                f"- date_max: `{system.get('date_max') or '-'}`",
                f"- data_mode: `{system.get('data_mode') or '-'}`",
            ]
        )
    path.write_text(existing.rstrip() + "\n" + "\n".join(lines) + "\n", encoding="utf-8")


def _md(value: Any) -> str:
    if value is None:
        return "-"
    return str(value).replace("|", "\\|").replace("\n", " ") or "-"
