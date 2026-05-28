from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import Settings
from .dashboard_prepare import run_prepare_live_dashboard
from .db_sqlite import get_system_status, initialize_schema
from .live_history import LiveHistoryPolicy, days_from_args, validate_requested_days
from .live_scope import ASSET_TYPES, release_scope_by_asset
from .live_validation import LiveValidationResult, validate_live_database, write_live_validation_report
from .provider_status import providers_status

DEFAULT_LIVE_CANDIDATE_DB = ".tmp/live-main-candidate.sqlite"
LIVE_BUILD_REPORT = Path("docs/LIVE_BUILD_REPORT.md")
LIVE_AUDIT_REPORT = Path("docs/LIVE_AUDIT_REPORT.md")
LIVE_FIRST_SCOPE = release_scope_by_asset()


def run_build_live_db(
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
    report_path: str | Path = LIVE_BUILD_REPORT,
) -> int:
    target_db = str(Path(db_path or settings.db_path or DEFAULT_LIVE_CANDIDATE_DB).expanduser().resolve())
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
        print(f"build-live-db aborted: {exc}")
        return 2
    effective = replace(settings, db_path=target_db, market_data_demo_mode=False)

    print("LIVE-FIRST DB BUILD")
    print(f"SQLite candidate DB path: {target_db}")
    print(f"Asset types: {', '.join(selected)}")
    print(f"Requested days: {requested_days}")
    print(f"History mode: {settings.live_history_mode}")
    print(f"Advanced history: {'enabled' if settings.live_history_mode == 'advanced' else 'disabled'}")
    print(f"Advanced max: {settings.live_advanced_max_years} years with paid providers")
    print(f"Symbols: {', '.join(symbols) if symbols else 'LIVE-FIRST scope'}")
    print(f"Top stocks: {top}")
    print(f"External provider test: {str(external_test).lower()}")
    print(f"Allow partial: {str(allow_partial).lower()}")
    if rate_limit_delay:
        print(f"Rate limit delay requested: {rate_limit_delay}s")

    try:
        _reset_candidate_db(target_db)
    except ValueError as exc:
        print(f"build-live-db aborted: {exc}")
        return 2
    initialize_schema(target_db)

    provider_state = providers_status(effective, test_external=external_test)
    missing = [
        item
        for item in provider_state.get("providers", [])
        if item.get("asset_type") in selected and (not item.get("configured") or not item.get("available") or item.get("external_test") == "fail")
    ]
    if missing and not allow_partial:
        print("build-live-db aborted before ingest: provider validation failed")
        for item in missing:
            print(_provider_line(item))
        _write_build_report("NOT READY", None, provider_state, [], report_path=report_path)
        print("LIVE-FIRST DB BUILD STATUS: NOT READY")
        _print_build_summary(
            "NOT READY",
            target_db,
            None,
            provider_state,
            sample_validation_required=True,
            sample_validation_status="NOT_RUN",
        )
        return 2

    attempts: list[dict[str, Any]] = []
    for current_type in selected:
        provider_item = next((row for row in provider_state.get("providers", []) if row.get("asset_type") == current_type), None)
        if provider_item and (not provider_item.get("configured") or not provider_item.get("available") or provider_item.get("external_test") == "fail"):
            attempts.append({"asset_type": current_type, "status": "UNSUPPORTED", "message": provider_item.get("message") or "provider not available"})
            print(f"Skipping {current_type}: provider unavailable (--allow-partial enabled)")
            continue
        scoped_symbols = _symbols_for_asset(current_type, symbols=symbols, top=top)
        code = run_prepare_live_dashboard(
            effective,
            days=requested_days,
            allow_mixed=True,
            replace_demo=True,
            symbols=scoped_symbols,
            asset_type=current_type,
            stock_limit=max(top, len(scoped_symbols)),
        )
        attempts.append({"asset_type": current_type, "status": "OK" if code == 0 else "FAIL", "exit_code": code, "symbols": ",".join(scoped_symbols)})
        if code != 0 and not allow_partial:
            validation = validate_live_database(target_db, expected_days=requested_days, report_path=None)
            _write_build_report("NOT READY", validation, provider_state, attempts, report_path=report_path)
            print("LIVE-FIRST DB BUILD STATUS: NOT READY")
            _print_build_summary(
                "NOT READY",
                target_db,
                validation,
                provider_state,
                sample_validation_required=True,
                sample_validation_status="NOT_RUN",
            )
            return code or 1

    validation = validate_live_database(target_db, expected_days=requested_days, report_path=None)
    final_status = _final_status(validation, attempts, allow_partial=allow_partial)
    _write_build_report(final_status, validation, provider_state, attempts, report_path=report_path)
    print(f"LIVE-FIRST DB BUILD STATUS: {final_status}")
    _print_build_summary(
        final_status,
        target_db,
        validation,
        provider_state,
        sample_validation_required=True,
        sample_validation_status="NOT_RUN",
    )
    return 1 if final_status == "NOT READY" else 0


def run_audit_live(db_path: str, *, expected_years: int | None = None, expected_days: int = 365, report_path: str | Path = LIVE_AUDIT_REPORT) -> int:
    result = validate_live_database(db_path, expected_years=expected_years, expected_days=expected_days, report_path=report_path)
    print(_format_audit_live(result))
    return 1 if result.status == "FAIL" else 0


def _format_audit_live(result: LiveValidationResult) -> str:
    lines = [
        "LIVE AUDIT",
        f"DB: {result.db_path}",
        f"LIVE AUDIT STATUS: {result.status}",
        f"Data mode: {result.summary.get('data_mode', '-')}",
        f"Providers: {', '.join(result.summary.get('providers') or []) or '-'}",
        f"Historical rows: {result.summary.get('historical_rows', 0)}",
        f"Date range: {result.summary.get('date_min') or '-'} to {result.summary.get('date_max') or '-'}",
        f"History mode: {result.summary.get('history_mode', '-')}",
        f"Requested days: {result.summary.get('requested_days', '-')}",
        f"Advanced history: {'enabled' if result.summary.get('advanced_history_enabled') else 'disabled'}",
        f"Symbols checked: {len(result.symbols)}",
        f"Critical failures: {len(result.critical_failures)}",
        f"Warnings: {len(result.warnings)}",
    ]
    lines.extend(f"FAIL: {item}" for item in result.critical_failures[:30])
    lines.extend(f"WARN: {item}" for item in result.warnings[:30])
    monthly_macro = [
        row
        for row in result.symbols
        if row.get("asset_type") == "MACRO" and row.get("expected_frequency") == "monthly"
    ]
    for row in monthly_macro:
        lines.extend(
            [
                f"MACRO {row.get('symbol')}:",
                f"  frequency: {row.get('expected_frequency')}",
                f"  latest_date: {row.get('date_max') or '-'}",
                f"  stale_days: {row.get('stale_days') if row.get('stale_days') is not None else '-'}",
                f"  allowed_stale_days: {row.get('allowed_stale_days') if row.get('allowed_stale_days') is not None else '-'}",
                f"  status: {row.get('stale_status') or row.get('status')}",
            ]
        )
    return "\n".join(lines)


def _selected_asset_types(asset_type: str | None) -> list[str]:
    if not asset_type or asset_type.strip().upper() == "ALL":
        return list(ASSET_TYPES)
    normalized = asset_type.strip().upper()
    if normalized not in ASSET_TYPES:
        raise ValueError("--asset-type deve ser STOCK, FX, CRYPTO, MACRO ou ALL")
    return [normalized]


def _symbols_for_asset(asset_type: str, *, symbols: list[str] | None, top: int) -> list[str]:
    if symbols:
        return [item.strip().upper() for item in symbols if item.strip()]
    scoped = list(release_scope_by_asset()[asset_type])
    if asset_type == "STOCK":
        return scoped[: max(1, min(top, len(scoped)))]
    return scoped


def _reset_candidate_db(db_path: str) -> None:
    path = Path(db_path).expanduser().resolve()
    data_fx = (Path.cwd() / "data" / "fx.sqlite").resolve()
    if path == data_fx:
        raise ValueError("refusing to build a fresh live candidate directly on data/fx.sqlite; use .tmp then promote-live")
    path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(path) + suffix)
        if candidate.exists():
            candidate.unlink()


def _final_status(validation: LiveValidationResult, attempts: list[dict[str, Any]], *, allow_partial: bool) -> str:
    failed = [item for item in attempts if item.get("status") in {"FAIL", "UNSUPPORTED"}]
    succeeded = [item for item in attempts if item.get("status") == "OK"]
    if failed and allow_partial and succeeded:
        return "PARTIALLY FUNCTIONAL"
    if validation.status == "FAIL" or failed:
        return "NOT READY"
    if validation.status == "WARN":
        return "READY_WITH_WARNINGS"
    return "CANDIDATE_READY"


def _print_build_summary(
    final_status: str,
    target_db: str,
    validation: LiveValidationResult | None,
    provider_state: dict[str, Any],
    *,
    sample_validation_required: bool,
    sample_validation_status: str,
) -> None:
    system = _candidate_system_summary(target_db)
    critical = validation.critical_failures if validation is not None else []
    warnings = validation.warnings if validation is not None else []
    providers = system.get("providers") or [
        item.get("provider")
        for item in provider_state.get("providers", [])
        if item.get("provider") and item.get("configured")
    ]
    print(f"Candidate DB path: {target_db}")
    print(f"total_instruments: {system.get('total_instruments', 0)}")
    print(f"historical_rows: {system.get('historical_row_count', 0)}")
    print(f"date_min/date_max: {system.get('date_min') or '-'} to {system.get('date_max') or '-'}")
    print(f"data_mode: {system.get('data_mode') or '-'}")
    print(f"providers: {', '.join(providers) if providers else '-'}")
    print(f"critical_failures: {len(critical)}")
    print(f"warnings: {len(warnings)}")
    print(f"sample_validation_required: {str(sample_validation_required).lower()}")
    print(f"sample_validation_status: {sample_validation_status}")
    for item in critical[:10]:
        print(f"FAIL: {item}")
    for item in warnings[:10]:
        print(f"WARN: {item}")
    if final_status == "NOT READY":
        print("next command: fix the failures above, then rerun build-live-db.")
    else:
        print("next command: python -m fx_rates dashboard validate-samples --db-path .tmp/live-main-candidate.sqlite --samples-per-symbol 5 --external-test")


def _candidate_system_summary(target_db: str) -> dict[str, Any]:
    path = Path(target_db)
    if not path.exists():
        return {}
    try:
        return get_system_status(str(path))
    except Exception:
        return {}


def _write_build_report(
    final_status: str,
    validation: LiveValidationResult | None,
    provider_state: dict[str, Any],
    attempts: list[dict[str, Any]],
    *,
    report_path: str | Path,
) -> None:
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    generated = datetime.now(timezone.utc).isoformat()
    system = {}
    if validation is not None:
        try:
            system = get_system_status(validation.db_path)
        except Exception:
            system = {}
    lines = [
        "# Live-First Build Report",
        "",
        f"Generated: {generated}",
        f"Final status: **{final_status}**",
        "",
        "## Scope",
        "",
        "| asset_type | symbols |",
        "|---|---|",
    ]
    for asset_type, values in LIVE_FIRST_SCOPE.items():
        lines.append(f"| {asset_type} | {', '.join(values)} |")
    lines.extend(["", "## Providers", "", "| asset_type | provider | configured | available | external_test | status | message |", "|---|---|---:|---:|---|---|---|"])
    for item in provider_state.get("providers", []):
        lines.append(
            "| {asset_type} | {provider} | {configured} | {available} | {external_test} | {status} | {message} |".format(
                **{key: _md(item.get(key)) for key in ("asset_type", "provider", "configured", "available", "external_test", "status", "message")}
            )
        )
    lines.extend(["", "## Asset Attempts", "", "| asset_type | status | exit_code | symbols | message |", "|---|---|---:|---|---|"])
    for item in attempts:
        lines.append(
            "| {asset_type} | {status} | {exit_code} | {symbols} | {message} |".format(
                asset_type=_md(item.get("asset_type")),
                status=_md(item.get("status")),
                exit_code=_md(item.get("exit_code")),
                symbols=_md(item.get("symbols")),
                message=_md(item.get("message")),
            )
        )
    if system:
        lines.extend(
            [
                "",
                "## Candidate DB",
                "",
                f"- path: `{system.get('db_path')}`",
                f"- data_mode: `{system.get('data_mode')}`",
                f"- instruments: `{system.get('total_instruments')}`",
                f"- historical rows: `{system.get('historical_row_count')}`",
                f"- date_min: `{system.get('date_min') or '-'}`",
                f"- date_max: `{system.get('date_max') or '-'}`",
                f"- providers: `{', '.join(system.get('providers') or []) or '-'}`",
                f"- sample_validation_required: `true`",
                f"- sample_validation_status: `NOT_RUN`",
                f"- next command: `python -m fx_rates dashboard validate-samples --db-path .tmp/live-main-candidate.sqlite --samples-per-symbol 5 --external-test`",
            ]
        )
    if validation is not None:
        lines.extend(["", "## Validation", "", f"- status: `{validation.status}`"])
        lines.extend([f"- FAIL: {item}" for item in validation.critical_failures] or ["- critical failures: none"])
        lines.extend([f"- WARN: {item}" for item in validation.warnings] or ["- warnings: none"])
    lines.extend(["", "## Safety", "", "- This command builds a new candidate DB and does not import demo rows.", "- API keys are never written to this report."])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if validation is not None and Path(report_path).resolve() == LIVE_BUILD_REPORT.resolve():
        write_live_validation_report(validation, LIVE_AUDIT_REPORT)


def _provider_line(item: dict[str, Any]) -> str:
    missing_env = ",".join(item.get("missing_env") or []) or "-"
    message = item.get("message") or "-"
    return (
        f"{item.get('asset_type')}: provider={item.get('provider')} configured={item.get('configured')} "
        f"available={item.get('available')} key_present={item.get('key_present')} "
        f"key_valid_format={item.get('key_valid_format')} external_test={item.get('external_test')} "
        f"missing_env={missing_env} message={message}"
    )


def _md(value: Any) -> str:
    if value is None:
        return "-"
    return str(value).replace("|", "\\|").replace("\n", " ") or "-"
