from __future__ import annotations

import shutil
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from .api_smoke import smoke_live_api
from .config import Settings
from .db_sqlite import initialize_schema
from .live_samples import REPORT_PATH as SAMPLE_REPORT_PATH
from .live_samples import stock_sample_validation_readiness, validate_samples
from .live_validation import validate_live_database


def run_promote_live(
    settings: Settings,
    *,
    from_db: str | None = None,
    candidate_db: str | None = None,
    to_db: str,
    backup: bool = False,
    dry_run: bool = False,
    skip_samples: bool = False,
    skip_api_smoke: bool = False,
    smoke_port: int = 8001,
    samples_per_symbol: int = 5,
    expected_years: int | None = None,
    expected_days: int = 365,
) -> int:
    selected_source = candidate_db or from_db
    if not selected_source:
        print("promote-live aborted: provide --candidate-db or --from-db")
        return 2
    source = Path(selected_source).expanduser().resolve()
    target = Path(to_db).expanduser().resolve()
    if not source.exists():
        print(f"promote-live aborted: source DB not found: {source}")
        return 2

    validation = validate_live_database(str(source), expected_years=expected_years, expected_days=expected_days, report_path=None)
    if validation.status == "FAIL":
        print("promote-live aborted: source validate-live returned FAIL")
        for item in validation.critical_failures[:20]:
            print(f"FAIL: {item}")
        return 3
    if skip_samples:
        sample_status = _sample_report_status(source)
        if sample_status == "FAIL":
            print(f"promote-live aborted: existing sample validation report is FAIL: {SAMPLE_REPORT_PATH}")
            return 5
        print("Sample validation skipped explicitly by user flag.")
    else:
        readiness = stock_sample_validation_readiness(settings, db_path=str(source))
        if not readiness["ready"]:
            if "invalid" in str(readiness.get("reason") or "").lower():
                print("promote-live aborted: TWELVE_DATA_API_KEY invalid for stock external sample validation.")
            else:
                print("promote-live aborted: TWELVE_DATA_API_KEY missing for stock external sample validation.")
            print("Run promote-live inside the same session as run_live_pipeline.ps1 or set the key in the current session.")
            return 5
        sample = validate_samples(settings, db_path=str(source), samples_per_symbol=samples_per_symbol, report_path=None)
        if sample.status == "FAIL":
            print("promote-live aborted: validate-samples returned FAIL")
            failures = [row for row in sample.samples if row.get("status") == "FAIL"]
            for row in failures[:20]:
                print(f"FAIL: {row.get('asset_type')} {row.get('symbol')} {row.get('sample_date')} - {row.get('note')}")
            return 5
        if sample.status == "NOT READY":
            print("promote-live aborted: validate-samples returned NOT READY")
            if sample.reason:
                print(f"Reason: {sample.reason}")
            if sample.action:
                print(f"Action: {sample.action}")
            return 5

    if not skip_api_smoke:
        smoke_settings = replace(settings, db_path=str(source), api_port=smoke_port, market_data_demo_mode=False)
        smoke = smoke_live_api(smoke_settings, db_path=str(source), port=smoke_port, report_path=None)
        if smoke.get("status") == "FAIL":
            print("promote-live aborted: api smoke-live returned FAIL")
            for endpoint in smoke.get("endpoints", []):
                if endpoint.get("status") == "FAIL":
                    print(f"FAIL: {endpoint.get('path')} - {endpoint.get('message')}")
            return 4
    else:
        print("API smoke-live skipped explicitly by user flag.")

    if dry_run:
        print(f"promote-live dry-run OK: {source} can be promoted to {target}")
        return 0

    target.parent.mkdir(parents=True, exist_ok=True)
    backup_path = None
    if backup and target.exists():
        backup_path = _backup_path(target)
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(target, backup_path)
        print(f"Backup created: {backup_path}")

    shutil.copy2(source, target)
    initialize_schema(str(target))
    print(f"Promoted live DB: {source} -> {target}")
    if backup_path:
        print(f"Rollback: python -m fx_rates dashboard restore-backup --backup {backup_path}")
    return 0


def _sample_report_status(source_db: Path) -> str | None:
    path = Path(SAMPLE_REPORT_PATH)
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8", errors="replace")
    source_text = str(source_db.resolve())
    if f"DB: `{source_text}`" not in text and f"DB: {source_text}" not in text:
        return None
    if "Overall status: **FAIL**" in text or "LIVE SAMPLE VALIDATION STATUS: FAIL" in text:
        return "FAIL"
    if "Overall status: **WARN**" in text:
        return "WARN"
    if "Overall status: **OK**" in text:
        return "OK"
    return None


def run_restore_backup(*, backup: str, to_db: str) -> int:
    source = Path(backup).expanduser().resolve()
    target = Path(to_db).expanduser().resolve()
    if not source.exists():
        print(f"restore-backup aborted: backup not found: {source}")
        return 2
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    initialize_schema(str(target))
    print(f"Restored backup: {source} -> {target}")
    return 0


def _backup_path(target: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return target.parent / "backups" / f"{target.stem}-before-live-{timestamp}{target.suffix}"
