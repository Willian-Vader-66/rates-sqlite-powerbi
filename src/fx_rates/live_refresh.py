from __future__ import annotations

import sqlite3
import time
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from .analysis import build_analysis_snapshots
from .config import Settings
from .dashboard_prepare import (
    DEFAULT_CRYPTO_REFERENCE,
    DEFAULT_CURRENCY_REFERENCE,
    DEFAULT_DASHBOARD_STOCK_REFERENCE,
    DEFAULT_MACRO_REFERENCE,
    LiveStage,
    _stage_live_crypto,
    _stage_live_fx,
    _stage_live_macro,
    _stage_live_stocks,
    _validate_live_stage,
)
from .db_sqlite import (
    commit_prepared_live_dataset,
    finish_ingest_run,
    get_system_status,
    insert_analysis_snapshots,
    start_ingest_run,
)
from .live_first import ASSET_TYPES
from .provider_status import providers_status
from .utils import normalize_symbol_list


def run_refresh_live(
    settings: Settings,
    *,
    asset_type: str | None = "ALL",
    symbols: list[str] | None = None,
    since: str | None = None,
    external_test: bool = False,
    dry_run: bool = False,
    rate_limit_delay: float = 0.0,
) -> int:
    selected_types = _selected_asset_types(asset_type)
    requested_symbols = normalize_symbol_list(symbols) if symbols else None
    target_db = str(Path(settings.db_path).expanduser().resolve())
    effective = replace(settings, db_path=target_db, market_data_demo_mode=False)

    print("LIVE REFRESH")
    print(f"SQLite DB path: {target_db}")
    print(f"Asset types: {', '.join(selected_types)}")
    print(f"Symbols: {', '.join(requested_symbols) if requested_symbols else 'active live instruments'}")
    print(f"Since: {since or 'last date per symbol'}")
    print(f"Dry run: {str(dry_run).lower()}")

    if external_test:
        provider_state = providers_status(effective, test_external=True)
        blocked = [
            item for item in provider_state.get("providers", [])
            if item.get("asset_type") in selected_types and (not item.get("configured") or not item.get("available") or item.get("external_test") == "fail")
        ]
        if blocked:
            print("refresh-live aborted before DB mutation: provider validation failed")
            for item in blocked:
                print(f"{item.get('asset_type')}: provider={item.get('provider')} status={item.get('status')} message={item.get('message')}")
            return 2

    targets = _refresh_targets(target_db, selected_types, requested_symbols)
    if not targets:
        print("refresh-live found no active live instruments to update.")
        return 1

    plan = [_target_plan(target_db, item, since=since) for item in targets]
    for item in plan:
        print(f"{item['asset_type']} {item['symbol']}: {item['start']} -> {item['end']} status={item['status']}")
    if dry_run:
        print("LIVE REFRESH STATUS: DRY RUN")
        return 0

    run_id = start_ingest_run(target_db, mode="dashboard_refresh_live", base="LIV", symbols=[item["symbol"] for item in plan], start=since, end=date.today().isoformat())
    updated = 0
    failures: list[str] = []
    try:
        for item in plan:
            if item["status"] == "UP_TO_DATE":
                continue
            if not _live_origin_ok(item):
                failures.append(f"{item['asset_type']} {item['symbol']}: existing row is not live/provider-real")
                continue
            stage = _stage_one(effective, item["asset_type"], item["symbol"], item["start"], item["end"])
            validation = _validate_live_stage(stage, [item["asset_type"]], [item["symbol"]], settings=effective)
            if stage.failures or validation:
                failures.extend(stage.failures)
                failures.extend(validation)
                continue
            row_count = commit_prepared_live_dataset(
                target_db,
                instruments=stage.instruments,
                stock_rows=stage.stock_rows,
                fx_rows=stage.fx_rows,
                crypto_rows=stage.crypto_rows,
                macro_rows=stage.macro_rows,
                quote_rows=stage.quote_rows,
                analysis_rows=[],
                replace_demo=False,
                asset_types=[item["asset_type"]],
                symbols=[item["symbol"]],
            )
            snapshots = build_analysis_snapshots(target_db, symbols=[item["symbol"]], asset_type=item["asset_type"])
            row_count += insert_analysis_snapshots(target_db, snapshots)
            updated += row_count
            if rate_limit_delay:
                time.sleep(rate_limit_delay)
        status = "FAIL" if failures and updated == 0 else "WARN" if failures else "OK"
        finish_ingest_run(target_db, run_id, status=status, row_count=updated, error="; ".join(sorted(set(failures))) if failures else None)
    except Exception as exc:
        finish_ingest_run(target_db, run_id, status="FAIL", row_count=updated, error=str(exc))
        raise

    summary = get_system_status(target_db)
    print(f"LIVE REFRESH STATUS: {'FAIL' if failures and updated == 0 else 'WARN' if failures else 'OK'}")
    print(f"Rows written: {updated}")
    print(f"Historical rows: {summary.get('historical_row_count')}")
    print(f"Date range: {summary.get('date_min')} to {summary.get('date_max')}")
    for failure in sorted(set(failures)):
        print(f"FAIL: {failure}")
    return 1 if failures and updated == 0 else 0


def _stage_one(settings: Settings, asset_type: str, symbol: str, start: str, end: str) -> LiveStage:
    if asset_type == "FX":
        return _stage_live_fx(settings, DEFAULT_CURRENCY_REFERENCE, date.fromisoformat(start), date.fromisoformat(end), [symbol])
    if asset_type == "CRYPTO":
        return _stage_live_crypto(settings, DEFAULT_CRYPTO_REFERENCE, start, end, [symbol])
    if asset_type == "MACRO":
        return _stage_live_macro(settings, DEFAULT_MACRO_REFERENCE, start, end, [symbol])
    return _stage_live_stocks(settings, DEFAULT_DASHBOARD_STOCK_REFERENCE, start, end, stock_limit=1000, symbols=[symbol])


def _selected_asset_types(asset_type: str | None) -> list[str]:
    if not asset_type or asset_type.strip().upper() == "ALL":
        return list(ASSET_TYPES)
    normalized = asset_type.strip().upper()
    if normalized not in ASSET_TYPES:
        raise ValueError("--asset-type deve ser STOCK, FX, CRYPTO, MACRO ou ALL")
    return [normalized]


def _refresh_targets(db_path: str, asset_types: list[str], symbols: list[str] | None) -> list[dict[str, Any]]:
    clauses = ["is_active=1", "data_mode='live'"]
    params: list[Any] = []
    if asset_types:
        placeholders = ",".join("?" for _ in asset_types)
        clauses.append(f"asset_type IN ({placeholders})")
        params.extend(asset_types)
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        clauses.append(f"symbol IN ({placeholders})")
        params.extend(symbols)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT symbol, asset_type, exchange, provider, data_mode
            FROM instruments
            WHERE {' AND '.join(clauses)}
            ORDER BY asset_type, priority, symbol
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def _target_plan(db_path: str, target: dict[str, Any], *, since: str | None) -> dict[str, Any]:
    last_date = _last_history_date(db_path, target["asset_type"], target["symbol"], target.get("exchange"))
    if since:
        start = date.fromisoformat(since)
    elif last_date:
        start = date.fromisoformat(last_date) + timedelta(days=1)
    else:
        start = date.today() - timedelta(days=7)
    end = date.today()
    planned = dict(target)
    planned["start"] = start.isoformat()
    planned["end"] = end.isoformat()
    planned["last_date"] = last_date
    planned["status"] = "UP_TO_DATE" if start > end else "READY"
    return planned


def _last_history_date(db_path: str, asset_type: str, symbol: str, exchange: str | None) -> str | None:
    with sqlite3.connect(db_path) as conn:
        if asset_type == "FX":
            row = conn.execute("SELECT MAX(date) FROM fx_rates WHERE base=? AND symbol=? AND data_mode='live'", (exchange or "USD", symbol)).fetchone()
        elif asset_type == "CRYPTO":
            row = conn.execute("SELECT MAX(date) FROM crypto_prices_daily WHERE symbol=? AND data_mode='live'", (symbol,)).fetchone()
        elif asset_type == "MACRO":
            row = conn.execute("SELECT MAX(date) FROM macro_indicators_daily WHERE indicator_code=? AND data_mode='live'", (symbol,)).fetchone()
        else:
            row = conn.execute("SELECT MAX(date) FROM stock_prices_daily WHERE symbol=? AND data_mode='live'", (symbol,)).fetchone()
    return str(row[0]) if row and row[0] else None


def _live_origin_ok(item: dict[str, Any]) -> bool:
    provider = str(item.get("provider") or "").lower()
    mode = str(item.get("data_mode") or "").lower()
    return mode == "live" and provider and "mock" not in provider and "demo" not in provider
