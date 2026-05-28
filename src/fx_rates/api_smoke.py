from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import uvicorn

from .api_server import create_app
from .config import Settings
from .db_sqlite import initialize_schema

REPORT_PATH = Path("docs/API_LIVE_SMOKE_REPORT.md")
HISTORY_SYMBOLS = ["BRL", "EUR", "BTC", "ETH", "AAPL", "MSFT", "NVDA", "SELIC_DAILY", "IPCA_MONTHLY"]
BASE_ENDPOINTS = [
    "/health",
    "/api/system/status",
    "/api/dashboard/summary",
    "/api/instruments",
    "/api/quotes/latest",
    "/api/analysis/latest",
]


def run_api_smoke_live(
    settings: Settings,
    *,
    db_path: str,
    host: str = "127.0.0.1",
    port: int = 8001,
    report_path: str | Path = REPORT_PATH,
) -> int:
    result = smoke_live_api(settings, db_path=db_path, host=host, port=port, report_path=report_path)
    print(format_api_smoke(result))
    return 1 if result["status"] == "FAIL" else 0


def smoke_live_api(
    settings: Settings,
    *,
    db_path: str,
    host: str = "127.0.0.1",
    port: int = 8001,
    report_path: str | Path | None = REPORT_PATH,
) -> dict[str, Any]:
    target_db = str(Path(db_path).expanduser().resolve())
    initialize_schema(target_db)
    effective = replace(settings, db_path=target_db, api_host=host, api_port=port, market_data_demo_mode=False)
    server = uvicorn.Server(
        uvicorn.Config(create_app(effective), host=host, port=port, log_level="warning", access_log=False)
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base_url = f"http://{host}:{port}"
    try:
        _wait_for_health(base_url)
        endpoint_results = [_check_endpoint(base_url, path) for path in _endpoints()]
        failures = [item for item in endpoint_results if item["status"] == "FAIL"]
        status = "FAIL" if failures else "OK"
        result = {
            "status": status,
            "db_path": target_db,
            "base_url": base_url,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "endpoints": endpoint_results,
            "failed_endpoints": [item["path"] for item in failures],
        }
        if report_path:
            write_api_smoke_report(result, report_path)
        return result
    finally:
        server.should_exit = True
        thread.join(timeout=5)


def format_api_smoke(result: dict[str, Any]) -> str:
    lines = [
        "API LIVE SMOKE",
        f"Status: {result.get('status')}",
        f"Base URL: {result.get('base_url')}",
        f"DB: {result.get('db_path')}",
        f"Endpoints tested: {len(result.get('endpoints') or [])}",
        f"Failed endpoints: {', '.join(result.get('failed_endpoints') or []) or '-'}",
    ]
    for item in result.get("endpoints", []):
        lines.append(f"{item['status']}: {item['path']} - {item.get('message') or 'OK'}")
    return "\n".join(lines)


def write_api_smoke_report(result: dict[str, Any], report_path: str | Path = REPORT_PATH) -> None:
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# API Live Smoke Report",
        "",
        f"Generated: {result.get('generated_at')}",
        f"DB: `{result.get('db_path')}`",
        f"Base URL: `{result.get('base_url')}`",
        f"Overall status: **{result.get('status')}**",
        "",
        "| endpoint | http_status | status | message |",
        "|---|---:|---|---|",
    ]
    for item in result.get("endpoints", []):
        lines.append(
            "| {path} | {http_status} | {status} | {message} |".format(
                path=_md(item.get("path")),
                http_status=_md(item.get("http_status")),
                status=_md(item.get("status")),
                message=_md(item.get("message")),
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _endpoints() -> list[str]:
    return BASE_ENDPOINTS + [f"/api/history/{symbol}" for symbol in HISTORY_SYMBOLS]


def _wait_for_health(base_url: str) -> None:
    deadline = time.time() + 20
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            _fetch_json(base_url + "/health")
            return
        except Exception as exc:
            last_error = exc
            time.sleep(0.25)
    raise RuntimeError(f"API did not become healthy: {last_error}")


def _check_endpoint(base_url: str, path: str) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(2):
        try:
            payload, http_status = _fetch_json(base_url + path, with_status=True)
            break
        except Exception as exc:
            last_error = exc
            if attempt == 0:
                time.sleep(0.5)
    else:
        return {"path": path, "http_status": None, "status": "FAIL", "message": str(last_error)}
    failures = _payload_failures(path, payload)
    return {
        "path": path,
        "http_status": http_status,
        "status": "FAIL" if failures else "OK",
        "message": "; ".join(failures) if failures else "OK",
    }


def _fetch_json(url: str, *, with_status: bool = False):
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            payload = json.loads(raw)
            return (payload, response.status) if with_status else payload
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {body[:200]}") from exc


def _payload_failures(path: str, payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        return ["response is not a JSON object"]
    if "traceback" in json.dumps(payload).lower() or "stack trace" in json.dumps(payload).lower():
        return ["response contains stack trace marker"]
    failures: list[str] = []
    if path == "/health":
        if payload.get("status") != "ok":
            failures.append("health status is not ok")
        if payload.get("data_mode") != "live":
            failures.append(f"expected data_mode=live, got {payload.get('data_mode')}")
    elif path == "/api/system/status":
        if payload.get("data_mode") != "live":
            failures.append(f"expected data_mode=live, got {payload.get('data_mode')}")
        if not payload.get("providers"):
            failures.append("providers missing")
        if not payload.get("historical_row_count"):
            failures.append("historical_row_count missing/zero")
        health = payload.get("data_health") if isinstance(payload.get("data_health"), dict) else {}
        if health.get("status") != "OK":
            failures.append(f"expected data_health=OK, got {health.get('status')}")
        counts = payload.get("data_mode_counts") if isinstance(payload.get("data_mode_counts"), dict) else {}
        for key in ("demo", "mixed", "unknown"):
            if int(counts.get(key) or 0) != 0:
                failures.append(f"expected {key}_count=0, got {counts.get(key)}")
        if int(payload.get("requested_days") or 0) != 365:
            failures.append(f"expected requested_days=365, got {payload.get('requested_days')}")
        if payload.get("history_mode") != "standard":
            failures.append(f"expected history_mode=standard, got {payload.get('history_mode')}")
    elif path == "/api/instruments":
        failures.extend(_items_failures(payload, require_origin=True, require_metadata=True))
    elif path in {"/api/quotes/latest", "/api/analysis/latest"}:
        failures.extend(_items_failures(payload, require_origin=True, require_metadata=False))
    elif path.startswith("/api/history/"):
        if int(payload.get("count") or payload.get("point_count") or 0) <= 0:
            failures.append("history is empty")
        items = payload.get("items") if isinstance(payload.get("items"), list) else []
        if items:
            modes = {str(item.get("data_mode") or "unknown").lower() for item in items if isinstance(item, dict)}
            if modes != {"live"}:
                failures.append("history items are not all live: " + ",".join(sorted(modes)))
            if not payload.get("unit_label") or not payload.get("value_label"):
                failures.append("history metadata unit_label/value_label missing")
    elif path.startswith("/api/dashboard/"):
        if not payload:
            failures.append("dashboard payload empty")
    return failures


def _items_failures(payload: dict[str, Any], *, require_origin: bool, require_metadata: bool) -> list[str]:
    failures: list[str] = []
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        return ["items empty or missing"]
    modes = {str(item.get("data_mode") or "unknown").lower() for item in items if isinstance(item, dict)}
    if require_origin and modes != {"live"}:
        failures.append("items are not all live: " + ",".join(sorted(modes)))
    missing_provider = [item.get("symbol") for item in items if isinstance(item, dict) and not item.get("provider")]
    if require_origin and missing_provider:
        failures.append("items missing provider")
    if require_metadata:
        missing_meta = [item.get("symbol") for item in items if isinstance(item, dict) and (not item.get("unit_label") or not item.get("value_label"))]
        if missing_meta:
            failures.append("items missing unit_label/value_label")
    return failures


def _md(value: Any) -> str:
    if value is None:
        return "-"
    return str(value).replace("|", "\\|").replace("\n", " ") or "-"
