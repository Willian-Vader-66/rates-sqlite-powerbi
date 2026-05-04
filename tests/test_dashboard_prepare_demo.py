from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from fx_rates.api_server import create_app
from fx_rates.dashboard_audit import audit_dashboard
from fx_rates.cli import main
from fx_rates.config import DEFAULTS, Settings


def _settings(db_path: str) -> Settings:
    return Settings(
        api_base_url=DEFAULTS.api_base_url,
        db_path=db_path,
        cache_dir="cache",
        log_file="logs/test.log",
        log_level="INFO",
        timeout_seconds=1,
        max_retries=0,
        use_cache=False,
        use_cache_latest=False,
        twelve_data_api_key="",
        market_data_provider="mock",
        market_data_demo_mode=True,
        api_host="127.0.0.1",
        api_port=8000,
    )


def test_prepare_demo_dashboard_populates_sqlite_without_api_key(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fx.sqlite"
    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)

    code = main(
        [
            "dashboard",
            "prepare-demo",
            "--years",
            "1",
            "--demo",
            "--db-path",
            str(db_path),
            "--cache-dir",
            str(tmp_path / "cache"),
            "--log-file",
            str(tmp_path / "app.log"),
        ]
    )

    assert code == 0

    client = TestClient(create_app(_settings(str(db_path))))
    status = client.get("/api/system/status").json()
    summary = client.get("/api/dashboard/summary").json()
    fixed = client.get("/api/dashboard/fixed-charts").json()
    top = client.get("/api/dashboard/top-stocks-30d").json()
    overview = client.get("/api/dashboard/market-overview").json()

    assert Path(status["db_path"]) == db_path.resolve()
    assert status["db_exists"] is True
    assert status["is_empty"] is False
    assert status["total_instruments"] > 30
    assert status["active_stocks"] >= 30
    assert status["active_currencies"] >= 5
    assert status["active_crypto"] >= 5
    assert status["active_macro"] >= 1
    assert status["latest_quote_count"] > 0
    assert status["latest_analysis_count"] > 0
    assert status["historical_row_count"] > 0
    assert status["date_min"] is not None
    assert status["date_max"] is not None

    assert summary["total_instruments"] > 30
    assert summary["active_stocks"] >= 30
    assert summary["active_currencies"] >= 5
    assert summary["active_crypto"] >= 5
    assert summary["active_macro"] >= 1
    assert summary["latest_quote_count"] > 0
    assert summary["latest_analysis_count"] > 0
    assert summary["instruments_without_analysis"] == 0
    assert summary["instruments_without_quotes"] == 0
    assert summary["failed_runs_count"] == 0

    fx_charts = {chart["id"]: chart for chart in fixed["fx"]}
    crypto_charts = {chart["id"]: chart for chart in fixed["crypto"]}
    macro_charts = {chart["id"]: chart for chart in fixed["macro"]}
    assert fx_charts["usd_brl_30d"]["points"]
    assert fx_charts["usd_eur_30d"]["points"]
    assert crypto_charts["btc_usd_30d"]["points"]
    assert crypto_charts["eth_usd_30d"]["points"]
    assert macro_charts["selic_30d"]["points"]
    assert top["count"] >= 10
    assert len(overview["cards"]) >= 4

    analysis = client.get("/api/analysis/latest").json()["items"]
    by_type: dict[str, list[dict]] = {}
    for item in analysis:
        by_type.setdefault(item["asset_type"], []).append(item)
    for asset_type in ["STOCK", "FX", "CRYPTO", "MACRO"]:
        useful = [
            item
            for item in by_type[asset_type]
            if item["trend"] != "UNKNOWN" and item["signal"] != "UNKNOWN"
        ]
        assert len(useful) / len(by_type[asset_type]) >= 0.9


def test_dashboard_audit_reports_coverage(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fx.sqlite"
    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)

    assert main(
        [
            "dashboard",
            "prepare-demo",
            "--years",
            "1",
            "--demo",
            "--db-path",
            str(db_path),
            "--cache-dir",
            str(tmp_path / "cache"),
            "--log-file",
            str(tmp_path / "app.log"),
        ]
    ) == 0

    audit = audit_dashboard(str(db_path), expected_years=1)

    assert audit["db_exists"] is True
    assert Path(audit["db_path"]) == db_path.resolve()
    assert audit["db_size_bytes"] > 0
    assert audit["historical_row_count"] > 0
    assert audit["date_min"] is not None
    assert audit["date_max"] is not None
    assert audit["is_empty"] is False
    assert audit["instruments_by_type"]["STOCK"] >= 30
    assert audit["quotes_by_type"]["CRYPTO"] >= 10
    assert audit["analysis_by_type"]["MACRO"] >= 1
    assert audit["important_ranges"]["USD/BRL"]["count"] > 0
    assert audit["important_ranges"]["BTC"]["count"] > 0
