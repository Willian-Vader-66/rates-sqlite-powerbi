from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from fx_rates.api_server import create_app
from fx_rates.config import DEFAULTS, Settings
from fx_rates.db_sqlite import initialize_schema, upsert_instruments
from fx_rates.watchlist import load_stock_watchlist


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


def test_api_health(tmp_path: Path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    initialize_schema(db_path)
    client = TestClient(create_app(_settings(db_path)))

    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["provider"]["name"] == "mock"


def test_api_system_status_reports_empty_database_path(tmp_path: Path) -> None:
    db_path = tmp_path / "fx.sqlite"
    initialize_schema(str(db_path))
    client = TestClient(create_app(_settings(str(db_path))))

    response = client.get("/api/system/status")

    assert response.status_code == 200
    payload = response.json()
    assert Path(payload["db_path"]) == db_path.resolve()
    assert payload["db_exists"] is True
    assert payload["db_size_bytes"] > 0
    assert payload["total_instruments"] == 0
    assert payload["latest_quote_count"] == 0
    assert payload["latest_analysis_count"] == 0
    assert payload["historical_row_count"] == 0
    assert payload["is_empty"] is True
    assert payload["recommended_prepare_command"] == "python -m fx_rates dashboard prepare-demo --years 4 --demo"
    assert "Run prepare-demo" in payload["message"]


def test_api_dashboard_summary(tmp_path: Path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    initialize_schema(db_path)
    rows = load_stock_watchlist("data/reference/sample_stocks.csv", provider="mock")
    upsert_instruments(db_path, rows)
    client = TestClient(create_app(_settings(db_path)))

    response = client.get("/api/dashboard/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_instruments"] == len(rows)
    assert payload["active_stocks"] == len(rows)
    assert payload["latest_quote_count"] == 0
