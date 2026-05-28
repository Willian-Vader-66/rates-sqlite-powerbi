from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from fx_rates.api_server import create_app
from fx_rates.cli import main
from fx_rates.config import DEFAULTS, Settings
from fx_rates.dashboard_audit import audit_dashboard
from fx_rates.dashboard_market_audit import audit_market


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


def test_dashboard_display_metadata_is_explicit(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fx.sqlite"
    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)

    assert main(
        [
            "dashboard",
            "prepare-demo",
            "--days",
            "365",
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
    assert audit["suspicious_values"] == []

    client = TestClient(create_app(_settings(str(db_path))))
    instruments = client.get("/api/instruments?active=true").json()["items"]
    assert instruments
    assert all(item["display_unit"] for item in instruments)
    assert all(item["value_format"] for item in instruments)

    by_key = {(item["asset_type"], item["symbol"]): item for item in instruments}
    assert by_key[("STOCK", "AAPL")]["display_pair"] == "AAPL/USD"
    assert by_key[("STOCK", "AAPL")]["value_format"] == "currency_usd"
    assert by_key[("FX", "BRL")]["display_pair"] == "USD/BRL"
    assert by_key[("FX", "EUR")]["display_pair"] == "USD/EUR"
    assert by_key[("CRYPTO", "BTC")]["display_pair"] == "BTC/USD"
    assert by_key[("MACRO", "SELIC_DAILY")]["display_unit"] == "% a.d."

    fixed = client.get("/api/dashboard/fixed-charts").json()
    usd_brl = fixed["fx"][0]
    btc = fixed["crypto"][0]
    selic = fixed["macro"][0]
    assert usd_brl["display_pair"] == "USD/BRL"
    assert usd_brl["axis_label"] == "BRL per USD"
    assert btc["display_unit"] == "USD"
    assert selic["display_unit"] == "% a.d."

    aapl_history = client.get("/api/stocks/history?symbol=AAPL").json()
    assert aapl_history["display_pair"] == "AAPL/USD"
    assert aapl_history["display_unit"] == "USD"
    assert aapl_history["point_count"] >= 250
    aapl_start = date.fromisoformat(aapl_history["start_date"])
    aapl_end = date.fromisoformat(aapl_history["end_date"])
    assert (aapl_end - aapl_start).days >= 360
    assert aapl_end >= date.today() - timedelta(days=10)

    brl_history = client.get("/api/fx/history?base=USD&symbol=BRL").json()
    assert brl_history["display_pair"] == "USD/BRL"
    assert brl_history["display_unit"] == "BRL per 1 USD"
    assert brl_history["point_count"] >= 360

    quote = client.get("/api/quotes/latest?symbols=AAPL&asset_type=STOCK").json()["items"][0]
    assert quote["display_pair"] == "AAPL/USD"
    assert 50 < quote["price"] < 1000

    fixed_90d = client.get("/api/dashboard/fixed-charts?period=90D").json()
    assert fixed_90d["fx"][0]["period"] == "90D"
    assert fixed_90d["fx"][0]["point_count"] >= 85

    overview = client.get("/api/dashboard/overview?period=90D").json()
    assert overview["period"] == "90D"
    assert overview["summary"]["total_instruments"] == 68
    assert overview["fixed_charts"]["fx"][0]["period"] == "90D"
    assert overview["technical_highlights"]["period"] == "90D"

    highlights = client.get("/api/dashboard/technical-highlights?period=90D").json()
    assert highlights["period"] == "90D"
    assert highlights["positive_momentum"] or highlights["negative_momentum"] or highlights["stable"]

    ranking = client.get("/api/dashboard/performance-ranking?period=90D&asset_type=ALL").json()
    assert ranking["period"] == "90D"
    assert ranking["count"] > 0
    assert ranking["top"]
    assert ranking["bottom"]
    assert "technical_label" in ranking["top"][0]

    system_status = client.get("/api/system/status").json()
    assert system_status["data_mode"] == "demo"
    assert "Values generated for UI testing" in system_status["data_warning"]

    market_audit = audit_market(str(db_path), with_live_sample=False)
    assert market_audit["data_mode"]["data_mode"] == "demo"
    assert market_audit["summary"]["total_instruments"] == 68
    assert market_audit["summary"]["without_history"] == 0
    assert market_audit["summary"]["without_quote"] == 0
    assert market_audit["ranking"]["status"] == "OK"

    for symbol in ["BRL", "EUR", "BTC", "ETH", "AAPL", "SELIC_DAILY", "FED_FUNDS_DAILY"]:
        generic_history = client.get(f"/api/history/{symbol}?period=90D").json()
        assert generic_history["point_count"] > 0
        assert generic_history["message"] is None
