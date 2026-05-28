from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from fx_rates.api_server import create_app
from fx_rates.config import DEFAULTS, Settings
from fx_rates.db_sqlite import initialize_schema, upsert_instruments, upsert_market_quotes_latest, upsert_stock_prices_daily
from fx_rates.models import InstrumentRow, MarketQuoteRow, StockPriceDailyRow
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
    assert payload["recommended_prepare_command"].startswith("python -m fx_rates dashboard build-live-db")
    assert "build-live-db" in payload["message"]


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


def test_api_history_and_latest_quotes_use_same_live_source_for_mixed_symbol(tmp_path: Path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    initialize_schema(db_path)
    upsert_instruments(
        db_path,
        [
            _instrument("AAPL", "mock", "demo"),
            _instrument("AAPL", "twelvedata", "live"),
        ],
    )
    upsert_stock_prices_daily(
        db_path,
        [
            _stock_row("2026-01-01", "AAPL", 10, "mock", "demo"),
            _stock_row("2026-01-02", "AAPL", 20, "twelvedata", "live"),
            _stock_row("2026-01-03", "AAPL", 30, "twelvedata", "live"),
        ],
    )
    upsert_market_quotes_latest(
        db_path,
        [
            _quote("AAPL", 30, "2026-01-03", "twelvedata", "live"),
        ],
    )
    client = TestClient(create_app(_settings(db_path)))

    history = client.get("/api/history/AAPL?asset_type=STOCK&start=2026-01-01&end=2026-01-03").json()
    quotes = client.get("/api/quotes/latest?asset_type=STOCK&symbols=AAPL").json()

    assert history["data_mode"] == "live"
    assert history["provider"] == "twelvedata"
    assert history["data_modes"] == ["live"]
    assert [item["close"] for item in history["items"]] == [20, 30]
    assert quotes["items"][0]["data_mode"] == "live"
    assert quotes["items"][0]["provider"] == "twelvedata"
    assert quotes["items"][0]["price"] == 30


def _instrument(symbol: str, provider: str, data_mode: str) -> InstrumentRow:
    return InstrumentRow(
        symbol=symbol,
        name=symbol,
        asset_type="STOCK",
        exchange="NASDAQ",
        currency="USD",
        sector="Technology",
        provider=provider,
        provider_symbol=symbol,
        data_mode=data_mode,
        is_active=1,
        priority=1,
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
    )


def _stock_row(raw_date: str, symbol: str, close: float, provider: str, data_mode: str) -> StockPriceDailyRow:
    return StockPriceDailyRow(
        date=raw_date,
        symbol=symbol,
        exchange="NASDAQ",
        open=close,
        high=close,
        low=close,
        close=close,
        adjusted_close=close,
        volume=100,
        currency="USD",
        provider=provider,
        fetched_at=f"{raw_date}T00:00:00Z",
        data_mode=data_mode,
    )


def _quote(symbol: str, price: float, quote_time: str, provider: str, data_mode: str) -> MarketQuoteRow:
    return MarketQuoteRow(
        symbol=symbol,
        asset_type="STOCK",
        exchange="NASDAQ",
        price=price,
        bid=None,
        ask=None,
        open=None,
        high=None,
        low=None,
        previous_close=None,
        change=None,
        percent_change=None,
        volume=None,
        quote_time=quote_time,
        provider=provider,
        fetched_at=f"{quote_time}T00:00:00Z",
        data_mode=data_mode,
    )
