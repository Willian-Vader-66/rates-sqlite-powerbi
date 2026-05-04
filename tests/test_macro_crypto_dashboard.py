from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from fx_rates.api_server import create_app
from fx_rates.config import DEFAULTS, Settings
from fx_rates.crypto_providers import MockCryptoProvider, load_crypto_reference
from fx_rates.db_sqlite import (
    initialize_schema,
    insert_analysis_snapshots,
    upsert_crypto_prices_daily,
    upsert_instruments,
    upsert_macro_indicators_daily,
    upsert_stock_prices_daily,
)
from fx_rates.macro_providers import MockMacroProvider, load_macro_reference
from fx_rates.models import AnalysisSnapshotRow, InstrumentRow, StockPriceDailyRow
from fx_rates.utils import utc_now_iso


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


def test_macro_and_crypto_schema_exist(tmp_path: Path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    initialize_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        macro_indexes = {row[1] for row in conn.execute("PRAGMA index_list('macro_indicators_daily')").fetchall()}
        crypto_indexes = {row[1] for row in conn.execute("PRAGMA index_list('crypto_prices_daily')").fetchall()}

    assert {"macro_indicators_daily", "crypto_prices_daily"} <= tables
    assert "idx_macro_indicator_date" in macro_indexes
    assert "idx_crypto_symbol_date" in crypto_indexes


def test_mock_macro_provider_is_deterministic() -> None:
    indicator = load_macro_reference("data/reference/macro_indicators.csv")[0]
    provider = MockMacroProvider()

    first = provider.fetch_daily(indicator, "2026-01-01", "2026-01-10")
    second = provider.fetch_daily(indicator, "2026-01-01", "2026-01-10")

    assert len(first) == len(second)
    assert [row.value for row in first] == [row.value for row in second]
    assert first[0].indicator_code == "SELIC_DAILY"


def test_mock_crypto_provider_is_deterministic() -> None:
    asset = load_crypto_reference("data/reference/crypto_assets.csv")[0]
    provider = MockCryptoProvider()

    first = provider.fetch_daily(asset, "2026-01-01", "2026-01-05")
    second = provider.fetch_daily(asset, "2026-01-01", "2026-01-05")

    assert len(first) == 5
    assert [row.price_usd for row in first] == [row.price_usd for row in second]
    assert first[0].symbol == "BTC"


def test_dashboard_new_endpoints(tmp_path: Path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    initialize_schema(db_path)
    now = utc_now_iso()
    upsert_instruments(
        db_path,
        [
            InstrumentRow("AAPL", "Apple Inc", "STOCK", "NASDAQ", "USD", "Technology", "mock", "AAPL", 1, 1, now, now),
            InstrumentRow("BTC", "Bitcoin", "CRYPTO", "CRYPTO", "USD", "Crypto", "mock", "bitcoin", 1, 2, now, now),
            InstrumentRow("SELIC_DAILY", "Selic Daily Rate", "MACRO", None, None, "Macro", "mock", "11", 1, 3, now, now),
        ],
    )
    upsert_stock_prices_daily(
        db_path,
        [
            StockPriceDailyRow("2026-01-01", "AAPL", "NASDAQ", 100, 102, 99, 100, 100, 1000, "USD", "mock", now),
            StockPriceDailyRow("2026-01-30", "AAPL", "NASDAQ", 108, 111, 107, 110, 110, 1100, "USD", "mock", now),
        ],
    )
    insert_analysis_snapshots(
        db_path,
        [
            AnalysisSnapshotRow(
                symbol="AAPL",
                asset_type="STOCK",
                exchange="NASDAQ",
                generated_at=now,
                last_price=110,
                last_close=110,
                daily_return=0.02,
                change_30d=0.10,
                change_90d=0.10,
                change_1y=0.10,
                sma_20=105,
                sma_50=101,
                volatility_20=0.01,
                min_30d=100,
                max_30d=110,
                trend="UP",
                signal="BREAKOUT",
                notes=None,
            )
        ],
    )
    macro_rows = MockMacroProvider().fetch_daily(load_macro_reference("data/reference/macro_indicators.csv")[0], "2026-01-01", "2026-01-10")
    crypto_rows = MockCryptoProvider().fetch_daily(load_crypto_reference("data/reference/crypto_assets.csv")[0], "2026-01-01", "2026-01-10")
    upsert_macro_indicators_daily(db_path, macro_rows)
    upsert_crypto_prices_daily(db_path, crypto_rows)

    client = TestClient(create_app(_settings(db_path)))

    overview = client.get("/api/dashboard/market-overview")
    fixed = client.get("/api/dashboard/fixed-charts")
    top = client.get("/api/dashboard/top-stocks-30d?symbols=AAPL")
    macro_history = client.get("/api/macro/history?indicator_code=SELIC_DAILY")
    crypto_history = client.get("/api/crypto/history?symbol=BTC")

    assert overview.status_code == 200
    assert any(card["label"] == "BTC/USD" for card in overview.json()["cards"])
    assert fixed.status_code == 200
    assert fixed.json()["crypto"][0]["points"]
    assert top.status_code == 200
    assert top.json()["items"][0]["symbol"] == "AAPL"
    assert macro_history.json()["count"] > 0
    assert crypto_history.json()["count"] > 0
