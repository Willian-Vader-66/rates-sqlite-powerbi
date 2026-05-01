from __future__ import annotations

import sqlite3
from pathlib import Path

from fx_rates.analysis import build_analysis_snapshots
from fx_rates.db_sqlite import (
    initialize_schema,
    list_instruments,
    upsert_instruments,
    upsert_stock_prices_daily,
)
from fx_rates.market_providers import MockMarketDataProvider
from fx_rates.models import StockPriceDailyRow
from fx_rates.watchlist import load_stock_watchlist


def test_instrument_import_from_csv(tmp_path: Path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    initialize_schema(db_path)

    rows = load_stock_watchlist("data/reference/sample_stocks.csv", provider="mock")
    count = upsert_instruments(db_path, rows)

    instruments = list_instruments(db_path, asset_type="STOCK", active=True)
    assert count == len(rows)
    assert len(instruments) >= 5
    assert instruments[0]["symbol"] == "AAPL"


def test_stock_daily_upsert_is_idempotent(tmp_path: Path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    initialize_schema(db_path)
    provider = MockMarketDataProvider()
    rows = provider.fetch_stock_daily("AAPL", "2026-01-01", "2026-01-10", exchange="NASDAQ")

    assert upsert_stock_prices_daily(db_path, rows) == len(rows)
    assert upsert_stock_prices_daily(db_path, rows) == len(rows)

    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM stock_prices_daily WHERE symbol='AAPL'").fetchone()[0]

    assert count == len(rows)


def test_demo_provider_is_deterministic() -> None:
    provider = MockMarketDataProvider()
    first = provider.fetch_stock_daily("MSFT", "2026-01-05", "2026-01-09")
    second = provider.fetch_stock_daily("MSFT", "2026-01-05", "2026-01-09")

    assert [row.close for row in first] == [row.close for row in second]
    assert first[0].provider == "mock"


def test_analysis_calculation_with_known_stock_data(tmp_path: Path) -> None:
    db_path = str(tmp_path / "fx.sqlite")
    initialize_schema(db_path)
    rows = [
        StockPriceDailyRow(
            date=f"2026-01-{day:02d}" if day <= 31 else f"2026-02-{day - 31:02d}",
            symbol="TEST",
            exchange="NYSE",
            open=float(99 + day),
            high=float(101 + day),
            low=float(98 + day),
            close=float(100 + day),
            adjusted_close=float(100 + day),
            volume=1000 + day,
            currency="USD",
            provider="mock",
            fetched_at="2026-03-01T00:00:00+00:00",
        )
        for day in range(1, 61)
    ]
    upsert_stock_prices_daily(db_path, rows)

    snapshots = build_analysis_snapshots(db_path, symbols=["TEST"], asset_type="STOCK")

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.last_close == 160.0
    assert round(snapshot.sma_20 or 0, 2) == 150.5
    assert round(snapshot.sma_50 or 0, 2) == 135.5
    assert snapshot.trend == "UP"
    assert snapshot.signal in {"BREAKOUT", "STABLE"}
