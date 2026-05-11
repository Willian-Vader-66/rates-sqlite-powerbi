from __future__ import annotations

from pathlib import Path

from fx_rates.config import DEFAULTS
from fx_rates.dashboard_market_audit import audit_market
from fx_rates.dashboard_prepare import run_prepare_demo_dashboard, run_prepare_live_dashboard
from fx_rates.db_sqlite import (
    get_data_mode_summary,
    get_system_status,
    initialize_schema,
    upsert_instruments,
    upsert_market_quotes_latest,
    upsert_stock_prices_daily,
)
from fx_rates.models import InstrumentRow, MarketQuoteRow, StockPriceDailyRow
from fx_rates.provider_status import providers_status
from fx_rates.cli import main


def _instrument(symbol: str, provider: str | None, data_mode: str = "unknown") -> InstrumentRow:
    return InstrumentRow(
        symbol=symbol,
        name=symbol,
        asset_type="STOCK",
        exchange="NASDAQ",
        currency="USD",
        sector="Technology",
        provider=provider,
        provider_symbol=symbol,
        is_active=1,
        priority=1,
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        data_mode=data_mode,
    )


def _stock_row(symbol: str, provider: str | None, data_mode: str = "unknown") -> StockPriceDailyRow:
    return StockPriceDailyRow(
        date="2026-01-02",
        symbol=symbol,
        exchange="NASDAQ",
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        adjusted_close=10.5,
        volume=100,
        currency="USD",
        provider=provider,
        fetched_at="2026-01-02T00:00:00Z",
        data_mode=data_mode,
    )


def _quote(symbol: str, provider: str | None, data_mode: str = "unknown") -> MarketQuoteRow:
    return MarketQuoteRow(
        symbol=symbol,
        asset_type="STOCK",
        exchange="NASDAQ",
        price=10.5,
        bid=None,
        ask=None,
        open=10.0,
        high=11.0,
        low=9.0,
        previous_close=10.0,
        change=0.5,
        percent_change=5.0,
        volume=100,
        quote_time="2026-01-02",
        provider=provider,
        fetched_at="2026-01-02T00:00:00Z",
        data_mode=data_mode,
    )


def _db(tmp_path: Path) -> str:
    db_path = str(tmp_path / "mode.sqlite")
    initialize_schema(db_path)
    return db_path


def test_data_mode_summary_empty_dataset_is_unknown(tmp_path: Path) -> None:
    db_path = _db(tmp_path)

    status = get_system_status(db_path)

    assert status["data_mode"] == "unknown"
    assert status["is_empty"] is True


def test_data_mode_summary_demo_dataset(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    upsert_instruments(db_path, [_instrument("AAPL", "mock")])
    upsert_stock_prices_daily(db_path, [_stock_row("AAPL", "mock")])
    upsert_market_quotes_latest(db_path, [_quote("AAPL", "mock")])

    summary = get_data_mode_summary(db_path)
    status = get_system_status(db_path)

    assert summary["data_mode"] == "demo"
    assert status["data_mode"] == "demo"
    assert "mock" in status["providers"]
    assert status["data_warning"]


def test_data_mode_summary_live_dataset(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    upsert_instruments(db_path, [_instrument("MSFT", "twelvedata", data_mode="live")])
    upsert_stock_prices_daily(db_path, [_stock_row("MSFT", "twelvedata", data_mode="live")])
    upsert_market_quotes_latest(db_path, [_quote("MSFT", "twelvedata", data_mode="live")])

    assert get_data_mode_summary(db_path)["data_mode"] == "live"


def test_data_mode_summary_mixed_dataset(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    upsert_instruments(db_path, [_instrument("AAPL", "mock"), _instrument("MSFT", "twelvedata", data_mode="live")])
    upsert_stock_prices_daily(db_path, [_stock_row("AAPL", "mock"), _stock_row("MSFT", "twelvedata", data_mode="live")])
    upsert_market_quotes_latest(db_path, [_quote("AAPL", "mock"), _quote("MSFT", "twelvedata", data_mode="live")])

    summary = get_data_mode_summary(db_path)

    assert summary["data_mode"] == "mixed"
    assert summary["data_mode_counts"]["demo"] > 0
    assert summary["data_mode_counts"]["live"] > 0


def test_data_mode_summary_unknown_origin(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    upsert_instruments(db_path, [_instrument("ZZZ", None)])

    assert get_data_mode_summary(db_path)["data_mode"] == "unknown"


def test_providers_status_does_not_require_real_internet_or_reveal_keys(monkeypatch) -> None:
    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)
    payload = providers_status(DEFAULTS)

    stock = next(item for item in payload["providers"] if item["asset_type"] == "STOCK")

    assert stock["status"] == "not_configured"
    assert stock["api_key_detected"] is False
    assert payload["external_test"] == "skipped"


def test_prepare_live_without_stock_key_fails_controlled(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)
    code = main([
        "dashboard",
        "prepare-live",
        "--years",
        "1",
        "--asset-type",
        "STOCK",
        "--db-path",
        str(tmp_path / "live.sqlite"),
        "--cache-dir",
        str(tmp_path / "cache"),
        "--log-file",
        str(tmp_path / "app.log"),
    ])

    assert code == 2


def test_prepare_live_fake_provider_writes_live_rows(tmp_path: Path) -> None:
    db_path = str(tmp_path / "live.sqlite")
    settings = _fake_live_settings(db_path, tmp_path)
    initialize_schema(db_path)

    code = run_prepare_live_dashboard(settings, years=1, asset_type="STOCK", symbols=["AAPL"], stock_limit=1)

    assert code == 0
    status = get_system_status(db_path)
    assert status["data_mode"] == "live"
    assert status["providers"] == ["fake_live"]


def test_prepare_live_mixed_requires_explicit_flag(tmp_path: Path) -> None:
    db_path = str(tmp_path / "mixed.sqlite")
    demo_settings = _demo_settings(db_path, tmp_path)
    live_settings = _fake_live_settings(db_path, tmp_path)
    initialize_schema(db_path)
    assert run_prepare_demo_dashboard(demo_settings, years=1, demo=True, stock_limit=1) == 0

    blocked = run_prepare_live_dashboard(live_settings, years=1, asset_type="STOCK", symbols=["AAPL"], stock_limit=1)
    allowed = run_prepare_live_dashboard(live_settings, years=1, asset_type="STOCK", symbols=["AAPL"], stock_limit=1, allow_mixed=True)

    assert blocked == 3
    assert allowed == 0
    assert get_system_status(db_path)["data_mode"] == "mixed"


def test_prepare_live_replace_demo_replaces_selected_demo_rows(tmp_path: Path) -> None:
    db_path = str(tmp_path / "replace.sqlite")
    demo_settings = _demo_settings(db_path, tmp_path)
    live_settings = _fake_live_settings(db_path, tmp_path)
    initialize_schema(db_path)
    assert run_prepare_demo_dashboard(demo_settings, years=1, demo=True, stock_limit=1) == 0

    code = run_prepare_live_dashboard(
        live_settings,
        years=1,
        asset_type="STOCK",
        symbols=["AAPL"],
        stock_limit=1,
        replace_demo=True,
        allow_mixed=True,
    )

    assert code == 0
    with __import__("sqlite3").connect(db_path) as conn:
        modes = {row[0] for row in conn.execute("SELECT DISTINCT data_mode FROM stock_prices_daily WHERE symbol='AAPL'")}
    assert modes == {"live"}


def test_audit_market_detects_bad_stock_unit(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    upsert_instruments(db_path, [_instrument("AAPL", "twelvedata", data_mode="live")])
    upsert_stock_prices_daily(db_path, [_stock_row("AAPL", "twelvedata", data_mode="live")])
    upsert_market_quotes_latest(db_path, [_quote("AAPL", "twelvedata", data_mode="live")])
    with __import__("sqlite3").connect(db_path) as conn:
        conn.execute("UPDATE stock_prices_daily SET currency='BRL' WHERE symbol='AAPL'")
        conn.commit()

    audit = audit_market(db_path)
    item = next(row for row in audit["items"] if row["symbol"] == "AAPL")

    assert "STOCK_UNIT_SUSPICIOUS" in item["flags"]


def _demo_settings(db_path: str, tmp_path: Path):
    return DEFAULTS.__class__(
        **{
            **DEFAULTS.__dict__,
            "db_path": db_path,
            "cache_dir": str(tmp_path / "cache"),
            "log_file": str(tmp_path / "app.log"),
            "timeout_seconds": 1,
            "max_retries": 0,
            "market_data_provider": "mock",
            "market_data_demo_mode": True,
        }
    )


def _fake_live_settings(db_path: str, tmp_path: Path):
    return DEFAULTS.__class__(
        **{
            **DEFAULTS.__dict__,
            "db_path": db_path,
            "cache_dir": str(tmp_path / "cache"),
            "log_file": str(tmp_path / "app.log"),
            "timeout_seconds": 1,
            "max_retries": 0,
            "market_data_provider": "mock",
            "market_data_demo_mode": False,
            "stock_provider": "fake_live",
            "fx_provider": "fake_live",
            "crypto_provider": "fake_live",
            "macro_provider": "fake_live",
        }
    )
