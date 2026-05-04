from __future__ import annotations

import sqlite3
from pathlib import Path
from datetime import date, timedelta

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
    history_end = date.fromisoformat(status["date_max"])
    history_30d = (history_end - timedelta(days=30)).isoformat()
    history_1y = history_end.replace(year=history_end.year - 1).isoformat()
    history_4y = history_end.replace(year=history_end.year - 4).isoformat()
    stock_30d = client.get(f"/api/stocks/history?symbol=AAPL&start={history_30d}&end={history_end}").json()
    stock_1y = client.get(f"/api/stocks/history?symbol=AAPL&start={history_1y}&end={history_end}").json()
    stock_4y = client.get(f"/api/stocks/history?symbol=AAPL&start={history_4y}&end={history_end}").json()
    fx_history = client.get(f"/api/fx/history?base=USD&symbol=BRL&start={history_30d}&end={history_end}").json()
    crypto_history = client.get(f"/api/crypto/history?symbol=BTC&start={history_30d}&end={history_end}").json()
    macro_history = client.get(f"/api/macro/history?indicator_code=SELIC_DAILY&start={history_30d}&end={history_end}").json()
    unknown_history = client.get(f"/api/stocks/history?symbol=NOPE&start={history_4y}&end={history_end}").json()
    aapl_quote = client.get("/api/quotes/latest?symbols=AAPL&asset_type=STOCK").json()
    latest_analysis = client.get("/api/analysis/latest").json()

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
    assert stock_30d["count"] > 0
    assert stock_1y["count"] > 0
    assert stock_4y["count"] > 0
    assert fx_history["count"] > 0
    assert crypto_history["count"] > 0
    assert macro_history["count"] > 0
    assert unknown_history["count"] == 0
    assert aapl_quote["count"] == 1
    assert 50 < aapl_quote["items"][0]["price"] < 1000
    assert latest_analysis["count"] == summary["latest_analysis_count"]

    analysis = latest_analysis["items"]
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
    aapl_consistency = next(item for item in audit["quote_consistency"] if item["label"] == "AAPL")
    assert aapl_consistency["status"] == "OK"
    assert 0.99 <= aapl_consistency["ratio"] <= 1.01


def test_prepare_demo_stock_latest_quotes_match_latest_history(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fx.sqlite"
    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)

    assert main(
        [
            "dashboard",
            "prepare-demo",
            "--years",
            "4",
            "--demo",
            "--db-path",
            str(db_path),
            "--cache-dir",
            str(tmp_path / "cache"),
            "--log-file",
            str(tmp_path / "app.log"),
        ]
    ) == 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        summary = TestClient(create_app(_settings(str(db_path)))).get("/api/dashboard/summary").json()
        aapl = conn.execute(
            """
            WITH latest_history AS (
                SELECT symbol, close, date,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn,
                       COUNT(*) OVER (PARTITION BY symbol) AS historical_rows,
                       MIN(date) OVER (PARTITION BY symbol) AS date_min,
                       MAX(date) OVER (PARTITION BY symbol) AS date_max
                FROM stock_prices_daily
                WHERE symbol='AAPL' AND close IS NOT NULL
            )
            SELECT q.price, q.bid, q.ask, h.close, h.date, h.historical_rows, h.date_min, h.date_max
            FROM market_quotes_latest AS q
            JOIN latest_history AS h ON h.symbol=q.symbol AND h.rn=1
            WHERE q.asset_type='STOCK' AND q.symbol='AAPL'
            """
        ).fetchone()
        inflated_stock_quotes = conn.execute(
            """
            SELECT symbol, price
            FROM market_quotes_latest
            WHERE asset_type='STOCK' AND price > 10000
            ORDER BY price DESC
            """
        ).fetchall()
        mismatched_quotes = conn.execute(
            """
            WITH latest_history AS (
                SELECT symbol, close,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
                FROM stock_prices_daily
                WHERE close IS NOT NULL
            )
            SELECT q.symbol, q.price, h.close, q.price / h.close AS ratio
            FROM market_quotes_latest AS q
            JOIN latest_history AS h ON h.symbol=q.symbol AND h.rn=1
            WHERE q.asset_type='STOCK'
              AND h.close IS NOT NULL
              AND (q.price / h.close > 100 OR q.price / h.close < 0.01)
            """
        ).fetchall()

    assert summary["total_instruments"] == 68
    assert summary["latest_quote_count"] == 68
    assert summary["latest_analysis_count"] == 68
    assert aapl is not None
    assert aapl["historical_rows"] > 1000
    assert 50 < aapl["price"] < 1000
    assert aapl["price"] == aapl["close"]
    assert aapl["bid"] < aapl["price"] < aapl["ask"]
    assert not inflated_stock_quotes
    assert not mismatched_quotes

    audit = audit_dashboard(str(db_path), expected_years=4)
    aapl_consistency = next(item for item in audit["quote_consistency"] if item["label"] == "AAPL")
    assert aapl_consistency["historical_rows"] > 1000
    assert aapl_consistency["status"] == "OK"
    assert aapl_consistency["ratio"] == 1.0
