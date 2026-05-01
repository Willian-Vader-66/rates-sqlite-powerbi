from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI

from .config import Settings
from .db_sqlite import (
    get_dashboard_summary,
    get_fx_history,
    get_latest_analysis,
    get_latest_quotes,
    get_stock_history,
    list_instruments,
)
from .market_providers import build_market_provider
from .utils import split_symbols


def create_app(settings: Settings) -> FastAPI:
    app = FastAPI(title="Local Market Data API", version="0.2.0")

    @app.get("/health")
    def health() -> dict[str, Any]:
        try:
            provider_status = build_market_provider(
                provider_name=settings.market_data_provider,
                api_key=settings.twelve_data_api_key,
                demo_mode=settings.market_data_demo_mode,
                timeout_seconds=settings.timeout_seconds,
                max_retries=settings.max_retries,
            ).status()
        except ValueError as exc:
            provider_status = {
                "name": settings.market_data_provider,
                "configured": False,
                "error": str(exc),
            }
        return {
            "status": "ok",
            "db_path": settings.db_path,
            "db_exists": Path(settings.db_path).exists(),
            "provider": provider_status,
        }

    @app.get("/api/instruments")
    def instruments(
        asset_type: str | None = None,
        active: bool | None = None,
        search: str | None = None,
    ) -> dict[str, Any]:
        rows = list_instruments(settings.db_path, asset_type=asset_type, active=active, search=search)
        return {"count": len(rows), "items": rows}

    @app.get("/api/stocks/history")
    def stocks_history(symbol: str, start: str | None = None, end: str | None = None) -> dict[str, Any]:
        rows = get_stock_history(settings.db_path, symbol=symbol, start=start, end=end)
        return {"symbol": symbol.strip().upper(), "count": len(rows), "items": rows}

    @app.get("/api/fx/history")
    def fx_history(base: str, symbol: str, start: str | None = None, end: str | None = None) -> dict[str, Any]:
        rows = get_fx_history(settings.db_path, base=base, symbol=symbol, start=start, end=end)
        return {"base": base.strip().upper(), "symbol": symbol.strip().upper(), "count": len(rows), "items": rows}

    @app.get("/api/quotes/latest")
    def quotes_latest(symbols: str | None = None, asset_type: str | None = None) -> dict[str, Any]:
        rows = get_latest_quotes(settings.db_path, symbols=_split_optional(symbols), asset_type=asset_type)
        return {"count": len(rows), "items": rows}

    @app.get("/api/analysis/latest")
    def analysis_latest(symbols: str | None = None, asset_type: str | None = None) -> dict[str, Any]:
        rows = get_latest_analysis(settings.db_path, symbols=_split_optional(symbols), asset_type=asset_type)
        return {"count": len(rows), "items": rows}

    @app.get("/api/dashboard/summary")
    def dashboard_summary() -> dict[str, Any]:
        return get_dashboard_summary(settings.db_path)

    return app


def _split_optional(raw: str | None) -> list[str] | None:
    if raw is None or not raw.strip():
        return None
    return split_symbols(raw)
