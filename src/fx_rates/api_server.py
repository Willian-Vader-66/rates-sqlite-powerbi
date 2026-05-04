from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI

from .config import Settings
from .display_metadata import build_display_metadata
from .db_sqlite import (
    get_crypto_history,
    get_dashboard_overview,
    get_dashboard_summary,
    get_fixed_dashboard_charts,
    get_fx_history,
    get_latest_analysis,
    get_latest_quotes,
    get_macro_history,
    get_market_overview,
    get_performance_ranking,
    get_stock_history,
    get_system_status,
    get_technical_highlights,
    get_top_stocks_30d,
    period_to_days,
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

    @app.get("/api/system/status")
    def system_status() -> dict[str, Any]:
        return get_system_status(settings.db_path)

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
        return _history_response(rows, symbol=symbol, asset_type="STOCK", start=start, end=end)

    @app.get("/api/fx/history")
    def fx_history(base: str, symbol: str, start: str | None = None, end: str | None = None) -> dict[str, Any]:
        rows = get_fx_history(settings.db_path, base=base, symbol=symbol, start=start, end=end)
        return _history_response(rows, symbol=symbol, asset_type="FX", base=base, start=start, end=end)

    @app.get("/api/crypto/history")
    def crypto_history(symbol: str, start: str | None = None, end: str | None = None) -> dict[str, Any]:
        rows = get_crypto_history(settings.db_path, symbol=symbol, start=start, end=end)
        return _history_response(rows, symbol=symbol, asset_type="CRYPTO", start=start, end=end)

    @app.get("/api/macro/history")
    def macro_history(indicator_code: str, start: str | None = None, end: str | None = None) -> dict[str, Any]:
        rows = get_macro_history(settings.db_path, indicator_code=indicator_code, start=start, end=end)
        return _history_response(rows, symbol=indicator_code, asset_type="MACRO", start=start, end=end)

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

    @app.get("/api/dashboard/fixed-charts")
    def dashboard_fixed_charts(days: int | None = None, period: str | None = None) -> dict[str, Any]:
        period_label, period_days = period_to_days(period, default=90)
        requested_days = max(1, min(days if days is not None else period_days, 1460))
        return get_fixed_dashboard_charts(settings.db_path, days=requested_days, period=period_label)

    @app.get("/api/dashboard/top-stocks-30d")
    def dashboard_top_stocks_30d(symbols: str | None = None, days: int = 30) -> dict[str, Any]:
        requested = _split_optional(symbols) or _default_top_stock_symbols()
        items = get_top_stocks_30d(settings.db_path, symbols=requested, days=max(1, min(days, 365)))
        return {"count": len(items), "items": items}

    @app.get("/api/dashboard/market-overview")
    def dashboard_market_overview(period: str | None = None, days: int | None = None) -> dict[str, Any]:
        period_label, period_days = period_to_days(period, default=90)
        requested_days = max(1, min(days if days is not None else period_days, 1460))
        return get_market_overview(settings.db_path, days=requested_days, period=period_label)

    @app.get("/api/dashboard/overview")
    def dashboard_overview(period: str | None = "90D") -> dict[str, Any]:
        period_label, days = period_to_days(period, default=90)
        return get_dashboard_overview(settings.db_path, days=days, period=period_label)

    @app.get("/api/dashboard/technical-highlights")
    def dashboard_technical_highlights(period: str | None = "90D") -> dict[str, Any]:
        period_label, days = period_to_days(period, default=90)
        return get_technical_highlights(settings.db_path, days=days, period=period_label)

    @app.get("/api/dashboard/performance-ranking")
    def dashboard_performance_ranking(
        period: str | None = "90D",
        asset_type: str | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        period_label, days = period_to_days(period, default=90)
        normalized_limit = max(1, min(limit, 50))
        normalized_asset_type = None if asset_type is None or asset_type.strip().upper() == "ALL" else asset_type
        return get_performance_ranking(
            settings.db_path,
            days=days,
            period=period_label,
            asset_type=normalized_asset_type,
            limit=normalized_limit,
        )

    return app


def _split_optional(raw: str | None) -> list[str] | None:
    if raw is None or not raw.strip():
        return None
    return split_symbols(raw)


def _default_top_stock_symbols() -> list[str]:
    path = Path("data/reference/top10_dashboard_stocks.csv")
    if not path.exists():
        return ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "JPM", "AVGO"]
    import csv

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        symbols = [(row.get("symbol") or "").strip().upper() for row in reader]
    return [symbol for symbol in symbols if symbol]


def _history_response(
    rows: list[dict[str, Any]],
    *,
    symbol: str,
    asset_type: str,
    base: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    normalized_symbol = symbol.strip().upper()
    metadata = _metadata_from_rows(rows, symbol=normalized_symbol, asset_type=asset_type, base=base)
    actual_start = rows[0]["date"] if rows else None
    actual_end = rows[-1]["date"] if rows else None
    label = metadata.get("display_pair") or normalized_symbol
    requested = _requested_period(start, end)
    message = None
    if not rows:
        message = f"No {label} history available for {requested}. Run: python -m fx_rates dashboard prepare-demo --years 4 --demo"
    payload = {
        "symbol": normalized_symbol,
        "asset_type": asset_type,
        "base": base.strip().upper() if base else metadata.get("base_currency"),
        "count": len(rows),
        "point_count": len(rows),
        "start_date": actual_start,
        "end_date": actual_end,
        "requested_start": start,
        "requested_end": end,
        "period": requested,
        "items": rows,
        "message": message,
    }
    payload.update(metadata)
    return payload


def _metadata_from_rows(
    rows: list[dict[str, Any]],
    *,
    symbol: str,
    asset_type: str,
    base: str | None = None,
) -> dict[str, Any]:
    if rows:
        first = rows[0]
        return {
            key: first.get(key)
            for key in (
                "display_name",
                "asset_type",
                "exchange",
                "sector",
                "currency",
                "base_currency",
                "quote_currency",
                "display_pair",
                "display_unit",
                "value_format",
                "chart_title",
                "chart_subtitle",
                "axis_label",
                "tooltip_label",
            )
            if key in first
        }
    return build_display_metadata(
        symbol=symbol,
        asset_type=asset_type,
        exchange=base,
        base_currency=base,
    )


def _requested_period(start: str | None, end: str | None) -> str:
    if not start or not end:
        return "requested range"
    return f"{start} to {end}"
