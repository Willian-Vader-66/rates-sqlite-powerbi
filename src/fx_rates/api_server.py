from __future__ import annotations

from datetime import date, timedelta
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
    get_data_mode_summary,
    get_technical_highlights,
    get_top_stocks_30d,
    period_to_days,
    list_instruments,
)
from .market_providers import build_market_provider
from .provider_status import providers_status
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
        data_mode = get_data_mode_summary(settings.db_path)
        live_provider_status = providers_status(settings)
        return {
            "status": "ok",
            "db_path": settings.db_path,
            "db_exists": Path(settings.db_path).exists(),
            "provider": provider_status,
            "data_mode": data_mode["data_mode"],
            "providers": data_mode["providers"],
            "data_generated_at": data_mode["generated_at"],
            "data_warning": data_mode["warning"],
            "live_provider_status": live_provider_status,
        }

    @app.get("/api/system/status")
    def system_status() -> dict[str, Any]:
        status = get_system_status(settings.db_path)
        status["live_provider_status"] = providers_status(settings)
        return status

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

    @app.get("/api/history/{symbol}")
    def generic_history(
        symbol: str,
        period: str | None = "90D",
        asset_type: str | None = None,
        base: str | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> dict[str, Any]:
        resolved = _resolve_history_target(settings.db_path, symbol=symbol, asset_type=asset_type, base=base)
        requested_start, requested_end = _period_bounds(period, start, end)
        if resolved["asset_type"] == "FX":
            rows = get_fx_history(settings.db_path, base=resolved.get("base") or "USD", symbol=resolved["symbol"], start=requested_start, end=requested_end)
        elif resolved["asset_type"] == "CRYPTO":
            rows = get_crypto_history(settings.db_path, symbol=resolved["symbol"], start=requested_start, end=requested_end)
        elif resolved["asset_type"] == "MACRO":
            rows = get_macro_history(settings.db_path, indicator_code=resolved["symbol"], start=requested_start, end=requested_end)
        else:
            rows = get_stock_history(settings.db_path, symbol=resolved["symbol"], start=requested_start, end=requested_end)
        response = _history_response(
            rows,
            symbol=resolved["symbol"],
            asset_type=resolved["asset_type"],
            base=resolved.get("base"),
            start=requested_start,
            end=requested_end,
        )
        response["requested_symbol"] = symbol.strip().upper()
        response["resolved_symbol"] = resolved["symbol"]
        return response

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



def _resolve_history_target(
    db_path: str,
    *,
    symbol: str,
    asset_type: str | None = None,
    base: str | None = None,
) -> dict[str, Any]:
    raw = symbol.strip().upper()
    normalized_type = asset_type.strip().upper() if asset_type else None
    if "/" in raw:
        left, right = [part.strip().upper() for part in raw.split("/", 1)]
        if left == "USD":
            raw = right
            normalized_type = normalized_type or "FX"
            base = base or left
        elif right == "USD":
            raw = left
            normalized_type = normalized_type or "CRYPTO"

    aliases = {"BRL": ("FX", "BRL", "USD"), "EUR": ("FX", "EUR", "USD"), "BTC": ("CRYPTO", "BTC", None), "ETH": ("CRYPTO", "ETH", None)}
    if normalized_type is None and raw in aliases:
        alias_type, alias_symbol, alias_base = aliases[raw]
        normalized_type = alias_type
        raw = alias_symbol
        base = base or alias_base

    candidates = list_instruments(db_path, asset_type=normalized_type, active=True, search=raw)
    exact = [row for row in candidates if str(row.get("symbol", "")).upper() == raw]
    row = exact[0] if exact else candidates[0] if candidates else None
    if row:
        resolved_type = str(row.get("asset_type") or normalized_type or "STOCK").upper()
        resolved_base = base or (row.get("exchange") if resolved_type == "FX" else None)
        return {"asset_type": resolved_type, "symbol": str(row.get("symbol") or raw).upper(), "base": resolved_base}
    return {"asset_type": normalized_type or "STOCK", "symbol": raw, "base": base}


def _period_bounds(period: str | None, start: str | None, end: str | None) -> tuple[str | None, str | None]:
    if start or end:
        return start, end
    period_label, days = period_to_days(period, default=90)
    requested_end = date.today()
    requested_start = requested_end - timedelta(days=days)
    return requested_start.isoformat(), requested_end.isoformat()

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
                "unit_label",
                "value_label",
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
