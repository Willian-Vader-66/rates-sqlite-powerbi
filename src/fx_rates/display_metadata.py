from __future__ import annotations

from typing import Any


def build_display_metadata(
    *,
    symbol: str,
    asset_type: str,
    display_name: str | None = None,
    exchange: str | None = None,
    currency: str | None = None,
    unit: str | None = None,
    base_currency: str | None = None,
) -> dict[str, Any]:
    normalized_symbol = (symbol or "").strip().upper()
    normalized_type = (asset_type or "").strip().upper()
    name = (display_name or normalized_symbol).strip() or normalized_symbol

    if normalized_type == "FX":
        base = (base_currency or exchange or "USD").strip().upper()
        quote = normalized_symbol
        pair = f"{base}/{quote}"
        unit_label = f"{quote} per 1 {base}"
        return _metadata(
            symbol=normalized_symbol,
            display_name=name,
            asset_type=normalized_type,
            exchange=exchange,
            currency=currency,
            base_currency=base,
            quote_currency=quote,
            display_pair=pair,
            display_unit=unit_label,
            unit_label=unit_label,
            value_label="Exchange Rate",
            value_format="fx_rate",
            chart_title=f"{pair} Exchange Rate",
            chart_subtitle=unit_label,
            axis_label=f"{quote} per {base}",
            tooltip_label=pair,
        )

    if normalized_type == "CRYPTO":
        pair = f"{normalized_symbol}/USD"
        return _metadata(
            symbol=normalized_symbol,
            display_name=name,
            asset_type=normalized_type,
            exchange=exchange or "CRYPTO",
            currency="USD",
            base_currency=normalized_symbol,
            quote_currency="USD",
            display_pair=pair,
            display_unit="USD",
            unit_label="USD",
            value_label="Crypto Price",
            value_format="currency_usd",
            chart_title=f"{name} ({pair})",
            chart_subtitle="Crypto price in USD",
            axis_label="Price (USD)",
            tooltip_label=pair,
        )

    if normalized_type == "MACRO":
        macro_unit = _macro_unit(normalized_symbol, unit)
        clean_title = _macro_title(normalized_symbol, name)
        return _metadata(
            symbol=normalized_symbol,
            display_name=name,
            asset_type=normalized_type,
            exchange=exchange or "MACRO",
            currency=currency,
            base_currency=None,
            quote_currency=None,
            display_pair=normalized_symbol,
            display_unit=macro_unit,
            unit_label=macro_unit,
            value_label=_macro_value_label(normalized_symbol, macro_unit),
            value_format="percent" if "%" in macro_unit else "index",
            chart_title=clean_title,
            chart_subtitle=_macro_subtitle(normalized_symbol, macro_unit),
            axis_label=f"Rate ({macro_unit})" if "%" in macro_unit else f"Value ({macro_unit})",
            tooltip_label=clean_title,
        )

    stock_currency = (currency or "USD").strip().upper()
    pair = f"{normalized_symbol}/{stock_currency}"
    return _metadata(
        symbol=normalized_symbol,
        display_name=name,
        asset_type=normalized_type or "STOCK",
        exchange=exchange,
        currency=stock_currency,
        base_currency=normalized_symbol,
        quote_currency=stock_currency,
        display_pair=pair,
        display_unit=stock_currency,
        unit_label=stock_currency,
        value_label="Stock Price",
        value_format="currency_usd" if stock_currency == "USD" else "currency",
        chart_title=f"{name} ({normalized_symbol})",
        chart_subtitle=f"Stock price in {stock_currency}",
        axis_label=f"Price ({stock_currency})",
        tooltip_label=f"{normalized_symbol} price",
    )


def apply_display_metadata(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result.update(
        build_display_metadata(
            symbol=str(result.get("symbol") or result.get("indicator_code") or ""),
            asset_type=str(result.get("asset_type") or _asset_type_from_row(result)),
            display_name=result.get("display_name") or result.get("name") or result.get("indicator_name"),
            exchange=result.get("exchange") or result.get("base"),
            currency=result.get("currency"),
            unit=result.get("unit"),
            base_currency=result.get("base"),
        )
    )
    return result


def _metadata(**values: Any) -> dict[str, Any]:
    return values


def _asset_type_from_row(row: dict[str, Any]) -> str:
    if "rate" in row or "base" in row:
        return "FX"
    if "price_usd" in row:
        return "CRYPTO"
    if "indicator_code" in row:
        return "MACRO"
    return "STOCK"


def _macro_unit(symbol: str, unit: str | None) -> str:
    if unit and unit.strip():
        return unit.strip()
    if "DAILY" in symbol:
        return "% a.d."
    if "MONTHLY" in symbol and "ANNUALIZED" not in symbol:
        return "% a.m."
    if "CPI" in symbol:
        return "index"
    return "% a.a."


def _macro_title(symbol: str, name: str) -> str:
    if symbol == "SELIC_MONTHLY":
        return "Selic Monthly Rate"
    if symbol == "SELIC_ANNUALIZED_MONTHLY":
        return "Selic Annualized Rate"
    if symbol == "SELIC_DAILY":
        return "Selic Daily Rate"
    if symbol == "CDI_DAILY":
        return "CDI Daily Rate"
    if symbol == "IPCA_MONTHLY":
        return "IPCA Monthly Inflation"
    if symbol == "FED_FUNDS_DAILY":
        return "Fed Funds Effective Rate"
    return name


def _macro_subtitle(symbol: str, unit: str) -> str:
    if symbol.endswith("_DAILY"):
        return f"Daily rate ({unit})"
    if "MONTHLY" in symbol and "ANNUALIZED" not in symbol:
        return f"Monthly accumulated rate ({unit})"
    if "CPI" in symbol:
        return f"Index level ({unit})"
    return f"Annualized rate ({unit})"


def _macro_value_label(symbol: str, unit: str) -> str:
    if symbol == "SELIC_DAILY" or symbol == "CDI_DAILY":
        return "Daily Rate"
    if symbol == "SELIC_TARGET":
        return "Target Rate"
    if symbol == "IPCA_MONTHLY":
        return "Monthly Inflation"
    if "MONTHLY" in symbol and "ANNUALIZED" not in symbol:
        return "Monthly Rate"
    if "CPI" in symbol or unit == "index":
        return "Index Level"
    return "Annualized Rate"
