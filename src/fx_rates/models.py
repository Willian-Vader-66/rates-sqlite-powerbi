from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FxRateRow:
    date: str
    base: str
    symbol: str
    rate: float
    source: str
    fetched_at: str

    def as_db_dict(self) -> dict[str, str | float]:
        return {
            "date": self.date,
            "base": self.base,
            "symbol": self.symbol,
            "rate": self.rate,
            "source": self.source,
            "fetched_at": self.fetched_at,
        }


@dataclass(frozen=True)
class InstrumentRow:
    symbol: str
    name: str | None
    asset_type: str
    exchange: str | None
    currency: str | None
    sector: str | None
    provider: str | None
    provider_symbol: str | None
    is_active: int
    priority: int
    created_at: str
    updated_at: str

    def as_db_dict(self) -> dict[str, str | int | None]:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "asset_type": self.asset_type,
            "exchange": self.exchange,
            "currency": self.currency,
            "sector": self.sector,
            "provider": self.provider,
            "provider_symbol": self.provider_symbol,
            "is_active": self.is_active,
            "priority": self.priority,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class StockPriceDailyRow:
    date: str
    symbol: str
    exchange: str | None
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    adjusted_close: float | None
    volume: int | None
    currency: str | None
    provider: str | None
    fetched_at: str

    def as_db_dict(self) -> dict[str, str | float | int | None]:
        return {
            "date": self.date,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "adjusted_close": self.adjusted_close,
            "volume": self.volume,
            "currency": self.currency,
            "provider": self.provider,
            "fetched_at": self.fetched_at,
        }


@dataclass(frozen=True)
class MarketQuoteRow:
    symbol: str
    asset_type: str
    exchange: str | None
    price: float | None
    bid: float | None
    ask: float | None
    open: float | None
    high: float | None
    low: float | None
    previous_close: float | None
    change: float | None
    percent_change: float | None
    volume: int | None
    quote_time: str | None
    provider: str | None
    fetched_at: str

    def as_db_dict(self) -> dict[str, str | float | int | None]:
        return {
            "symbol": self.symbol,
            "asset_type": self.asset_type,
            "exchange": self.exchange,
            "price": self.price,
            "bid": self.bid,
            "ask": self.ask,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "previous_close": self.previous_close,
            "change": self.change,
            "percent_change": self.percent_change,
            "volume": self.volume,
            "quote_time": self.quote_time,
            "provider": self.provider,
            "fetched_at": self.fetched_at,
        }


@dataclass(frozen=True)
class AnalysisSnapshotRow:
    symbol: str
    asset_type: str
    exchange: str | None
    generated_at: str
    last_price: float | None
    last_close: float | None
    daily_return: float | None
    change_30d: float | None
    change_90d: float | None
    change_1y: float | None
    sma_20: float | None
    sma_50: float | None
    volatility_20: float | None
    min_30d: float | None
    max_30d: float | None
    trend: str
    signal: str
    notes: str | None

    def as_db_dict(self) -> dict[str, str | float | None]:
        return {
            "symbol": self.symbol,
            "asset_type": self.asset_type,
            "exchange": self.exchange,
            "generated_at": self.generated_at,
            "last_price": self.last_price,
            "last_close": self.last_close,
            "daily_return": self.daily_return,
            "change_30d": self.change_30d,
            "change_90d": self.change_90d,
            "change_1y": self.change_1y,
            "sma_20": self.sma_20,
            "sma_50": self.sma_50,
            "volatility_20": self.volatility_20,
            "min_30d": self.min_30d,
            "max_30d": self.max_30d,
            "trend": self.trend,
            "signal": self.signal,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class MacroIndicatorDailyRow:
    date: str
    indicator_code: str
    indicator_name: str
    value: float | None
    unit: str | None
    source: str | None
    fetched_at: str

    def as_db_dict(self) -> dict[str, str | float | None]:
        return {
            "date": self.date,
            "indicator_code": self.indicator_code,
            "indicator_name": self.indicator_name,
            "value": self.value,
            "unit": self.unit,
            "source": self.source,
            "fetched_at": self.fetched_at,
        }


@dataclass(frozen=True)
class CryptoPriceDailyRow:
    date: str
    symbol: str
    name: str | None
    price_usd: float | None
    market_cap: float | None
    volume_24h: float | None
    change_24h: float | None
    provider: str | None
    fetched_at: str

    def as_db_dict(self) -> dict[str, str | float | None]:
        return {
            "date": self.date,
            "symbol": self.symbol,
            "name": self.name,
            "price_usd": self.price_usd,
            "market_cap": self.market_cap,
            "volume_24h": self.volume_24h,
            "change_24h": self.change_24h,
            "provider": self.provider,
            "fetched_at": self.fetched_at,
        }
