from __future__ import annotations

import hashlib
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Protocol

import requests

from .models import MarketQuoteRow, StockPriceDailyRow
from .redaction import redact_params, redact_text, truncate_text
from .utils import parse_yyyy_mm_dd, utc_now_iso

logger = logging.getLogger(__name__)


class MarketDataProvider(Protocol):
    name: str

    def fetch_stock_daily(self, symbol: str, start: str, end: str, exchange: str | None = None) -> list[StockPriceDailyRow]:
        ...

    def fetch_quote(self, symbol: str, asset_type: str = "STOCK", exchange: str | None = None) -> MarketQuoteRow:
        ...

    def status(self) -> dict[str, Any]:
        ...


@dataclass
class RateLimiter:
    min_interval_seconds: float
    sleep_func: Callable[[float], None] = time.sleep

    def __post_init__(self) -> None:
        self._last_call = 0.0

    def wait(self) -> None:
        if self.min_interval_seconds <= 0:
            return
        elapsed = time.monotonic() - self._last_call
        if elapsed < self.min_interval_seconds:
            self.sleep_func(self.min_interval_seconds - elapsed)
        self._last_call = time.monotonic()


class TwelveDataProvider:
    name = "twelvedata"

    def __init__(
        self,
        api_key: str,
        timeout_seconds: int = 20,
        max_retries: int = 3,
        rate_limiter: RateLimiter | None = None,
        session: requests.Session | None = None,
        request_logger: logging.Logger | logging.LoggerAdapter | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("TWELVE_DATA_API_KEY nao configurada; use MARKET_DATA_DEMO_MODE=true para demo")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.rate_limiter = rate_limiter or RateLimiter(min_interval_seconds=8.0)
        self.session = session or requests.Session()
        self.request_logger = request_logger or logger
        self.base_url = "https://api.twelvedata.com"

    def fetch_stock_daily(self, symbol: str, start: str, end: str, exchange: str | None = None) -> list[StockPriceDailyRow]:
        parse_yyyy_mm_dd(start)
        parse_yyyy_mm_dd(end)
        payload = self._request_json(
            "time_series",
            {
                "symbol": symbol.strip().upper(),
                "interval": "1day",
                "start_date": start,
                "end_date": end,
                "format": "JSON",
                "apikey": self.api_key,
            },
        )
        if payload.get("status") == "error":
            raise ValueError(str(payload.get("message", "twelvedata error")))

        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        currency = _clean_str(meta.get("currency"))
        provider_exchange = exchange or _clean_str(meta.get("exchange"))
        fetched_at = utc_now_iso()
        rows: list[StockPriceDailyRow] = []
        for item in payload.get("values", []) or []:
            day = _clean_str(item.get("datetime"))
            if not day:
                continue
            rows.append(
                StockPriceDailyRow(
                    date=parse_yyyy_mm_dd(day[:10]),
                    symbol=symbol.strip().upper(),
                    exchange=provider_exchange,
                    open=_to_float(item.get("open")),
                    high=_to_float(item.get("high")),
                    low=_to_float(item.get("low")),
                    close=_to_float(item.get("close")),
                    adjusted_close=_to_float(item.get("close")),
                    volume=_to_int(item.get("volume")),
                    currency=currency,
                    provider=self.name,
                    fetched_at=fetched_at,
                )
            )
        return sorted(rows, key=lambda row: row.date)

    def fetch_quote(self, symbol: str, asset_type: str = "STOCK", exchange: str | None = None) -> MarketQuoteRow:
        payload = self._request_json("quote", {"symbol": symbol.strip().upper(), "apikey": self.api_key})
        if payload.get("status") == "error":
            raise ValueError(str(payload.get("message", "twelvedata error")))

        fetched_at = utc_now_iso()
        return MarketQuoteRow(
            symbol=symbol.strip().upper(),
            asset_type=asset_type.strip().upper(),
            exchange=exchange or _clean_str(payload.get("exchange")),
            price=_to_float(payload.get("close") or payload.get("price")),
            bid=None,
            ask=None,
            open=_to_float(payload.get("open")),
            high=_to_float(payload.get("high")),
            low=_to_float(payload.get("low")),
            previous_close=_to_float(payload.get("previous_close")),
            change=_to_float(payload.get("change")),
            percent_change=_to_float(payload.get("percent_change")),
            volume=_to_int(payload.get("volume")),
            quote_time=_clean_str(payload.get("datetime")) or fetched_at,
            provider=self.name,
            fetched_at=fetched_at,
        )

    def status(self) -> dict[str, Any]:
        return {"name": self.name, "configured": True, "rate_limited": True}

    def _request_json(self, endpoint: str, params: dict[str, str]) -> dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        redacted = redact_params(params)
        for attempt in range(self.max_retries + 1):
            self.rate_limiter.wait()
            self.request_logger.info("provider_call provider=%s endpoint=%s params=%s", self.name, endpoint, redacted)
            try:
                response = self.session.get(url, params=params, timeout=self.timeout_seconds)
            except requests.RequestException as exc:
                if attempt >= self.max_retries:
                    raise RuntimeError(redact_text(exc)) from exc
                time.sleep(0.5 * (2**attempt))
                continue

            if response.status_code == 429 or 500 <= response.status_code <= 599:
                if attempt < self.max_retries:
                    retry_after = _to_float(response.headers.get("Retry-After")) or (0.5 * (2**attempt))
                    time.sleep(retry_after)
                    continue
            if response.status_code >= 400:
                body = truncate_text(getattr(response, "text", ""), limit=500)
                raise RuntimeError(f"twelvedata HTTP {response.status_code} endpoint={endpoint} body={body}")
            return response.json()
        raise RuntimeError(f"falha inesperada em provider endpoint={endpoint}")


class MockMarketDataProvider:
    name = "mock"

    def fetch_stock_daily(self, symbol: str, start: str, end: str, exchange: str | None = None) -> list[StockPriceDailyRow]:
        start_day = datetime.strptime(parse_yyyy_mm_dd(start), "%Y-%m-%d").date()
        end_day = datetime.strptime(parse_yyyy_mm_dd(end), "%Y-%m-%d").date()
        if start_day > end_day:
            raise ValueError("start precisa ser menor ou igual a end")

        fetched_at = utc_now_iso()
        rows: list[StockPriceDailyRow] = []
        index = 0
        current = start_day
        while current <= end_day:
            if current.weekday() < 5:
                close = _mock_price(symbol, index)
                open_price = round(close * (0.995 + (_stable_unit(symbol, index + 10) * 0.01)), 2)
                high = round(max(open_price, close) * 1.012, 2)
                low = round(min(open_price, close) * 0.988, 2)
                rows.append(
                    StockPriceDailyRow(
                        date=current.isoformat(),
                        symbol=symbol.strip().upper(),
                        exchange=exchange or "NASDAQ",
                        open=open_price,
                        high=high,
                        low=low,
                        close=close,
                        adjusted_close=close,
                        volume=1_000_000 + int(_stable_unit(symbol, index) * 8_000_000),
                        currency="USD",
                        provider=self.name,
                        fetched_at=fetched_at,
                    )
                )
                index += 1
            current += timedelta(days=1)
        return rows

    def fetch_quote(self, symbol: str, asset_type: str = "STOCK", exchange: str | None = None) -> MarketQuoteRow:
        fetched_at = utc_now_iso()
        day_index = int(datetime.now(timezone.utc).timestamp() // 86_400) % 2_500
        price = _mock_price(symbol, day_index)
        previous_close = _mock_price(symbol, day_index - 1)
        change = round(price - previous_close, 4)
        percent_change = round((change / previous_close) * 100, 4) if previous_close else None
        spread = max(0.01, price * 0.0005)
        return MarketQuoteRow(
            symbol=symbol.strip().upper(),
            asset_type=asset_type.strip().upper(),
            exchange=exchange or ("FX" if asset_type.strip().upper() == "FX" else "NASDAQ"),
            price=price,
            bid=round(price - spread, 4),
            ask=round(price + spread, 4),
            open=round(previous_close * 0.998, 4),
            high=round(max(price, previous_close) * 1.01, 4),
            low=round(min(price, previous_close) * 0.99, 4),
            previous_close=previous_close,
            change=change,
            percent_change=percent_change,
            volume=1_000_000 + int(_stable_unit(symbol, day_index) * 8_000_000),
            quote_time=fetched_at,
            provider=self.name,
            fetched_at=fetched_at,
        )

    def status(self) -> dict[str, Any]:
        return {"name": self.name, "configured": True, "demo": True}


def build_market_provider(
    provider_name: str,
    api_key: str,
    demo_mode: bool,
    timeout_seconds: int,
    max_retries: int,
    request_logger: logging.Logger | logging.LoggerAdapter | None = None,
) -> MarketDataProvider:
    if demo_mode or provider_name == "mock":
        return MockMarketDataProvider()
    return TwelveDataProvider(
        api_key=api_key,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        request_logger=request_logger,
    )


def _mock_price(symbol: str, index: int) -> float:
    base = 35 + int(hashlib.sha256(symbol.strip().upper().encode("utf-8")).hexdigest()[:6], 16) % 300
    trend = index * 0.09
    wave = math.sin(index / 4.0 + _stable_unit(symbol, 0) * math.pi) * 2.8
    noise = (_stable_unit(symbol, index) - 0.5) * 1.2
    return round(max(1.0, base + trend + wave + noise), 4)


def _stable_unit(symbol: str, salt: int) -> float:
    raw = f"{symbol.strip().upper()}:{salt}".encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def _clean_str(value: Any) -> str | None:
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped or None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace("%", ""))
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return None
