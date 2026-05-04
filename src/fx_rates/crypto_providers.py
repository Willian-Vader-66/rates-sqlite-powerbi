from __future__ import annotations

import csv
import hashlib
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

import requests

from .models import CryptoPriceDailyRow, MarketQuoteRow
from .utils import parse_yyyy_mm_dd, utc_now_iso

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CryptoAssetConfig:
    symbol: str
    name: str
    provider_code: str
    is_active: bool
    priority: int


class CryptoDataProvider(Protocol):
    name: str

    def fetch_daily(self, asset: CryptoAssetConfig, start: str, end: str) -> list[CryptoPriceDailyRow]:
        ...

    def fetch_quote(self, asset: CryptoAssetConfig) -> MarketQuoteRow:
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


class CoinGeckoProvider:
    name = "coingecko"

    def __init__(
        self,
        timeout_seconds: int = 20,
        max_retries: int = 3,
        rate_limiter: RateLimiter | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.rate_limiter = rate_limiter or RateLimiter(min_interval_seconds=6.0)
        self.session = session or requests.Session()
        self.base_url = "https://api.coingecko.com/api/v3"

    def fetch_daily(self, asset: CryptoAssetConfig, start: str, end: str) -> list[CryptoPriceDailyRow]:
        start_day = datetime.strptime(parse_yyyy_mm_dd(start), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_day = datetime.strptime(parse_yyyy_mm_dd(end), "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        payload = self._request_json(
            f"coins/{asset.provider_code}/market_chart/range",
            {
                "vs_currency": "usd",
                "from": str(int(start_day.timestamp())),
                "to": str(int(end_day.timestamp())),
            },
        )
        prices = _daily_map(payload.get("prices", []))
        market_caps = _daily_map(payload.get("market_caps", []))
        volumes = _daily_map(payload.get("total_volumes", []))
        fetched_at = utc_now_iso()
        rows: list[CryptoPriceDailyRow] = []
        previous_price: float | None = None
        for day in sorted(prices):
            price = prices[day]
            change_24h = _percent_change(previous_price, price) if previous_price is not None else None
            rows.append(
                CryptoPriceDailyRow(
                    date=day,
                    symbol=asset.symbol,
                    name=asset.name,
                    price_usd=price,
                    market_cap=market_caps.get(day),
                    volume_24h=volumes.get(day),
                    change_24h=change_24h,
                    provider=self.name,
                    fetched_at=fetched_at,
                )
            )
            previous_price = price
        return rows

    def fetch_quote(self, asset: CryptoAssetConfig) -> MarketQuoteRow:
        payload = self._request_json(
            "simple/price",
            {
                "ids": asset.provider_code,
                "vs_currencies": "usd",
                "include_24hr_change": "true",
                "include_24hr_vol": "true",
                "include_market_cap": "true",
            },
        )
        item = payload.get(asset.provider_code, {}) if isinstance(payload, dict) else {}
        fetched_at = utc_now_iso()
        price = _to_float(item.get("usd"))
        percent_change = _to_float(item.get("usd_24h_change"))
        return MarketQuoteRow(
            symbol=asset.symbol,
            asset_type="CRYPTO",
            exchange="CRYPTO",
            price=price,
            bid=None,
            ask=None,
            open=None,
            high=None,
            low=None,
            previous_close=None,
            change=None,
            percent_change=percent_change,
            volume=_to_int(item.get("usd_24h_vol")),
            quote_time=fetched_at,
            provider=self.name,
            fetched_at=fetched_at,
        )

    def status(self) -> dict[str, Any]:
        return {"name": self.name, "configured": True, "rate_limited": True}

    def _request_json(self, endpoint: str, params: dict[str, str]) -> dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        for attempt in range(self.max_retries + 1):
            self.rate_limiter.wait()
            logger.info("provider_call provider=%s endpoint=%s params=%s", self.name, endpoint, params)
            try:
                response = self.session.get(url, params=params, timeout=self.timeout_seconds)
            except requests.RequestException:
                if attempt >= self.max_retries:
                    raise
                time.sleep(0.5 * (2**attempt))
                continue
            if response.status_code == 429 or 500 <= response.status_code <= 599:
                if attempt < self.max_retries:
                    time.sleep(0.5 * (2**attempt))
                    continue
            response.raise_for_status()
            data = response.json()
            return data if isinstance(data, dict) else {}
        raise RuntimeError(f"falha inesperada em provider crypto={endpoint}")


class MockCryptoProvider:
    name = "mock_crypto"

    def fetch_daily(self, asset: CryptoAssetConfig, start: str, end: str) -> list[CryptoPriceDailyRow]:
        start_day = datetime.strptime(parse_yyyy_mm_dd(start), "%Y-%m-%d").date()
        end_day = datetime.strptime(parse_yyyy_mm_dd(end), "%Y-%m-%d").date()
        if start_day > end_day:
            raise ValueError("start precisa ser menor ou igual a end")

        fetched_at = utc_now_iso()
        rows: list[CryptoPriceDailyRow] = []
        previous_price: float | None = None
        index = 0
        current = start_day
        while current <= end_day:
            price = _mock_price(asset.symbol, index)
            change = _percent_change(previous_price, price) if previous_price is not None else None
            rows.append(
                CryptoPriceDailyRow(
                    date=current.isoformat(),
                    symbol=asset.symbol,
                    name=asset.name,
                    price_usd=price,
                    market_cap=price * _mock_supply(asset.symbol),
                    volume_24h=price * (500_000 + int(_stable_unit(asset.symbol, index) * 3_000_000)),
                    change_24h=change,
                    provider=self.name,
                    fetched_at=fetched_at,
                )
            )
            previous_price = price
            index += 1
            current += timedelta(days=1)
        return rows

    def fetch_quote(self, asset: CryptoAssetConfig) -> MarketQuoteRow:
        fetched_at = utc_now_iso()
        day_index = int(datetime.now(timezone.utc).timestamp() // 86_400) % 2_500
        price = _mock_price(asset.symbol, day_index)
        previous = _mock_price(asset.symbol, day_index - 1)
        spread = max(0.0001, price * 0.0008)
        return MarketQuoteRow(
            symbol=asset.symbol,
            asset_type="CRYPTO",
            exchange="CRYPTO",
            price=price,
            bid=round(price - spread, 4),
            ask=round(price + spread, 4),
            open=previous,
            high=round(max(price, previous) * 1.02, 4),
            low=round(min(price, previous) * 0.98, 4),
            previous_close=previous,
            change=round(price - previous, 4),
            percent_change=_percent_change(previous, price),
            volume=500_000 + int(_stable_unit(asset.symbol, day_index) * 3_000_000),
            quote_time=fetched_at,
            provider=self.name,
            fetched_at=fetched_at,
        )

    def status(self) -> dict[str, Any]:
        return {"name": self.name, "configured": True, "demo": True}


def build_crypto_provider(demo_mode: bool, timeout_seconds: int, max_retries: int) -> CryptoDataProvider:
    if demo_mode:
        return MockCryptoProvider()
    return CoinGeckoProvider(timeout_seconds=timeout_seconds, max_retries=max_retries)


def load_crypto_reference(path: str) -> list[CryptoAssetConfig]:
    rows: list[CryptoAssetConfig] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"symbol", "name", "provider_code", "is_active", "priority"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"crypto reference sem colunas obrigatorias: {', '.join(sorted(missing))}")
        for item in reader:
            symbol = (item.get("symbol") or "").strip().upper()
            provider_code = (item.get("provider_code") or "").strip().lower()
            if not symbol or not provider_code:
                continue
            rows.append(
                CryptoAssetConfig(
                    symbol=symbol,
                    name=(item.get("name") or symbol).strip(),
                    provider_code=provider_code,
                    is_active=_parse_active(item.get("is_active")),
                    priority=_parse_priority(item.get("priority")),
                )
            )
    return sorted(rows, key=lambda row: (row.priority, row.symbol))


def _daily_map(raw_points: Any) -> dict[str, float]:
    result: dict[str, float] = {}
    if not isinstance(raw_points, list):
        return result
    for point in raw_points:
        if not isinstance(point, list) or len(point) < 2:
            continue
        timestamp = _to_float(point[0])
        value = _to_float(point[1])
        if timestamp is None or value is None:
            continue
        day = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).date().isoformat()
        result[day] = value
    return result


def _mock_price(symbol: str, index: int) -> float:
    bases = {
        "BTC": 64000,
        "ETH": 3200,
        "BNB": 600,
        "SOL": 145,
        "XRP": 0.62,
        "ADA": 0.45,
        "DOGE": 0.15,
        "AVAX": 35,
        "DOT": 7.2,
        "LINK": 16,
    }
    normalized = symbol.strip().upper()
    base = bases.get(normalized, 50 + int(hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:4], 16) % 500)
    wave = math.sin(index / 5.0 + _stable_unit(normalized, 0) * math.pi) * base * 0.025
    drift = index * base * 0.00045
    noise = (_stable_unit(normalized, index) - 0.5) * base * 0.012
    return round(max(0.0001, base + drift + wave + noise), 4)


def _mock_supply(symbol: str) -> float:
    supplies = {
        "BTC": 19_700_000,
        "ETH": 120_000_000,
        "BNB": 150_000_000,
        "SOL": 450_000_000,
        "XRP": 55_000_000_000,
        "ADA": 35_000_000_000,
        "DOGE": 145_000_000_000,
        "AVAX": 380_000_000,
        "DOT": 1_400_000_000,
        "LINK": 600_000_000,
    }
    return float(supplies.get(symbol.strip().upper(), 100_000_000))


def _stable_unit(symbol: str, salt: int) -> float:
    raw = f"{symbol.strip().upper()}:{salt}".encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def _percent_change(start: float | None, end: float | None) -> float | None:
    if start is None or end is None or float(start) == 0:
        return None
    return round(((float(end) / float(start)) - 1.0) * 100.0, 4)


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    number = _to_float(value)
    return int(number) if number is not None else None


def _parse_active(value: str | None) -> bool:
    if value is None:
        return True
    return value.strip().lower() not in {"0", "false", "no", "n"}


def _parse_priority(value: str | None) -> int:
    if value is None or not value.strip():
        return 100
    return int(value)
