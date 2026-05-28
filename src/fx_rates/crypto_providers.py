from __future__ import annotations

import csv
import hashlib
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

import requests

from .models import CryptoPriceDailyRow, MarketQuoteRow
from .redaction import redact_params, redact_text, truncate_text
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


class CoinGeckoProviderError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, retryable: bool = False) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable


class CoinGeckoProvider:
    name = "coingecko"
    public_base_url = "https://api.coingecko.com/api/v3"
    pro_base_url = "https://pro-api.coingecko.com/api/v3"

    def __init__(
        self,
        timeout_seconds: int = 20,
        max_retries: int = 3,
        rate_limiter: RateLimiter | None = None,
        session: requests.Session | None = None,
        api_plan: str = "public",
        demo_api_key: str = "",
        pro_api_key: str = "",
        sleep_func: Callable[[float], None] = time.sleep,
        request_logger: logging.Logger | logging.LoggerAdapter | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.rate_limiter = rate_limiter or RateLimiter(min_interval_seconds=_coingecko_rate_limit_delay())
        self.session = session or requests.Session()
        self.api_plan = (api_plan or "public").strip().lower()
        if self.api_plan not in {"public", "demo", "pro"}:
            raise ValueError("COINGECKO_API_PLAN invalido: use public, demo ou pro")
        if self.api_plan == "pro" and not pro_api_key:
            raise ValueError("COINGECKO_PRO_API_KEY required when COINGECKO_API_PLAN=pro")
        self.base_url = self.pro_base_url if self.api_plan == "pro" else self.public_base_url
        self.headers: dict[str, str] = {}
        if self.api_plan == "demo" and demo_api_key:
            self.headers["x-cg-demo-api-key"] = demo_api_key
        elif self.api_plan == "pro" and pro_api_key:
            self.headers["x-cg-pro-api-key"] = pro_api_key
        self.sleep_func = sleep_func
        self.request_logger = request_logger or logger

    def fetch_daily(self, asset: CryptoAssetConfig, start: str, end: str) -> list[CryptoPriceDailyRow]:
        start_day = date.fromisoformat(parse_yyyy_mm_dd(start))
        requested_end = date.fromisoformat(parse_yyyy_mm_dd(end))
        end_day = min(requested_end, datetime.now(timezone.utc).date())
        if start_day > end_day:
            raise ValueError("start precisa ser menor ou igual ao ultimo dia UTC disponivel")
        requested_days = (end_day - start_day).days + 1
        if self.api_plan in {"public", "demo"} and requested_days > 365:
            raise CoinGeckoProviderError(
                "CoinGecko public/demo historical range supports up to 365 days. "
                "Use --days 365 in standard mode, or configure COINGECKO_API_PLAN=pro "
                "for advanced history.",
                status_code=401,
                retryable=False,
            )

        payloads = self._market_chart_payloads(asset.provider_code, start_day, end_day, symbol=asset.symbol)
        prices, price_diag = _daily_map_with_diagnostics(_extend_points(payloads, "prices"), start_day=start_day, end_day=end_day)
        market_caps, market_cap_diag = _daily_map_with_diagnostics(_extend_points(payloads, "market_caps"), start_day=start_day, end_day=end_day)
        volumes, volume_diag = _daily_map_with_diagnostics(_extend_points(payloads, "total_volumes"), start_day=start_day, end_day=end_day)
        self.request_logger.info(
            "coingecko_history_normalized coin_id=%s symbol=%s diagnostics=%s",
            asset.provider_code,
            asset.symbol,
            {
                "prices": price_diag,
                "market_caps": market_cap_diag,
                "total_volumes": volume_diag,
                "payloads": len(payloads),
            },
        )
        if not prices:
            self.request_logger.warning(
                "coingecko_history_rejected coin_id=%s symbol=%s reason=no_normalized_prices diagnostics=%s",
                asset.provider_code,
                asset.symbol,
                price_diag,
            )
            raise ValueError(
                "coingecko returned no daily prices "
                f"coin_id={asset.provider_code} start={start_day.isoformat()} end={end_day.isoformat()} diagnostics={price_diag}"
            )
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
        return {"name": self.name, "configured": True, "rate_limited": True, "api_plan": self.api_plan}

    def _market_chart_payloads(self, coin_id: str, start_day: date, end_day: date, *, symbol: str | None = None) -> list[dict[str, Any]]:
        endpoint = f"coins/{coin_id}/market_chart/range"
        self.request_logger.info(
            "coingecko_history_request symbol=%s coin_id=%s requested_start_date=%s requested_end_date=%s requested_from_epoch=%s requested_to_epoch=%s",
            symbol or "-",
            coin_id,
            start_day.isoformat(),
            end_day.isoformat(),
            _range_params(start_day, end_day)["from"],
            _range_params(start_day, end_day)["to"],
        )
        try:
            payload = self._request_json(endpoint, _range_params(start_day, end_day), coin_id=coin_id)
            if _has_prices(payload):
                return [payload]
            self.request_logger.warning(
                "coingecko_history_rejected coin_id=%s reason=no_prices diagnostics=%s",
                coin_id,
                _payload_diagnostics(payload, status_code=200, endpoint=endpoint, coin_id=coin_id),
            )
        except CoinGeckoProviderError as exc:
            if not exc.retryable and not _can_fallback_with_smaller_range(exc):
                raise
            self.request_logger.warning("coingecko_full_range_failed coin_id=%s error=%s", coin_id, redact_text(exc))

        yearly: list[dict[str, Any]] = []
        try:
            yearly = self._chunked_market_chart_payloads(coin_id, start_day, end_day, chunk_days=365)
            if _has_prices_many(yearly):
                return yearly
            self.request_logger.warning("coingecko_year_chunks_empty coin_id=%s chunks=%s", coin_id, len(yearly))
        except CoinGeckoProviderError as exc:
            if not exc.retryable and not _can_fallback_with_smaller_range(exc):
                raise
            self.request_logger.warning("coingecko_year_chunks_failed coin_id=%s error=%s", coin_id, redact_text(exc))
        quarterly = self._chunked_market_chart_payloads(coin_id, start_day, end_day, chunk_days=90)
        if _has_prices_many(quarterly):
            return quarterly
        raise ValueError(f"coingecko returned no prices after full/year/90d fallback coin_id={coin_id}")

    def _chunked_market_chart_payloads(self, coin_id: str, start_day: date, end_day: date, *, chunk_days: int) -> list[dict[str, Any]]:
        endpoint = f"coins/{coin_id}/market_chart/range"
        payloads: list[dict[str, Any]] = []
        current = start_day
        while current <= end_day:
            chunk_end = min(end_day, current + timedelta(days=chunk_days - 1))
            try:
                payloads.append(self._request_json(endpoint, _range_params(current, chunk_end), coin_id=coin_id))
            except CoinGeckoProviderError as exc:
                self.request_logger.warning(
                    "coingecko_chunk_failed coin_id=%s chunk_start=%s chunk_end=%s error=%s",
                    coin_id,
                    current.isoformat(),
                    chunk_end.isoformat(),
                    redact_text(exc),
                )
                raise
            current = chunk_end + timedelta(days=1)
        return payloads

    def _request_json(self, endpoint: str, params: dict[str, str], *, coin_id: str | None = None) -> dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        for attempt in range(self.max_retries + 1):
            self.rate_limiter.wait()
            self.request_logger.info("provider_call provider=%s endpoint=%s params=%s", self.name, endpoint, redact_params(params))
            try:
                response = self.session.get(url, params=params, headers=self.headers or None, timeout=self.timeout_seconds)
            except requests.RequestException as exc:
                if attempt >= self.max_retries:
                    raise CoinGeckoProviderError(
                        f"coingecko request error endpoint={endpoint} coin_id={coin_id or '-'} error={redact_text(exc)}",
                        retryable=True,
                    ) from exc
                self.sleep_func(0.5 * (2**attempt))
                continue
            if _retryable_status(response.status_code):
                if attempt < self.max_retries:
                    self.sleep_func(_retry_delay(response, attempt))
                    continue
                raise CoinGeckoProviderError(
                    _http_error_message(response, endpoint=endpoint, coin_id=coin_id),
                    status_code=response.status_code,
                    retryable=True,
                )
            if response.status_code >= 400:
                raise CoinGeckoProviderError(
                    _http_error_message(response, endpoint=endpoint, coin_id=coin_id),
                    status_code=response.status_code,
                    retryable=False,
                )
            try:
                data = response.json()
            except ValueError as exc:
                self.request_logger.warning(
                    "coingecko_response diagnostics=%s",
                    _response_diagnostics(response, endpoint=endpoint, coin_id=coin_id, json_parsed=False),
                )
                raise CoinGeckoProviderError(
                    f"coingecko invalid JSON endpoint={endpoint} coin_id={coin_id or '-'} body={truncate_text(getattr(response, 'text', ''), limit=500)}",
                    retryable=False,
                ) from exc
            if not isinstance(data, dict):
                raise CoinGeckoProviderError(f"coingecko invalid payload type endpoint={endpoint} coin_id={coin_id or '-'}", retryable=False)
            self.request_logger.info(
                "coingecko_response diagnostics=%s",
                {
                    **_response_diagnostics(response, endpoint=endpoint, coin_id=coin_id, json_parsed=True),
                    **_payload_diagnostics(data, status_code=response.status_code, endpoint=endpoint, coin_id=coin_id),
                },
            )
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


def build_crypto_provider(
    demo_mode: bool,
    timeout_seconds: int,
    max_retries: int,
    *,
    coingecko_api_plan: str = "public",
    coingecko_demo_api_key: str = "",
    coingecko_pro_api_key: str = "",
) -> CryptoDataProvider:
    if demo_mode:
        return MockCryptoProvider()
    return CoinGeckoProvider(
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        api_plan=coingecko_api_plan,
        demo_api_key=coingecko_demo_api_key,
        pro_api_key=coingecko_pro_api_key,
    )


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


def _daily_map(raw_points: Any, *, start_day: date | None = None, end_day: date | None = None) -> dict[str, float]:
    return _daily_map_with_diagnostics(raw_points, start_day=start_day, end_day=end_day)[0]


def _daily_map_with_diagnostics(raw_points: Any, *, start_day: date | None = None, end_day: date | None = None) -> tuple[dict[str, float], dict[str, Any]]:
    result: dict[str, tuple[float, float]] = {}
    invalid_points = 0
    outside_range = 0
    raw_count = len(raw_points) if isinstance(raw_points, list) else 0
    raw_timestamps: list[float] = []
    if not isinstance(raw_points, list):
        return {}, {
            "raw_points": 0,
            "invalid_points": 0,
            "outside_range": 0,
            "normalized_points": 0,
            "deduplicated_points": 0,
            "first_timestamp_raw": None,
            "last_timestamp_raw": None,
            "first_date": None,
            "last_date": None,
            "reason": "field_not_list",
        }
    for point in raw_points:
        if not isinstance(point, list) or len(point) < 2:
            invalid_points += 1
            continue
        timestamp = _to_float(point[0])
        value = _to_float(point[1])
        if timestamp is None or value is None:
            invalid_points += 1
            continue
        raw_timestamps.append(timestamp)
        day = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).date().isoformat()
        parsed_day = date.fromisoformat(day)
        if start_day and parsed_day < start_day:
            outside_range += 1
            continue
        if end_day and parsed_day > end_day:
            outside_range += 1
            continue
        previous = result.get(day)
        if previous is None or timestamp >= previous[0]:
            result[day] = (timestamp, value)
    values = {day: value for day, (_timestamp, value) in result.items()}
    sorted_days = sorted(values)
    diagnostics = {
        "raw_points": raw_count,
        "invalid_points": invalid_points,
        "outside_range": outside_range,
        "normalized_points": raw_count - invalid_points - outside_range,
        "deduplicated_points": len(values),
        "first_timestamp_raw": min(raw_timestamps) if raw_timestamps else None,
        "last_timestamp_raw": max(raw_timestamps) if raw_timestamps else None,
        "first_timestamp_iso": _timestamp_iso(min(raw_timestamps)) if raw_timestamps else None,
        "last_timestamp_iso": _timestamp_iso(max(raw_timestamps)) if raw_timestamps else None,
        "first_date": sorted_days[0] if sorted_days else None,
        "last_date": sorted_days[-1] if sorted_days else None,
        "reason": "ok" if values else "no_valid_points",
    }
    return values, diagnostics


def _extend_points(payloads: list[dict[str, Any]], field: str) -> list[Any]:
    points: list[Any] = []
    for payload in payloads:
        raw = payload.get(field)
        if isinstance(raw, list):
            points.extend(raw)
    return points


def _range_params(start_day: date, end_day: date) -> dict[str, str]:
    start_dt = datetime(start_day.year, start_day.month, start_day.day, tzinfo=timezone.utc)
    today = datetime.now(timezone.utc).date()
    if end_day >= today:
        end_dt = datetime.now(timezone.utc)
    else:
        end_dt = datetime(end_day.year, end_day.month, end_day.day, tzinfo=timezone.utc) + timedelta(days=1)
    if end_dt <= start_dt:
        end_dt = start_dt + timedelta(days=1)
    return {"vs_currency": "usd", "from": str(int(start_dt.timestamp())), "to": str(int(end_dt.timestamp()))}


def _has_prices(payload: dict[str, Any]) -> bool:
    prices = payload.get("prices")
    return isinstance(prices, list) and len(prices) > 0


def _has_prices_many(payloads: list[dict[str, Any]]) -> bool:
    return any(_has_prices(payload) for payload in payloads)


def _payload_diagnostics(payload: dict[str, Any], *, status_code: int, endpoint: str, coin_id: str | None) -> dict[str, Any]:
    prices = payload.get("prices")
    market_caps = payload.get("market_caps")
    total_volumes = payload.get("total_volumes")
    price_count = len(prices) if isinstance(prices, list) else 0
    market_cap_count = len(market_caps) if isinstance(market_caps, list) else 0
    volume_count = len(total_volumes) if isinstance(total_volumes, list) else 0
    timestamps = [
        _to_float(point[0])
        for point in prices or []
        if isinstance(point, list) and point
    ] if isinstance(prices, list) else []
    timestamps = [item for item in timestamps if item is not None]
    return {
        "status_code": status_code,
        "endpoint": endpoint,
        "coin_id": coin_id or "-",
        "keys": sorted(str(key) for key in payload.keys()),
        "prices_points": price_count,
        "market_caps_points": market_cap_count,
        "total_volumes_points": volume_count,
        "first_timestamp_raw": min(timestamps) if timestamps else None,
        "last_timestamp_raw": max(timestamps) if timestamps else None,
        "first_timestamp": _timestamp_iso(min(timestamps)) if timestamps else None,
        "last_timestamp": _timestamp_iso(max(timestamps)) if timestamps else None,
        "first_date": datetime.fromtimestamp(min(timestamps) / 1000, tz=timezone.utc).date().isoformat() if timestamps else None,
        "last_date": datetime.fromtimestamp(max(timestamps) / 1000, tz=timezone.utc).date().isoformat() if timestamps else None,
    }


def _response_diagnostics(response: requests.Response, *, endpoint: str, coin_id: str | None, json_parsed: bool) -> dict[str, Any]:
    body = getattr(response, "text", "")
    return {
        "status_code": response.status_code,
        "endpoint": endpoint,
        "coin_id": coin_id or "-",
        "content_type": response.headers.get("Content-Type") or response.headers.get("content-type"),
        "body_length": len(body.encode("utf-8", errors="replace")),
        "json_parsed": json_parsed,
    }


def _timestamp_iso(timestamp_ms: float) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def _retryable_status(status_code: int) -> bool:
    return status_code == 408 or status_code == 429 or 500 <= status_code <= 599


def _can_fallback_with_smaller_range(exc: CoinGeckoProviderError) -> bool:
    message = str(exc).lower()
    return exc.status_code == 400 or (
        exc.status_code == 401
        and (
            "10012" in message
            or "allowed time range" in message
            or "past 365 days" in message
        )
    )


def _retry_delay(response: requests.Response, attempt: int) -> float:
    retry_after = _to_float(response.headers.get("Retry-After"))
    if retry_after is not None:
        return max(0.0, retry_after)
    return 0.5 * (2**attempt)


def _http_error_message(response: requests.Response, *, endpoint: str, coin_id: str | None) -> str:
    body = truncate_text(getattr(response, "text", ""), limit=500)
    content_type = response.headers.get("Content-Type") or response.headers.get("content-type") or "-"
    body_length = len(getattr(response, "text", "").encode("utf-8", errors="replace"))
    return f"coingecko HTTP {response.status_code} endpoint={endpoint} coin_id={coin_id or '-'} content_type={content_type} body_length={body_length} body={body}"


def _coingecko_rate_limit_delay() -> float:
    raw = os.getenv("COINGECKO_RATE_LIMIT_DELAY_SECONDS", "6").strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 6.0


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
