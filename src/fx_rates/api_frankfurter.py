from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from .models import FxRateRow
from .utils import cache_key, parse_yyyy_mm_dd, utc_now_iso

logger = logging.getLogger(__name__)


class FrankfurterClient:
    def __init__(
        self,
        base_url: str,
        cache_dir: str,
        timeout_seconds: int,
        use_cache: bool,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.cache_dir = Path(cache_dir)
        self.timeout_seconds = timeout_seconds
        self.use_cache = use_cache
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def fetch_latest(self, base: str, symbols: list[str]) -> dict[str, Any]:
        params = {"base": base.upper(), "symbols": ",".join(symbols)}
        payload = self._request_json("latest", params)
        validate_latest_payload(payload)
        return payload

    def fetch_timeseries(self, start: str, end: str, base: str, symbols: list[str]) -> dict[str, Any]:
        parse_yyyy_mm_dd(start)
        parse_yyyy_mm_dd(end)
        params = {"base": base.upper(), "symbols": ",".join(symbols)}
        payload = self._request_json(f"{start}..{end}", params)
        validate_timeseries_payload(payload)
        return payload

    def _request_json(self, endpoint: str, params: dict[str, str]) -> dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        cache_file = self.cache_dir / f"{cache_key(url, params)}.json"

        if self.use_cache and cache_file.exists():
            logger.info("cache hit endpoint=%s file=%s", endpoint, cache_file)
            return json.loads(cache_file.read_text(encoding="utf-8"))

        logger.info("cache miss endpoint=%s", endpoint)
        response = requests.get(url, params=params, timeout=self.timeout_seconds)
        response.raise_for_status()
        payload = response.json()

        if self.use_cache:
            cache_file.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")

        return payload


def validate_latest_payload(payload: dict[str, Any]) -> None:
    _validate_payload_base(payload)
    if "date" not in payload:
        raise ValueError("latest invalido: campo date ausente")
    parse_yyyy_mm_dd(str(payload["date"]))

    for symbol, value in payload["rates"].items():
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("latest invalido: symbol vazio")
        if not isinstance(value, (int, float)):
            raise ValueError("latest invalido: rate nao numerico")


def validate_timeseries_payload(payload: dict[str, Any]) -> None:
    _validate_payload_base(payload)
    for date_key, day_rates in payload["rates"].items():
        parse_yyyy_mm_dd(str(date_key))
        if not isinstance(day_rates, dict):
            raise ValueError("timeseries invalido: rates por data deve ser dict")
        for symbol, value in day_rates.items():
            if not isinstance(symbol, str) or not symbol.strip():
                raise ValueError("timeseries invalido: symbol vazio")
            if not isinstance(value, (int, float)):
                raise ValueError("timeseries invalido: rate nao numerico")


def normalize_latest(payload: dict[str, Any], base: str, source: str = "frankfurter") -> list[FxRateRow]:
    validate_latest_payload(payload)
    fetched_at = utc_now_iso()
    rate_date = str(payload["date"])

    return [
        FxRateRow(
            date=rate_date,
            base=base.upper(),
            symbol=symbol.upper(),
            rate=float(rate),
            source=source,
            fetched_at=fetched_at,
        )
        for symbol, rate in payload["rates"].items()
    ]


def normalize_timeseries(payload: dict[str, Any], base: str, source: str = "frankfurter") -> list[FxRateRow]:
    validate_timeseries_payload(payload)
    fetched_at = utc_now_iso()

    rows: list[FxRateRow] = []
    for rate_date, day_rates in payload["rates"].items():
        for symbol, rate in day_rates.items():
            rows.append(
                FxRateRow(
                    date=str(rate_date),
                    base=base.upper(),
                    symbol=symbol.upper(),
                    rate=float(rate),
                    source=source,
                    fetched_at=fetched_at,
                )
            )
    return rows


def _validate_payload_base(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("payload invalido: esperado dict")
    if "rates" not in payload:
        raise ValueError("payload invalido: campo rates ausente")
    if not isinstance(payload["rates"], dict):
        raise ValueError("payload invalido: rates deve ser dict")


def normalize_payload(payload: dict[str, Any], base: str, source: str = "frankfurter") -> list[FxRateRow]:
    rates = payload.get("rates") if isinstance(payload, dict) else None
    if isinstance(rates, dict) and rates and all(isinstance(v, (int, float)) for v in rates.values()):
        return normalize_latest(payload, base=base, source=source)
    return normalize_timeseries(payload, base=base, source=source)
