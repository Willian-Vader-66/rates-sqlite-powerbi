from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import requests

from .models import FxRateRow
from .utils import stable_hash, utc_now_iso


class FrankfurterClientError(RuntimeError):
    """Raised when the Frankfurter API cannot be read safely."""


class FrankfurterClient:
    def __init__(
        self,
        base_url: str,
        cache_dir: str,
        timeout: int,
        use_cache: bool,
        logger: logging.Logger | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.cache_dir = Path(cache_dir)
        self.timeout = timeout
        self.use_cache = use_cache
        self.logger = logger or logging.getLogger("fx_rates.api")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def fetch_latest(self, base: str, symbols: list[str]) -> dict[str, Any]:
        return self._get_json("/v1/latest", {"base": base, "symbols": ",".join(symbols)})

    def fetch_timeseries(self, start: str, end: str, base: str, symbols: list[str]) -> dict[str, Any]:
        return self._get_json(f"/v1/{start}..{end}", {"base": base, "symbols": ",".join(symbols)})

    def _get_json(self, endpoint: str, params: dict[str, str]) -> dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        cache_path = self.cache_dir / f"{stable_hash(url, params)}.json"

        if self.use_cache and cache_path.exists():
            self.logger.info("Cache hit for %s", url)
            try:
                cached_payload = json.loads(cache_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                raise FrankfurterClientError(f"Cached JSON is invalid: {cache_path}") from exc
            validate_payload_shape(cached_payload)
            return cached_payload

        self.logger.info("Cache miss for %s", url)
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise FrankfurterClientError(f"HTTP request failed: {exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise FrankfurterClientError("API response is not valid JSON.") from exc

        validate_payload_shape(payload)

        if self.use_cache:
            cache_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

        return payload


def validate_payload_shape(payload: dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        raise FrankfurterClientError("API payload must be a JSON object.")

    rates = payload.get("rates")
    if not isinstance(rates, dict) or not rates:
        raise FrankfurterClientError("API payload is missing a non-empty rates object.")

    if isinstance(payload.get("date"), str):
        return "latest"

    if all(isinstance(key, str) and isinstance(value, dict) for key, value in rates.items()):
        return "timeseries"

    raise FrankfurterClientError("API payload is neither a latest payload nor a time-series payload.")


def normalize_payload(
    payload: dict[str, Any],
    source: str = "frankfurter",
    fetched_at: str | None = None,
    logger: logging.Logger | None = None,
) -> list[FxRateRow]:
    mode = validate_payload_shape(payload)
    base = str(payload.get("base", "")).upper()
    if not base:
        raise FrankfurterClientError("API payload is missing the base currency.")

    normalized: list[FxRateRow] = []
    seen_logger = logger or logging.getLogger("fx_rates.normalize")
    resolved_fetched_at = fetched_at or utc_now_iso()

    if mode == "latest":
        payload_date = str(payload["date"])
        rates = payload["rates"]
        for symbol, rate in rates.items():
            numeric_rate = _safe_rate(rate, payload_date, str(symbol), seen_logger)
            if numeric_rate is None:
                continue
            normalized.append(
                FxRateRow(
                    date=payload_date,
                    base=base,
                    symbol=str(symbol).upper(),
                    rate=numeric_rate,
                    source=source,
                    fetched_at=resolved_fetched_at,
                )
            )
        return normalized

    for row_date, by_symbol in payload["rates"].items():
        if not isinstance(by_symbol, dict):
            raise FrankfurterClientError(f"Invalid time-series rates payload for date {row_date}.")
        for symbol, rate in by_symbol.items():
            numeric_rate = _safe_rate(rate, str(row_date), str(symbol), seen_logger)
            if numeric_rate is None:
                continue
            normalized.append(
                FxRateRow(
                    date=str(row_date),
                    base=base,
                    symbol=str(symbol).upper(),
                    rate=numeric_rate,
                    source=source,
                    fetched_at=resolved_fetched_at,
                )
            )

    return normalized


def _safe_rate(
    rate: Any,
    date_value: str,
    symbol: str,
    logger: logging.Logger,
) -> float | None:
    if isinstance(rate, bool):
        logger.warning("Skipping invalid boolean rate for %s on %s.", symbol, date_value)
        return None
    try:
        return float(rate)
    except (TypeError, ValueError):
        logger.warning("Skipping invalid rate for %s on %s: %r", symbol, date_value, rate)
        return None
