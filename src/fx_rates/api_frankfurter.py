from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable

import requests

from .models import FxRateRow
from .utils import cache_key, normalize_base, normalize_symbol_list, parse_yyyy_mm_dd, utc_now_iso

logger = logging.getLogger(__name__)


class FrankfurterClient:
    def __init__(
        self,
        base_url: str,
        cache_dir: str,
        timeout_seconds: int,
        use_cache: bool,
        max_retries: int = 3,
        use_cache_latest: bool = False,
        request_logger: logging.Logger | logging.LoggerAdapter | None = None,
        session: requests.Session | None = None,
        sleep_func: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.cache_dir = Path(cache_dir)
        self.timeout_seconds = timeout_seconds
        self.use_cache = use_cache
        self.max_retries = max_retries
        self.use_cache_latest = use_cache_latest
        self.request_logger = request_logger or logger
        self.session = session or requests.Session()
        self.sleep_func = sleep_func
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def fetch_latest(self, base: str, symbols: list[str]) -> dict[str, Any]:
        params = self._build_params(base, symbols)
        payload = self._request_json("latest", params, use_cache=self.use_cache and self.use_cache_latest)
        validate_latest_payload(payload)
        return payload

    def fetch_timeseries(self, start: str, end: str, base: str, symbols: list[str]) -> dict[str, Any]:
        parse_yyyy_mm_dd(start)
        parse_yyyy_mm_dd(end)
        params = self._build_params(base, symbols)
        payload = self._request_json(f"{start}..{end}", params, use_cache=self.use_cache)
        validate_timeseries_payload(payload)
        return payload

    def _build_params(self, base: str, symbols: list[str]) -> dict[str, str]:
        normalized_base = normalize_base(base)
        normalized_symbols = normalize_symbol_list(symbols)
        return {"base": normalized_base, "symbols": ",".join(normalized_symbols)}

    def _request_json(self, endpoint: str, params: dict[str, str], *, use_cache: bool) -> dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        cache_file = self.cache_dir / f"{cache_key(url, params)}.json"

        if use_cache and cache_file.exists():
            self.request_logger.info(
                "cache=hit endpoint=%s file=%s",
                endpoint,
                cache_file,
                extra={"event": "cache_hit"},
            )
            return json.loads(cache_file.read_text(encoding="utf-8"))

        if use_cache:
            self.request_logger.info(
                "cache=miss endpoint=%s file=%s",
                endpoint,
                cache_file,
                extra={"event": "cache_miss"},
            )

        for attempt in range(self.max_retries + 1):
            attempt_number = attempt + 1
            try:
                response = self.session.get(url, params=params, timeout=self.timeout_seconds)
            except requests.RequestException as exc:
                if attempt >= self.max_retries:
                    raise
                sleep_seconds = _retry_delay_seconds(None, retry_number=attempt_number)
                self.request_logger.warning(
                    "retrying request endpoint=%s attempt=%s error=%s sleep_seconds=%.3f",
                    endpoint,
                    attempt_number,
                    exc.__class__.__name__,
                    sleep_seconds,
                    extra={"event": "http_retry"},
                )
                self.sleep_func(sleep_seconds)
                continue

            if _should_retry_response(response) and attempt < self.max_retries:
                sleep_seconds = _retry_delay_seconds(response.headers.get("Retry-After"), retry_number=attempt_number)
                self.request_logger.warning(
                    "retrying request endpoint=%s attempt=%s status_code=%s sleep_seconds=%.3f",
                    endpoint,
                    attempt_number,
                    response.status_code,
                    sleep_seconds,
                    extra={"event": "http_retry"},
                )
                self.sleep_func(sleep_seconds)
                continue

            response.raise_for_status()
            payload = response.json()

            if use_cache:
                cache_file.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")

            return payload

        raise RuntimeError(f"falha inesperada ao buscar endpoint={endpoint}")


def validate_latest_payload(payload: dict[str, Any]) -> None:
    _validate_payload_envelope(payload)
    if "date" not in payload:
        raise ValueError("latest invalido: campo date ausente")
    parse_yyyy_mm_dd(str(payload["date"]))


def validate_timeseries_payload(payload: dict[str, Any]) -> None:
    _validate_payload_envelope(payload)
    for date_key, day_rates in payload["rates"].items():
        parse_yyyy_mm_dd(str(date_key))
        if not isinstance(day_rates, dict):
            raise ValueError("timeseries invalido: rates por data deve ser dict")


def normalize_latest(
    payload: dict[str, Any],
    base: str,
    source: str = "frankfurter",
    row_logger: logging.Logger | logging.LoggerAdapter | None = None,
) -> list[FxRateRow]:
    rows, _ = _normalize_latest_with_stats(payload, base=base, source=source, row_logger=row_logger)
    return rows


def normalize_timeseries(
    payload: dict[str, Any],
    base: str,
    source: str = "frankfurter",
    row_logger: logging.Logger | logging.LoggerAdapter | None = None,
) -> list[FxRateRow]:
    rows, _ = _normalize_timeseries_with_stats(payload, base=base, source=source, row_logger=row_logger)
    return rows


def _normalize_latest_with_stats(
    payload: dict[str, Any],
    base: str,
    source: str = "frankfurter",
    row_logger: logging.Logger | logging.LoggerAdapter | None = None,
) -> tuple[list[FxRateRow], int]:
    validate_latest_payload(payload)
    rate_date = str(payload["date"])
    entries = [(rate_date, symbol, rate) for symbol, rate in payload["rates"].items()]
    return _normalize_entries(entries, base=base, source=source, row_logger=row_logger)


def _normalize_timeseries_with_stats(
    payload: dict[str, Any],
    base: str,
    source: str = "frankfurter",
    row_logger: logging.Logger | logging.LoggerAdapter | None = None,
) -> tuple[list[FxRateRow], int]:
    validate_timeseries_payload(payload)
    entries = [
        (str(rate_date), symbol, rate)
        for rate_date, day_rates in payload["rates"].items()
        for symbol, rate in day_rates.items()
    ]
    return _normalize_entries(entries, base=base, source=source, row_logger=row_logger)


def _normalize_entries(
    entries: list[tuple[str, Any, Any]],
    base: str,
    source: str,
    row_logger: logging.Logger | logging.LoggerAdapter | None,
) -> tuple[list[FxRateRow], int]:
    fetched_at = utc_now_iso()
    normalized_base = normalize_base(base)
    active_logger = row_logger or logger
    rows: list[FxRateRow] = []
    skipped_invalid = 0

    for rate_date, symbol, rate in entries:
        row = _build_row(
            rate_date=rate_date,
            base=normalized_base,
            symbol=symbol,
            rate=rate,
            source=source,
            fetched_at=fetched_at,
            row_logger=active_logger,
        )
        if row is None:
            skipped_invalid += 1
            continue
        rows.append(row)

    return rows, skipped_invalid


def _validate_payload_envelope(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("payload invalido: esperado dict")
    if "rates" not in payload:
        raise ValueError("payload invalido: campo rates ausente")
    if not isinstance(payload["rates"], dict):
        raise ValueError("payload invalido: rates deve ser dict")


def _build_row(
    rate_date: str,
    base: str,
    symbol: Any,
    rate: Any,
    source: str,
    fetched_at: str,
    row_logger: logging.Logger | logging.LoggerAdapter,
) -> FxRateRow | None:
    if not isinstance(symbol, str) or not symbol.strip():
        row_logger.warning(
            "skipping row with invalid symbol date=%s base=%s symbol=%r",
            rate_date,
            base,
            symbol,
            extra={"event": "skip_invalid_symbol"},
        )
        return None
    if not isinstance(rate, (int, float)):
        row_logger.warning(
            "skipping row with invalid rate date=%s base=%s symbol=%s rate=%r",
            rate_date,
            base,
            symbol.strip().upper(),
            rate,
            extra={"event": "skip_invalid_rate"},
        )
        return None
    return FxRateRow(
        date=rate_date,
        base=base,
        symbol=symbol.strip().upper(),
        rate=float(rate),
        source=source,
        fetched_at=fetched_at,
    )


def _should_retry_response(response: requests.Response) -> bool:
    return response.status_code == 429 or 500 <= response.status_code <= 599


def _retry_delay_seconds(retry_after: str | None, *, retry_number: int) -> float:
    parsed_retry_after = _parse_retry_after_seconds(retry_after)
    if parsed_retry_after is not None:
        return parsed_retry_after
    return 0.5 * (2 ** (retry_number - 1))


def _parse_retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None

    try:
        return max(0.0, float(value))
    except ValueError:
        pass

    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None

    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)

    delta = (retry_at - datetime.now(timezone.utc)).total_seconds()
    return max(0.0, delta)
