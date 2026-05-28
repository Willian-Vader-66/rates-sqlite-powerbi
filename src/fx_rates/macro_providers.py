from __future__ import annotations

import csv
import hashlib
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Protocol

import requests

from .models import MacroIndicatorDailyRow
from .redaction import redact_params
from .utils import parse_yyyy_mm_dd, utc_now_iso

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MacroIndicatorConfig:
    indicator_code: str
    indicator_name: str
    source: str
    provider_code: str
    unit: str | None
    is_active: bool
    priority: int


class MacroDataProvider(Protocol):
    name: str

    def fetch_daily(self, indicator: MacroIndicatorConfig, start: str, end: str) -> list[MacroIndicatorDailyRow]:
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


class BcbSgsProvider:
    name = "bcb_sgs"

    def __init__(
        self,
        timeout_seconds: int = 20,
        max_retries: int = 3,
        rate_limiter: RateLimiter | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.rate_limiter = rate_limiter or RateLimiter(min_interval_seconds=1.0)
        self.session = session or requests.Session()
        self.base_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs"

    def fetch_daily(self, indicator: MacroIndicatorConfig, start: str, end: str) -> list[MacroIndicatorDailyRow]:
        parse_yyyy_mm_dd(start)
        parse_yyyy_mm_dd(end)
        payload = self._request_json(
            indicator.provider_code,
            {
                "formato": "json",
                "dataInicial": _to_bcb_date(start),
                "dataFinal": _to_bcb_date(end),
            },
        )
        fetched_at = utc_now_iso()
        rows: list[MacroIndicatorDailyRow] = []
        for item in payload:
            day = _from_bcb_date(str(item.get("data", "")))
            value = _to_float(item.get("valor"))
            if day:
                rows.append(
                    MacroIndicatorDailyRow(
                        date=day,
                        indicator_code=indicator.indicator_code,
                        indicator_name=indicator.indicator_name,
                        value=value,
                        unit=indicator.unit,
                        source=indicator.source,
                        fetched_at=fetched_at,
                    )
                )
        return rows

    def status(self) -> dict[str, Any]:
        return {"name": self.name, "configured": True, "rate_limited": True}

    def _request_json(self, provider_code: str, params: dict[str, str]) -> list[dict[str, Any]]:
        url = f"{self.base_url}.{provider_code}/dados"
        for attempt in range(self.max_retries + 1):
            self.rate_limiter.wait()
            logger.info("provider_call provider=%s endpoint=%s params=%s", self.name, provider_code, redact_params(params))
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
            return data if isinstance(data, list) else []
        raise RuntimeError(f"falha inesperada em provider macro={provider_code}")


class MockMacroProvider:
    name = "mock_macro"

    def fetch_daily(self, indicator: MacroIndicatorConfig, start: str, end: str) -> list[MacroIndicatorDailyRow]:
        start_day = datetime.strptime(parse_yyyy_mm_dd(start), "%Y-%m-%d").date()
        end_day = datetime.strptime(parse_yyyy_mm_dd(end), "%Y-%m-%d").date()
        if start_day > end_day:
            raise ValueError("start precisa ser menor ou igual a end")

        fetched_at = utc_now_iso()
        rows: list[MacroIndicatorDailyRow] = []
        index = 0
        current = start_day
        while current <= end_day:
            if current.weekday() < 5:
                rows.append(
                    MacroIndicatorDailyRow(
                        date=current.isoformat(),
                        indicator_code=indicator.indicator_code,
                        indicator_name=indicator.indicator_name,
                        value=_mock_indicator_value(indicator.indicator_code, index),
                        unit=indicator.unit,
                        source="mock",
                        fetched_at=fetched_at,
                    )
                )
                index += 1
            current += timedelta(days=1)
        return rows

    def status(self) -> dict[str, Any]:
        return {"name": self.name, "configured": True, "demo": True}


def build_macro_provider(demo_mode: bool, timeout_seconds: int, max_retries: int) -> MacroDataProvider:
    if demo_mode:
        return MockMacroProvider()
    return BcbSgsProvider(timeout_seconds=timeout_seconds, max_retries=max_retries)


def load_macro_reference(path: str) -> list[MacroIndicatorConfig]:
    rows: list[MacroIndicatorConfig] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"indicator_code", "indicator_name", "source", "provider_code", "unit", "is_active", "priority"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"macro reference sem colunas obrigatorias: {', '.join(sorted(missing))}")
        for item in reader:
            code = (item.get("indicator_code") or "").strip().upper()
            provider_code = (item.get("provider_code") or "").strip()
            if not code or not provider_code:
                continue
            rows.append(
                MacroIndicatorConfig(
                    indicator_code=code,
                    indicator_name=(item.get("indicator_name") or code).strip(),
                    source=(item.get("source") or "unknown").strip(),
                    provider_code=provider_code,
                    unit=(item.get("unit") or "").strip() or None,
                    is_active=_parse_active(item.get("is_active")),
                    priority=_parse_priority(item.get("priority")),
                )
            )
    return sorted(rows, key=lambda row: (row.priority, row.indicator_code))


def _mock_indicator_value(indicator_code: str, index: int) -> float:
    normalized = indicator_code.strip().upper()
    profiles = {
        "SELIC_DAILY": (0.040, 0.0008),
        "CDI_DAILY": (0.039, 0.0007),
        "FED_FUNDS_DAILY": (5.25, 0.035),
        "SELIC_MONTHLY": (0.88, 0.018),
        "IPCA_MONTHLY": (0.36, 0.055),
        "SELIC_ANNUALIZED_MONTHLY": (10.50, 0.08),
        "US_CPI_MONTHLY": (315.0, 0.9),
    }
    if normalized in profiles:
        base, amplitude = profiles[normalized]
    elif "DAILY" in normalized:
        base, amplitude = 0.040, 0.0008
    elif "MONTHLY" in normalized and "ANNUALIZED" not in normalized:
        base, amplitude = 0.65, 0.04
    else:
        base, amplitude = 10.50, 0.08
    phase = _stable_unit(normalized, 0) * math.pi
    slow_wave = math.sin(index / 35.0 + phase) * amplitude
    small_noise = (_stable_unit(normalized, index) - 0.5) * amplitude * 0.08
    return round(base + slow_wave + small_noise, 4)


def _stable_unit(value: str, salt: int) -> float:
    raw = f"{value.strip().upper()}:{salt}".encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def _to_bcb_date(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").strftime("%d/%m/%Y")


def _from_bcb_date(value: str) -> str | None:
    try:
        return datetime.strptime(value, "%d/%m/%Y").date().isoformat()
    except ValueError:
        return None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _parse_active(value: str | None) -> bool:
    if value is None:
        return True
    return value.strip().lower() not in {"0", "false", "no", "n"}


def _parse_priority(value: str | None) -> int:
    if value is None or not value.strip():
        return 100
    return int(value)
