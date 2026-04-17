from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
import requests

from fx_rates.api_frankfurter import FrankfurterClient, normalize_latest, normalize_timeseries, validate_latest_payload
from fx_rates.utils import cache_key


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200, headers: dict[str, str] | None = None):
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}", response=self)

    def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse | Exception]):
        self._responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, params: dict[str, str], timeout: int) -> _FakeResponse:
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_normalize_latest_rows() -> None:
    payload = {
        "base": "USD",
        "date": "2026-02-10",
        "rates": {"BRL": 5.12, "EUR": 0.93},
    }

    rows = normalize_latest(payload, base="USD")

    assert len(rows) == 2
    assert {row.symbol for row in rows} == {"BRL", "EUR"}
    assert all(row.base == "USD" for row in rows)


def test_normalize_timeseries_rows() -> None:
    payload = {
        "base": "USD",
        "rates": {
            "2026-02-09": {"BRL": 5.10},
            "2026-02-10": {"BRL": 5.12, "EUR": 0.93},
        },
    }

    rows = normalize_timeseries(payload, base="USD")

    assert len(rows) == 3
    assert any(row.date == "2026-02-09" for row in rows)
    assert any(row.symbol == "EUR" for row in rows)


def test_invalid_latest_payload_raises() -> None:
    payload = {"base": "USD", "rates": {"BRL": 5.12}}
    with pytest.raises(ValueError):
        validate_latest_payload(payload)


def test_invalid_rate_is_skipped_with_warning(caplog: pytest.LogCaptureFixture) -> None:
    payload = {
        "base": "USD",
        "date": "2026-02-10",
        "rates": {"BRL": "bad", "EUR": 0.93},
    }

    with caplog.at_level(logging.WARNING):
        rows = normalize_latest(payload, base="USD")

    assert len(rows) == 1
    assert rows[0].symbol == "EUR"
    assert "skipping row with invalid rate" in caplog.text


def test_invalid_timeseries_payload_raises() -> None:
    payload = {
        "base": "USD",
        "rates": {
            "bad-date": {"BRL": 5.12},
        },
    }

    with pytest.raises(ValueError):
        normalize_timeseries(payload, base="USD")


def test_latest_ignores_stale_cache_by_default(tmp_path: Path) -> None:
    stale_payload = {"base": "USD", "date": "2026-02-09", "rates": {"BRL": 5.00}}
    fresh_payload = {"base": "USD", "date": "2026-02-10", "rates": {"BRL": 5.12}}
    params = {"base": "USD", "symbols": "BRL"}
    cache_file = tmp_path / f"{cache_key('https://api.frankfurter.dev/v1/latest', params)}.json"
    cache_file.write_text(json.dumps(stale_payload), encoding="utf-8")

    session = _FakeSession([_FakeResponse(fresh_payload)])
    client = FrankfurterClient(
        base_url="https://api.frankfurter.dev/v1",
        cache_dir=str(tmp_path),
        timeout_seconds=5,
        use_cache=True,
        use_cache_latest=False,
        session=session,
    )

    payload = client.fetch_latest(base="USD", symbols=["BRL"])

    assert payload == fresh_payload
    assert len(session.calls) == 1


def test_cache_key_is_deterministic_for_normalized_symbols(tmp_path: Path) -> None:
    payload = {
        "base": "USD",
        "rates": {
            "2026-02-09": {"BRL": 5.10, "EUR": 0.93},
        },
    }
    session = _FakeSession([_FakeResponse(payload)])
    client = FrankfurterClient(
        base_url="https://api.frankfurter.dev/v1",
        cache_dir=str(tmp_path),
        timeout_seconds=5,
        use_cache=True,
        max_retries=0,
        session=session,
    )

    first = client.fetch_timeseries(start="2026-02-09", end="2026-02-09", base="usd", symbols=["eur", " BRL ", "eur"])
    second = client.fetch_timeseries(start="2026-02-09", end="2026-02-09", base="USD", symbols=["BRL", "EUR"])

    assert first == second
    assert len(session.calls) == 1
    assert session.calls[0]["params"] == {"base": "USD", "symbols": "BRL,EUR"}


def test_retry_on_429_respects_retry_after() -> None:
    payload = {"base": "USD", "date": "2026-02-10", "rates": {"BRL": 5.12}}
    session = _FakeSession(
        [
            _FakeResponse({"error": "slow down"}, status_code=429, headers={"Retry-After": "1"}),
            _FakeResponse(payload),
        ]
    )
    sleep_calls: list[float] = []
    client = FrankfurterClient(
        base_url="https://api.frankfurter.dev/v1",
        cache_dir="cache",
        timeout_seconds=5,
        use_cache=False,
        max_retries=1,
        session=session,
        sleep_func=sleep_calls.append,
    )

    result = client.fetch_latest(base="USD", symbols=["BRL"])

    assert result == payload
    assert len(session.calls) == 2
    assert sleep_calls == [1.0]
