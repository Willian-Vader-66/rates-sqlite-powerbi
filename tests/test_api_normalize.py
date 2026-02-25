from __future__ import annotations

from unittest.mock import patch

import pytest

from fx_rates.api_frankfurter import (
    FrankfurterClient,
    normalize_latest,
    normalize_timeseries,
    validate_latest_payload,
)


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


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


@patch("fx_rates.api_frankfurter.requests.get")
def test_cache_hit_miss(mock_get, tmp_path) -> None:
    payload = {"base": "USD", "date": "2026-02-10", "rates": {"BRL": 5.12}}
    mock_get.return_value = _FakeResponse(payload)

    client = FrankfurterClient(
        base_url="https://api.frankfurter.dev/v1",
        cache_dir=str(tmp_path),
        timeout_seconds=5,
        use_cache=True,
    )

    first = client.fetch_latest(base="USD", symbols=["BRL"])
    second = client.fetch_latest(base="USD", symbols=["BRL"])

    assert first == second
    assert mock_get.call_count == 1
