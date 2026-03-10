import logging

import pytest

from fx_rates.api_frankfurter import FrankfurterPayloadError, normalize_payload


def test_normalize_latest_and_skip_invalid_rate(caplog):
    payload = {
        "amount": 1.0,
        "base": "USD",
        "date": "2026-03-09",
        "rates": {
            "BRL": 5.23,
            "EUR": "0.86",
            "JPY": "bad-value",
        },
    }

    with caplog.at_level(logging.WARNING):
        rows = normalize_payload(payload)

    assert len(rows) == 2
    assert rows[0].date == "2026-03-09"
    assert rows[0].base == "USD"
    assert {row.symbol for row in rows} == {"BRL", "EUR"}
    assert "Skipping invalid rate for JPY" in caplog.text


def test_normalize_timeseries_payload():
    payload = {
        "amount": 1.0,
        "base": "USD",
        "rates": {
            "2026-03-06": {"BRL": 5.21, "EUR": 0.86},
            "2026-03-09": {"BRL": 5.23, "EUR": 0.87},
        },
    }

    rows = normalize_payload(payload, fetched_at="2026-03-09T12:00:00+00:00")

    assert len(rows) == 4
    assert rows[0].fetched_at == "2026-03-09T12:00:00+00:00"
    assert rows[-1].symbol == "EUR"


def test_normalize_latest_rejects_invalid_date():
    payload = {
        "amount": 1.0,
        "base": "USD",
        "date": "09-03-2026",
        "rates": {"BRL": 5.23},
    }

    with pytest.raises(FrankfurterPayloadError, match="invalid date"):
        normalize_payload(payload)


def test_normalize_timeseries_rejects_invalid_date_key():
    payload = {
        "amount": 1.0,
        "base": "USD",
        "rates": {"03-09-2026": {"BRL": 5.23}},
    }

    with pytest.raises(FrankfurterPayloadError, match="invalid rates date key"):
        normalize_payload(payload)
