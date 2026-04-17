import pytest
import requests

from fx_rates.api_frankfurter import (
    FrankfurterCacheError,
    FrankfurterClient,
    FrankfurterHttpError,
    FrankfurterJsonError,
)
from fx_rates.utils import stable_hash


class _FakeResponse:
    def __init__(self, payload=None, json_error: Exception | None = None):
        self._payload = payload
        self._json_error = json_error

    def raise_for_status(self):
        return None

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._payload


def test_fetch_latest_raises_http_error(monkeypatch, tmp_path):
    client = FrankfurterClient(
        base_url="https://api.frankfurter.dev",
        cache_dir=str(tmp_path),
        timeout=20,
        use_cache=False,
    )

    def fake_get(*args, **kwargs):
        raise requests.HTTPError("503 Service Unavailable")

    monkeypatch.setattr("fx_rates.api_frankfurter.requests.get", fake_get)

    with pytest.raises(FrankfurterHttpError, match="HTTP request failed"):
        client.fetch_latest("USD", ["BRL"])


def test_fetch_latest_raises_json_error(monkeypatch, tmp_path):
    client = FrankfurterClient(
        base_url="https://api.frankfurter.dev",
        cache_dir=str(tmp_path),
        timeout=20,
        use_cache=False,
    )

    monkeypatch.setattr(
        "fx_rates.api_frankfurter.requests.get",
        lambda *args, **kwargs: _FakeResponse(json_error=ValueError("broken json")),
    )

    with pytest.raises(FrankfurterJsonError, match="not valid JSON"):
        client.fetch_latest("USD", ["BRL"])


def test_fetch_latest_raises_cache_error_for_corrupt_json(tmp_path):
    client = FrankfurterClient(
        base_url="https://api.frankfurter.dev",
        cache_dir=str(tmp_path),
        timeout=20,
        use_cache=True,
    )
    url = "https://api.frankfurter.dev/v1/latest"
    params = {"base": "USD", "symbols": "BRL"}
    cache_path = tmp_path / f"{stable_hash(url, params)}.json"
    cache_path.write_text("{broken", encoding="utf-8")

    with pytest.raises(FrankfurterCacheError, match="Cached JSON is invalid"):
        client.fetch_latest("USD", ["BRL"])


def test_fetch_latest_ignores_bad_cache_when_no_cache(monkeypatch, tmp_path):
    client = FrankfurterClient(
        base_url="https://api.frankfurter.dev",
        cache_dir=str(tmp_path),
        timeout=20,
        use_cache=False,
    )
    url = "https://api.frankfurter.dev/v1/latest"
    params = {"base": "USD", "symbols": "BRL"}
    cache_path = tmp_path / f"{stable_hash(url, params)}.json"
    cache_path.write_text("{broken", encoding="utf-8")

    monkeypatch.setattr(
        "fx_rates.api_frankfurter.requests.get",
        lambda *args, **kwargs: _FakeResponse(
            payload={"amount": 1.0, "base": "USD", "date": "2026-03-09", "rates": {"BRL": 5.23}}
        ),
    )

    payload = client.fetch_latest("USD", ["BRL"])

    assert payload["rates"]["BRL"] == 5.23
