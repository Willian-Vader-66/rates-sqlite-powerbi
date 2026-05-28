from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest

from fx_rates.crypto_providers import CoinGeckoProvider, CoinGeckoProviderError, CryptoAssetConfig, RateLimiter, _daily_map_with_diagnostics
from fx_rates.redaction import redact_params, redact_secret, redact_text


ASSET = CryptoAssetConfig("BTC", "Bitcoin", "bitcoin", True, 1)


class _Response:
    def __init__(self, payload, status_code: int = 200, text: str | None = None, headers: dict[str, str] | None = None):
        self._payload = payload
        self.status_code = status_code
        self.text = text if text is not None else str(payload)
        self.headers = headers or {}

    def json(self):
        return self._payload


class _Session:
    def __init__(self, responses: list[_Response]):
        self.responses = list(responses)
        self.calls: list[dict] = []

    def get(self, url, params, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers or {}, "timeout": timeout})
        if not self.responses:
            raise AssertionError("unexpected request")
        return self.responses.pop(0)


def _provider(session: _Session, **kwargs) -> CoinGeckoProvider:
    return CoinGeckoProvider(
        timeout_seconds=1,
        max_retries=0,
        rate_limiter=RateLimiter(0),
        session=session,
        sleep_func=lambda _seconds: None,
        **kwargs,
    )


def _ms(raw: str) -> int:
    return int(datetime.fromisoformat(raw).replace(tzinfo=timezone.utc).timestamp() * 1000)


def _payload(*prices: tuple[str, float]) -> dict:
    return {
        "prices": [[_ms(day), price] for day, price in prices],
        "market_caps": [[_ms(day), price * 1000] for day, price in prices],
        "total_volumes": [[_ms(day), price * 100] for day, price in prices],
    }


def test_coingecko_parses_normal_market_chart_range() -> None:
    session = _Session([_Response(_payload(("2026-01-01", 100.0), ("2026-01-02", 110.0)))])

    rows = _provider(session).fetch_daily(ASSET, "2026-01-01", "2026-01-02")

    assert [row.date for row in rows] == ["2026-01-01", "2026-01-02"]
    assert rows[-1].price_usd == 110.0
    assert rows[-1].market_cap == 110000.0
    assert session.calls[0]["url"].endswith("/coins/bitcoin/market_chart/range")


def test_coingecko_public_rejects_history_above_365_days_without_api_call() -> None:
    session = _Session([])

    with pytest.raises(CoinGeckoProviderError, match="up to 365 days"):
        _provider(session).fetch_daily(ASSET, "2024-01-01", "2025-01-01")

    assert session.calls == []


def test_coingecko_public_accepts_365_day_history_request() -> None:
    session = _Session([_Response(_payload(("2025-01-01", 100.0), ("2025-12-31", 110.0)))])

    rows = _provider(session).fetch_daily(ASSET, "2025-01-01", "2025-12-31")

    assert len(rows) == 2
    assert len(session.calls) == 1


def test_coingecko_sorts_timestamps_and_keeps_latest_point_per_day() -> None:
    session = _Session(
        [
            _Response(
                {
                    "prices": [
                        [_ms("2026-01-02T00:00:00"), 101.0],
                        [_ms("2026-01-01T00:00:00"), 90.0],
                        [_ms("2026-01-02T23:00:00"), 111.0],
                    ],
                    "market_caps": [],
                    "total_volumes": [],
                }
            )
        ]
    )

    rows = _provider(session).fetch_daily(ASSET, "2026-01-01", "2026-01-02")

    assert [row.date for row in rows] == ["2026-01-01", "2026-01-02"]
    assert rows[-1].price_usd == 111.0


def test_coingecko_empty_payload_raises_clear_error() -> None:
    session = _Session([_Response({"prices": [], "market_caps": [], "total_volumes": []})] * 3)

    with pytest.raises(ValueError, match="no prices"):
        _provider(session).fetch_daily(ASSET, "2026-01-01", "2026-01-02")


def test_coingecko_missing_prices_raises_clear_error() -> None:
    session = _Session([_Response({"market_caps": [], "total_volumes": []})] * 3)

    with pytest.raises(ValueError, match="no prices"):
        _provider(session).fetch_daily(ASSET, "2026-01-01", "2026-01-02")


def test_coingecko_ignores_invalid_points_with_diagnostics() -> None:
    points = [
        [_ms("2026-01-01T00:00:00"), 100.0],
        ["bad", 101.0],
        [_ms("2026-01-02T00:00:00")],
        [_ms("2026-01-03T00:00:00"), None],
    ]

    mapped, diagnostics = _daily_map_with_diagnostics(points)

    assert mapped == {"2026-01-01": 100.0}
    assert diagnostics["raw_points"] == 4
    assert diagnostics["invalid_points"] == 3
    assert diagnostics["deduplicated_points"] == 1


def test_coingecko_retries_429_then_succeeds() -> None:
    session = _Session([
        _Response({"error": "slow down"}, status_code=429, headers={"Retry-After": "0"}),
        _Response(_payload(("2026-01-01", 100.0))),
    ])
    provider = CoinGeckoProvider(
        timeout_seconds=1,
        max_retries=1,
        rate_limiter=RateLimiter(0),
        session=session,
        sleep_func=lambda _seconds: None,
    )

    rows = provider.fetch_daily(ASSET, "2026-01-01", "2026-01-01")

    assert len(rows) == 1
    assert len(session.calls) == 2


def test_coingecko_does_not_retry_401_or_403() -> None:
    session = _Session([_Response({"error": "bad key"}, status_code=401, text="bad key")])

    with pytest.raises(CoinGeckoProviderError, match="HTTP 401"):
        _provider(session).fetch_daily(ASSET, "2026-01-01", "2026-01-01")

    assert len(session.calls) == 1


def test_coingecko_range_limit_401_falls_back_to_smaller_ranges() -> None:
    range_limit = _Response(
        {"error": {"status": {"error_code": 10012, "error_message": "allowed time range past 365 days"}}},
        status_code=401,
        text='{"error":{"status":{"error_code":10012,"error_message":"allowed time range past 365 days"}}}',
    )
    session = _Session([range_limit, range_limit, _Response(_payload(("2026-01-01", 100.0)))])

    rows = _provider(session).fetch_daily(ASSET, "2026-01-01", "2026-01-02")

    assert [row.date for row in rows] == ["2026-01-01"]
    assert len(session.calls) == 3


def test_coingecko_falls_back_to_year_chunks_when_full_range_is_empty() -> None:
    session = _Session(
        [
            _Response({"prices": [], "market_caps": [], "total_volumes": []}),
            _Response(_payload(("2024-01-01", 100.0))),
            _Response(_payload(("2025-01-01", 130.0))),
        ]
    )

    rows = _provider(session, api_plan="pro", pro_api_key="pro_test_key").fetch_daily(ASSET, "2024-01-01", "2025-01-02")

    assert [row.date for row in rows] == ["2024-01-01", "2025-01-01"]
    assert len(session.calls) == 3


def test_coingecko_falls_back_to_year_chunks_when_full_range_is_rejected() -> None:
    session = _Session(
        [
            _Response({"error": "range too large"}, status_code=400, text="range too large"),
            _Response(_payload(("2026-01-01", 100.0))),
        ]
    )

    rows = _provider(session).fetch_daily(ASSET, "2026-01-01", "2026-01-02")

    assert [row.date for row in rows] == ["2026-01-01"]
    assert len(session.calls) == 2


def test_coingecko_falls_back_to_90_day_chunks_when_year_chunk_is_retryable() -> None:
    session = _Session(
        [
            _Response({"prices": [], "market_caps": [], "total_volumes": []}),
            _Response({"error": "rate limit"}, status_code=429, text="rate limit"),
            _Response(_payload(("2026-01-01", 100.0))),
            _Response(_payload(("2026-04-01", 120.0))),
        ]
    )

    rows = _provider(session).fetch_daily(ASSET, "2026-01-01", "2026-04-01")

    assert [row.date for row in rows] == ["2026-01-01", "2026-04-01"]
    assert len(session.calls) == 4


def test_coingecko_falls_back_to_90_day_chunks_when_year_chunk_is_rejected() -> None:
    session = _Session(
        [
            _Response({"prices": [], "market_caps": [], "total_volumes": []}),
            _Response({"error": "range too large"}, status_code=400, text="range too large"),
            _Response(_payload(("2026-01-01", 100.0))),
            _Response(_payload(("2026-04-01", 120.0))),
        ]
    )

    rows = _provider(session).fetch_daily(ASSET, "2026-01-01", "2026-04-01")

    assert [row.date for row in rows] == ["2026-01-01", "2026-04-01"]
    assert len(session.calls) == 4


def test_coingecko_demo_key_is_sent_but_never_logged(caplog) -> None:
    secret = "cg_demo_secret_123456"
    session = _Session([_Response({"bitcoin": {"usd": 100.0}})])
    provider = CoinGeckoProvider(
        timeout_seconds=1,
        max_retries=0,
        rate_limiter=RateLimiter(0),
        session=session,
        api_plan="demo",
        demo_api_key=secret,
        sleep_func=lambda _seconds: None,
    )

    with caplog.at_level(logging.INFO):
        quote = provider.fetch_quote(ASSET)

    assert quote.price == 100.0
    assert session.calls[0]["headers"]["x-cg-demo-api-key"] == secret
    assert secret not in caplog.text


def test_coingecko_logs_response_diagnostics(caplog) -> None:
    session = _Session([_Response(_payload(("2026-01-01", 100.0)), headers={"Content-Type": "application/json"})])

    with caplog.at_level(logging.INFO):
        rows = _provider(session).fetch_daily(ASSET, "2026-01-01", "2026-01-01")

    assert len(rows) == 1
    assert "coingecko_response diagnostics" in caplog.text
    assert "content_type" in caplog.text
    assert "body_length" in caplog.text
    assert "coingecko_history_normalized" in caplog.text


def test_coingecko_history_request_log_includes_symbol(caplog) -> None:
    session = _Session([_Response(_payload(("2026-01-01", 100.0)))])

    with caplog.at_level(logging.INFO):
        rows = _provider(session).fetch_daily(ASSET, "2026-01-01", "2026-01-01")

    assert len(rows) == 1
    assert "coingecko_history_request symbol=BTC coin_id=bitcoin" in caplog.text


def test_redaction_masks_params_and_text() -> None:
    assert redact_secret("abcd12345678wxyz") == "abcd****wxyz"
    assert redact_params({"apikey": "abcd12345678wxyz", "symbol": "AAPL"}) == {"apikey": "****", "symbol": "AAPL"}
    assert "abcd12345678wxyz" not in redact_text("url?apikey=abcd12345678wxyz")
