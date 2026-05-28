from __future__ import annotations

import pytest

from fx_rates.live_history import LiveHistoryPolicy, days_from_args, validate_requested_days


def test_standard_history_allows_365_days() -> None:
    policy = LiveHistoryPolicy(mode="standard", max_free_days=365)

    validate_requested_days(policy, 365, provider_plan="public")


def test_standard_history_rejects_years_above_one() -> None:
    policy = LiveHistoryPolicy(mode="standard", max_free_days=365)
    requested_days = days_from_args(days=None, years=4, default_days=365)

    with pytest.raises(ValueError, match="Standard live mode supports up to 365 days"):
        validate_requested_days(policy, requested_days, provider_plan="public")


def test_advanced_history_requires_paid_crypto_provider() -> None:
    policy = LiveHistoryPolicy(mode="advanced", max_free_days=365, advanced_max_years=10)

    with pytest.raises(ValueError, match="requires COINGECKO_API_PLAN=pro"):
        validate_requested_days(policy, 730, provider_plan="public")

    validate_requested_days(policy, 730, provider_plan="pro")
