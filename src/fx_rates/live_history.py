from __future__ import annotations

from dataclasses import dataclass

STANDARD_HISTORY_ERROR = (
    "Standard live mode supports up to 365 days. For longer history, set "
    "LIVE_HISTORY_MODE=advanced and configure a paid provider that supports the requested range."
)


@dataclass(frozen=True)
class LiveHistoryPolicy:
    default_days: int = 365
    max_free_days: int = 365
    mode: str = "standard"
    advanced_max_years: int = 10

    @property
    def advanced_history_enabled(self) -> bool:
        return self.mode == "advanced"

    @property
    def advanced_history_available(self) -> bool:
        return self.advanced_history_enabled


def normalize_history_mode(value: str) -> str:
    normalized = (value or "standard").strip().lower()
    if normalized not in {"standard", "advanced"}:
        raise ValueError("LIVE_HISTORY_MODE invalido: use standard ou advanced")
    return normalized


def days_from_args(*, days: int | None, years: int | None, default_days: int) -> int:
    if days is not None:
        if days <= 0:
            raise ValueError("--days deve ser maior que zero")
        return days
    if years is not None:
        if years <= 0:
            raise ValueError("--years deve ser maior que zero")
        return years * 365
    return default_days


def validate_requested_days(policy: LiveHistoryPolicy, requested_days: int, *, provider_plan: str | None = None) -> None:
    if requested_days <= 0:
        raise ValueError("requested_days deve ser maior que zero")
    if policy.mode == "standard" and requested_days > policy.max_free_days:
        raise ValueError(STANDARD_HISTORY_ERROR)
    if policy.mode == "advanced":
        max_days = max(1, policy.advanced_max_years) * 365
        if requested_days > max_days:
            raise ValueError(f"Advanced history supports up to {policy.advanced_max_years} years.")
        if provider_plan and provider_plan.strip().lower() in {"public", "demo"} and requested_days > policy.max_free_days:
            raise ValueError(
                "Advanced crypto history above 365 days requires COINGECKO_API_PLAN=pro "
                "or another paid provider that supports the requested range."
            )
