from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Protocol

from .config import Settings


class ProviderContract(Protocol):
    provider_name: str

    def fetch_latest(self, symbols: list[str]) -> list[dict[str, Any]]:
        ...

    def fetch_history(self, symbol: str, start: str, end: str) -> list[dict[str, Any]]:
        ...

    def normalize_symbol(self, symbol: str) -> str:
        ...

    def supports(self, symbol: str, asset_type: str | None = None) -> bool:
        ...


@dataclass(frozen=True)
class ProviderStatus:
    asset_type: str
    provider: str
    configured: bool
    available: bool
    status: str
    requires_api_key: bool
    api_key_detected: bool
    missing_env: list[str]
    supported_assets: list[str]
    supported_symbols: list[str]
    external_test: str = "skipped"
    message: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "asset_type": self.asset_type,
            "provider": self.provider,
            "configured": self.configured,
            "available": self.available,
            "status": self.status,
            "requires_api_key": self.requires_api_key,
            "api_key_detected": self.api_key_detected,
            "missing_env": self.missing_env,
            "supported_assets": self.supported_assets,
            "supported_symbols": self.supported_symbols,
            "external_test": self.external_test,
            "message": self.message,
        }


def normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def providers_status(settings: Settings, *, test_external: bool = False) -> dict[str, Any]:
    statuses = [
        _provider_status("FX", settings.fx_provider, requires_key=False, key_name=None, supported_symbols=_fx_symbols()),
        _provider_status("CRYPTO", settings.crypto_provider, requires_key=False, key_name=None, supported_symbols=_crypto_symbols()),
        _provider_status("STOCK", settings.stock_provider, requires_key=settings.stock_provider != "fake_live", key_name="TWELVE_DATA_API_KEY", supported_symbols=_stock_symbols()),
        _provider_status("MACRO", settings.macro_provider, requires_key=settings.macro_provider == "fred", key_name="FRED_API_KEY", supported_symbols=_macro_symbols(settings.macro_provider)),
    ]
    if test_external:
        statuses = [
            ProviderStatus(**{**status.as_dict(), "external_test": "not_implemented", "message": status.message or "External smoke test is intentionally not run by default."})
            for status in statuses
        ]
    missing = [status for status in statuses if not status.configured]
    return {
        "providers": [status.as_dict() for status in statuses],
        "api_keys": {
            "TWELVE_DATA_API_KEY": _key_detected("TWELVE_DATA_API_KEY"),
            "FRED_API_KEY": _key_detected("FRED_API_KEY"),
        },
        "external_test": "requested" if test_external else "skipped",
        "all_configured": not missing,
        "recommendation": _recommendation(missing),
    }


def print_providers_status(settings: Settings, *, test_external: bool = False) -> int:
    payload = providers_status(settings, test_external=test_external)
    print("PROVIDERS STATUS")
    for item in payload["providers"]:
        configured = "configured" if item["configured"] else "not_configured"
        key = "key=yes" if item["api_key_detected"] else "key=no"
        print(
            f"{item['asset_type']}: provider={item['provider']} status={configured} "
            f"available={str(item['available']).lower()} requires_api_key={str(item['requires_api_key']).lower()} "
            f"missing_env={','.join(item['missing_env']) or '-'} {key} external_test={item['external_test']}"
        )
        print(f"  supported_assets={','.join(item['supported_assets']) or '-'} supported_symbols={','.join(item['supported_symbols'][:20]) or '-'}")
        if item.get("message"):
            print(f"  {item['message']}")
    print("API keys detected: " + ", ".join(f"{name}={'yes' if detected else 'no'}" for name, detected in payload["api_keys"].items()))
    print("Recommendation: " + payload["recommendation"])
    return 0


def _provider_status(
    asset_type: str,
    provider: str,
    *,
    requires_key: bool,
    key_name: str | None,
    supported_symbols: list[str],
) -> ProviderStatus:
    normalized = (provider or "none").strip().lower()
    api_key_detected = _key_detected(key_name) if key_name else False
    disabled = normalized in {"", "none", "disabled", "off"}
    missing_env = [key_name] if requires_key and key_name and not api_key_detected else []
    configured = not disabled and (api_key_detected if requires_key else True)
    available = configured
    status = "configured" if configured else "not_configured"
    message = None
    if disabled:
        message = f"{asset_type} provider disabled."
    elif requires_key and not api_key_detected:
        message = f"{key_name} not set; live {asset_type.lower()} ingestion is unavailable."
    return ProviderStatus(
        asset_type=asset_type,
        provider=normalized,
        configured=configured,
        available=available,
        status=status,
        requires_api_key=requires_key,
        api_key_detected=api_key_detected,
        missing_env=missing_env,
        supported_assets=[asset_type],
        supported_symbols=supported_symbols if configured or normalized == "fake_live" else [],
        message=message,
    )


def _key_detected(name: str | None) -> bool:
    return bool(name and os.getenv(name, "").strip())


def _recommendation(missing: list[ProviderStatus]) -> str:
    if not missing:
        return "Providers are configured. Next: python -m fx_rates dashboard prepare-live --years 4"
    assets = ", ".join(status.asset_type for status in missing)
    return f"Configure providers/API keys for: {assets}. Demo remains available with dashboard prepare-demo --years 4 --demo."


def _fx_symbols() -> list[str]:
    return ["BRL", "EUR", "ARS", "CAD", "CHF", "CLP", "CNY", "JPY", "MXN", "GBP", "AUD", "NZD", "COP", "ZAR", "SEK", "NOK", "DKK", "PLN"]


def _crypto_symbols() -> list[str]:
    return ["BTC", "ETH", "ADA", "SOL", "BNB", "XRP", "DOGE", "AVAX", "DOT", "LINK"]


def _stock_symbols() -> list[str]:
    return ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM", "KO", "AMD"]


def _macro_symbols(provider: str) -> list[str]:
    if provider == "fred":
        return ["FED_FUNDS_DAILY", "US_CPI_MONTHLY"]
    return ["SELIC_DAILY", "SELIC_MONTHLY", "SELIC_ANNUALIZED_MONTHLY", "CDI_DAILY", "IPCA_MONTHLY"]
