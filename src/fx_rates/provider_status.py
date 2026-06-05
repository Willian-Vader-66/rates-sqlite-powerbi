from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Protocol

from .config import Settings
from .redaction import redact_text


INVALID_KEY_MARKERS = {
    "",
    "none",
    "null",
    "sua_chave_aqui",
    "your_key",
    "your_api_key",
    "your_twelve_data_api_key",
    "change_me",
    "changeme",
    "todo",
    "test",
    "fake",
    "demo",
    "placeholder",
}


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
    key_present: bool
    key_valid_format: bool
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
            "key_present": self.key_present,
            "key_valid_format": self.key_valid_format,
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
        _provider_status("FX", settings.fx_provider, requires_key=False, key_name=None, api_key=settings.fx_api_key, supported_symbols=_fx_symbols()),
        _provider_status(
            "CRYPTO",
            settings.crypto_provider,
            requires_key=settings.crypto_provider == "coingecko" and settings.coingecko_api_plan == "pro",
            key_name="COINGECKO_PRO_API_KEY" if settings.coingecko_api_plan == "pro" else None,
            api_key=settings.coingecko_pro_api_key if settings.coingecko_api_plan == "pro" else settings.coingecko_demo_api_key,
            supported_symbols=_crypto_symbols(),
        ),
        _provider_status("STOCK", settings.stock_provider, requires_key=settings.stock_provider != "fake_live", key_name="TWELVE_DATA_API_KEY", api_key=settings.twelve_data_api_key, supported_symbols=_stock_symbols()),
        _provider_status("MACRO", settings.macro_provider, requires_key=settings.macro_provider == "fred", key_name="FRED_API_KEY", api_key=settings.fred_api_key, supported_symbols=_macro_symbols(settings.macro_provider)),
    ]
    if test_external:
        statuses = [_with_external_test(status, settings) for status in statuses]
    missing = [status for status in statuses if not status.configured]
    return {
        "providers": [status.as_dict() for status in statuses],
        "api_keys": {
            "TWELVE_DATA_API_KEY": key_status(settings.twelve_data_api_key),
            "COINGECKO_DEMO_API_KEY": key_status(settings.coingecko_demo_api_key),
            "COINGECKO_PRO_API_KEY": key_status(settings.coingecko_pro_api_key),
            "FRED_API_KEY": key_status(settings.fred_api_key),
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
        key = "key_present=yes" if item["key_present"] else "key_present=no"
        print(
            f"{item['asset_type']}: provider={item['provider']} status={configured} "
            f"available={str(item['available']).lower()} requires_api_key={str(item['requires_api_key']).lower()} "
            f"missing_env={','.join(item['missing_env']) or '-'} {key} "
            f"key_valid_format={str(item['key_valid_format']).lower()} external_test={item['external_test']}"
        )
        print(f"  supported_assets={','.join(item['supported_assets']) or '-'} supported_symbols={','.join(item['supported_symbols'][:20]) or '-'}")
        if item.get("message"):
            print(f"  {item['message']}")
    print(
        "API keys detected: "
        + ", ".join(
            f"{name}=present:{str(info['present']).lower()},valid_format:{str(info['valid_format']).lower()}"
            for name, info in payload["api_keys"].items()
        )
    )
    print("Recommendation: " + payload["recommendation"])
    return 0


def _provider_status(
    asset_type: str,
    provider: str,
    *,
    requires_key: bool,
    key_name: str | None,
    api_key: str,
    supported_symbols: list[str],
) -> ProviderStatus:
    normalized = (provider or "none").strip().lower()
    key_info = key_status(api_key)
    key_present = key_info["present"] if key_name else False
    key_valid_format = key_info["valid_format"] if key_name else True
    api_key_detected = key_present and key_valid_format
    disabled = normalized in {"", "none", "disabled", "off"}
    missing_env = [key_name] if requires_key and key_name and not api_key_detected else []
    configured = not disabled and (api_key_detected if requires_key else True)
    available = configured
    status = "configured" if configured else "not_configured"
    message = None
    if disabled:
        message = f"{asset_type} provider disabled."
    elif requires_key and not key_present:
        message = f"{key_name} not set; live {asset_type.lower()} ingestion is unavailable."
    elif requires_key and not key_valid_format:
        message = f"{key_name} is present but invalid or placeholder-like; live {asset_type.lower()} ingestion is unavailable."
    return ProviderStatus(
        asset_type=asset_type,
        provider=normalized,
        configured=configured,
        available=available,
        status=status,
        requires_api_key=requires_key,
        key_present=key_present,
        key_valid_format=key_valid_format,
        api_key_detected=api_key_detected,
        missing_env=missing_env,
        supported_assets=[asset_type],
        supported_symbols=supported_symbols if configured or normalized == "fake_live" else [],
        message=message,
    )


def key_status(value: str | None) -> dict[str, bool]:
    raw = "" if value is None else str(value)
    stripped = raw.strip()
    present = bool(stripped)
    normalized = re.sub(r"[\s\-]+", "_", stripped.lower())
    command_markers = (
        "$env:",
        "python",
        "powershell",
        "pwsh",
        "fx_rates",
        "run_live_pipeline",
        "run_finance_monitor",
        "twelve_data_api_key=",
        "twleve_data_api_key=",
        "cd ",
        " c:",
        "c:\\",
        ";",
        "|",
        "&",
        '"',
        "'",
        "`",
    )
    valid_format = present
    if not present:
        valid_format = False
    elif stripped != raw or any(ch.isspace() for ch in stripped):
        valid_format = False
    elif len(stripped) > 128:
        valid_format = False
    elif any(marker in stripped.lower() for marker in command_markers):
        valid_format = False
    elif re.fullmatch(r"[A-Za-z0-9._-]+", stripped) is None:
        valid_format = False
    elif normalized in INVALID_KEY_MARKERS:
        valid_format = False
    elif any(marker in normalized for marker in INVALID_KEY_MARKERS if marker):
        valid_format = False
    elif len(stripped) < 12:
        valid_format = False
    return {"present": present, "valid_format": valid_format}


def _with_external_test(status: ProviderStatus, settings: Settings) -> ProviderStatus:
    if not status.configured:
        return ProviderStatus(**{**status.as_dict(), "external_test": "fail", "available": False})
    if status.provider == "fake_live":
        return ProviderStatus(**{**status.as_dict(), "external_test": "pass", "available": True})
    try:
        if status.asset_type == "STOCK":
            from .market_providers import build_market_provider

            provider = build_market_provider(
                provider_name=settings.stock_provider,
                api_key=settings.twelve_data_api_key,
                demo_mode=False,
                timeout_seconds=min(settings.timeout_seconds, 10),
                max_retries=0,
            )
            quote = provider.fetch_quote("AAPL")
            if quote.price is None or quote.price <= 0:
                raise ValueError("invalid quote payload")
        elif status.asset_type == "CRYPTO":
            from .crypto_providers import build_crypto_provider, load_crypto_reference

            asset = next(item for item in load_crypto_reference("data/reference/crypto_assets.csv") if item.symbol == "BTC")
            quote = build_crypto_provider(
                False,
                min(settings.timeout_seconds, 10),
                0,
                coingecko_api_plan=settings.coingecko_api_plan,
                coingecko_demo_api_key=settings.coingecko_demo_api_key,
                coingecko_pro_api_key=settings.coingecko_pro_api_key,
            ).fetch_quote(asset)
            if quote.price is None or quote.price <= 0:
                raise ValueError("invalid crypto payload")
        elif status.asset_type == "MACRO":
            from .macro_providers import build_macro_provider, load_macro_reference

            indicator = next(item for item in load_macro_reference("data/reference/macro_indicators.csv") if item.indicator_code == "SELIC_DAILY")
            rows = build_macro_provider(False, min(settings.timeout_seconds, 10), 0).fetch_daily(indicator, "2026-01-01", "2026-01-10")
            if not rows:
                raise ValueError("empty macro payload")
        elif status.asset_type == "FX":
            from .api_frankfurter import FrankfurterClient

            payload = FrankfurterClient(
                base_url=settings.api_base_url,
                cache_dir=settings.cache_dir,
                timeout_seconds=min(settings.timeout_seconds, 10),
                use_cache=False,
                max_retries=0,
            ).fetch_latest("USD", ["BRL", "EUR"])
            if not payload.get("rates"):
                raise ValueError("empty FX payload")
        return ProviderStatus(**{**status.as_dict(), "external_test": "pass", "available": True, "message": status.message})
    except Exception as exc:
        return ProviderStatus(**{**status.as_dict(), "external_test": "fail", "available": False, "message": _external_error_message(exc)})


def _external_error_message(exc: Exception) -> str:
    from .env_doctor import classify_external_error

    classified = classify_external_error(exc)
    message = redact_text(str(exc).strip() or exc.__class__.__name__)
    lowered = message.lower()
    if classified.error_type == "SSL_ERROR":
        return (
            "external test failed: SSL_ERROR: TLS/CA validation failed. "
            "Recommendations: python -m pip install --upgrade certifi truststore; "
            "set SSL_CERT_FILE/REQUESTS_CA_BUNDLE to certifi.where(); "
            'optional on Windows: $env:FX_RATES_USE_TRUSTSTORE="1".'
        )
    if classified.error_type in {"DNS_ERROR", "TIMEOUT", "HTTP_ERROR", "UNKNOWN"}:
        return f"external test failed: {classified.error_type}: {classified.message}"
    if "rate" in lowered and "limit" in lowered:
        return f"external test failed: RATE_LIMIT: {message}"
    if "unauthorized" in lowered or "apikey" in lowered or "api key" in lowered or "invalid key" in lowered:
        return f"external test failed: AUTH_ERROR: {message}"
    return f"external test failed: {message}"


def _recommendation(missing: list[ProviderStatus]) -> str:
    if not missing:
        return "Providers are configured. Next: python -m fx_rates dashboard build-live-db --days 365 --db-path .tmp/live-main-candidate.sqlite --external-test"
    assets = ", ".join(status.asset_type for status in missing)
    return (
        f"Configure providers/API keys for: {assets}. "
        "Live-first staging command: python -m fx_rates dashboard build-live-db --days 365 "
        "--db-path .tmp/live-main-candidate.sqlite --external-test."
    )


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
