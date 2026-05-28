from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

SECRET_ENV_NAMES = {
    "TWELVE_DATA_API_KEY",
    "COINGECKO_DEMO_API_KEY",
    "COINGECKO_PRO_API_KEY",
    "FRED_API_KEY",
    "FX_API_KEY",
    "CRYPTO_API_KEY",
    "MACRO_API_KEY",
}

SECRET_KEY_NAMES = {
    "apikey",
    "api_key",
    "api-key",
    "token",
    "key",
    "authorization",
    "x-cg-demo-api-key",
    "x-cg-pro-api-key",
}


def redact_secret(value: Any) -> str:
    text = "" if value is None else str(value)
    if not text:
        return "****"
    if len(text) <= 8:
        return "****"
    return f"{text[:4]}****{text[-4:]}"


def redact_params(params: Any) -> Any:
    if isinstance(params, Mapping):
        redacted: dict[Any, Any] = {}
        for key, value in params.items():
            normalized = str(key).strip().lower()
            if normalized in SECRET_KEY_NAMES or normalized.upper() in SECRET_ENV_NAMES:
                redacted[key] = "****"
            else:
                redacted[key] = redact_params(value)
        return redacted
    if isinstance(params, list):
        return [redact_params(item) for item in params]
    if isinstance(params, tuple):
        return tuple(redact_params(item) for item in params)
    return params


def redact_text(text: Any) -> str:
    raw = "" if text is None else str(text)
    if not raw:
        return raw
    redacted = raw
    for name in SECRET_ENV_NAMES:
        redacted = re.sub(
            rf"({re.escape(name)}\s*[=:]\s*)([^\s,;&|]+)",
            rf"\1****",
            redacted,
            flags=re.IGNORECASE,
        )
    redacted = re.sub(
        r"((?:apikey|api_key|api-key|token|key)=)([^&\s]+)",
        r"\1****",
        redacted,
        flags=re.IGNORECASE,
    )
    redacted = re.sub(
        r"((?:x-cg-demo-api-key|x-cg-pro-api-key|authorization)\s*[=:]\s*)([^\s,;&|]+)",
        r"\1****",
        redacted,
        flags=re.IGNORECASE,
    )
    return redacted


def truncate_text(value: Any, *, limit: int = 500) -> str:
    text = redact_text(value)
    if len(text) <= limit:
        return text
    return text[:limit] + "...<truncated>"
