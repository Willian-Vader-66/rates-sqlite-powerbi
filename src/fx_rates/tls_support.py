from __future__ import annotations

import os
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class TruststoreStatus:
    requested: bool
    enabled: bool
    installed: bool
    message: str


def maybe_inject_truststore() -> TruststoreStatus:
    requested = os.getenv("FX_RATES_USE_TRUSTSTORE", "").strip().lower() in {"1", "true", "yes", "on"}
    if not requested:
        return TruststoreStatus(requested=False, enabled=False, installed=False, message="FX_RATES_USE_TRUSTSTORE not set")
    try:
        import truststore  # type: ignore
    except Exception:
        message = "FX_RATES_USE_TRUSTSTORE=1 requested but truststore is not installed. Install with: python -m pip install truststore"
        print(message, file=sys.stderr)
        return TruststoreStatus(requested=True, enabled=False, installed=False, message=message)
    try:
        truststore.inject_into_ssl()
    except Exception as exc:
        message = f"truststore import succeeded but inject_into_ssl failed: {exc.__class__.__name__}: {exc}"
        print(message, file=sys.stderr)
        return TruststoreStatus(requested=True, enabled=False, installed=True, message=message)
    return TruststoreStatus(requested=True, enabled=True, installed=True, message="truststore injected into ssl")
