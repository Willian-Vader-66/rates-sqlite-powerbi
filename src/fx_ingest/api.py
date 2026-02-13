from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


class FrankfurterClient:
    def __init__(
        self,
        base_url: str = "https://api.frankfurter.dev",
        cache_dir: str = ".cache/http",
        timeout_seconds: int = 20,
        use_cache: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.cache_dir = Path(cache_dir)
        self.timeout_seconds = timeout_seconds
        self.use_cache = use_cache
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def fetch_latest(self, base: str, symbols: list[str]) -> dict[str, Any]:
        params = {"base": base.upper(), "symbols": ",".join(_clean_symbols(symbols))}
        return self._get_json("/v1/latest", params)

    def fetch_timeseries(self, start: str, end: str, base: str, symbols: list[str]) -> dict[str, Any]:
        params = {"base": base.upper(), "symbols": ",".join(_clean_symbols(symbols))}
        return self._get_json(f"/v1/{start}..{end}", params)

    def _cache_key(self, endpoint: str, params: dict[str, Any]) -> str:
        key = json.dumps({"endpoint": endpoint, "params": params}, sort_keys=True)
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    def _cache_file(self, endpoint: str, params: dict[str, Any]) -> Path:
        return self.cache_dir / f"{self._cache_key(endpoint, params)}.json"

    def _get_json(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        cache_file = self._cache_file(endpoint, params)
        if self.use_cache and cache_file.exists():
            return json.loads(cache_file.read_text(encoding="utf-8"))

        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, params=params, timeout=self.timeout_seconds)
        response.raise_for_status()
        payload = response.json()

        _validate_payload(payload)

        if self.use_cache:
            cache_file.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")

        return payload


def normalize_payload(
    payload: dict[str, Any],
    base: str | None = None,
    source: str = "frankfurter",
    fetched_at: str | None = None,
) -> list[dict[str, Any]]:
    _validate_payload(payload)

    base_value = (base or payload.get("base") or "").upper()
    if not base_value:
        raise ValueError("Payload sem base valida.")

    timestamp = fetched_at or datetime.now(timezone.utc).isoformat()
    rates = payload["rates"]
    rows: list[dict[str, Any]] = []

    if rates and all(isinstance(v, (int, float)) for v in rates.values()):
        row_date = str(payload.get("date", ""))
        if not row_date:
            raise ValueError("Payload latest sem campo date.")

        for symbol, rate in rates.items():
            rows.append(
                {
                    "date": row_date,
                    "base": base_value,
                    "symbol": str(symbol).upper(),
                    "rate": float(rate),
                    "source": source,
                    "fetched_at": timestamp,
                }
            )
        return rows

    for row_date, per_symbol in rates.items():
        if not isinstance(per_symbol, dict):
            raise ValueError("Formato de rates invalido para serie temporal.")
        for symbol, rate in per_symbol.items():
            rows.append(
                {
                    "date": str(row_date),
                    "base": base_value,
                    "symbol": str(symbol).upper(),
                    "rate": float(rate),
                    "source": source,
                    "fetched_at": timestamp,
                }
            )

    return rows


def _validate_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Resposta da API invalida: payload nao e objeto.")
    if "rates" not in payload:
        raise ValueError("Resposta da API invalida: campo rates ausente.")
    if not isinstance(payload["rates"], dict):
        raise ValueError("Resposta da API invalida: rates deve ser objeto.")


def _clean_symbols(symbols: list[str]) -> list[str]:
    cleaned = [s.strip().upper() for s in symbols if s.strip()]
    if not cleaned:
        raise ValueError("Informe ao menos um symbol.")
    return cleaned
