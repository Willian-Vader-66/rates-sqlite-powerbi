from __future__ import annotations

import os
import platform
import socket
import ssl
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from .provider_status import key_status
from .redaction import redact_text
from .tls_support import maybe_inject_truststore
from .utils import utc_now_iso


REPORT_PATH = Path("docs/ENV_DOCTOR_REPORT.md")
NETWORK_ENDPOINTS = {
    "FX": "https://api.frankfurter.dev",
    "Crypto": "https://api.coingecko.com",
    "Macro": "https://api.bcb.gov.br",
    "Twelve": "https://api.twelvedata.com",
}
KEY_ENV_NAMES = ["TWELVE_DATA_API_KEY", "COINGECKO_DEMO_API_KEY", "COINGECKO_PRO_API_KEY", "FRED_API_KEY"]
SUSPICIOUS_KEY_MARKERS = ["cd c:", "python", "fx_rates", "$env:", "powershell", "setx ", " -m ", "dashboard ", "providers "]


@dataclass(frozen=True)
class ClassifiedError:
    error_type: str
    message: str
    recommendation: str | None = None


def classify_external_error(exc: BaseException) -> ClassifiedError:
    message = redact_text(str(exc).strip() or exc.__class__.__name__)
    lowered = message.lower()
    if isinstance(exc, requests.exceptions.SSLError) or "ssl" in lowered or "certificate" in lowered or "cert" in lowered:
        return ClassifiedError("SSL_ERROR", _short_message(message), _tls_recommendation())
    if isinstance(exc, (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout, requests.exceptions.Timeout)) or "timed out" in lowered or "timeout" in lowered:
        return ClassifiedError("TIMEOUT", _short_message(message), "Increase --timeout or retry after checking network connectivity.")
    if isinstance(exc, requests.exceptions.ConnectionError):
        if "name resolution" in lowered or "getaddrinfo" in lowered or isinstance(getattr(exc, "__cause__", None), socket.gaierror):
            return ClassifiedError("DNS_ERROR", _short_message(message), "Check DNS, proxy, VPN, and firewall settings.")
        return ClassifiedError("UNKNOWN", _short_message(message), "Check network, proxy, VPN, and firewall settings.")
    if isinstance(exc, requests.exceptions.HTTPError):
        return ClassifiedError("HTTP_ERROR", _short_message(message), None)
    if "unauthorized" in lowered or "apikey" in lowered or "api key" in lowered or "invalid key" in lowered:
        return ClassifiedError("AUTH_ERROR", _short_message(message), "Check the provider API key environment variable without printing the key.")
    return ClassifiedError("UNKNOWN", _short_message(message), None)


def run_env_doctor(*, timeout_seconds: int = 10, truststore_status: Any | None = None) -> int:
    truststore_status = truststore_status or maybe_inject_truststore()
    payload = collect_env_doctor(timeout_seconds=timeout_seconds, truststore_status=truststore_status)
    write_env_doctor_report(payload, REPORT_PATH)
    print_env_doctor(payload, REPORT_PATH)
    return 0 if _overall_ok(payload) else 1


def collect_env_doctor(*, timeout_seconds: int = 10, truststore_status: Any | None = None) -> dict[str, Any]:
    certifi_info = _certifi_info()
    cert_env = {name: _path_env_status(name) for name in ["SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE"]}
    network = {name: _test_https(url, timeout_seconds=timeout_seconds) for name, url in NETWORK_ENDPOINTS.items()}
    keys = {name: _key_env_status(name) for name in KEY_ENV_NAMES}
    return {
        "generated_at": utc_now_iso(),
        "python": {
            "version": sys.version.replace("\n", " "),
            "version_info": platform.python_version(),
            "executable": sys.executable,
            "prefix": sys.prefix,
            "base_prefix": getattr(sys, "base_prefix", sys.prefix),
            "venv_active": sys.prefix != getattr(sys, "base_prefix", sys.prefix) or bool(os.getenv("VIRTUAL_ENV")),
            "virtual_env": os.getenv("VIRTUAL_ENV", ""),
        },
        "certificates": {
            "openssl": ssl.OPENSSL_VERSION,
            "certifi": certifi_info,
            "env": cert_env,
            "truststore": {
                "requested": bool(getattr(truststore_status, "requested", False)),
                "enabled": bool(getattr(truststore_status, "enabled", False)),
                "installed": bool(getattr(truststore_status, "installed", _module_installed("truststore"))),
                "message": getattr(truststore_status, "message", ""),
            },
        },
        "network": network,
        "api_keys": keys,
        "recommendations": _recommendations(network, keys),
    }


def print_env_doctor(payload: dict[str, Any], report_path: Path = REPORT_PATH) -> None:
    python_info = payload["python"]
    certs = payload["certificates"]
    print("ENV DOCTOR")
    print(f"Python: {python_info['version_info']} executable={python_info['executable']}")
    print(f"Venv active: {str(python_info['venv_active']).lower()} virtual_env={python_info['virtual_env'] or '-'}")
    print(f"OpenSSL: {certs['openssl']}")
    print(f"certifi: installed={str(certs['certifi']['installed']).lower()} path={certs['certifi']['path'] or '-'} exists={str(certs['certifi']['exists']).lower()}")
    for name, info in certs["env"].items():
        print(f"{name}: set={str(info['set']).lower()} path={info['value'] or '-'} exists={str(info['exists']).lower()}")
    trust = certs["truststore"]
    print(f"truststore: installed={str(trust['installed']).lower()} requested={str(trust['requested']).lower()} enabled={str(trust['enabled']).lower()} message={trust['message'] or '-'}")
    for name, info in payload["network"].items():
        print(f"{name} endpoint: status={info['status']} type={info['error_type']} http_status={info.get('http_status') or '-'} message={info['message'] or '-'}")
    for name, info in payload["api_keys"].items():
        print(
            f"{name}: present={str(info['present']).lower()} key_length={info['key_length']} "
            f"key_valid_format={str(info['key_valid_format']).lower()} suspicious={str(info['suspicious']).lower()}"
        )
    if payload["recommendations"]:
        print("Recommendations:")
        for item in payload["recommendations"]:
            print(f"- {item}")
    print(f"Report: {report_path}")


def write_env_doctor_report(payload: dict[str, Any], path: Path = REPORT_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Environment Doctor Report",
        "",
        f"Generated: {payload['generated_at']}",
        "",
        "## Python",
        "",
        f"- Version: `{payload['python']['version_info']}`",
        f"- Executable: `{payload['python']['executable']}`",
        f"- Venv active: `{payload['python']['venv_active']}`",
        f"- Virtual env: `{payload['python']['virtual_env'] or '-'}`",
        "",
        "## Certificates",
        "",
        f"- OpenSSL: `{payload['certificates']['openssl']}`",
        f"- certifi installed: `{payload['certificates']['certifi']['installed']}`",
        f"- certifi path: `{payload['certificates']['certifi']['path'] or '-'}`",
        f"- certifi exists: `{payload['certificates']['certifi']['exists']}`",
    ]
    for name, info in payload["certificates"]["env"].items():
        lines.append(f"- {name}: set=`{info['set']}`, exists=`{info['exists']}`, path=`{info['value'] or '-'}`")
    trust = payload["certificates"]["truststore"]
    lines.extend(
        [
            f"- truststore installed: `{trust['installed']}`",
            f"- truststore requested: `{trust['requested']}`",
            f"- truststore enabled: `{trust['enabled']}`",
            f"- truststore message: `{trust['message'] or '-'}`",
            "",
            "## Network",
            "",
            "| Provider | URL | Status | Error type | HTTP status | Message |",
            "|---|---|---|---|---:|---|",
        ]
    )
    for name, info in payload["network"].items():
        lines.append(f"| {name} | {info['url']} | {info['status']} | {info['error_type']} | {info.get('http_status') or ''} | {info['message'] or '-'} |")
    lines.extend(["", "## API Keys", "", "| Name | Present | Key length | Valid format | Suspicious | Note |", "|---|---:|---:|---:|---:|---|"])
    for name, info in payload["api_keys"].items():
        lines.append(
            f"| {name} | {info['present']} | {info['key_length']} | {info['key_valid_format']} | {info['suspicious']} | {info['note'] or '-'} |"
        )
    lines.extend(["", "## Recommendations", ""])
    if payload["recommendations"]:
        lines.extend(f"- {item}" for item in payload["recommendations"])
    else:
        lines.append("- Environment checks passed.")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def _certifi_info() -> dict[str, Any]:
    try:
        import certifi
    except Exception as exc:
        return {"installed": False, "path": "", "exists": False, "message": str(exc)}
    cert_path = certifi.where()
    return {"installed": True, "path": cert_path, "exists": Path(cert_path).exists(), "message": ""}


def _path_env_status(name: str) -> dict[str, Any]:
    value = os.getenv(name, "").strip()
    return {"set": bool(value), "value": value, "exists": Path(value).exists() if value else False}


def _test_https(url: str, *, timeout_seconds: int) -> dict[str, Any]:
    try:
        response = requests.get(url, timeout=timeout_seconds)
        status = "OK" if response.status_code < 500 else "FAIL"
        error_type = "OK" if status == "OK" else "HTTP_ERROR"
        message = "HTTPS connection succeeded" if status == "OK" else f"HTTP {response.status_code}"
        return {"url": url, "status": status, "error_type": error_type, "http_status": response.status_code, "message": message}
    except requests.exceptions.RequestException as exc:
        classified = classify_external_error(exc)
        return {"url": url, "status": "FAIL", "error_type": classified.error_type, "http_status": None, "message": classified.message}


def _key_env_status(name: str) -> dict[str, Any]:
    raw = os.getenv(name, "")
    stripped = raw.strip()
    base = key_status(raw)
    suspicious_note = _suspicious_key_note(raw)
    valid = bool(base["valid_format"]) and not suspicious_note
    return {
        "present": bool(base["present"]),
        "key_length": len(stripped),
        "key_valid_format": valid,
        "suspicious": bool(suspicious_note),
        "note": suspicious_note,
    }


def _suspicious_key_note(value: str) -> str:
    if not value:
        return ""
    stripped = value.strip()
    lowered = stripped.lower()
    if "\n" in value or "\r" in value:
        return "value contains multiple lines"
    if any(marker in lowered for marker in SUSPICIOUS_KEY_MARKERS):
        return "value looks like a command, not an API key"
    if len(stripped.split()) > 1:
        return "value contains spaces"
    return ""


def _recommendations(network: dict[str, Any], keys: dict[str, Any]) -> list[str]:
    items: list[str] = []
    if any(item["error_type"] == "SSL_ERROR" for item in network.values()):
        items.extend(_tls_recommendation().split(" | "))
    twelve = keys.get("TWELVE_DATA_API_KEY", {})
    if not twelve.get("present"):
        items.append('$env:TWELVE_DATA_API_KEY = "sua-chave"')
    elif not twelve.get("key_valid_format"):
        items.append("Re-enter TWELVE_DATA_API_KEY with only the key text, not commands or placeholders.")
    return items


def _tls_recommendation() -> str:
    return (
        "python -m pip install --upgrade certifi truststore"
        " | set SSL_CERT_FILE/REQUESTS_CA_BUNDLE to certifi.where()"
        " | optional on Windows: $env:FX_RATES_USE_TRUSTSTORE=\"1\""
    )


def _short_message(message: str, *, limit: int = 220) -> str:
    compact = " ".join(message.split())
    return compact if len(compact) <= limit else compact[: limit - 3] + "..."


def _module_installed(name: str) -> bool:
    try:
        __import__(name)
    except Exception:
        return False
    return True


def _overall_ok(payload: dict[str, Any]) -> bool:
    network_ok = all(item["status"] == "OK" for item in payload["network"].values())
    twelve_ok = bool(payload["api_keys"]["TWELVE_DATA_API_KEY"]["key_valid_format"])
    return network_ok and twelve_ok
