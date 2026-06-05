from __future__ import annotations

import requests

from fx_rates.env_doctor import _key_env_status, _recommendations, classify_external_error, collect_env_doctor
from fx_rates.tls_support import TruststoreStatus


def test_env_doctor_detects_command_pasted_as_twelve_key(monkeypatch) -> None:
    monkeypatch.setenv("TWELVE_DATA_API_KEY", 'python -m fx_rates providers status --external-test')

    status = _key_env_status("TWELVE_DATA_API_KEY")

    assert status["present"] is True
    assert status["key_valid_format"] is False
    assert status["suspicious"] is True
    assert "command" in status["note"]


def test_env_doctor_rejects_path_or_long_text_as_twelve_key(monkeypatch) -> None:
    monkeypatch.setenv("TWELVE_DATA_API_KEY", r"C:\Projetos_Local\rates-sqlite-powerbi-git")
    path_status = _key_env_status("TWELVE_DATA_API_KEY")
    assert path_status["present"] is True
    assert path_status["key_valid_format"] is False

    monkeypatch.setenv("TWELVE_DATA_API_KEY", "A" * 129)
    long_status = _key_env_status("TWELVE_DATA_API_KEY")
    assert long_status["present"] is True
    assert long_status["key_valid_format"] is False


def test_env_doctor_classifies_ssl_error_with_recommendation() -> None:
    classified = classify_external_error(requests.exceptions.SSLError("certificate verify failed: secret=abc"))

    assert classified.error_type == "SSL_ERROR"
    assert "certifi" in (classified.recommendation or "")
    assert "truststore" in (classified.recommendation or "")


def test_env_doctor_collects_without_printing_key(monkeypatch) -> None:
    secret = "valid-looking-key-12345"
    monkeypatch.setenv("TWELVE_DATA_API_KEY", secret)
    monkeypatch.setattr(
        "fx_rates.env_doctor._test_https",
        lambda url, timeout_seconds: {"url": url, "status": "OK", "error_type": "OK", "http_status": 200, "message": "ok"},
    )

    payload = collect_env_doctor(
        timeout_seconds=1,
        truststore_status=TruststoreStatus(requested=True, enabled=True, installed=True, message="truststore injected"),
    )

    assert payload["api_keys"]["TWELVE_DATA_API_KEY"]["present"] is True
    assert payload["api_keys"]["TWELVE_DATA_API_KEY"]["key_valid_format"] is True
    assert secret not in str(payload)


def test_env_doctor_recommends_twelve_key_when_missing() -> None:
    network = {"FX": {"error_type": "OK"}}
    keys = {"TWELVE_DATA_API_KEY": {"present": False, "key_valid_format": False}}

    recommendations = _recommendations(network, keys)

    assert '$env:TWELVE_DATA_API_KEY = "sua-chave"' in recommendations
