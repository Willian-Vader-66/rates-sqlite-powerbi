from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

from fx_rates.api_smoke import smoke_live_api
from fx_rates.config import DEFAULTS
from fx_rates.db_sqlite import (
    get_system_status,
    initialize_schema,
    upsert_instruments,
    upsert_market_quotes_latest,
    upsert_stock_prices_daily,
)
from fx_rates.live_first import run_audit_live, run_build_live_db
from fx_rates.live_full_test import run_live_full_test
from fx_rates.live_promotion import run_promote_live, run_restore_backup
from fx_rates.live_refresh import run_refresh_live
from fx_rates.live_samples import SampleValidationResult, format_sample_validation, validate_samples
from fx_rates.live_scope import required_scope_items
from fx_rates.live_validation import LiveValidationResult, _stale_status, validate_live_database
from fx_rates.crypto_providers import CoinGeckoProviderError
from fx_rates.models import CryptoPriceDailyRow, InstrumentRow, MarketQuoteRow, StockPriceDailyRow


def _settings(db_path: str, tmp_path: Path):
    return DEFAULTS.__class__(
        **{
            **DEFAULTS.__dict__,
            "db_path": db_path,
            "cache_dir": str(tmp_path / "cache"),
            "log_file": str(tmp_path / "app.log"),
            "timeout_seconds": 1,
            "max_retries": 0,
            "market_data_provider": "mock",
            "market_data_demo_mode": False,
            "stock_provider": "fake_live",
            "fx_provider": "fake_live",
            "crypto_provider": "fake_live",
            "macro_provider": "fake_live",
            "api_host": "127.0.0.1",
            "api_port": 8001,
        }
    )


def _make_full_fake_live_db(tmp_path: Path, name: str = "live.sqlite") -> tuple[str, object]:
    db_path = str(tmp_path / name)
    settings = _settings(db_path, tmp_path)
    code = run_live_full_test(
        settings,
        years=1,
        db_path=db_path,
        asset_type="ALL",
        top=10,
        allow_partial=False,
        external_test=False,
        report_path=tmp_path / "live-report.md",
    )
    assert code == 0
    return db_path, settings


def test_live_full_test_fake_providers_generates_live_db(tmp_path: Path) -> None:
    db_path, _settings_obj = _make_full_fake_live_db(tmp_path)

    status = get_system_status(db_path)

    assert status["data_mode"] == "live"
    assert status["historical_row_count"] > 0
    assert "fake_live" in status["providers"]


def test_build_live_db_fake_providers_creates_live_first_scope(tmp_path: Path) -> None:
    db_path = str(tmp_path / "candidate.sqlite")
    settings = _settings(db_path, tmp_path)

    code = run_build_live_db(
        settings,
        years=1,
        db_path=db_path,
        asset_type="ALL",
        top=10,
        allow_partial=False,
        external_test=False,
        report_path=tmp_path / "build.md",
    )

    status = get_system_status(db_path)
    assert code == 0
    assert status["data_mode"] == "live"
    assert status["active_stocks"] == 10
    assert status["active_currencies"] == 6
    assert status["active_crypto"] == 5
    assert status["active_macro"] == 3


def test_build_live_db_reports_candidate_ready_not_not_ready(tmp_path: Path, capsys) -> None:
    db_path = str(tmp_path / "candidate-status.sqlite")
    settings = _settings(db_path, tmp_path)

    code = run_build_live_db(
        settings,
        years=1,
        db_path=db_path,
        asset_type="ALL",
        top=10,
        allow_partial=False,
        external_test=False,
        report_path=tmp_path / "build-status.md",
    )

    output = capsys.readouterr().out
    assert code == 0
    assert "LIVE-FIRST DB BUILD STATUS: CANDIDATE_READY" in output
    assert "sample_validation_required: true" in output
    assert "sample_validation_status: NOT_RUN" in output


def test_validate_live_passes_with_fake_live_db(tmp_path: Path) -> None:
    db_path, _settings_obj = _make_full_fake_live_db(tmp_path)

    result = validate_live_database(db_path, expected_years=1, report_path=tmp_path / "validate.md")

    assert result.status == "OK"
    assert not result.critical_failures


def test_audit_live_passes_with_fake_live_db(tmp_path: Path) -> None:
    db_path, _settings_obj = _make_full_fake_live_db(tmp_path)

    code = run_audit_live(db_path, expected_years=1, report_path=tmp_path / "audit-live.md")

    assert code == 0


def test_ipca_monthly_uses_monthly_stale_threshold() -> None:
    item = next(row for row in required_scope_items() if row.symbol == "IPCA_MONTHLY")
    latest_date = (date.today() - timedelta(days=60)).isoformat()

    result = _stale_status("MACRO", "IPCA_MONTHLY", latest_date, item)

    assert result["status"] == "OK"
    assert result["allowed_stale_days"] == 75


def test_audit_live_does_not_treat_ipca_monthly_as_daily(tmp_path: Path) -> None:
    db_path, _settings_obj = _make_full_fake_live_db(tmp_path)
    latest_date = (date.today() - timedelta(days=60)).isoformat()
    with sqlite3.connect(db_path) as conn:
        old_max = conn.execute("SELECT MAX(date) FROM macro_indicators_daily WHERE indicator_code='IPCA_MONTHLY'").fetchone()[0]
        conn.execute("DELETE FROM macro_indicators_daily WHERE indicator_code='IPCA_MONTHLY' AND date=?", (latest_date,))
        conn.execute("UPDATE macro_indicators_daily SET date=? WHERE indicator_code='IPCA_MONTHLY' AND date=?", (latest_date, old_max))
        conn.execute("DELETE FROM macro_indicators_daily WHERE indicator_code='IPCA_MONTHLY' AND date > ?", (latest_date,))
        conn.execute("UPDATE market_quotes_latest SET quote_time=?, source_updated_at=? WHERE asset_type='MACRO' AND symbol='IPCA_MONTHLY'", (latest_date, latest_date))
        conn.commit()

    result = validate_live_database(db_path, expected_years=1, report_path=tmp_path / "ipca-audit.md")
    row = next(item for item in result.symbols if item["symbol"] == "IPCA_MONTHLY")

    assert result.status in {"OK", "WARN"}
    assert row["status"] == "OK"
    assert row["rows"] >= 10
    assert row["stale_status"] == "OK"
    assert row["allowed_stale_days"] == 75
    assert not any("IPCA_MONTHLY: history range shorter than expected" in item for item in result.critical_failures)
    assert not any("IPCA_MONTHLY: STALE_DATA" in item for item in result.warnings)


def test_audit_live_warns_for_ipca_monthly_after_allowed_window(tmp_path: Path) -> None:
    db_path, _settings_obj = _make_full_fake_live_db(tmp_path)
    latest_date = (date.today() - timedelta(days=90)).isoformat()
    with sqlite3.connect(db_path) as conn:
        old_max = conn.execute("SELECT MAX(date) FROM macro_indicators_daily WHERE indicator_code='IPCA_MONTHLY'").fetchone()[0]
        conn.execute("DELETE FROM macro_indicators_daily WHERE indicator_code='IPCA_MONTHLY' AND date=?", (latest_date,))
        conn.execute("UPDATE macro_indicators_daily SET date=? WHERE indicator_code='IPCA_MONTHLY' AND date=?", (latest_date, old_max))
        conn.execute("DELETE FROM macro_indicators_daily WHERE indicator_code='IPCA_MONTHLY' AND date > ?", (latest_date,))
        conn.execute("UPDATE market_quotes_latest SET quote_time=?, source_updated_at=? WHERE asset_type='MACRO' AND symbol='IPCA_MONTHLY'", (latest_date, latest_date))
        conn.commit()

    result = validate_live_database(db_path, expected_years=1, report_path=tmp_path / "ipca-warn.md")
    row = next(item for item in result.symbols if item["symbol"] == "IPCA_MONTHLY")

    assert result.status == "WARN"
    assert row["status"] == "WARN"
    assert row["stale_status"] == "WARN"
    assert not result.critical_failures
    assert any("IPCA_MONTHLY: monthly macro series validated" in item for item in result.warnings)
    assert not any("IPCA_MONTHLY: history range shorter than expected" in item for item in result.critical_failures)


def test_audit_live_fails_when_ipca_monthly_missing(tmp_path: Path) -> None:
    db_path, _settings_obj = _make_full_fake_live_db(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM macro_indicators_daily WHERE indicator_code='IPCA_MONTHLY'")
        conn.execute("DELETE FROM market_quotes_latest WHERE asset_type='MACRO' AND symbol='IPCA_MONTHLY'")
        conn.commit()

    result = validate_live_database(db_path, expected_years=1, report_path=tmp_path / "ipca-missing.md")

    assert result.status == "FAIL"
    assert any("MACRO IPCA_MONTHLY: no live history" in item for item in result.critical_failures)


def test_validate_live_fails_with_mock_provider_marked_live(tmp_path: Path) -> None:
    db_path = str(tmp_path / "bad-provider.sqlite")
    initialize_schema(db_path)
    upsert_instruments(db_path, [_instrument("AAPL", "mock", data_mode="live")])
    upsert_stock_prices_daily(db_path, [_stock_row("AAPL", "mock", data_mode="live")])
    upsert_market_quotes_latest(db_path, [_quote("AAPL", 10.5, "mock", data_mode="live")])

    result = validate_live_database(db_path, expected_years=1, report_path=tmp_path / "bad-provider.md")

    assert result.status == "FAIL"
    assert any("mock/demo provider" in item for item in result.critical_failures)


def test_audit_live_fails_with_demo_data(tmp_path: Path) -> None:
    db_path = str(tmp_path / "demo.sqlite")
    initialize_schema(db_path)
    upsert_instruments(db_path, [_instrument("AAPL", "mock", data_mode="demo")])
    upsert_stock_prices_daily(db_path, [_stock_row("AAPL", "mock", data_mode="demo")])
    upsert_market_quotes_latest(db_path, [_quote("AAPL", 10.5, "mock", data_mode="demo")])

    code = run_audit_live(db_path, expected_years=1, report_path=tmp_path / "audit-demo.md")

    assert code == 1


def test_validate_live_fails_on_quote_history_divergence(tmp_path: Path) -> None:
    db_path = str(tmp_path / "divergence.sqlite")
    initialize_schema(db_path)
    upsert_instruments(db_path, [_instrument("AAPL", "twelvedata", data_mode="live")])
    upsert_stock_prices_daily(
        db_path,
        [
            _stock_row("AAPL", "twelvedata", data_mode="live", date="2026-05-18", close=100.0),
            _stock_row("AAPL", "twelvedata", data_mode="live", date="2026-05-19", close=110.0),
        ],
    )
    upsert_market_quotes_latest(db_path, [_quote("AAPL", 1000.0, "twelvedata", data_mode="live", quote_time="2026-05-19")])

    result = validate_live_database(db_path, expected_years=1, report_path=tmp_path / "divergence.md")

    assert result.status == "FAIL"
    assert any("latest quote differs" in item or "Quote/history ratio" in item for item in result.critical_failures)


def test_api_smoke_live_validates_fake_live_backend(tmp_path: Path) -> None:
    db_path, settings = _make_full_fake_live_db(tmp_path)

    result = smoke_live_api(settings, db_path=db_path, port=8061, report_path=tmp_path / "api-smoke.md")

    assert result["status"] == "OK"
    assert not result["failed_endpoints"]


def test_validate_samples_fake_live_passes_when_values_match(tmp_path: Path) -> None:
    db_path, settings = _make_full_fake_live_db(tmp_path)

    result = validate_samples(settings, db_path=db_path, samples_per_symbol=3, report_path=tmp_path / "samples.md")

    assert result.status == "READY"
    assert all(row["status"] == "OK" for row in result.samples)


def test_validate_samples_internal_history_is_chronological(tmp_path: Path) -> None:
    db_path, settings = _make_full_fake_live_db(tmp_path, "ordered-history.sqlite")

    result = validate_samples(settings, db_path=db_path, samples_per_symbol=3, external_test=False, report_path=tmp_path / "samples-ordered.md")

    assert result.status == "READY"
    assert not any(row["reason_code"] == "HISTORY_NOT_ORDERED" for row in result.samples)


def test_validate_samples_fake_live_fails_when_db_value_diverges(tmp_path: Path) -> None:
    db_path, settings = _make_full_fake_live_db(tmp_path)
    with sqlite3.connect(db_path) as conn:
        sample_date = conn.execute("SELECT MIN(date) FROM stock_prices_daily WHERE symbol='AAPL'").fetchone()[0]
        conn.execute("UPDATE stock_prices_daily SET close=close * 10 WHERE symbol='AAPL' AND date=?", (sample_date,))
        conn.commit()

    result = validate_samples(settings, db_path=db_path, samples_per_symbol=3, external_test=True, report_path=tmp_path / "samples-fail.md")

    assert result.status == "NOT_READY"
    assert any(row["symbol"] == "AAPL" and row["status"] == "FAIL" for row in result.samples)


def test_validate_samples_fails_for_empty_live_db(tmp_path: Path) -> None:
    db_path = str(tmp_path / "empty.sqlite")
    settings = _settings(db_path, tmp_path)
    initialize_schema(db_path)

    result = validate_samples(settings, db_path=db_path, samples_per_symbol=3, report_path=tmp_path / "samples-empty.md")

    assert result.status == "NOT_READY"
    assert result.samples[0]["symbol"] == "NO_INSTRUMENTS"


def test_validate_samples_fails_early_when_twelve_key_missing(tmp_path: Path) -> None:
    db_path, _settings_obj = _make_full_fake_live_db(tmp_path, "missing-twelve.sqlite")
    _mark_stock_provider(db_path, "twelvedata")
    settings = _twelvedata_settings(db_path, tmp_path, api_key="")

    result = validate_samples(settings, db_path=db_path, samples_per_symbol=5, external_test=True, report_path=tmp_path / "samples-not-ready.md")
    output = format_sample_validation(result)

    assert result.status == "NOT_READY"
    assert result.samples
    assert not any(row["asset_type"] == "STOCK" and row["endpoint"] == "twelvedata time_series" for row in result.samples)
    assert any(item.get("reason_code") == "PROVIDER_KEY_MISSING" for item in result.provider_checks)
    assert "LIVE SAMPLE VALIDATION STATUS: NOT_READY" in output
    assert "Reason: TWELVE_DATA_API_KEY missing for stock sample validation." in output
    assert "FAIL: STOCK AAPL" not in output


def test_validate_samples_crypto_historical_uses_range_not_simple_price(monkeypatch, tmp_path: Path) -> None:
    db_path, settings = _make_full_fake_live_db(tmp_path, "crypto-range.sqlite")
    recorder = _RecordingCryptoProvider(db_path)
    settings = DEFAULTS.__class__(**{**settings.__dict__, "crypto_provider": "coingecko"})
    monkeypatch.setattr("fx_rates.live_samples.build_crypto_provider", lambda *_args, **_kwargs: recorder)

    result = validate_samples(settings, db_path=db_path, samples_per_symbol=5, external_test=True, report_path=tmp_path / "samples-crypto-range.md")

    crypto_historical = [
        row for row in result.samples
        if row["asset_type"] == "CRYPTO" and row["reason_code"] == "HISTORICAL_SAMPLE_VALIDATED_FROM_RANGE"
    ]
    crypto_latest = [
        row for row in result.samples
        if row["asset_type"] == "CRYPTO" and row["reason_code"] == "CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST"
    ]
    assert crypto_historical
    assert crypto_latest
    assert all(row["endpoint"].endswith("market_chart/range") for row in crypto_historical)
    assert all(row["endpoint"] == "coingecko simple/price" for row in crypto_latest)
    assert len(recorder.quote_calls) == len({row["symbol"] for row in crypto_latest})


def test_validate_samples_latest_quote_can_use_simple_price(monkeypatch, tmp_path: Path) -> None:
    db_path, settings = _make_full_fake_live_db(tmp_path, "crypto-latest.sqlite")
    recorder = _RecordingCryptoProvider(db_path)
    settings = DEFAULTS.__class__(**{**settings.__dict__, "crypto_provider": "coingecko"})
    monkeypatch.setattr("fx_rates.live_samples.build_crypto_provider", lambda *_args, **_kwargs: recorder)

    result = validate_samples(settings, db_path=db_path, samples_per_symbol=2, external_test=True, report_path=tmp_path / "samples-crypto-latest.md")

    assert recorder.quote_calls
    assert any(row["endpoint"] == "coingecko simple/price" and row["reason_code"] == "CURRENT_PRICE_ENDPOINT_USED_ONLY_FOR_LATEST" for row in result.samples)


def test_validate_samples_rate_limit_has_clear_reason_code(monkeypatch, tmp_path: Path) -> None:
    db_path, settings = _make_full_fake_live_db(tmp_path, "crypto-rate-limit.sqlite")
    settings = DEFAULTS.__class__(**{**settings.__dict__, "crypto_provider": "coingecko"})
    monkeypatch.setattr("fx_rates.live_samples.build_crypto_provider", lambda *_args, **_kwargs: _RateLimitedCryptoProvider())

    result = validate_samples(settings, db_path=db_path, samples_per_symbol=2, external_test=True, report_path=tmp_path / "samples-rate-limit.md")

    assert result.status == "READY_WITH_WARNINGS"
    assert result.release_gate == "PASS_WITH_WARNINGS"
    assert result.promotion_allowed is True
    assert any(row["reason_code"] == "EXTERNAL_RATE_LIMIT" for row in result.samples)
    assert "EXTERNAL_RATE_LIMIT" in format_sample_validation(result)


def test_validate_samples_detects_latest_quote_history_divergence(tmp_path: Path) -> None:
    db_path = str(tmp_path / "sample-divergence.sqlite")
    settings = DEFAULTS.__class__(**{**_settings(db_path, tmp_path).__dict__, "live_default_days": 2})
    initialize_schema(db_path)
    upsert_instruments(db_path, [_instrument("AAPL", "fake_live", data_mode="live")])
    upsert_stock_prices_daily(
        db_path,
        [
            _stock_row("AAPL", "fake_live", data_mode="live", date="2026-05-18", close=100.0),
            _stock_row("AAPL", "fake_live", data_mode="live", date="2026-05-19", close=110.0),
        ],
    )
    upsert_market_quotes_latest(db_path, [_quote("AAPL", 1000.0, "fake_live", data_mode="live", quote_time="2026-05-19")])

    result = validate_samples(settings, db_path=db_path, samples_per_symbol=2, external_test=False, report_path=tmp_path / "samples-divergence.md")

    assert result.status == "NOT_READY"
    assert any(row["reason_code"] == "LATEST_QUOTE_DIVERGENCE_FAIL" for row in result.samples)


def test_validate_samples_demo_data_fails_live_validation(tmp_path: Path) -> None:
    db_path = str(tmp_path / "sample-demo.sqlite")
    settings = DEFAULTS.__class__(**{**_settings(db_path, tmp_path).__dict__, "live_default_days": 2})
    initialize_schema(db_path)
    upsert_instruments(db_path, [_instrument("AAPL", "mock", data_mode="demo")])
    upsert_stock_prices_daily(db_path, [_stock_row("AAPL", "mock", data_mode="demo", date="2026-05-19")])
    upsert_market_quotes_latest(db_path, [_quote("AAPL", 10.5, "mock", data_mode="demo", quote_time="2026-05-19")])

    result = validate_samples(settings, db_path=db_path, samples_per_symbol=2, external_test=False, report_path=tmp_path / "samples-demo.md")

    assert result.status == "NOT_READY"
    assert any(row["reason_code"] in {"DEMO_DATA_IN_LIVE_VALIDATION", "MISSING_LIVE_HISTORY"} for row in result.samples)


def test_validate_samples_output_does_not_contain_api_key(tmp_path: Path) -> None:
    secret = "valid-looking-key-12345"
    result = SampleValidationResult(
        status="NOT READY",
        db_path=str(tmp_path / "x.sqlite"),
        generated_at="2026-05-26T00:00:00+00:00",
        requested_period_days=365,
        history_mode="standard",
        advanced_history_available=False,
        samples=[],
        provider_checks=[],
        reason="TWELVE_DATA_API_KEY missing for stock sample validation.",
        action="Run inside run_live_pipeline.ps1 or set TWELVE_DATA_API_KEY in the current PowerShell session.",
    )

    assert secret not in format_sample_validation(result)


def test_refresh_live_fake_updates_only_new_rows(tmp_path: Path) -> None:
    db_path = str(tmp_path / "refresh.sqlite")
    settings = _settings(db_path, tmp_path)
    initialize_schema(db_path)
    old_day = (date.today() - timedelta(days=10)).isoformat()
    upsert_instruments(db_path, [_instrument("AAPL", "fake_live", data_mode="live")])
    upsert_stock_prices_daily(db_path, [_stock_row("AAPL", "fake_live", data_mode="live", date=old_day)])
    upsert_market_quotes_latest(db_path, [_quote("AAPL", 10.5, "fake_live", data_mode="live", quote_time=old_day)])
    before = _stock_count(db_path, "AAPL")

    code = run_refresh_live(settings, asset_type="STOCK", symbols=["AAPL"], dry_run=False)

    assert code == 0
    assert _stock_count(db_path, "AAPL") > before
    with sqlite3.connect(db_path) as conn:
        modes = conn.execute("SELECT DISTINCT data_mode FROM stock_prices_daily WHERE symbol='AAPL'").fetchall()
    assert {row[0] for row in modes} == {"live"}


def test_refresh_live_dry_run_does_not_mutate_db(tmp_path: Path) -> None:
    db_path = str(tmp_path / "refresh-dry.sqlite")
    settings = _settings(db_path, tmp_path)
    initialize_schema(db_path)
    old_day = (date.today() - timedelta(days=10)).isoformat()
    upsert_instruments(db_path, [_instrument("AAPL", "fake_live", data_mode="live")])
    upsert_stock_prices_daily(db_path, [_stock_row("AAPL", "fake_live", data_mode="live", date=old_day)])
    before = _stock_count(db_path, "AAPL")

    code = run_refresh_live(settings, asset_type="STOCK", symbols=["AAPL"], dry_run=True)

    assert code == 0
    assert _stock_count(db_path, "AAPL") == before


def test_promote_live_creates_backup_and_restore_backup(tmp_path: Path) -> None:
    source_db, settings = _make_full_fake_live_db(tmp_path, "source.sqlite")
    target_db = tmp_path / "target.sqlite"
    initialize_schema(str(target_db))

    code = run_promote_live(
        settings,
        from_db=source_db,
        to_db=str(target_db),
        backup=True,
        skip_api_smoke=True,
        expected_years=1,
    )

    assert code == 0
    backups = list((tmp_path / "backups").glob("target-before-live-*.sqlite"))
    assert backups
    assert get_system_status(str(target_db))["data_mode"] == "live"

    restore_code = run_restore_backup(backup=str(backups[0]), to_db=str(target_db))

    assert restore_code == 0
    assert get_system_status(str(target_db))["is_empty"] is True


def test_promote_live_refuses_invalid_source(tmp_path: Path) -> None:
    source_db = tmp_path / "invalid.sqlite"
    target_db = tmp_path / "target.sqlite"
    initialize_schema(str(source_db))
    initialize_schema(str(target_db))
    settings = _settings(str(target_db), tmp_path)

    code = run_promote_live(
        settings,
        from_db=str(source_db),
        to_db=str(target_db),
        backup=True,
        skip_api_smoke=True,
    )

    assert code == 3
    assert get_system_status(str(target_db))["is_empty"] is True


def test_promote_live_dry_run_fails_early_without_twelve_key(tmp_path: Path) -> None:
    source_db, _settings_obj = _make_full_fake_live_db(tmp_path, "source-twelve-missing.sqlite")
    _mark_stock_provider(source_db, "twelvedata")
    target_db = tmp_path / "target.sqlite"
    initialize_schema(str(target_db))
    settings = _twelvedata_settings(str(target_db), tmp_path, api_key="")

    code = run_promote_live(
        settings,
        from_db=source_db,
        to_db=str(target_db),
        dry_run=True,
        skip_api_smoke=True,
        expected_years=1,
    )

    assert code == 5


def test_promote_live_dry_run_blocks_validate_live_warning(monkeypatch, tmp_path: Path) -> None:
    source_db, settings = _make_full_fake_live_db(tmp_path, "source-validate-warn.sqlite")
    target_db = tmp_path / "target.sqlite"
    initialize_schema(str(target_db))

    monkeypatch.setattr(
        "fx_rates.live_promotion.validate_live_database",
        lambda *_args, **_kwargs: LiveValidationResult(
            status="WARN",
            db_path=source_db,
            summary={"data_health": {"status": "OK"}},
            symbols=[],
            critical_failures=[],
            warnings=["Data health is WARN"],
            dashboard_audit={},
            market_audit={},
        ),
    )

    code = run_promote_live(
        settings,
        from_db=source_db,
        to_db=str(target_db),
        dry_run=True,
        skip_samples=True,
        skip_api_smoke=True,
        expected_years=1,
    )

    assert code == 3
    assert get_system_status(str(target_db))["is_empty"] is True


def test_promote_live_dry_run_accepts_allowed_ipca_monthly_warning(monkeypatch, tmp_path: Path, capsys) -> None:
    source_db, settings = _make_full_fake_live_db(tmp_path, "source-ipca-warn.sqlite")
    target_db = tmp_path / "target.sqlite"
    initialize_schema(str(target_db))

    monkeypatch.setattr(
        "fx_rates.live_promotion.validate_live_database",
        lambda *_args, **_kwargs: LiveValidationResult(
            status="WARN",
            db_path=source_db,
            summary={"data_health": {"status": "OK"}},
            symbols=[],
            critical_failures=[],
            warnings=[
                "MACRO IPCA_MONTHLY: monthly macro series validated by monthly point count and stale window; latest history appears stale (90 days old; allowed 75)"
            ],
            dashboard_audit={},
            market_audit={},
        ),
    )

    code = run_promote_live(
        settings,
        from_db=source_db,
        to_db=str(target_db),
        dry_run=True,
        skip_samples=True,
        skip_api_smoke=True,
        expected_years=1,
    )

    output = capsys.readouterr().out
    assert code == 0
    assert "PROMOTE LIVE DRY-RUN STATUS: READY_WITH_WARNINGS" in output
    assert get_system_status(str(target_db))["is_empty"] is True


def test_promote_live_dry_run_blocks_non_ok_data_health(monkeypatch, tmp_path: Path) -> None:
    source_db, settings = _make_full_fake_live_db(tmp_path, "source-health-warn.sqlite")
    target_db = tmp_path / "target.sqlite"
    initialize_schema(str(target_db))

    monkeypatch.setattr(
        "fx_rates.live_promotion.validate_live_database",
        lambda *_args, **_kwargs: LiveValidationResult(
            status="OK",
            db_path=source_db,
            summary={"data_health": {"status": "WARN"}},
            symbols=[],
            critical_failures=[],
            warnings=[],
            dashboard_audit={},
            market_audit={},
        ),
    )

    code = run_promote_live(
        settings,
        from_db=source_db,
        to_db=str(target_db),
        dry_run=True,
        skip_samples=True,
        skip_api_smoke=True,
        expected_years=1,
    )

    assert code == 3
    assert get_system_status(str(target_db))["is_empty"] is True


def test_promote_live_dry_run_blocks_missing_data_health(monkeypatch, tmp_path: Path) -> None:
    source_db, settings = _make_full_fake_live_db(tmp_path, "source-health-missing.sqlite")
    target_db = tmp_path / "target.sqlite"
    initialize_schema(str(target_db))

    monkeypatch.setattr(
        "fx_rates.live_promotion.validate_live_database",
        lambda *_args, **_kwargs: LiveValidationResult(
            status="OK",
            db_path=source_db,
            summary={},
            symbols=[],
            critical_failures=[],
            warnings=[],
            dashboard_audit={},
            market_audit={},
        ),
    )

    code = run_promote_live(
        settings,
        from_db=source_db,
        to_db=str(target_db),
        dry_run=True,
        skip_samples=True,
        skip_api_smoke=True,
        expected_years=1,
    )

    assert code == 3
    assert get_system_status(str(target_db))["is_empty"] is True


def test_promote_live_dry_run_calls_validate_samples_when_twelve_key_present(monkeypatch, tmp_path: Path) -> None:
    source_db, _settings_obj = _make_full_fake_live_db(tmp_path, "source-twelve-present.sqlite")
    _mark_stock_provider(source_db, "twelvedata")
    target_db = tmp_path / "target.sqlite"
    initialize_schema(str(target_db))
    settings = _twelvedata_settings(str(target_db), tmp_path, api_key="valid-looking-key-12345")
    called = {}

    def fake_validate_samples(*_args, **kwargs):
        called["db_path"] = kwargs["db_path"]
        return SampleValidationResult(
            status="READY",
            db_path=kwargs["db_path"],
            generated_at="2026-05-26T00:00:00+00:00",
            requested_period_days=365,
            history_mode="standard",
            advanced_history_available=False,
            samples=[],
            provider_checks=[],
        )

    monkeypatch.setattr("fx_rates.live_promotion.validate_samples", fake_validate_samples)

    code = run_promote_live(
        settings,
        from_db=source_db,
        to_db=str(target_db),
        dry_run=True,
        skip_api_smoke=True,
        expected_years=1,
    )

    assert code == 0
    assert called["db_path"] == str(Path(source_db).resolve())


def test_promote_live_dry_run_blocks_non_transient_ready_warning(monkeypatch, tmp_path: Path) -> None:
    source_db, _settings_obj = _make_full_fake_live_db(tmp_path, "source-warning-blocked.sqlite")
    _mark_stock_provider(source_db, "twelvedata")
    target_db = tmp_path / "target.sqlite"
    initialize_schema(str(target_db))
    settings = _twelvedata_settings(str(target_db), tmp_path, api_key="valid-looking-key-12345")

    def fake_validate_samples(*_args, **kwargs):
        return SampleValidationResult(
            status="READY_WITH_WARNINGS",
            db_path=kwargs["db_path"],
            generated_at="2026-05-26T00:00:00+00:00",
            requested_period_days=365,
            history_mode="standard",
            advanced_history_available=False,
            samples=[],
            provider_checks=[],
            reason_codes=["LATEST_QUOTE_DIVERGENCE_WARN"],
        )

    monkeypatch.setattr("fx_rates.live_promotion.validate_samples", fake_validate_samples)

    code = run_promote_live(
        settings,
        from_db=source_db,
        to_db=str(target_db),
        dry_run=True,
        skip_api_smoke=True,
        expected_years=1,
    )

    assert code == 5
    assert get_system_status(str(target_db))["is_empty"] is True


class _RecordingCryptoProvider:
    name = "coingecko"

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.daily_calls: list[str] = []
        self.quote_calls: list[str] = []

    def fetch_daily(self, asset, start: str, end: str) -> list[CryptoPriceDailyRow]:
        self.daily_calls.append(asset.symbol)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT date, symbol, name, price_usd, market_cap, volume_24h, change_24h, provider, fetched_at, data_mode, source_updated_at
                FROM crypto_prices_daily
                WHERE symbol=? AND date BETWEEN ? AND ?
                ORDER BY date
                """,
                (asset.symbol, start, end),
            ).fetchall()
        return [
            CryptoPriceDailyRow(
                date=row[0],
                symbol=row[1],
                name=row[2],
                price_usd=row[3],
                market_cap=row[4],
                volume_24h=row[5],
                change_24h=row[6],
                provider="coingecko",
                fetched_at=row[8],
                data_mode=row[9],
                source_updated_at=row[10],
            )
            for row in rows
        ]

    def fetch_quote(self, asset) -> MarketQuoteRow:
        self.quote_calls.append(asset.symbol)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT date, price_usd FROM crypto_prices_daily WHERE symbol=? ORDER BY date DESC LIMIT 1",
                (asset.symbol,),
            ).fetchone()
        return MarketQuoteRow(
            symbol=asset.symbol,
            asset_type="CRYPTO",
            exchange="CRYPTO",
            price=float(row[1]),
            bid=None,
            ask=None,
            open=None,
            high=None,
            low=None,
            previous_close=None,
            change=None,
            percent_change=None,
            volume=None,
            quote_time=row[0],
            provider="coingecko",
            fetched_at=f"{row[0]}T00:00:00Z",
            data_mode="live",
            source_updated_at=row[0],
        )

    def status(self):
        return {"name": self.name, "configured": True}


class _RateLimitedCryptoProvider:
    name = "coingecko"

    def fetch_daily(self, *_args, **_kwargs):
        raise CoinGeckoProviderError("coingecko HTTP 429 endpoint=coins/bitcoin/market_chart/range body=rate limit", status_code=429, retryable=True)

    def fetch_quote(self, *_args, **_kwargs):
        raise CoinGeckoProviderError("coingecko HTTP 429 endpoint=simple/price body=rate limit", status_code=429, retryable=True)

    def status(self):
        return {"name": self.name, "configured": True}


def _stock_count(db_path: str, symbol: str) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM stock_prices_daily WHERE symbol=?", (symbol,)).fetchone()
    return int(row[0] or 0)


def _mark_stock_provider(db_path: str, provider: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE instruments SET provider=? WHERE asset_type='STOCK'", (provider,))
        conn.execute("UPDATE stock_prices_daily SET provider=? WHERE symbol IN (SELECT symbol FROM instruments WHERE asset_type='STOCK')", (provider,))
        conn.execute("UPDATE market_quotes_latest SET provider=? WHERE asset_type='STOCK'", (provider,))
        conn.commit()


def _twelvedata_settings(db_path: str, tmp_path: Path, api_key: str):
    return DEFAULTS.__class__(
        **{
            **DEFAULTS.__dict__,
            "db_path": db_path,
            "cache_dir": str(tmp_path / "cache"),
            "log_file": str(tmp_path / "app.log"),
            "timeout_seconds": 1,
            "max_retries": 0,
            "market_data_provider": "twelvedata",
            "market_data_demo_mode": False,
            "stock_provider": "twelvedata",
            "twelve_data_api_key": api_key,
            "fx_provider": "fake_live",
            "crypto_provider": "fake_live",
            "macro_provider": "fake_live",
        }
    )


def _instrument(symbol: str, provider: str, data_mode: str = "live") -> InstrumentRow:
    return InstrumentRow(
        symbol=symbol,
        name=symbol,
        asset_type="STOCK",
        exchange="NASDAQ",
        currency="USD",
        sector="Technology",
        provider=provider,
        provider_symbol=symbol,
        is_active=1,
        priority=1,
        created_at="2026-05-19T00:00:00Z",
        updated_at="2026-05-19T00:00:00Z",
        data_mode=data_mode,
    )


def _stock_row(
    symbol: str,
    provider: str,
    data_mode: str = "live",
    date: str = "2026-05-19",
    close: float = 10.5,
) -> StockPriceDailyRow:
    return StockPriceDailyRow(
        date=date,
        symbol=symbol,
        exchange="NASDAQ",
        open=close,
        high=close + 1.0,
        low=max(0.01, close - 1.0),
        close=close,
        adjusted_close=close,
        volume=100,
        currency="USD",
        provider=provider,
        fetched_at="2026-05-19T00:00:00Z",
        data_mode=data_mode,
        source_updated_at=date,
    )


def _quote(
    symbol: str,
    price: float,
    provider: str,
    data_mode: str = "live",
    quote_time: str = "2026-05-19",
) -> MarketQuoteRow:
    return MarketQuoteRow(
        symbol=symbol,
        asset_type="STOCK",
        exchange="NASDAQ",
        price=price,
        bid=None,
        ask=None,
        open=None,
        high=None,
        low=None,
        previous_close=None,
        change=None,
        percent_change=None,
        volume=None,
        quote_time=quote_time,
        provider=provider,
        fetched_at="2026-05-19T00:00:00Z",
        data_mode=data_mode,
        source_updated_at=quote_time,
    )
