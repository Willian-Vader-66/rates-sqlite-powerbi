from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_run_live_pipeline_checkonly_does_not_prompt_for_key() -> None:
    text = (ROOT / "run_live_pipeline.ps1").read_text(encoding="utf-8")

    assert text.index("if ($CheckOnly)") < text.index("    Read-TwelveKeyIntoEnvironment")


def test_finance_monitor_checkonly_does_not_open_ui_or_touch_data() -> None:
    text = (ROOT / "run_finance_monitor.ps1").read_text(encoding="utf-8")

    assert "[switch]$CheckOnly" in text
    assert "Read-Host" not in text
    assert text.index("if ($CheckOnly)") < text.index("Opening JavaFX Control Center")
    assert "build-live-db" not in text
    assert "prepare-demo" not in text
    assert "promote-live" not in text


def test_setup_live_env_help_does_not_prompt_for_key() -> None:
    text = (ROOT / "scripts" / "setup_live_env.ps1").read_text(encoding="utf-8")

    assert text.index("if ($Help)") < text.index("Read-Host")


def test_live_powershell_scripts_use_hidden_and_masked_key_input() -> None:
    combined = "\n".join(
        [
            (ROOT / "run_live_pipeline.ps1").read_text(encoding="utf-8"),
            (ROOT / "scripts" / "setup_live_env.ps1").read_text(encoding="utf-8"),
        ]
    )

    assert "-AsSecureString" in combined
    assert "masked_preview" in combined
    assert "Cole SOMENTE" not in combined


def test_current_docs_do_not_reference_legacy_four_year_live_scope() -> None:
    docs = [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md")), *sorted((ROOT / "scripts").glob("*.ps1"))]
    combined = "\n".join(path.read_text(encoding="utf-8", errors="replace") for path in docs)

    assert "--years 4" not in combined
    assert "expected 4-year coverage" not in combined
