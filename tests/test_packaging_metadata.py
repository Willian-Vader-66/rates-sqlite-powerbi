from __future__ import annotations

import tomllib
from pathlib import Path


def test_console_script_entrypoint_is_declared() -> None:
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["project"]["scripts"]["fx-rates"] == "fx_rates.cli:main"
