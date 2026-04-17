from __future__ import annotations

from pathlib import Path


_ROOT = Path(__file__).resolve().parent.parent
_SRC_PACKAGE = _ROOT / "src" / "fx_rates"

if _SRC_PACKAGE.is_dir():
    __path__.append(str(_SRC_PACKAGE))

from .cli import main

__all__ = ["main"]
