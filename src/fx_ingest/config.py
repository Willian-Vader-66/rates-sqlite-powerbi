from __future__ import annotations

from fx_rates.config import Settings, ensure_runtime_dirs, load_settings


def ensure_runtime_paths(settings: Settings) -> None:
    ensure_runtime_dirs(settings)


__all__ = ["Settings", "load_settings", "ensure_runtime_paths"]
