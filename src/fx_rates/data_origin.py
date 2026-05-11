from __future__ import annotations

DEMO_DATA_MODE = "demo"
LIVE_DATA_MODE = "live"
MIXED_DATA_MODE = "mixed"
UNKNOWN_DATA_MODE = "unknown"
VALID_DATA_MODES = {DEMO_DATA_MODE, LIVE_DATA_MODE, MIXED_DATA_MODE, UNKNOWN_DATA_MODE}


def is_demo_marker(value: object | None) -> bool:
    if value is None:
        return False
    normalized = str(value).strip().lower()
    return "demo" in normalized or "mock" in normalized or "synthetic" in normalized


def canonical_record_mode(data_mode: object | None = None, *markers: object | None) -> str:
    raw = str(data_mode or "").strip().lower()
    if raw in {DEMO_DATA_MODE, LIVE_DATA_MODE, MIXED_DATA_MODE}:
        return raw
    present = [marker for marker in markers if marker not in (None, "")]
    if not present:
        return UNKNOWN_DATA_MODE
    if any(is_demo_marker(marker) for marker in present):
        return DEMO_DATA_MODE
    return LIVE_DATA_MODE


def mode_booleans(data_mode: object | None) -> dict[str, bool]:
    mode = canonical_record_mode(data_mode)
    return {
        "is_demo": mode == DEMO_DATA_MODE,
        "is_live": mode == LIVE_DATA_MODE,
    }


def warning_for_mode(data_mode: object | None) -> str | None:
    mode = canonical_record_mode(data_mode)
    if mode == DEMO_DATA_MODE:
        return "Demo data. Do not present as live market data."
    if mode == MIXED_DATA_MODE:
        return "Dataset mixes demo and live records. Validate source before analysis."
    if mode == UNKNOWN_DATA_MODE:
        return "Data origin is unknown. Validate provider/source before analysis."
    return None
