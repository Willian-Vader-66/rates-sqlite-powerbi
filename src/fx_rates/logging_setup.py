from __future__ import annotations

import logging


def setup_logging(log_level: str, log_file: str) -> None:
    _ = (log_level, log_file)
    logging.getLogger().handlers.clear()
