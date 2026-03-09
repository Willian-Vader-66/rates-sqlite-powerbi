from __future__ import annotations

import logging
from contextvars import ContextVar
from pathlib import Path


_RUN_ID: ContextVar[str] = ContextVar("run_id", default="-")


class RunIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = _RUN_ID.get()
        return True


def set_run_id(run_id: int | None) -> None:
    _RUN_ID.set("-" if run_id is None else str(run_id))


def configure_logging(log_file: str, level: str) -> logging.Logger:
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("fx_rates")
    logger.handlers.clear()
    logger.setLevel(level.upper())
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | run_id=%(run_id)s | %(name)s | %(message)s"
    )
    filter_ = RunIdFilter()

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(filter_)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.addFilter(filter_)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger
