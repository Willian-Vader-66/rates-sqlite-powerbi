from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FxRateRow:
    date: str
    base: str
    symbol: str
    rate: float
    source: str
    fetched_at: str

    def as_db_params(self) -> dict[str, str | float]:
        return {
            "date": self.date,
            "base": self.base,
            "symbol": self.symbol,
            "rate": self.rate,
            "source": self.source,
            "fetched_at": self.fetched_at,
        }


@dataclass(frozen=True)
class IngestRunRow:
    run_id: int
    started_at: str
    finished_at: str | None
    mode: str
    base: str
    symbols: str
    start: str | None
    end: str | None
    row_count: int
    status: str
    error: str | None
