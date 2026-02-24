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

    def as_db_dict(self) -> dict[str, str | float]:
        return {
            "date": self.date,
            "base": self.base,
            "symbol": self.symbol,
            "rate": self.rate,
            "source": self.source,
            "fetched_at": self.fetched_at,
        }
