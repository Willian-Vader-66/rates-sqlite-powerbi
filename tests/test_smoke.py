import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fx_ingest.api import FrankfurterClient, normalize_payload
from fx_ingest.db import init_db, upsert_rates


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class SmokeTest(unittest.TestCase):
    @patch("fx_ingest.api.requests.get")
    def test_normalize_and_upsert(self, mock_get):
        payload = {
            "amount": 1.0,
            "base": "USD",
            "date": "2026-02-10",
            "rates": {"BRL": 5.12, "EUR": 0.93},
        }
        mock_get.return_value = _FakeResponse(payload)

        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "fx.sqlite")
            cache_dir = str(Path(tmp) / "cache")

            init_db(db_path)
            client = FrankfurterClient(cache_dir=cache_dir, use_cache=False)
            raw = client.fetch_latest(base="USD", symbols=["BRL", "EUR"])
            rows = normalize_payload(raw, base="USD", fetched_at="2026-02-10T00:00:00+00:00")

            self.assertEqual(2, len(rows))
            self.assertEqual("BRL", rows[0]["symbol"])

            inserted_first = upsert_rates(db_path, rows)
            self.assertEqual(2, inserted_first)

            rows[0]["rate"] = 5.22
            inserted_second = upsert_rates(db_path, rows)
            self.assertEqual(2, inserted_second)

            with sqlite3.connect(db_path) as conn:
                rate = conn.execute(
                    "select rate from fx_rates where date=? and base=? and symbol=?",
                    ("2026-02-10", "USD", "BRL"),
                ).fetchone()[0]

            self.assertAlmostEqual(5.22, float(rate), places=6)


if __name__ == "__main__":
    unittest.main()
