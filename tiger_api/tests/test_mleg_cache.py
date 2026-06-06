"""Tests for MLEG cache flatten/inflate (pure functions, no I/O).

The gSheet mirror writes the same flat shape as the parquet, so this
round-trip is the wire format on both sides.
"""
from __future__ import annotations

import unittest

from tiger_api.tiger_data import _flatten_mleg_cache, _inflate_mleg_cache


class FlattenInflateRoundTripTests(unittest.TestCase):
    def test_two_orders_round_trip(self):
        cache = {
            "10001": [
                {"Ticker": "SPY", "Strike": 735.0, "Side": "SELL"},
                {"Ticker": "SPY", "Strike": 740.0, "Side": "BUY"},
            ],
            "10002": [
                {"Ticker": "QQQ", "Strike": 500.0, "Side": "SELL"},
            ],
        }
        rows = _flatten_mleg_cache(cache)
        self.assertEqual(len(rows), 3)
        for r in rows:
            self.assertIn("_mleg_order_id", r)

        restored = _inflate_mleg_cache(rows)
        self.assertEqual(restored, cache)

    def test_empty_cache_flattens_to_empty(self):
        self.assertEqual(_flatten_mleg_cache({}), [])

    def test_empty_rows_inflates_to_empty(self):
        self.assertEqual(_inflate_mleg_cache([]), {})
        self.assertEqual(_inflate_mleg_cache(None), {})

    def test_inflate_drops_rows_without_order_id(self):
        rows = [
            {"_mleg_order_id": "1", "Ticker": "SPY"},
            {"Ticker": "QQQ"},                    # no order id → drop
            {"_mleg_order_id": "", "Ticker": "X"}, # empty order id → drop
            {"_mleg_order_id": "1", "Ticker": "VOO"},
        ]
        cache = _inflate_mleg_cache(rows)
        self.assertEqual(set(cache.keys()), {"1"})
        self.assertEqual(len(cache["1"]), 2)

    def test_order_id_coerced_to_string(self):
        # gSheet returns everything as strings — and parquet may store as int.
        # Either way the cache key is the str form.
        cache = {12345: [{"Ticker": "SPY"}]}
        rows = _flatten_mleg_cache(cache)
        self.assertEqual(rows[0]["_mleg_order_id"], "12345")
        restored = _inflate_mleg_cache(rows)
        self.assertIn("12345", restored)

    def test_legs_preserve_field_order_independent(self):
        # Field order in dicts shouldn't matter for round-trip equality.
        cache_a = {"1": [{"a": 1, "b": 2}]}
        cache_b = {"1": [{"b": 2, "a": 1}]}
        self.assertEqual(
            _inflate_mleg_cache(_flatten_mleg_cache(cache_a)),
            _inflate_mleg_cache(_flatten_mleg_cache(cache_b)),
        )


if __name__ == "__main__":
    unittest.main()
