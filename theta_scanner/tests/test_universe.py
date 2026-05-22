"""Tests for the candidate universe resolver."""
from __future__ import annotations

import unittest

from theta_scanner import universe


class BundledUniverseTests(unittest.TestCase):
    def test_bundled_list_non_empty(self):
        self.assertGreater(len(universe.BUNDLED_UNIVERSE), 50)

    def test_bundled_list_has_no_duplicates(self):
        self.assertEqual(len(universe.BUNDLED_UNIVERSE), len(set(universe.BUNDLED_UNIVERSE)))

    def test_bundled_list_all_uppercase_symbols(self):
        for sym in universe.BUNDLED_UNIVERSE:
            self.assertTrue(sym.isupper(), f"{sym} not upper-case")
            self.assertTrue(1 <= len(sym) <= 5, f"{sym} implausible length")

    def test_includes_core_wheel_names(self):
        for sym in ("SPY", "QQQ", "AAPL", "MSFT", "NVDA"):
            self.assertIn(sym, universe.BUNDLED_UNIVERSE)


class GetUniverseTests(unittest.TestCase):
    def test_falls_back_to_bundled_without_fmp_key(self):
        # In the test environment no FMP_API_KEY is set → bundled list
        uni = universe.get_universe()
        self.assertIn("tickers", uni)
        self.assertIn("source", uni)
        self.assertEqual(uni["count"], len(uni["tickers"]))
        # With no key configured the source must be the bundled list
        if not universe.fmp_configured():
            self.assertEqual(uni["source"], "bundled list")
            self.assertEqual(uni["tickers"], list(universe.BUNDLED_UNIVERSE))

    def test_get_universe_count_matches_tickers(self):
        uni = universe.get_universe()
        self.assertEqual(uni["count"], len(uni["tickers"]))


if __name__ == "__main__":
    unittest.main()
