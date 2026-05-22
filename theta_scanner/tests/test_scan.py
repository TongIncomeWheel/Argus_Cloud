"""Tests for scan orchestration helpers (no network)."""
from __future__ import annotations

import unittest
from datetime import date, timedelta

from theta_scanner import scan as scan_mod


class DaysBetweenTests(unittest.TestCase):
    def test_future_date(self):
        self.assertEqual(scan_mod._days_between(date.today() + timedelta(days=10)), 10)

    def test_today(self):
        self.assertEqual(scan_mod._days_between(date.today()), 0)

    def test_none(self):
        self.assertIsNone(scan_mod._days_between(None))

    def test_non_date(self):
        self.assertIsNone(scan_mod._days_between("2026-01-01"))


class EmptyScanTests(unittest.TestCase):
    def test_empty_tickers_returns_empty_result(self):
        result = scan_mod.run_scan([], "Puts", 30, 45)
        self.assertTrue(result.df.empty)
        self.assertEqual(result.n_tickers_ok, 0)

    def test_blank_tickers_filtered_out(self):
        result = scan_mod.run_scan(["", "  ", None], "Puts", 30, 45)
        self.assertTrue(result.df.empty)


if __name__ == "__main__":
    unittest.main()
