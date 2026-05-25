"""Tests for the pure parsers in fundamentals.py.

The HTTP layer (FMP + yfinance) is integration-tested in the live app;
here we cover the field-name-defensive parsing and the rating normalization
that drives consistent 1-5-where-5-is-best across both data sources.
"""
from __future__ import annotations

import unittest
from datetime import date

from theta_scanner import fundamentals as fund


class ForwardEpsTests(unittest.TestCase):
    TODAY = date(2026, 5, 25)

    def test_picks_nearest_future_year_v3_field(self):
        # v3 endpoint uses `estimatedEpsAvg`
        rows = [
            {"date": "2030-09-27", "estimatedEpsAvg": 12.6},
            {"date": "2028-09-27", "estimatedEpsAvg": 10.5},
            {"date": "2029-09-27", "estimatedEpsAvg": 11.6},
        ]
        self.assertEqual(fund._pick_forward_eps(rows, self.TODAY), 10.5)

    def test_picks_nearest_future_year_stable_field(self):
        # stable endpoint uses `epsAvg`
        rows = [
            {"date": "2027-12-31", "epsAvg": 5.0},
            {"date": "2028-12-31", "epsAvg": 5.5},
        ]
        self.assertEqual(fund._pick_forward_eps(rows, self.TODAY), 5.0)

    def test_past_dates_ignored(self):
        rows = [
            {"date": "2024-12-31", "epsAvg": 99.0},
            {"date": "2027-12-31", "epsAvg": 3.0},
        ]
        self.assertEqual(fund._pick_forward_eps(rows, self.TODAY), 3.0)

    def test_non_positive_eps_skipped(self):
        rows = [
            {"date": "2027-12-31", "epsAvg": 0.0},
            {"date": "2028-12-31", "epsAvg": -1.0},
            {"date": "2029-12-31", "epsAvg": 2.0},
        ]
        self.assertEqual(fund._pick_forward_eps(rows, self.TODAY), 2.0)

    def test_empty_returns_none(self):
        self.assertIsNone(fund._pick_forward_eps([], self.TODAY))
        self.assertIsNone(fund._pick_forward_eps(None, self.TODAY))

    def test_malformed_date_skipped(self):
        rows = [
            {"date": "garbage", "epsAvg": 5.0},
            {"date": "2027-12-31", "epsAvg": 3.0},
        ]
        self.assertEqual(fund._pick_forward_eps(rows, self.TODAY), 3.0)


class RatingNormalizationTests(unittest.TestCase):
    def test_strong_buy_becomes_top_score(self):
        # yfinance 1 = Strong Buy → normalized 5 (best)
        self.assertEqual(fund._normalize_yf_rating(1.0), 5.0)

    def test_sell_becomes_bottom_score(self):
        # yfinance 5 = Sell → normalized 1 (worst)
        self.assertEqual(fund._normalize_yf_rating(5.0), 1.0)

    def test_hold_is_neutral(self):
        self.assertEqual(fund._normalize_yf_rating(3.0), 3.0)

    def test_invalid_range_returns_none(self):
        self.assertIsNone(fund._normalize_yf_rating(0.5))
        self.assertIsNone(fund._normalize_yf_rating(6.0))

    def test_none_returns_none(self):
        self.assertIsNone(fund._normalize_yf_rating(None))

    def test_garbage_returns_none(self):
        self.assertIsNone(fund._normalize_yf_rating("buy"))


class BlankRecordTests(unittest.TestCase):
    def test_blank_has_all_required_keys(self):
        blank = fund._blank()
        for key in ("market_cap", "sector", "is_etf", "pe", "fwd_pe",
                    "eps_ttm", "beta", "short_float", "analyst_rating",
                    "dividend", "div_yield", "ex_div_date", "earnings_date"):
            self.assertIn(key, blank)
            self.assertIsNone(blank[key])


if __name__ == "__main__":
    unittest.main()
