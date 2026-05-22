"""Tests for the filter catalog and apply logic."""
from __future__ import annotations

import unittest
from datetime import date, timedelta

import pandas as pd

from theta_scanner import filters as filt


def _sample_df() -> pd.DataFrame:
    """Two contracts on two underlyings, with a deliberately empty `pe` column."""
    return pd.DataFrame([
        {
            "symbol": "AAA", "strike": 90.0, "dte": 30, "roc": 2.0, "delta": 0.20,
            "iv_pct": 25.0, "stock_iv": 28.0, "underlying_price": 100.0, "ma20": 95.0,
            "sector": "Technology", "div_yield": 0.0, "liquidity_ok": True,
            "is_etf": False, "pe": None,
            "expiration": "2026-06-19",
            "earnings_date": date.today() + timedelta(days=45),
            "ex_div_date": None,
        },
        {
            "symbol": "BBB", "strike": 50.0, "dte": 40, "roc": 5.0, "delta": 0.40,
            "iv_pct": 60.0, "stock_iv": 55.0, "underlying_price": 60.0, "ma20": 65.0,
            "sector": "Energy", "div_yield": 3.0, "liquidity_ok": False,
            "is_etf": True, "pe": None,
            "expiration": "2026-06-19",
            "earnings_date": date.today() + timedelta(days=5),
            "ex_div_date": None,
        },
    ])


class DefaultStateTests(unittest.TestCase):
    def test_default_state_keys(self):
        state = filt.default_filter_state()
        self.assertEqual(state["iv_basis"], "Option IV")
        self.assertIsNone(state["strike_min"])
        self.assertTrue(state["cleanup_illiquid"])  # default-on
        self.assertFalse(state["hide_etfs"])
        self.assertEqual(state["sector"], filt.ANY_SECTOR)
        self.assertEqual(state["ma20"], "Any")

    def test_count_active_zero_by_default(self):
        self.assertEqual(filt.count_active(filt.default_filter_state()), 0)

    def test_count_active_increments(self):
        state = filt.default_filter_state()
        state["strike_min"] = 50.0
        state["hide_etfs"] = True
        self.assertEqual(filt.count_active(state), 2)


class ApplyFilterTests(unittest.TestCase):
    def test_range_filter(self):
        state = filt.default_filter_state()
        state["cleanup_illiquid"] = False  # isolate the range filter
        state["strike_min"] = 80.0
        out = filt.apply_filters(_sample_df(), state)
        self.assertEqual(list(out["symbol"]), ["AAA"])

    def test_cleanup_illiquid_is_on_by_default(self):
        # default state has cleanup_illiquid → only the liquid contract survives
        out = filt.apply_filters(_sample_df(), filt.default_filter_state())
        self.assertEqual(list(out["symbol"]), ["AAA"])

    def test_hide_etfs(self):
        state = filt.default_filter_state()
        state["cleanup_illiquid"] = False
        state["hide_etfs"] = True
        out = filt.apply_filters(_sample_df(), state)
        self.assertEqual(list(out["symbol"]), ["AAA"])

    def test_sector_choice(self):
        state = filt.default_filter_state()
        state["cleanup_illiquid"] = False
        state["sector"] = "Energy"
        out = filt.apply_filters(_sample_df(), state)
        self.assertEqual(list(out["symbol"]), ["BBB"])

    def test_ma_direction_price_above(self):
        state = filt.default_filter_state()
        state["cleanup_illiquid"] = False
        state["ma20"] = "Price Above"
        out = filt.apply_filters(_sample_df(), state)
        self.assertEqual(list(out["symbol"]), ["AAA"])  # 100>95, 60<65

    def test_empty_column_filter_is_skipped(self):
        # `pe` is entirely empty — a P/E filter must not drop every row.
        state = filt.default_filter_state()
        state["cleanup_illiquid"] = False
        state["pe_min"] = 10.0
        out = filt.apply_filters(_sample_df(), state)
        self.assertEqual(len(out), 2)

    def test_iv_basis_switches_target_column(self):
        state = filt.default_filter_state()
        state["cleanup_illiquid"] = False
        state["iv_min"] = 56.0
        state["iv_basis"] = "Option IV"      # iv_pct: 25, 60 → keeps BBB
        self.assertEqual(list(filt.apply_filters(_sample_df(), state)["symbol"]), ["BBB"])
        state["iv_basis"] = "Stock IV"        # stock_iv: 28, 55 → keeps none
        self.assertEqual(len(filt.apply_filters(_sample_df(), state)), 0)

    def test_show_only_upcoming_earnings(self):
        state = filt.default_filter_state()
        state["cleanup_illiquid"] = False
        state["show_only_upcoming_earnings"] = True
        out = filt.apply_filters(_sample_df(), state)
        self.assertEqual(len(out), 2)  # both have future earnings dates

    def test_watchlist_only(self):
        state = filt.default_filter_state()
        state["cleanup_illiquid"] = False
        state["watchlist_only"] = True
        out = filt.apply_filters(_sample_df(), state, watchlist=["BBB"])
        self.assertEqual(list(out["symbol"]), ["BBB"])


if __name__ == "__main__":
    unittest.main()
