"""Tests for doctrine helpers (array_description, array_guidance, default state seeds)."""
from __future__ import annotations

import unittest

from pmcc_engine import doctrine
from pmcc_engine import state


class ArrayDescriptionTests(unittest.TestCase):
    def test_known_codes(self):
        self.assertEqual(doctrine.array_description("2_2"), "2 ITM + 2 OTM short calls")
        self.assertEqual(doctrine.array_description("3_3"), "3 ITM + 3 OTM short calls")
        self.assertEqual(doctrine.array_description("all_otm"), "All shorts OTM")
        self.assertIn("ITM", doctrine.array_description("all_itm_3pct_below"))

    def test_unknown_code_returns_input(self):
        self.assertEqual(doctrine.array_description("custom_code"), "custom_code")

    def test_none_returns_dash(self):
        self.assertEqual(doctrine.array_description(None), "—")


class ParseArrayCodeTests(unittest.TestCase):
    def test_numeric_codes(self):
        self.assertEqual(doctrine.parse_array_code("2_2"), (2, 2))
        self.assertEqual(doctrine.parse_array_code("3_3"), (3, 3))
        # Suffixed numeric codes (e.g. 2_2_otm_lean) still parse to first two ints
        self.assertEqual(doctrine.parse_array_code("2_2_otm_lean"), (2, 2))

    def test_qualitative_codes(self):
        self.assertEqual(doctrine.parse_array_code("all_otm"), (0, None))
        self.assertEqual(doctrine.parse_array_code("all_itm_3pct_below"), (None, 0))
        self.assertIsNone(doctrine.parse_array_code("itm_lean"))
        self.assertIsNone(doctrine.parse_array_code(""))
        self.assertIsNone(doctrine.parse_array_code(None))


class ArrayGuidanceTests(unittest.TestCase):
    def test_on_doctrine_2_2(self):
        g = doctrine.array_guidance(current_itm=2, current_otm=2, target_code="2_2")
        self.assertTrue(g["match"])
        self.assertIn("matches", g["headline"].lower())

    def test_3_3_when_target_is_2_2(self):
        g = doctrine.array_guidance(current_itm=3, current_otm=3, target_code="2_2")
        self.assertFalse(g["match"])
        self.assertEqual(g["delta_itm"], 1)
        self.assertEqual(g["delta_otm"], 1)
        # Should suggest closing one of each
        self.assertTrue(any("Close 1 ITM" in a for a in g["actions"]))
        self.assertTrue(any("Close 1 OTM" in a for a in g["actions"]))

    def test_2_2_when_target_is_3_3(self):
        g = doctrine.array_guidance(current_itm=2, current_otm=2, target_code="3_3")
        self.assertFalse(g["match"])
        self.assertEqual(g["delta_itm"], -1)
        self.assertEqual(g["delta_otm"], -1)
        self.assertTrue(any("Add 1 ITM" in a for a in g["actions"]))
        self.assertTrue(any("Add 1 OTM" in a for a in g["actions"]))

    def test_all_otm_target_with_itm_shorts(self):
        g = doctrine.array_guidance(current_itm=2, current_otm=2, target_code="all_otm")
        self.assertFalse(g["match"])
        self.assertTrue(any("ITM short" in a for a in g["actions"]))

    def test_all_otm_target_when_clean(self):
        g = doctrine.array_guidance(current_itm=0, current_otm=4, target_code="all_otm")
        self.assertTrue(g["match"])

    def test_all_itm_target_with_otm_shorts(self):
        g = doctrine.array_guidance(current_itm=4, current_otm=1, target_code="all_itm_3pct_below")
        self.assertFalse(g["match"])
        self.assertTrue(any("OTM short" in a for a in g["actions"]))


class DefaultStateSeedTests(unittest.TestCase):
    def test_spy_has_ex_div_calendar(self):
        spy = doctrine.DEFAULT_TICKER_STATE["SPY"]
        self.assertIn("ex_div_calendar", spy)
        cal = spy["ex_div_calendar"]
        self.assertGreater(len(cal), 4)
        # Entries are date+est_dividend dicts
        for entry in cal:
            self.assertIn("date", entry)
            self.assertIn("est_dividend", entry)
            self.assertGreater(float(entry["est_dividend"]), 0)

    def test_get_ticker_state_seeds_ex_div_for_fresh_install(self):
        # Empty settings — should fall back to SPY seed including ex_div_calendar
        ts = state.get_ticker_state({}, "SPY")
        self.assertGreater(len(ts["ex_div_calendar"]), 0)
        self.assertEqual(ts["vol_axis"], "VIX")
        self.assertAlmostEqual(ts["vol_median_5yr"], 18.0)

    def test_user_calendar_overrides_seed(self):
        settings = {
            state.STATE_KEY: {
                "SPY": {
                    "ex_div_calendar": [{"date": "2099-01-01", "est_dividend": 99.0}],
                }
            }
        }
        ts = state.get_ticker_state(settings, "SPY")
        self.assertEqual(len(ts["ex_div_calendar"]), 1)
        self.assertEqual(ts["ex_div_calendar"][0]["date"], "2099-01-01")


if __name__ == "__main__":
    unittest.main()
