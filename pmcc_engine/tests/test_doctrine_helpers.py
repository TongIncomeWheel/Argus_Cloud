"""Tests for doctrine helpers (array_description, array_guidance, default state seeds)."""
from __future__ import annotations

import unittest

from pmcc_engine import doctrine
from pmcc_engine import state


class ShapeDescriptionTests(unittest.TestCase):
    def test_known_codes(self):
        self.assertIn("Centered", doctrine.shape_description("centered"))
        self.assertIn("ITM-lean", doctrine.shape_description("lean_itm"))
        self.assertIn("OTM-lean", doctrine.shape_description("lean_otm"))
        self.assertIn("All shorts ITM", doctrine.shape_description("all_itm"))
        self.assertIn("All shorts OTM", doctrine.shape_description("all_otm"))

    def test_unknown_code_returns_input(self):
        self.assertEqual(doctrine.shape_description("custom_code"), "custom_code")

    def test_none_returns_dash(self):
        self.assertEqual(doctrine.shape_description(None), "—")


class ClassifyShapeTests(unittest.TestCase):
    def test_centered_at_any_count(self):
        self.assertEqual(doctrine.classify_shape(1, 1), "centered")
        self.assertEqual(doctrine.classify_shape(3, 3), "centered")
        self.assertEqual(doctrine.classify_shape(5, 5), "centered")

    def test_itm_lean(self):
        self.assertEqual(doctrine.classify_shape(4, 2), "lean_itm")
        self.assertEqual(doctrine.classify_shape(3, 1), "lean_itm")

    def test_otm_lean(self):
        self.assertEqual(doctrine.classify_shape(1, 3), "lean_otm")
        self.assertEqual(doctrine.classify_shape(0, 4), "all_otm")  # 0 ITM is the special case

    def test_all_itm_when_no_otm(self):
        self.assertEqual(doctrine.classify_shape(4, 0), "all_itm")

    def test_all_otm_when_no_itm(self):
        self.assertEqual(doctrine.classify_shape(0, 4), "all_otm")

    def test_empty(self):
        self.assertEqual(doctrine.classify_shape(0, 0), "empty")


class ShapeGuidanceTests(unittest.TestCase):
    def test_on_doctrine_centered_3_3(self):
        # User has 3-3 centered, regime calls for centered → match regardless of count
        g = doctrine.shape_guidance(current_itm=3, current_otm=3, target_shape="centered")
        self.assertTrue(g["match"])

    def test_on_doctrine_centered_1_1(self):
        # User has 1-1, regime calls for centered → also a match (count is operator's choice)
        g = doctrine.shape_guidance(current_itm=1, current_otm=1, target_shape="centered")
        self.assertTrue(g["match"])

    def test_on_doctrine_centered_5_5(self):
        g = doctrine.shape_guidance(current_itm=5, current_otm=5, target_shape="centered")
        self.assertTrue(g["match"])

    def test_off_doctrine_otm_lean_when_target_centered(self):
        # User has 2 ITM + 4 OTM (OTM-lean), regime calls for centered → off
        g = doctrine.shape_guidance(current_itm=2, current_otm=4, target_shape="centered")
        self.assertFalse(g["match"])
        # Action should suggest rolling some OTM → ITM to re-balance
        self.assertTrue(any("Roll" in a and ("OTM" in a) for a in g["actions"]))

    def test_off_doctrine_itm_lean_when_target_centered(self):
        # User has 4 ITM + 2 OTM, regime calls for centered → off
        g = doctrine.shape_guidance(current_itm=4, current_otm=2, target_shape="centered")
        self.assertFalse(g["match"])
        self.assertTrue(any("Roll" in a and ("ITM" in a) for a in g["actions"]))

    def test_all_otm_target_with_itm_shorts(self):
        g = doctrine.shape_guidance(current_itm=2, current_otm=2, target_shape="all_otm")
        self.assertFalse(g["match"])
        self.assertTrue(any("ITM" in a for a in g["actions"]))

    def test_all_otm_target_when_already_all_otm(self):
        g = doctrine.shape_guidance(current_itm=0, current_otm=4, target_shape="all_otm")
        self.assertTrue(g["match"])

    def test_stand_down_regime(self):
        g = doctrine.shape_guidance(current_itm=2, current_otm=2, target_shape="stand_down")
        self.assertFalse(g["match"])
        self.assertIn("Stand down", g["headline"]) if "Stand down" in g["headline"] else self.assertIn("stand down", g["headline"].lower())


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


class RegimeChangeDetectionTests(unittest.TestCase):
    def test_no_last_snapshot_returns_no_change(self):
        diff = state.regime_changed_since(None, {"vol_band": "M", "ivr_band": "neutral", "target_shape": "centered"})
        self.assertFalse(diff["changed"])

    def test_same_snapshot_returns_no_change(self):
        snap = {"vol_band": "M", "ivr_band": "neutral", "posture": "base_case", "target_shape": "centered"}
        diff = state.regime_changed_since(snap, dict(snap))
        self.assertFalse(diff["changed"])

    def test_vol_band_shift_detected(self):
        last = {"vol_band": "M", "ivr_band": "neutral", "target_shape": "centered"}
        current = {"vol_band": "L", "ivr_band": "neutral", "target_shape": "centered"}
        diff = state.regime_changed_since(last, current)
        self.assertTrue(diff["changed"])
        self.assertIn("vol_band", diff["fields"])
        self.assertFalse(diff["shape_changed"])

    def test_shape_shift_detected(self):
        last = {"vol_band": "M", "ivr_band": "neutral", "target_shape": "centered"}
        current = {"vol_band": "M", "ivr_band": "rich", "target_shape": "lean_itm"}
        diff = state.regime_changed_since(last, current)
        self.assertTrue(diff["changed"])
        self.assertTrue(diff["shape_changed"])
        self.assertIn("target_shape", diff["fields"])

    def test_save_and_retrieve_snapshot(self):
        settings = {}
        snap = {"timestamp": "2026-05-19T13:00", "vol_band": "M", "ivr_band": "neutral",
                "posture": "base_case", "target_shape": "centered", "current_shape": "centered"}
        state.save_last_review_snapshot(settings, "SPY", snap)
        retrieved = state.get_last_review_snapshot(settings, "SPY")
        self.assertEqual(retrieved["target_shape"], "centered")
        self.assertEqual(retrieved["vol_band"], "M")


if __name__ == "__main__":
    unittest.main()
