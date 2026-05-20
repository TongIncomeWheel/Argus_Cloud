"""Tests for pure helpers in pmcc_engine.ui.

ui.py imports streamlit, but the helpers tested here (_effective_tripwires,
_fmt_level, _tripwire_test_description, _short_expiry) are pure functions
with no Streamlit calls — safe to test directly.
"""
from __future__ import annotations

import unittest

from pmcc_engine import ui


class EffectiveTripwiresTests(unittest.TestCase):
    def test_auto_derives_when_no_tripwires(self):
        tw, auto = ui._effective_tripwires(
            {}, spot=737.0, shorts=[{"strike": 710}, {"strike": 755}])
        self.assertTrue(auto)
        self.assertTrue(tw["upper"])
        self.assertTrue(tw["lower"])
        self.assertTrue(tw["vix_shock"])

    def test_uses_configured_when_complete(self):
        cfg = {"tripwires": {"upper": 800, "lower": 700, "vix_shock": 25.0}}
        tw, auto = ui._effective_tripwires(cfg, spot=737.0, shorts=[{"strike": 710}])
        self.assertFalse(auto)
        self.assertEqual(tw["upper"], 800)
        self.assertEqual(tw["lower"], 700)

    def test_partial_config_user_value_wins_gaps_filled(self):
        cfg = {"tripwires": {"upper": 999}}   # only upper set
        tw, auto = ui._effective_tripwires(cfg, spot=737.0, shorts=[{"strike": 710}])
        self.assertTrue(auto)                 # incomplete → still flagged auto
        self.assertEqual(tw["upper"], 999)    # user value preserved
        self.assertTrue(tw["lower"])          # gap filled from derivation

    def test_derived_levels_track_the_array(self):
        # lower = lowest ITM short, upper = highest OTM short - 5
        shorts = [{"strike": 710}, {"strike": 725}, {"strike": 745}, {"strike": 755}]
        tw, _ = ui._effective_tripwires({}, spot=737.0, shorts=shorts)
        self.assertEqual(tw["lower"], 710.0)
        self.assertEqual(tw["upper"], 750.0)   # 755 - 5


class FmtLevelTests(unittest.TestCase):
    def test_formats_number(self):
        self.assertEqual(ui._fmt_level(750), "$750.00")
        self.assertEqual(ui._fmt_level(737.5), "$737.50")

    def test_missing_returns_not_set(self):
        self.assertEqual(ui._fmt_level(None), "not set")
        self.assertEqual(ui._fmt_level("?"), "not set")


class TripwireDescriptionTests(unittest.TestCase):
    def test_no_question_marks_when_levels_present(self):
        state = {"tripwires": {
            "upper": 750.0, "lower": 710.0, "vix_shock": 24.5,
            "disorderly": {"price": 685.0, "vix": 22.0},
        }}
        for name in ("Upper", "Lower", "VIX shock", "Disorderly"):
            desc = ui._tripwire_test_description(name, state, current_vol=18.0)
            self.assertNotIn("?", desc, f"{name} description still has a '?': {desc}")

    def test_missing_level_says_not_set_not_question_mark(self):
        desc = ui._tripwire_test_description("Upper", {"tripwires": {}}, current_vol=18.0)
        self.assertIn("not set", desc)
        self.assertNotIn("?", desc)


class ShortExpiryTests(unittest.TestCase):
    def test_iso_date_compacted(self):
        self.assertEqual(ui._short_expiry("2026-06-19"), "Jun 19")

    def test_empty_returns_empty(self):
        self.assertEqual(ui._short_expiry(None), "")
        self.assertEqual(ui._short_expiry(""), "")


if __name__ == "__main__":
    unittest.main()
