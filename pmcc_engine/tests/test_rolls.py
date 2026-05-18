"""Tests for roll decomposition + stagger."""
from __future__ import annotations

import unittest

from pmcc_engine import rolls


class RollDecompTests(unittest.TestCase):
    def test_roll_up_and_out_passes(self):
        # Old: SPY 735C @ 8, 14 DTE. New: SPY 740C @ 9, 30 DTE.
        # Strike lift +5, DTE gain +16, theta likely better → pass
        old = {"mark": 8.0, "strike": 735.0, "dte": 14, "delta": 0.55,
               "gamma": 0.02, "vega": 0.10, "theta": -0.12}
        new = {"mark": 9.0, "strike": 740.0, "dte": 30, "delta": 0.50,
               "gamma": 0.015, "vega": 0.12, "theta": -0.10}
        spot = 738.0
        d = rolls.roll_decomposition(old, new, spot=spot, is_call=True)
        self.assertEqual(d.strike_lift, 5.0)
        self.assertEqual(d.dte_gained, 16)
        self.assertEqual(d.net_cash, 100.0)   # +$100 credit
        # Intrinsic uncapped = 5 × 100 = 500
        self.assertEqual(d.intrinsic_uncapped, 500.0)
        # Gamma decreased (0.02 → 0.015) → gamma_change negative → gamma_reduced positive
        self.assertLess(d.gamma_change, 0)
        self.assertEqual(d.verdict, "pass")

    def test_failing_roll_rejected(self):
        # Strike DROPS, DTE unchanged, gamma rises — should fail
        old = {"mark": 4.0, "strike": 740.0, "dte": 30, "delta": 0.50,
               "gamma": 0.015, "vega": 0.12, "theta": -0.10}
        new = {"mark": 5.0, "strike": 735.0, "dte": 30, "delta": 0.55,
               "gamma": 0.020, "vega": 0.12, "theta": -0.08}
        d = rolls.roll_decomposition(old, new, spot=738.0, is_call=True)
        self.assertEqual(d.strike_lift, -5.0)
        self.assertEqual(d.verdict, "fail")
        self.assertIn("intrinsic_uncap", d.rejection_reason)

    def test_stagger_violation_detected(self):
        shorts = [
            {"strike": 735, "expiry": "2026-06-19"},
            {"strike": 740, "expiry": "2026-06-19"},   # same date → violation
            {"strike": 745, "expiry": "2026-06-26"},
        ]
        result = rolls.check_stagger(shorts)
        self.assertFalse(result["ok"])
        self.assertEqual(len(result["violations"]), 1)

    def test_stagger_pass_when_unique(self):
        shorts = [
            {"strike": 735, "expiry": "2026-06-19"},
            {"strike": 740, "expiry": "2026-06-26"},
        ]
        result = rolls.check_stagger(shorts)
        self.assertTrue(result["ok"])

    def test_estimated_rally_cost_non_negative(self):
        # The estimator is clamped to ≥0 per leg. Sanity: returns a finite number.
        shorts = [
            {"strike": 735.0, "extrinsic": 2.0},
            {"strike": 740.0, "extrinsic": 3.0},
        ]
        cost = rolls.estimated_roll_cost_rally(shorts, spot=738.0, rally_amount=5.0)
        self.assertGreaterEqual(cost, 0)
        # And on a big rally with already-rich shorts, cost should still be a real number
        big = rolls.estimated_roll_cost_rally(shorts, spot=738.0, rally_amount=50.0)
        self.assertGreaterEqual(big, 0)


if __name__ == "__main__":
    unittest.main()
