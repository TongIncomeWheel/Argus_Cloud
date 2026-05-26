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


class WaitVsRollTests(unittest.TestCase):
    """Doctrine §7: wait-vs-roll-now decision."""

    def test_dead_extrinsic_says_roll(self):
        # Spot 750, strike 735C, mark 15.05 → ext = 0.05/share = $5/contract.
        # Old leg also barely earns; new fresh leg throws meaningful theta.
        old = {"mark": 15.05, "strike": 735.0, "theta": -0.005}   # 0.5¢/share
        new = {"mark": 5.00, "strike": 755.0, "theta": -0.10}     # 10¢/share
        d = rolls.wait_vs_roll(old, new, spot=750.0, wait_days=3, is_call=True)
        self.assertEqual(d.verdict, "roll")
        self.assertLess(d.current_extrinsic, 20.0)
        self.assertIn("dead", d.reason.lower())

    def test_fat_extrinsic_with_fading_theta_says_wait(self):
        # Old short: $5/share extrinsic, $0.40/day theta — still earning fat
        # New leg: only $0.20/day. Holding 3 TD captures $1.20 of old theta vs
        # $0.60 of fresh foregone → wait wins by $0.60/share = $60/contract.
        old = {"mark": 5.00, "strike": 760.0, "theta": -0.40}   # OTM call, all extrinsic
        new = {"mark": 3.00, "strike": 765.0, "theta": -0.20}
        d = rolls.wait_vs_roll(old, new, spot=750.0, wait_days=3, is_call=True)
        self.assertEqual(d.verdict, "wait")
        self.assertGreater(d.net_advantage, 0)
        self.assertAlmostEqual(d.extrinsic_savings, 120.0, places=1)
        self.assertAlmostEqual(d.opportunity_cost, 60.0, places=1)

    def test_fresh_leg_wins_says_roll(self):
        # Old: thin extrinsic + low theta; new: rich theta → roll now.
        old = {"mark": 2.00, "strike": 760.0, "theta": -0.05}   # earns $5/day
        new = {"mark": 4.00, "strike": 770.0, "theta": -0.25}   # earns $25/day
        d = rolls.wait_vs_roll(old, new, spot=750.0, wait_days=3, is_call=True)
        self.assertEqual(d.verdict, "roll")
        self.assertLess(d.net_advantage, 0)

    def test_savings_capped_at_current_extrinsic(self):
        # Old leg has $1/share ext but theta would imply $5/share decay in 5d —
        # savings must cap at the actual extrinsic available.
        old = {"mark": 1.00, "strike": 760.0, "theta": -1.00}
        new = {"mark": 4.00, "strike": 770.0, "theta": -0.05}
        d = rolls.wait_vs_roll(old, new, spot=750.0, wait_days=5, is_call=True)
        self.assertAlmostEqual(d.extrinsic_savings, d.current_extrinsic, places=2)
        self.assertEqual(d.current_extrinsic, 100.0)  # $1.00/share × 100

    def test_verdict_text_carries_numbers(self):
        old = {"mark": 5.00, "strike": 760.0, "theta": -0.40}
        new = {"mark": 3.00, "strike": 765.0, "theta": -0.20}
        d = rolls.wait_vs_roll(old, new, spot=750.0, wait_days=3, is_call=True)
        # The reason text should surface the per-day theta numbers so the user
        # can sanity-check the recommendation at a glance.
        self.assertIn("40.00", d.reason)
        self.assertIn("20.00", d.reason)


class ExtrinsicForecastTests(unittest.TestCase):
    def test_decay_projects_into_future(self):
        # $3/share extrinsic, $0.20/day theta, 3 TD → projected $2.40/share
        leg = {"mark": 3.00, "strike": 760.0, "theta": -0.20}
        f = rolls.extrinsic_forecast(leg, spot=750.0, days=3, is_call=True)
        self.assertAlmostEqual(f.current_extrinsic, 300.0, places=2)
        self.assertAlmostEqual(f.projected_extrinsic, 240.0, places=2)
        self.assertAlmostEqual(f.decay_pct_of_current, 0.20, places=3)

    def test_projected_floors_at_zero(self):
        # Theta would over-shoot — projection clamps to 0, not negative.
        leg = {"mark": 1.00, "strike": 760.0, "theta": -1.00}
        f = rolls.extrinsic_forecast(leg, spot=750.0, days=5, is_call=True)
        self.assertEqual(f.projected_extrinsic, 0.0)
        self.assertEqual(f.decay_pct_of_current, 1.0)

    def test_zero_extrinsic_handled(self):
        # Pure intrinsic (deep ITM) → ext 0, decay pct 0.
        leg = {"mark": 15.0, "strike": 735.0, "theta": 0.0}
        f = rolls.extrinsic_forecast(leg, spot=750.0, days=3, is_call=True)
        self.assertEqual(f.current_extrinsic, 0.0)
        self.assertEqual(f.decay_pct_of_current, 0.0)


if __name__ == "__main__":
    unittest.main()
