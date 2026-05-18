"""Tests for Monte Carlo scorecard.

We seed the RNG for determinism. Stochastic outputs are still tested via
sanity bounds (probabilities ∈ [0, 1], Sharpe finite, etc.) rather than
exact values where the path count would make a closed-form match impractical.
"""
from __future__ import annotations

import unittest

from pmcc_engine import scorecard


class ScorecardTests(unittest.TestCase):
    def test_short_call_sane_bounds(self):
        sc = scorecard.short_call_scorecard(
            spot=738.0, strike=780.0, premium=5.0,
            dte_days=30, hv30=0.17, paths=1000, seed=42,
        )
        self.assertEqual(sc["paths"], 1000)
        self.assertGreaterEqual(sc["p_loss"], 0.0)
        self.assertLessEqual(sc["p_loss"], 1.0)
        self.assertGreaterEqual(sc["p_profit_50"], 0.0)
        self.assertLessEqual(sc["p_profit_50"], 1.0)
        self.assertGreaterEqual(sc["p_assignment"], 0.0)
        self.assertLessEqual(sc["p_assignment"], 1.0)
        # Premium $5 well above BS fair value (~$3.50 at 5.7% OTM, 17% vol) → mean positive
        self.assertGreater(sc["mean_pnl"], 0)

    def test_short_put_sane_bounds(self):
        sc = scorecard.short_put_scorecard(
            spot=738.0, strike=700.0, premium=4.0,
            dte_days=30, hv30=0.17, paths=1000, seed=42,
        )
        self.assertEqual(sc["paths"], 1000)
        self.assertGreaterEqual(sc["p_loss"], 0.0)
        self.assertLessEqual(sc["p_loss"], 1.0)
        # 5.1% OTM, premium $4 above fair (~$2.80) → positive
        self.assertGreater(sc["mean_pnl"], 0)

    def test_below_fair_value_short_call_flagged_negative_pnl(self):
        # Confirms the scorecard correctly identifies a losing trade.
        sc = scorecard.short_call_scorecard(
            spot=738.0, strike=750.0, premium=1.0,
            dte_days=30, hv30=0.17, paths=500, seed=42,
        )
        # $1 premium on a 1.6% OTM call at 17% vol is far below fair → mean P&L < 0
        self.assertLess(sc["mean_pnl"], 0)

    def test_zero_inputs_return_zero_scorecard(self):
        sc = scorecard.short_call_scorecard(
            spot=0.0, strike=750.0, premium=5.0,
            dte_days=30, hv30=0.17, paths=100, seed=42,
        )
        self.assertEqual(sc["paths"], 0)

    def test_verdict_pass(self):
        # Construct a hand-built scorecard that should clearly pass
        sc = {
            "paths": 1000, "mean_pnl": 100.0, "stdev": 50.0,
            "p_profit_50": 0.70, "p_profit_80": 0.40,
            "p_loss": 0.20, "p_assignment": 0.10, "cvar_5": -50.0,
            "sharpe": 1.5, "ann_return": 0.20, "ann_vol": 0.10,
        }
        verdict, reasons = scorecard.verdict(sc)
        self.assertEqual(verdict, "pass")
        self.assertEqual(reasons, [])

    def test_verdict_auto_reject_on_loss_and_negative_mean(self):
        sc = {
            "paths": 1000, "mean_pnl": -50.0, "stdev": 200.0,
            "p_profit_50": 0.10, "p_profit_80": 0.05,
            "p_loss": 0.60, "p_assignment": 0.40, "cvar_5": -500.0,
            "sharpe": -0.2, "ann_return": -0.1, "ann_vol": 0.20,
        }
        verdict, _ = scorecard.verdict(sc)
        self.assertEqual(verdict, "fail")


if __name__ == "__main__":
    unittest.main()
