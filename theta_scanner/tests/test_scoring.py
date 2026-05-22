"""Tests for CSP scoring math."""
from __future__ import annotations

import unittest

from theta_scanner import scoring


class CoreMathTests(unittest.TestCase):
    def test_collateral(self):
        self.assertEqual(scoring.csp_collateral(100.0), 10_000.0)

    def test_ror(self):
        # $2 premium on a $100 strike → 2% single-cycle RoR
        self.assertAlmostEqual(scoring.csp_ror(2.0, 100.0), 0.02)

    def test_annualized_ror(self):
        # 2% over 30 DTE → 2% × (365/30) ≈ 24.3% annualized
        ann = scoring.csp_annualized_ror(2.0, 100.0, 30)
        self.assertAlmostEqual(ann, 0.02 * (365 / 30), places=4)

    def test_annualized_ror_zero_dte(self):
        self.assertEqual(scoring.csp_annualized_ror(2.0, 100.0, 0), 0.0)

    def test_distance_to_spot(self):
        # strike 90, spot 100 → 10% OTM
        self.assertAlmostEqual(scoring.distance_to_spot_pct(100.0, 90.0), 0.10)

    def test_distance_negative_when_itm(self):
        # strike 105 above spot 100 → -5% (ITM put)
        self.assertAlmostEqual(scoring.distance_to_spot_pct(100.0, 105.0), -0.05)

    def test_pop_from_delta(self):
        self.assertAlmostEqual(scoring.pop_from_delta(0.25), 0.75)
        self.assertAlmostEqual(scoring.pop_from_delta(0.0), 1.0)

    def test_breakeven(self):
        # strike 90, premium 2 → cost basis 88 if assigned
        self.assertEqual(scoring.breakeven(90.0, 2.0), 88.0)


class AxisScoreTests(unittest.TestCase):
    def test_yield_score_clamps_at_full_marks(self):
        self.assertEqual(scoring.yield_score(scoring.ANN_ROR_FULL_MARKS), 100.0)
        self.assertEqual(scoring.yield_score(scoring.ANN_ROR_FULL_MARKS * 2), 100.0)
        self.assertEqual(scoring.yield_score(0.0), 0.0)

    def test_distance_score_clamps(self):
        self.assertEqual(scoring.distance_score(scoring.DISTANCE_FULL_MARKS), 100.0)
        self.assertEqual(scoring.distance_score(0.0), 0.0)

    def test_delta_score_peaks_at_target(self):
        self.assertAlmostEqual(scoring.delta_score(scoring.DELTA_TARGET), 100.0)
        # At the band edges (0.0 and 0.50) the score should hit 0
        self.assertAlmostEqual(scoring.delta_score(0.0), 0.0, places=4)
        self.assertAlmostEqual(scoring.delta_score(0.50), 0.0, places=4)

    def test_delta_score_symmetric(self):
        # equal distance either side of target → equal score
        below = scoring.delta_score(scoring.DELTA_TARGET - 0.10)
        above = scoring.delta_score(scoring.DELTA_TARGET + 0.10)
        self.assertAlmostEqual(below, above, places=4)


class CompositeTests(unittest.TestCase):
    def test_perfect_candidate_scores_high(self):
        # full yield, full distance, perfect delta → 100
        comp = scoring.composite_score(
            annualized_ror=scoring.ANN_ROR_FULL_MARKS,
            distance_pct=scoring.DISTANCE_FULL_MARKS,
            delta_abs=scoring.DELTA_TARGET,
        )
        self.assertAlmostEqual(comp, 100.0, places=2)

    def test_zero_candidate_scores_low(self):
        comp = scoring.composite_score(annualized_ror=0.0, distance_pct=0.0, delta_abs=0.0)
        self.assertAlmostEqual(comp, 0.0, places=2)

    def test_verdict_bands(self):
        self.assertEqual(scoring.verdict(80), "Strong")
        self.assertEqual(scoring.verdict(65), "Good")
        self.assertEqual(scoring.verdict(50), "Marginal")
        self.assertEqual(scoring.verdict(30), "Weak")


class LiquidityTests(unittest.TestCase):
    def test_oi_floor(self):
        self.assertFalse(scoring.liquidity_ok(50, 1.0, 1.05, 1.025))
        self.assertTrue(scoring.liquidity_ok(500, 1.0, 1.05, 1.025))

    def test_wide_spread_rejected(self):
        # 20% spread → reject
        self.assertFalse(scoring.liquidity_ok(500, 1.0, 1.20, 1.10))

    def test_missing_quote_not_rejected(self):
        # No bid/ask → data gap, not a real defect
        self.assertTrue(scoring.liquidity_ok(500, None, None, None))


class ScoreCandidateTests(unittest.TestCase):
    def test_full_candidate_dict(self):
        # spot 100, OTM put strike 92, $2.10 premium, 35 DTE, delta -0.24
        row = {
            "symbol": "XYZ260101P00092000", "strike": 92.0, "mid": 2.10,
            "bid": 2.05, "ask": 2.15, "dte": 35, "delta": -0.24,
            "open_interest": 800, "expiry": "2026-01-01", "iv": 0.30,
        }
        sc = scoring.score_csp_candidate(spot=100.0, row=row)
        self.assertAlmostEqual(sc["distance_pct"], 8.0, places=1)
        self.assertEqual(sc["delta"], 0.24)              # abs value
        self.assertAlmostEqual(sc["ror_pct"], 2.10 / 92.0 * 100, places=2)
        self.assertAlmostEqual(sc["breakeven"], 89.90, places=2)
        self.assertTrue(sc["liquidity_ok"])
        self.assertIn(sc["verdict"], ("Strong", "Good", "Marginal", "Weak"))
        self.assertGreaterEqual(sc["composite"], 0.0)
        self.assertLessEqual(sc["composite"], 100.0)

    def test_mid_derived_from_bid_ask_when_absent(self):
        row = {"strike": 90.0, "bid": 1.0, "ask": 1.40, "dte": 30, "delta": -0.20,
               "open_interest": 500}
        sc = scoring.score_csp_candidate(spot=100.0, row=row)
        self.assertAlmostEqual(sc["premium"], 1.20, places=2)


if __name__ == "__main__":
    unittest.main()
