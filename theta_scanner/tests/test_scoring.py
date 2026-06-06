"""Tests for Scanner scoring math."""
from __future__ import annotations

import unittest

from theta_scanner import scoring


class PutEconomicsTests(unittest.TestCase):
    """Cash-secured put: collateral basis is the strike."""

    def setUp(self):
        # spot 100, OTM put strike 92, $2.10 premium, 35 DTE, delta -0.24
        self.e = scoring.option_economics("Put", 100.0, 92.0, 2.10, 35, -0.24)

    def test_roc_on_strike(self):
        self.assertAlmostEqual(self.e["roc"], 2.10 / 92.0 * 100.0, places=4)

    def test_annual_yield(self):
        self.assertAlmostEqual(
            self.e["annual_yield"], (2.10 / 92.0 * 100.0) * (365.0 / 35.0), places=3)

    def test_pct_otm_positive_below_spot(self):
        self.assertAlmostEqual(self.e["pct_otm"], 8.0, places=4)

    def test_breakeven(self):
        self.assertAlmostEqual(self.e["breakeven"], 89.90, places=2)

    def test_pop_from_abs_delta(self):
        self.assertAlmostEqual(self.e["pop"], 76.0, places=4)


class CallEconomicsTests(unittest.TestCase):
    """Covered call: collateral basis is the held shares (≈ spot)."""

    def setUp(self):
        # spot 100, OTM call strike 108, $1.80 premium, 30 DTE, delta 0.22
        self.e = scoring.option_economics("Call", 100.0, 108.0, 1.80, 30, 0.22)

    def test_roc_on_spot(self):
        self.assertAlmostEqual(self.e["roc"], 1.80 / 100.0 * 100.0, places=4)

    def test_pct_otm_positive_above_spot(self):
        self.assertAlmostEqual(self.e["pct_otm"], 8.0, places=4)

    def test_breakeven_below_spot(self):
        self.assertAlmostEqual(self.e["breakeven"], 98.20, places=2)


class EconomicsGuardTests(unittest.TestCase):
    def test_zero_dte_no_annualization(self):
        e = scoring.option_economics("Put", 100.0, 95.0, 1.0, 0, -0.2)
        self.assertEqual(e["annual_yield"], 0.0)

    def test_none_delta_yields_none_pop(self):
        e = scoring.option_economics("Put", 100.0, 95.0, 1.0, 30, None)
        self.assertIsNone(e["pop"])


class AxisScoreTests(unittest.TestCase):
    def test_yield_score_clamps(self):
        self.assertEqual(scoring.yield_score(scoring.ANN_YIELD_FULL_MARKS), 100.0)
        self.assertEqual(scoring.yield_score(scoring.ANN_YIELD_FULL_MARKS * 3), 100.0)
        self.assertEqual(scoring.yield_score(0.0), 0.0)

    def test_distance_score_clamps(self):
        self.assertEqual(scoring.distance_score(scoring.DISTANCE_FULL_MARKS), 100.0)
        self.assertEqual(scoring.distance_score(0.0), 0.0)

    def test_delta_score_tent(self):
        self.assertAlmostEqual(scoring.delta_score(scoring.DELTA_TARGET), 100.0)
        self.assertAlmostEqual(scoring.delta_score(0.0), 0.0, places=4)
        self.assertAlmostEqual(scoring.delta_score(0.50), 0.0, places=4)
        self.assertEqual(scoring.delta_score(None), 0.0)

    def test_delta_score_symmetric(self):
        self.assertAlmostEqual(
            scoring.delta_score(scoring.DELTA_TARGET - 0.1),
            scoring.delta_score(scoring.DELTA_TARGET + 0.1), places=4)


class CompositeTests(unittest.TestCase):
    def test_perfect_contract(self):
        comp = scoring.option_score(
            scoring.ANN_YIELD_FULL_MARKS, scoring.DISTANCE_FULL_MARKS,
            scoring.DELTA_TARGET)
        self.assertAlmostEqual(comp, 100.0, places=2)

    def test_zero_contract(self):
        self.assertAlmostEqual(scoring.option_score(0.0, 0.0, 0.0), 0.0, places=2)

    def test_verdict_bands(self):
        self.assertEqual(scoring.verdict(80), "Strong")
        self.assertEqual(scoring.verdict(65), "Good")
        self.assertEqual(scoring.verdict(50), "Marginal")
        self.assertEqual(scoring.verdict(20), "Weak")


class StockScoreTests(unittest.TestCase):
    def test_stock_rating_strong_uptrend(self):
        # price above all three MAs, healthy RSI, positive quarter
        r = scoring.stock_rating(price=110, ma20=105, ma50=100, ma200=95,
                                 rsi=55, perf_quarter=10)
        self.assertGreater(r, 80.0)

    def test_stock_rating_none_when_no_data(self):
        self.assertIsNone(scoring.stock_rating(None, None, None, None, None, None))

    def test_stock_rating_partial_data(self):
        # only RSI known — still produces a number
        r = scoring.stock_rating(None, None, None, None, rsi=55, perf_quarter=None)
        self.assertAlmostEqual(r, 100.0, places=2)

    def test_rel_strength_outperformer(self):
        self.assertAlmostEqual(scoring.rel_strength(20.0, 10.0), 60.0, places=2)

    def test_rel_strength_matches_benchmark(self):
        self.assertAlmostEqual(scoring.rel_strength(8.0, 8.0), 50.0, places=2)

    def test_rel_strength_none_without_benchmark(self):
        self.assertIsNone(scoring.rel_strength(10.0, None))


class LiquidityTests(unittest.TestCase):
    def test_oi_floor(self):
        self.assertFalse(scoring.liquidity_ok(50, 1.0, 1.05, 1.025))
        self.assertTrue(scoring.liquidity_ok(500, 1.0, 1.05, 1.025))

    def test_wide_spread_rejected(self):
        self.assertFalse(scoring.liquidity_ok(500, 1.0, 1.20, 1.10))

    def test_missing_quote_not_rejected(self):
        self.assertTrue(scoring.liquidity_ok(500, None, None, None))

    def test_spread_pct(self):
        self.assertAlmostEqual(scoring.spread_pct(1.0, 1.05, 1.025),
                               0.05 / 1.025 * 100.0, places=4)
        self.assertIsNone(scoring.spread_pct(None, 1.0, 1.0))


if __name__ == "__main__":
    unittest.main()
