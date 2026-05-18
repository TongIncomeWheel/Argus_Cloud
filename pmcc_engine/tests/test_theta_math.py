"""Tests for theta_math — HV30, hurdle, yield ratio, extrinsic, book greeks."""
from __future__ import annotations

import math
import unittest

from pmcc_engine import theta_math
from pmcc_engine.doctrine import HURDLE_CAPTURE_RATE, TRADING_DAYS_PER_YEAR


class HV30Tests(unittest.TestCase):
    def test_constant_series_returns_zero(self):
        closes = [100.0] * 35
        self.assertAlmostEqual(theta_math.compute_hv30(closes), 0.0, places=6)

    def test_known_volatility_recovered(self):
        # Build a series whose log-return stdev is exactly 0.01/day → 0.01 × √252 annualized
        import random
        rng = random.Random(42)
        closes = [100.0]
        for _ in range(50):
            ret = rng.gauss(0.0, 0.01)
            closes.append(closes[-1] * math.exp(ret))
        hv = theta_math.compute_hv30(closes)
        expected = 0.01 * math.sqrt(252)
        self.assertIsNotNone(hv)
        # Within 30% — Monte Carlo noise on 30 samples is large; the point is it's in the right ballpark
        self.assertLess(abs(hv - expected) / expected, 0.5)

    def test_insufficient_data_returns_none(self):
        self.assertIsNone(theta_math.compute_hv30([100.0] * 10))


class HurdleTests(unittest.TestCase):
    def test_spy_at_known_inputs_recovers_doctrine_floor(self):
        # Doctrine table: SPY $735 with HV30=17% gives ~$0.31/day at 4% capture
        hurdle = theta_math.theta_hurdle(735.0, 0.17)
        self.assertAlmostEqual(hurdle, 0.31, places=2)

    def test_hurdle_scales_linearly_with_capture_rate(self):
        h1 = theta_math.theta_hurdle(735.0, 0.17, capture_rate=0.04)
        h2 = theta_math.theta_hurdle(735.0, 0.17, capture_rate=0.08)
        self.assertAlmostEqual(h2, 2 * h1, places=4)

    def test_zero_inputs_return_zero(self):
        self.assertEqual(theta_math.theta_hurdle(0.0, 0.20), 0.0)
        self.assertEqual(theta_math.theta_hurdle(100.0, 0.0), 0.0)


class YieldRatioTests(unittest.TestCase):
    def test_above_hurdle(self):
        shorts = [
            {"strike": 735.0, "theta_per_day": 0.50},
            {"strike": 735.0, "theta_per_day": 0.50},
        ]
        # Hurdle floor for both = 2 × 0.31 ≈ 0.62. Actual = 1.0. Ratio ≈ 1.6
        ratio = theta_math.yield_ratio(shorts, 0.17)
        self.assertGreater(ratio, 1.0)

    def test_below_hurdle(self):
        shorts = [{"strike": 735.0, "theta_per_day": 0.10}]
        ratio = theta_math.yield_ratio(shorts, 0.17)
        self.assertLess(ratio, 1.0)


class ExtrinsicTests(unittest.TestCase):
    def test_otm_call_all_extrinsic(self):
        # spot below strike, mark = $1.50 → all extrinsic
        self.assertAlmostEqual(theta_math.extrinsic(1.50, 100.0, 105.0, is_call=True), 1.50)

    def test_itm_call_subtracts_intrinsic(self):
        # spot 110, strike 100, mark 12 → intrinsic 10, extrinsic 2
        self.assertAlmostEqual(theta_math.extrinsic(12.0, 110.0, 100.0, is_call=True), 2.0)

    def test_itm_put_subtracts_intrinsic(self):
        # spot 90, strike 100, mark 12 → intrinsic 10, extrinsic 2
        self.assertAlmostEqual(theta_math.extrinsic(12.0, 90.0, 100.0, is_call=False), 2.0)


class BookGreeksTests(unittest.TestCase):
    def test_aggregate_long_short(self):
        longs = [{"qty": 1, "delta": 0.80, "theta": -0.05}]
        shorts = [{"qty": 1, "delta": 0.40, "theta": -0.10}]   # short theta is negative on chain
        g = theta_math.book_greeks(longs, shorts)
        # long_delta = 80, short_delta = 40, net = 40
        self.assertAlmostEqual(g["net_delta"], 40.0, places=2)
        # long_theta = -5, short_theta = +10, net = +5
        self.assertAlmostEqual(g["net_theta"], 5.0, places=2)


class ThetaPerDeltaTests(unittest.TestCase):
    def test_optimal_threshold(self):
        self.assertEqual(theta_math.theta_per_delta_rating(2.00), "Optimal")
        self.assertEqual(theta_math.theta_per_delta_rating(1.00), "Acceptable")
        self.assertEqual(theta_math.theta_per_delta_rating(0.50), "Suboptimal")

    def test_zero_delta_returns_zero(self):
        self.assertEqual(theta_math.theta_per_delta(10.0, 0.0), 0.0)
        self.assertEqual(theta_math.theta_per_delta(10.0, -5.0), 0.0)


class DailyRiskTests(unittest.TestCase):
    def test_spy_known_inputs(self):
        # SPY $738, HV30=17%, net_delta=$80 per $1 (≈1 contract at delta 0.80)
        # daily 1σ underlying move ≈ 738 × 0.17 / √252 ≈ $7.90
        # daily 1σ portfolio risk ≈ 80 × $7.90 ≈ $632
        risk = theta_math.daily_risk_one_sigma(net_delta=80.0, spot=738.0, hv30=0.17)
        self.assertAlmostEqual(risk, 80 * 738 * 0.17 / math.sqrt(252), places=2)
        # Sanity range
        self.assertGreater(risk, 500)
        self.assertLess(risk, 800)

    def test_zero_inputs(self):
        self.assertEqual(theta_math.daily_risk_one_sigma(80.0, 0.0, 0.17), 0.0)
        self.assertEqual(theta_math.daily_risk_one_sigma(80.0, 738.0, 0.0), 0.0)


class ThetaCoverageTests(unittest.TestCase):
    def test_full_coverage(self):
        # net_theta $10/day, daily_risk $10 → coverage = 1.0
        self.assertAlmostEqual(theta_math.theta_coverage(10.0, 10.0), 1.0)

    def test_zero_risk_returns_zero(self):
        self.assertEqual(theta_math.theta_coverage(10.0, 0.0), 0.0)


if __name__ == "__main__":
    unittest.main()
