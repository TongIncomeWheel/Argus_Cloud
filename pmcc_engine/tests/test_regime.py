"""Tests for regime classifier."""
from __future__ import annotations

import unittest

from pmcc_engine import regime


class VolBandTests(unittest.TestCase):
    def test_low_band(self):
        # VIX=12 vs median 18 → 0.67× → L
        self.assertEqual(regime.vol_band(12.0, 18.0), "L")

    def test_medium_band(self):
        # VIX=22 vs median 18 → 1.22× → M
        self.assertEqual(regime.vol_band(22.0, 18.0), "M")

    def test_high_band(self):
        # VIX=27 vs median 18 → 1.5× → H
        self.assertEqual(regime.vol_band(27.0, 18.0), "H")

    def test_extreme_band(self):
        # VIX=40 vs median 18 → 2.22× → X
        self.assertEqual(regime.vol_band(40.0, 18.0), "X")


class IVRBandTests(unittest.TestCase):
    def test_cheap(self):
        self.assertEqual(regime.ivr_band(15.0), "cheap")

    def test_neutral(self):
        self.assertEqual(regime.ivr_band(40.0), "neutral")

    def test_rich(self):
        self.assertEqual(regime.ivr_band(65.0), "rich")

    def test_extreme(self):
        self.assertEqual(regime.ivr_band(85.0), "extreme")


class RegimeCellTests(unittest.TestCase):
    def test_base_case_resolves_to_centered_shape(self):
        cell = regime.regime_cell(current_vol=22.0, median_vol=18.0, ivr=40.0)
        self.assertEqual(cell["vol_band"], "M")
        self.assertEqual(cell["ivr_band"], "neutral")
        self.assertEqual(cell["posture"], "base_case")
        self.assertEqual(cell["shape"], "centered")
        self.assertEqual(cell["array"], "centered")   # legacy alias also exposes the same
        self.assertTrue(regime.is_base_case(cell))

    def test_extreme_vol_extreme_ivr_calls_for_half_size_otm(self):
        cell = regime.regime_cell(current_vol=40.0, median_vol=18.0, ivr=85.0)
        self.assertEqual(cell["vol_band"], "X")
        self.assertEqual(cell["ivr_band"], "extreme")
        self.assertEqual(cell["posture"], "all_otm_half_gamma")

    def test_extreme_vol_cheap_ivr_stands_down(self):
        cell = regime.regime_cell(current_vol=40.0, median_vol=18.0, ivr=10.0)
        self.assertTrue(regime.is_stand_down(cell))


class IVR52wTests(unittest.TestCase):
    def test_at_high_returns_100(self):
        hist = [float(x) for x in range(10, 50)]   # 40 values, [10..49]
        ivr = regime.compute_ivr_52w(current_iv=49.0, iv_history=hist)
        self.assertAlmostEqual(ivr, 100.0, places=1)

    def test_at_low_returns_zero(self):
        hist = [float(x) for x in range(10, 50)]
        ivr = regime.compute_ivr_52w(current_iv=10.0, iv_history=hist)
        self.assertAlmostEqual(ivr, 0.0, places=1)

    def test_at_midpoint(self):
        hist = [float(x) for x in range(10, 50)]   # min=10, max=49
        ivr = regime.compute_ivr_52w(current_iv=29.5, iv_history=hist)
        self.assertAlmostEqual(ivr, 50.0, places=0)

    def test_short_history_returns_none(self):
        self.assertIsNone(regime.compute_ivr_52w(current_iv=20.0, iv_history=[10.0, 11.0]))


if __name__ == "__main__":
    unittest.main()
