"""Tests for posture/array optimization helpers."""
from __future__ import annotations

import unittest

from pmcc_engine import posture


class ArrayLayoutTests(unittest.TestCase):
    def test_3_3_array_detected(self):
        spot = 738.0
        shorts = [
            {"strike": 720}, {"strike": 725}, {"strike": 730},
            {"strike": 745}, {"strike": 750}, {"strike": 755},
        ]
        layout = posture.array_layout(spot, shorts)
        self.assertEqual(layout["itm_count"], 3)
        self.assertEqual(layout["otm_count"], 3)
        self.assertTrue(layout["is_3_3"])

    def test_all_itm_array(self):
        shorts = [{"strike": 700}, {"strike": 720}, {"strike": 730}]
        layout = posture.array_layout(spot=738.0, shorts=shorts)
        self.assertTrue(layout["is_all_itm"])
        self.assertFalse(layout["is_all_otm"])


class CoverageTests(unittest.TestCase):
    def test_chassis_only_excludes_bricks(self):
        longs = [
            {"qty": 1, "delta": 0.80},   # chassis
            {"qty": 1, "delta": 0.85},   # chassis
            {"qty": 1, "delta": 0.97},   # brick
        ]
        shorts = [{"qty": 1}, {"qty": 1}]
        cov = posture.coverage_ratios(longs, shorts)
        self.assertEqual(cov["long_total"], 3)
        self.assertEqual(cov["chassis_qty"], 2)
        self.assertEqual(cov["bricks_qty"], 1)
        self.assertEqual(cov["contract_ratio_long_short"], 1.5)
        self.assertEqual(cov["chassis_ratio_long_short"], 1.0)


class DefensiveFlipTests(unittest.TestCase):
    def test_compliant_when_all_shorts_below_97pct(self):
        spot = 100.0
        shorts = [{"strike": 95}, {"strike": 90}]
        result = posture.defensive_flip_compliance(shorts, spot=spot)
        self.assertTrue(result["compliant"])

    def test_violation_when_any_short_above_threshold(self):
        spot = 100.0
        shorts = [{"strike": 95}, {"strike": 99}]   # 99 > 97
        result = posture.defensive_flip_compliance(shorts, spot=spot)
        self.assertFalse(result["compliant"])
        self.assertEqual(len(result["violators"]), 1)


class ReoptCheckTests(unittest.TestCase):
    def test_suboptimal_theta_per_delta_triggers(self):
        result = posture.reoptimization_check(
            net_theta=10.0, net_delta=100.0,
            shorts=[{"strike": 100, "extrinsic": 5.0}], spot=100.0,
        )
        self.assertTrue(result["reoptimize"])
        self.assertIn("theta/delta", result["reasons"][0])

    def test_dead_weight_flagged(self):
        result = posture.reoptimization_check(
            net_theta=300.0, net_delta=100.0,   # theta/delta = 3.0 = optimal
            shorts=[{"strike": 100, "extrinsic": 1.0}],   # extrinsic < $2 dead weight
            spot=100.0,
        )
        self.assertTrue(result["reoptimize"])
        self.assertEqual(result["dead_weight_count"], 1)

    def test_all_clear(self):
        result = posture.reoptimization_check(
            net_theta=300.0, net_delta=100.0,
            shorts=[{"strike": 100, "extrinsic": 8.0}],
            spot=100.0,
        )
        self.assertFalse(result["reoptimize"])


if __name__ == "__main__":
    unittest.main()
