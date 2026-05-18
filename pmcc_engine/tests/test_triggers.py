"""Tests for tripwires + roll/refresh triggers."""
from __future__ import annotations

import unittest
from datetime import date, timedelta

from pmcc_engine import triggers
from pmcc_engine import doctrine


class TripwireTests(unittest.TestCase):
    def test_upper_breach_fires_at_or_above_level(self):
        r = triggers.check_upper_breach(spot=750.0, upper=725.0)
        self.assertTrue(r.triggered)
        r2 = triggers.check_upper_breach(spot=720.0, upper=725.0)
        self.assertFalse(r2.triggered)

    def test_lower_breach(self):
        r = triggers.check_lower_breach(spot=685.0, lower=690.0)
        self.assertTrue(r.triggered)

    def test_vix_shock(self):
        r = triggers.check_vix_shock(vix=25.0, vix_shock_level=24.5)
        self.assertTrue(r.triggered)

    def test_disorderly_requires_both(self):
        r = triggers.check_disorderly(spot=685.0, vix=23.0, spot_floor=690.0, vix_floor=22.0)
        self.assertTrue(r.triggered)
        # spot good, vix bad → no trip
        r2 = triggers.check_disorderly(spot=700.0, vix=23.0, spot_floor=690.0, vix_floor=22.0)
        self.assertFalse(r2.triggered)

    def test_dte_profit_breach(self):
        shorts = [
            {"dte": 5, "premium_received": 10.0, "mark": 6.0},   # 40% profit → BREACH
            {"dte": 5, "premium_received": 10.0, "mark": 4.0},   # 60% profit → ok
        ]
        r = triggers.check_dte_profit(shorts)
        self.assertTrue(r.triggered)

    def test_ex_div_breach(self):
        today = date(2026, 6, 17)
        ex_div = date(2026, 6, 19)   # 2 trading days out
        shorts = [{
            "spot": 738.0, "strike": 735.0,    # ITM
            "mark": 4.0,                        # intrinsic 3, extrinsic 1
        }]
        r = triggers.check_ex_div_window(
            shorts=shorts, ex_div_date=ex_div, today=today, projected_dividend=1.85
        )
        # 1.25 × 1.85 = 2.31. Extrinsic 1.0 < 2.31 → trigger
        self.assertTrue(r.triggered)

    def test_ex_div_pass_when_otm(self):
        today = date(2026, 6, 17)
        ex_div = date(2026, 6, 19)
        shorts = [{
            "spot": 720.0, "strike": 735.0,    # OTM
            "mark": 2.0,
        }]
        r = triggers.check_ex_div_window(
            shorts=shorts, ex_div_date=ex_div, today=today, projected_dividend=1.85
        )
        self.assertFalse(r.triggered)

    def test_check_all_returns_six(self):
        state = {
            "tripwires": {
                "upper": 725.0,
                "lower": 690.0,
                "vix_shock": 24.5,
                "disorderly": {"price": 690.0, "vix": 22.0},
            },
            "ex_div_calendar": [],
        }
        results = triggers.check_all_tripwires(
            spot=710.0, vix=18.0, shorts=[],
            state=state, today=date(2026, 5, 18),
        )
        self.assertEqual(len(results), 6)
        for r in results:
            self.assertIsInstance(r, triggers.TripwireResult)


class RollTriggerTests(unittest.TestCase):
    def test_80pct_profit_fires_close(self):
        leg = {"mark": 1.0, "strike": 100.0, "dte": 30, "premium_received": 5.0}
        r = triggers.short_roll_trigger(leg, spot=95.0)
        self.assertTrue(r.triggered)
        self.assertIn("80%", r.reason)

    def test_50pct_profit_fires_harvest(self):
        leg = {"mark": 2.5, "strike": 100.0, "dte": 30, "premium_received": 5.0}
        r = triggers.short_roll_trigger(leg, spot=95.0)
        self.assertTrue(r.triggered)
        self.assertIn("50%", r.reason)

    def test_low_extrinsic_fires(self):
        # spot 105, strike 100 (ITM), mark 5.5 → intrinsic 5, extrinsic 0.5
        leg = {"mark": 5.5, "strike": 100.0, "dte": 30, "premium_received": 5.0}
        r = triggers.short_roll_trigger(leg, spot=105.0)
        self.assertTrue(r.triggered)
        self.assertIn("extrinsic", r.reason)

    def test_hold_when_safe(self):
        leg = {"mark": 4.0, "strike": 100.0, "dte": 30, "premium_received": 5.0}
        r = triggers.short_roll_trigger(leg, spot=95.0)
        self.assertFalse(r.triggered)


class RefreshTriggerTests(unittest.TestCase):
    def test_survival_floor_takes_precedence(self):
        r = triggers.leaps_refresh_trigger({"delta": 0.80, "dte": 90})
        self.assertTrue(r.triggered)
        self.assertEqual(r.urgency, "forced")

    def test_delta_drift(self):
        r = triggers.leaps_refresh_trigger({"delta": 0.65, "dte": 500})
        self.assertTrue(r.triggered)
        self.assertEqual(r.urgency, "immediate")

    def test_brick(self):
        r = triggers.leaps_refresh_trigger({"delta": 0.97, "dte": 500})
        self.assertTrue(r.triggered)
        self.assertEqual(r.urgency, "evaluate")

    def test_efficiency_trigger(self):
        r = triggers.leaps_refresh_trigger({"delta": 0.80, "dte": 300})
        self.assertTrue(r.triggered)
        self.assertEqual(r.urgency, "schedule")

    def test_hold(self):
        r = triggers.leaps_refresh_trigger({"delta": 0.80, "dte": 500})
        self.assertFalse(r.triggered)


if __name__ == "__main__":
    unittest.main()
