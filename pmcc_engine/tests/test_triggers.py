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


class ShortStatusLabelTests(unittest.TestCase):
    def test_sit_when_clean(self):
        s = triggers.short_status_label(
            {"mark": 4.5, "strike": 750.0, "dte": 30, "premium_received": 5.0}, spot=738.0)
        self.assertEqual(s["tier"], "ok")
        self.assertIn("Sit", s["label"])

    def test_approaching_50pct(self):
        # OTM call: strike 750 vs spot 738 — premium $5, mark $3.45 → 31% profit
        # Extrinsic = mark = $3.45 (OTM has no intrinsic). >$3 so not 'declining' watch.
        # Should hit 'Approaching 50% harvest'.
        s = triggers.short_status_label(
            {"mark": 3.45, "strike": 750.0, "dte": 37, "premium_received": 5.0}, spot=738.0)
        self.assertEqual(s["tier"], "watch")
        self.assertIn("Approaching 50%", s["label"])

    def test_harvest_at_50pct(self):
        # OTM call: premium $5, mark $2.50 = 50% profit. ext = $2.50 (no intrinsic).
        # profit ≥50% wins ordering ahead of extrinsic-declining watch.
        s = triggers.short_status_label(
            {"mark": 2.5, "strike": 760.0, "dte": 30, "premium_received": 5.0}, spot=738.0)
        self.assertEqual(s["tier"], "triggered")
        self.assertIn("Harvest", s["label"])

    def test_close_at_80pct(self):
        # OTM call: premium $5, mark $1.00 = 80% profit
        s = triggers.short_status_label(
            {"mark": 1.0, "strike": 760.0, "dte": 30, "premium_received": 5.0}, spot=738.0)
        self.assertEqual(s["tier"], "triggered")
        self.assertIn("Close", s["label"])

    def test_extrinsic_critical(self):
        # ITM short with extrinsic < $1
        s = triggers.short_status_label(
            {"mark": 23.5, "strike": 715.0, "dte": 30, "premium_received": 20.0}, spot=738.0)
        # intrinsic 23, extrinsic 0.5 → critical
        self.assertEqual(s["tier"], "triggered")
        self.assertIn("Extrinsic", s["label"])

    def test_extrinsic_declining_watch(self):
        # ITM short with extrinsic between $1 and $3
        s = triggers.short_status_label(
            {"mark": 25.0, "strike": 715.0, "dte": 30, "premium_received": 20.0}, spot=738.0)
        # intrinsic 23, extrinsic 2 → watch
        self.assertEqual(s["tier"], "watch")

    def test_dte_critical(self):
        s = triggers.short_status_label(
            {"mark": 3.0, "strike": 750.0, "dte": 8, "premium_received": 4.0}, spot=738.0)
        # DTE 8, profit 25% → DTE forced roll
        self.assertEqual(s["tier"], "triggered")

    def test_paper_red(self):
        # Short above spot that's now near spot, mark > premium
        s = triggers.short_status_label(
            {"mark": 9.0, "strike": 745.0, "dte": 30, "premium_received": 5.0}, spot=738.0)
        # 745 ≤ 738 * 1.02 = 752.76 → strike threatened
        # profit = (5-9)/5 = -80% → paper red
        self.assertEqual(s["tier"], "watch")
        self.assertIn("Paper red", s["label"])


class ItemsOnWatchTests(unittest.TestCase):
    def test_extrinsic_approaching_floor(self):
        from datetime import date
        shorts = [{
            "strike": 710.0, "mark": 31.0, "extrinsic": 3.0,
            "theta_per_day": 0.15, "dte": 29, "premium_received": 30.0,
            "label": "$710",
        }]
        items = triggers.items_on_watch(shorts, state={}, today=date(2026, 5, 19))
        # Should surface extrinsic watch
        self.assertTrue(any("extrinsic" in i["item"].lower() for i in items))

    def test_profit_approaching_50(self):
        from datetime import date
        shorts = [{
            "strike": 725.0, "mark": 3.5, "extrinsic": 11.25,
            "theta_per_day": 0.20, "dte": 37, "premium_received": 5.0,
            "label": "$725",
        }]
        items = triggers.items_on_watch(shorts, state={}, today=date(2026, 5, 19))
        # 30% profit → on watch for 50%
        self.assertTrue(any("profit" in i["item"].lower() for i in items))

    def test_ex_div_approaching(self):
        from datetime import date
        state = {"ex_div_calendar": [{"date": "2026-06-19", "est_dividend": 1.85}]}
        items = triggers.items_on_watch([], state=state, today=date(2026, 5, 25))
        # ~17 business days to ex-div, within 30-day window
        self.assertTrue(any("Ex-div" in i["item"] for i in items))


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
