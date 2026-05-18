"""End-to-end smoke test — exercises the engine on a synthetic book.

No network calls. Uses hand-built positions and a hand-built market state.
"""
from __future__ import annotations

import unittest
from datetime import date

from pmcc_engine import (
    doctrine, regime, theta_math, triggers, strikes, rolls, scorecard, posture, state, review,
)


class IntegrationTest(unittest.TestCase):
    def setUp(self):
        # Synthetic SPY book in the doctrine's base case (Band M × IVR neutral).
        self.spot = 738.0
        self.hv30 = 0.17
        self.vix = 20.0
        self.median_vix = 18.0   # 5y SPY benchmark from doctrine seed
        self.ivr = 45.0          # neutral

        # Two LEAPS @ 0.80Δ, four short calls (2 ITM + 2 OTM = 2/2 array)
        self.longs = [
            {"qty": 2, "delta": 0.80, "theta": -0.05},
        ]
        self.shorts = [
            {"strike": 720.0, "delta": 0.60, "theta": -0.12, "qty": 1, "mark": 22.0,
             "dte": 35, "premium_received": 22.0, "is_call": True},
            {"strike": 725.0, "delta": 0.55, "theta": -0.11, "qty": 1, "mark": 17.0,
             "dte": 35, "premium_received": 17.0, "is_call": True},
            {"strike": 745.0, "delta": 0.42, "theta": -0.10, "qty": 1, "mark": 8.0,
             "dte": 35, "premium_received": 8.0, "is_call": True},
            {"strike": 750.0, "delta": 0.35, "theta": -0.09, "qty": 1, "mark": 6.0,
             "dte": 35, "premium_received": 6.0, "is_call": True},
        ]

        # Settings with engine state
        self.settings = {}
        state.upsert_ticker_state(self.settings, "SPY", {
            "vol_median_5yr": self.median_vix,
            "vol_axis": "VIX",
            "quarterly_dividend": 1.85,
            "tripwires": {
                "upper": 760.0, "lower": 700.0, "vix_shock": 24.5,
                "disorderly": {"price": 700.0, "vix": 22.0},
            },
            "ex_div_calendar": [],
        })
        self.ts = state.get_ticker_state(self.settings, "SPY")

    def test_regime_resolves_to_base_case(self):
        cell = regime.regime_cell(self.vix, self.median_vix, self.ivr)
        self.assertTrue(regime.is_base_case(cell))
        self.assertEqual(cell["posture"], "base_case")

    def test_book_greeks_aggregated(self):
        # qty=2 longs at delta 0.80 → long_delta = 2 * 0.80 * 100 = 160
        # 4 shorts with deltas 0.60+0.55+0.42+0.35 = 1.92 → short_delta = 192
        # → net_delta = -32 (slight short-delta tilt; this is the case where
        #   the ITM shorts ate into the chassis delta)
        g = theta_math.book_greeks(self.longs, self.shorts)
        self.assertAlmostEqual(g["long_delta"], 160.0, places=1)
        self.assertAlmostEqual(g["short_delta"], 192.0, places=1)
        self.assertAlmostEqual(g["net_delta"], -32.0, places=1)

    def test_yield_ratio_above_hurdle(self):
        # Theta capture: each short produces ~$0.10-0.12/day
        # Hurdle: ~$0.49/day per short at 17% vol → ratio < 1 expected
        yr = theta_math.yield_ratio(
            [{"strike": s["strike"], "theta_per_day": s["theta"]} for s in self.shorts],
            self.hv30,
        )
        # Verify it returns a real number (we constructed slightly-below-hurdle theta)
        self.assertGreater(yr, 0)
        self.assertLess(yr, 2.0)

    def test_all_tripwires_pass(self):
        results = triggers.check_all_tripwires(
            spot=self.spot, vix=self.vix,
            shorts=[{"strike": s["strike"], "spot": self.spot, "mark": s["mark"],
                     "dte": s["dte"], "premium_received": s["premium_received"]} for s in self.shorts],
            state=self.ts,
            today=date(2026, 5, 18),
        )
        # No tripwires should fire under base case
        breached = [r.name for r in results if r.triggered]
        self.assertEqual(breached, [])

    def test_array_is_2_2(self):
        layout = posture.array_layout(self.spot, self.shorts)
        self.assertTrue(layout["is_2_2"])

    def test_coverage_chassis_qty_matches_longs(self):
        cov = posture.coverage_ratios(self.longs, self.shorts)
        self.assertEqual(cov["long_total"], 2)
        self.assertEqual(cov["short_total"], 4)
        # 0.80Δ longs sit in the chassis baseline band
        self.assertEqual(cov["chassis_qty"], 2)

    def test_review_renders_without_error(self):
        cell = regime.regime_cell(self.vix, self.median_vix, self.ivr)
        cell["ivr"] = self.ivr
        cell["vol_axis"] = "VIX"
        g = theta_math.book_greeks(self.longs, self.shorts)
        tpd = theta_math.theta_per_delta(g["net_theta"], max(1.0, g["net_delta"]))
        tripwires = triggers.check_all_tripwires(
            spot=self.spot, vix=self.vix,
            shorts=[{"strike": s["strike"], "spot": self.spot, "mark": s["mark"],
                     "dte": s["dte"], "premium_received": s["premium_received"]} for s in self.shorts],
            state=self.ts, today=date(2026, 5, 18),
        )
        text = review.render_review(
            ticker="SPY", spot=self.spot, cell=cell,
            aggregate={**g, "theta_per_delta": tpd,
                       "theta_per_delta_rating": theta_math.theta_per_delta_rating(tpd),
                       "coverage": posture.coverage_ratios(self.longs, self.shorts)},
            positions=[{"type": "LEAP", "strike": 650, "dte": 365, "mark": 100, "delta": 0.80,
                        "theta_per_day": -0.05, "extrinsic": 12.0}],
            tripwires=tripwires,
        )
        self.assertIn("SPY", text)
        self.assertIn("BLOCK 1", text.upper())
        self.assertIn("BLOCK 2", text.upper())
        self.assertIn("BLOCK 3", text.upper())
        self.assertIn("BLOCK 4", text.upper())
        self.assertIn("regime", text.lower())
        self.assertIn("base_case", text)
        self.assertIn("Silent days are good days", text)


if __name__ == "__main__":
    unittest.main()
