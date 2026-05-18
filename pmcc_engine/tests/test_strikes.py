"""Tests for strike candidate filters."""
from __future__ import annotations

import unittest

from pmcc_engine import strikes


class CandidateTests(unittest.TestCase):
    def test_itm_candidates_filtered_to_band(self):
        chain = [
            # Way ITM — outside band
            {"strike": 600.0, "mid": 140.0, "theta": -0.20, "dte": 35, "open_interest": 500},
            # In band (1-5% below)
            {"strike": 720.0, "mid": 22.0, "theta": -0.35, "dte": 35, "open_interest": 500},  # 2.4% below 738
            {"strike": 715.0, "mid": 27.0, "theta": -0.30, "dte": 35, "open_interest": 500},  # 3.1% below
            # OTM — wrong side
            {"strike": 745.0, "mid": 8.0, "theta": -0.25, "dte": 35, "open_interest": 500},
        ]
        candidates = strikes.itm_candidates(spot=738.0, chain=chain, hv30=0.17)
        self.assertEqual(len(candidates), 2)
        self.assertTrue(all(c["side"] == "ITM" for c in candidates))
        # First should be closest to 3% below (715 is 3.1% below — closest to target)
        self.assertEqual(candidates[0]["strike"], 715.0)

    def test_otm_candidates_filtered_to_band(self):
        chain = [
            {"strike": 735.0, "mid": 12.0, "theta": -0.30, "dte": 35, "open_interest": 500},  # ITM — wrong side
            {"strike": 745.0, "mid": 8.0, "theta": -0.25, "dte": 35, "open_interest": 500},   # 0.95% — too close
            {"strike": 755.0, "mid": 6.0, "theta": -0.20, "dte": 35, "open_interest": 500},   # 2.3% — in band
        ]
        candidates = strikes.otm_candidates(spot=738.0, chain=chain, hv30=0.17)
        # Only 755 should pass (745 is 0.95% above = below 1% band)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["strike"], 755.0)
        self.assertEqual(candidates[0]["side"], "OTM")

    def test_hurdle_pass_flag(self):
        # spot 738, strike 720, hv30 17% → hurdle ~ 720 × 0.17 / √252 × 0.04 ≈ 0.31
        # Theta = 0.50 — passes
        # Theta = 0.10 — fails
        chain = [
            {"strike": 720.0, "mid": 22.0, "theta": -0.50, "dte": 35, "open_interest": 500},
            {"strike": 720.0, "mid": 22.0, "theta": -0.10, "dte": 35, "open_interest": 500},
        ]
        candidates = strikes.itm_candidates(spot=738.0, chain=chain, hv30=0.17)
        self.assertTrue(candidates[0]["hurdle_pass"])
        self.assertFalse(candidates[1]["hurdle_pass"])

    def test_dte_band_check(self):
        self.assertTrue(strikes.dte_in_band(35, 4, 6))   # 4-6 weeks = 28-42 days
        self.assertFalse(strikes.dte_in_band(45, 4, 6))
        self.assertFalse(strikes.dte_in_band(20, 4, 6))

    def test_liquidity_floor(self):
        good = {"open_interest": 500, "bid": 9.0, "ask": 9.20, "mid": 9.10}
        bad_oi = {"open_interest": 50, "bid": 9.0, "ask": 9.20, "mid": 9.10}
        bad_spread = {"open_interest": 500, "bid": 9.0, "ask": 10.0, "mid": 9.50}  # 10.5% spread
        self.assertTrue(strikes.liquidity_ok(good))
        self.assertFalse(strikes.liquidity_ok(bad_oi))
        self.assertFalse(strikes.liquidity_ok(bad_spread))

    def test_split_calls_puts(self):
        chain = [
            {"strike": 100, "right": "C"},
            {"strike": 100, "right": "P"},
            {"strike": 105, "put_call": "CALL"},
            {"strike": 105, "type": "PUT"},
        ]
        calls, puts = strikes.split_chain_calls_puts(chain)
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(puts), 2)


if __name__ == "__main__":
    unittest.main()
