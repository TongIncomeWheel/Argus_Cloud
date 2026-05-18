"""Monte Carlo trade scorecard — §12.

Geometric Brownian motion path generator + P&L distribution at expiry.
Used to score a single short call leg (short put extension included).
"""
from __future__ import annotations

import math
import random
from typing import Optional

from . import doctrine


def _mc_paths(spot: float, sigma: float, r: float, dte_days: int,
              paths: int, seed: Optional[int] = None) -> list:
    """Return `paths` simulated terminal spots under GBM."""
    if seed is not None:
        rnd = random.Random(seed)
        gauss = rnd.gauss
    else:
        gauss = random.gauss
    T = dte_days / 365.0
    if T <= 0 or sigma <= 0:
        return [spot] * paths
    drift = (r - 0.5 * sigma * sigma) * T
    diffusion = sigma * math.sqrt(T)
    out = []
    for _ in range(paths):
        z = gauss(0, 1)
        out.append(spot * math.exp(drift + diffusion * z))
    return out


def short_call_scorecard(
    spot: float,
    strike: float,
    premium: float,
    dte_days: int,
    hv30: float,
    rfr: float = doctrine.MC_DEFAULT_RISK_FREE_RATE,
    paths: int = doctrine.MC_DEFAULT_PATHS,
    seed: Optional[int] = None,
) -> dict:
    """MC scorecard for a short call leg.

    P&L at expiry per contract: (premium − max(0, S_T − K)) × 100.

    Returns dict with mean_pnl, stdev, p_profit_50, p_profit_80, p_loss,
    p_assignment, cvar_5, sharpe, ann_return, ann_vol.
    """
    if spot <= 0 or strike <= 0 or premium <= 0 or dte_days <= 0 or hv30 <= 0:
        return _zero_scorecard()
    terminals = _mc_paths(spot, hv30, rfr, dte_days, paths, seed=seed)
    results = [(premium - max(0.0, s_t - strike)) * 100.0 for s_t in terminals]
    return _aggregate_scorecard(results, premium, dte_days, hv30, rfr,
                                assignment_predicate=lambda s_t: s_t > strike,
                                terminals=terminals)


def short_put_scorecard(
    spot: float,
    strike: float,
    premium: float,
    dte_days: int,
    hv30: float,
    rfr: float = doctrine.MC_DEFAULT_RISK_FREE_RATE,
    paths: int = doctrine.MC_DEFAULT_PATHS,
    seed: Optional[int] = None,
) -> dict:
    """MC scorecard for a short put leg (CSP).

    P&L at expiry per contract: (premium − max(0, K − S_T)) × 100.
    """
    if spot <= 0 or strike <= 0 or premium <= 0 or dte_days <= 0 or hv30 <= 0:
        return _zero_scorecard()
    terminals = _mc_paths(spot, hv30, rfr, dte_days, paths, seed=seed)
    results = [(premium - max(0.0, strike - s_t)) * 100.0 for s_t in terminals]
    return _aggregate_scorecard(results, premium, dte_days, hv30, rfr,
                                assignment_predicate=lambda s_t: s_t < strike,
                                terminals=terminals)


def _aggregate_scorecard(results, premium, dte_days, hv30, rfr,
                         assignment_predicate, terminals) -> dict:
    results_sorted = sorted(results)
    n = len(results_sorted)
    mean_pnl = sum(results_sorted) / n
    stdev = math.sqrt(sum((r - mean_pnl) ** 2 for r in results_sorted) / max(n - 1, 1))

    profit_50_floor = premium * 50.0   # 50% of max profit
    profit_80_floor = premium * 80.0
    p_profit_50 = sum(1 for r in results_sorted if r >= profit_50_floor) / n
    p_profit_80 = sum(1 for r in results_sorted if r >= profit_80_floor) / n
    p_loss = sum(1 for r in results_sorted if r < 0) / n
    p_assignment = sum(1 for s_t in terminals if assignment_predicate(s_t)) / n

    tail_n = max(1, int(n * 0.05))
    cvar_5 = sum(results_sorted[:tail_n]) / tail_n

    capital = premium * 100.0
    ann_return = (mean_pnl / capital) * (365.0 / max(dte_days, 1)) if capital > 0 else 0.0
    ann_vol = (stdev / capital) * math.sqrt(365.0 / max(dte_days, 1)) if capital > 0 else 0.0
    sharpe = (ann_return - rfr) / ann_vol if ann_vol > 0 else 0.0

    return {
        "mean_pnl": mean_pnl,
        "stdev": stdev,
        "p_profit_50": p_profit_50,
        "p_profit_80": p_profit_80,
        "p_loss": p_loss,
        "p_assignment": p_assignment,
        "cvar_5": cvar_5,
        "sharpe": sharpe,
        "ann_return": ann_return,
        "ann_vol": ann_vol,
        "paths": n,
        "hv30_used": hv30,
        "rfr_used": rfr,
    }


def _zero_scorecard() -> dict:
    return {k: 0.0 for k in (
        "mean_pnl", "stdev", "p_profit_50", "p_profit_80",
        "p_loss", "p_assignment", "cvar_5", "sharpe", "ann_return", "ann_vol",
    )} | {"paths": 0, "hv30_used": 0.0, "rfr_used": 0.0}


def verdict(sc: dict) -> tuple:
    """Apply §12 cutoffs. Returns (label, reasons[])."""
    reasons = []
    if sc["paths"] == 0:
        return "fail", ["zero-input scorecard"]
    if sc["p_loss"] > doctrine.P_LOSS_AUTO_REJECT and sc["mean_pnl"] < 0:
        reasons.append(f"P(loss) {sc['p_loss']*100:.0f}% > {doctrine.P_LOSS_AUTO_REJECT*100:.0f}% AND expected P&L < 0 — auto-reject")
        return "fail", reasons
    if sc["sharpe"] < doctrine.SHARPE_FLAG_THRESHOLD:
        reasons.append(f"Sharpe-eq {sc['sharpe']:.2f} < {doctrine.SHARPE_FLAG_THRESHOLD}")
    if sc["mean_pnl"] != 0 and abs(sc["cvar_5"]) > doctrine.CVAR_FLAG_MULTIPLIER * abs(sc["mean_pnl"]):
        reasons.append(f"CVaR 5% ${sc['cvar_5']:.0f} > {doctrine.CVAR_FLAG_MULTIPLIER}× expected ${sc['mean_pnl']:.0f}")
    return ("conditional" if reasons else "pass"), reasons
