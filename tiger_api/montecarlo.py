"""Monte Carlo helpers — geometric Brownian motion terminal-price simulation
and short-option P&L distribution metrics.

Built for the PMCC Master Doctrine v3 §12 Trade Evaluation Scorecard. Pure
stdlib (random, math, statistics) so the Cloud Run image stays slim — no
numpy/scipy dependency. ~5,000-path runs complete in ~30ms on a single
Cloud Run vCPU; we run them inline on tool calls.

Conventions:
  - Risk-free rate `r` and vol `sigma` are annualised decimals (0.045, 0.17).
  - Time `t` is in years.
  - Spot/strike are per-share. P&L is per-contract ($, contract_multiplier=100).
  - Short-option sign convention: positive P&L = profit for the seller.
"""
from __future__ import annotations

import math
import random
import statistics
from typing import Optional

CONTRACT_MULTIPLIER = 100  # standard US equity option multiplier


# ── Geometric Brownian Motion terminal prices ────────────────────────────────


def mc_terminal_prices(
    spot: float,
    sigma: float,
    t: float,
    r: float = 0.045,
    n_paths: int = 5000,
    seed: Optional[int] = None,
) -> list[float]:
    """Sample n_paths terminal underlying prices from GBM at horizon t (years).

        S_T = S × exp((r − 0.5σ²)t + σ√t × Z),  Z ~ N(0,1)

    Returns: list[float] of length n_paths, each one a possible S_T.
    """
    if t <= 0:
        return [float(spot)] * n_paths
    rng = random.Random(seed) if seed is not None else random.Random()
    drift = (r - 0.5 * sigma * sigma) * t
    diffusion_scale = sigma * math.sqrt(t)
    out: list[float] = []
    for _ in range(n_paths):
        z = rng.gauss(0.0, 1.0)
        out.append(spot * math.exp(drift + diffusion_scale * z))
    return out


# ── Short-option terminal P&L distribution ───────────────────────────────────


def short_option_pnl_distribution(
    terminal_prices: list[float],
    strike: float,
    premium: float,
    is_call: bool,
) -> dict:
    """Compute the P&L distribution stats for a short option held to expiry.

    For each terminal spot S_T:
        payoff_intrinsic = max(0, S_T - K) for call, max(0, K - S_T) for put
        per-contract P&L = (premium - payoff_intrinsic) × 100

    The full §12 distribution block.
    """
    if not terminal_prices:
        return {
            "p_profit_50": 0.0, "p_profit_80": 0.0, "p_loss": 0.0, "p_assignment": 0.0,
            "expected_pnl": 0.0, "pnl_stdev": 0.0, "cvar_5": 0.0, "max_profit": 0.0,
        }

    pnls: list[float] = []
    assignments = 0
    for s_t in terminal_prices:
        if is_call:
            assigned = s_t > strike
            payoff = max(0.0, s_t - strike)
        else:
            assigned = s_t < strike
            payoff = max(0.0, strike - s_t)
        pnl = (premium - payoff) * CONTRACT_MULTIPLIER
        pnls.append(pnl)
        if assigned:
            assignments += 1

    n = len(pnls)
    max_profit = premium * CONTRACT_MULTIPLIER
    expected_pnl = sum(pnls) / n
    pnl_stdev = statistics.stdev(pnls) if n > 1 else 0.0

    # CVaR at 5% — mean of worst 5% paths (at least 1 path).
    pnls_sorted = sorted(pnls)
    worst_count = max(1, n // 20)
    cvar_5 = sum(pnls_sorted[:worst_count]) / worst_count

    return {
        "p_profit_50": sum(1 for p in pnls if p >= 0.5 * max_profit) / n,
        "p_profit_80": sum(1 for p in pnls if p >= 0.8 * max_profit) / n,
        "p_loss": sum(1 for p in pnls if p < 0) / n,
        "p_assignment": assignments / n,
        "expected_pnl": expected_pnl,
        "pnl_stdev": pnl_stdev,
        "cvar_5": cvar_5,
        "max_profit": max_profit,
    }


# ── PMCC §12 verdict / cutoff logic ──────────────────────────────────────────


def pmcc_verdict(
    distribution: dict,
    theta_per_day: float,
    theta_hurdle: float,
    annualised_return: float,
    annualised_vol: float,
) -> dict:
    """Apply §12 cutoffs and return a verdict block.

    Cutoffs from PMCC v3 §12:
      - Sharpe < 1.0                                  → notify (conditional)
      - P(loss) > 50% AND expected P&L < 0            → auto-reject (fail)
      - CVaR > 3× expected return                     → flag (conditional)
      - Theta hurdle fail                             → notify (conditional)

    `fail` dominates `conditional`. `pass` only when no rule fires.
    """
    reasons: list[str] = []

    sharpe = (annualised_return / annualised_vol) if annualised_vol > 0 else float("inf")

    # Theta hurdle (§2) — fail = below hurdle
    theta_pass = theta_per_day >= theta_hurdle

    expected_pnl = distribution.get("expected_pnl", 0.0)
    p_loss = distribution.get("p_loss", 0.0)
    cvar_5 = distribution.get("cvar_5", 0.0)

    # Auto-reject path
    is_fail = (p_loss > 0.5 and expected_pnl < 0)
    if is_fail:
        reasons.append(f"AUTO-REJECT: P(loss)={p_loss:.0%} > 50% AND expected P&L=${expected_pnl:.0f} < 0")

    # Conditionals
    if sharpe < 1.0:
        reasons.append(f"Sharpe-equivalent {sharpe:.2f} < 1.0 — notify, not auto-reject")
    if expected_pnl > 0 and abs(cvar_5) > 3 * expected_pnl:
        reasons.append(
            f"CVaR ${cvar_5:.0f} > 3× expected ${expected_pnl:.0f} — flag for explicit acceptance"
        )
    if not theta_pass:
        reasons.append(
            f"Theta {theta_per_day:.3f}/day < hurdle {theta_hurdle:.3f}/day — "
            "notify, document regime caveat"
        )

    if is_fail:
        verdict = "fail"
    elif reasons:
        verdict = "conditional"
    else:
        verdict = "pass"

    return {
        "verdict": verdict,
        "verdict_reasons": reasons,
        "theta_pass": theta_pass,
        "sharpe_equiv": round(sharpe, 4) if math.isfinite(sharpe) else None,
    }


# ── Realized volatility ──────────────────────────────────────────────────────


def realized_vol(close_prices: list[float], window: Optional[int] = None) -> float:
    """Annualised realised vol from a list of daily closes.

    Standard PMCC HV30 formula:
        log_returns = [ln(C_i / C_{i-1}) for last (window+1) closes]
        hv = stdev(log_returns, ddof=1) × √252

    If `window` is None, uses ALL returns implied by the input list. Pass
    `window=30` to get HV30 from a list of 31+ closes; the function will
    use only the last 30 returns regardless of input length.

    Returns 0.0 for inputs too short or with non-positive prices.
    """
    if not close_prices or len(close_prices) < 2:
        return 0.0
    log_returns: list[float] = []
    for i in range(1, len(close_prices)):
        prev = close_prices[i - 1]
        cur = close_prices[i]
        if prev is None or cur is None:
            continue
        if prev <= 0 or cur <= 0:
            continue
        log_returns.append(math.log(cur / prev))
    if window is not None and len(log_returns) > window:
        log_returns = log_returns[-window:]
    if len(log_returns) < 2:
        return 0.0
    return statistics.stdev(log_returns) * math.sqrt(252)
