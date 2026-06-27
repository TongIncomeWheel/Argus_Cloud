"""Black-Scholes Greeks — computed locally because Tiger denies Greeks for retail TBSG.

We compute Delta + Theta from the inputs we already have in our positions DataFrame:
    spot, strike, days-to-expiry, market price (for IV solve), is_call, is_long.

Why not just use Tiger's Greeks?
    Tiger's quote endpoint returns 'permission denied' for retail TBSG accounts.
    We already have spot prices (Yahoo/QuoteClient) and option market price (from
    position fills). All that's missing is implied volatility — we solve for it
    using the market option price, then plug back into BS to get Delta + Theta.

API:
    compute_greeks(spot, strike, dte_days, market_price, is_call, is_long=True,
                   r=0.045, q=0.0)
        → {'delta': float, 'theta_per_day': float, 'iv': float}

Math notes:
    • r = 0.045  (US risk-free ~4.5% short-term)
    • q = 0.00   (most underlying we trade don't pay dividends in our window;
                   stocks like SPY pay quarterly — small enough to ignore)
    • IV solver: Newton-Raphson on price; bisection fallback if Newton diverges.
    • Output theta is per-DAY (BS theta is per-year — divide by 365).
    • Short positions: delta and theta have the OPPOSITE sign of long.
"""
from __future__ import annotations

import math
from typing import Optional


SQRT_2PI = math.sqrt(2.0 * math.pi)


def _phi(x: float) -> float:
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / SQRT_2PI


def _Phi(x: float) -> float:
    """Standard normal CDF (via erf)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_price(spot: float, strike: float, t: float, r: float, sigma: float,
             is_call: bool, q: float = 0.0) -> float:
    """Black-Scholes price for a European option.
        spot   — underlying spot
        strike — option strike
        t      — time to expiry in YEARS
        r      — risk-free rate (annual)
        sigma  — volatility (annual)
        is_call — True for call, False for put
        q      — continuous dividend yield (annual)
    """
    if t <= 0 or sigma <= 0 or spot <= 0 or strike <= 0:
        # Intrinsic value at expiry
        if is_call:
            return max(spot - strike, 0.0)
        return max(strike - spot, 0.0)
    sqrt_t = math.sqrt(t)
    d1 = (math.log(spot / strike) + (r - q + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    if is_call:
        return spot * math.exp(-q * t) * _Phi(d1) - strike * math.exp(-r * t) * _Phi(d2)
    return strike * math.exp(-r * t) * _Phi(-d2) - spot * math.exp(-q * t) * _Phi(-d1)


def bs_vega(spot: float, strike: float, t: float, r: float, sigma: float,
            q: float = 0.0) -> float:
    """Vega = ∂Price/∂sigma — needed for the Newton step in implied-vol solve."""
    if t <= 0 or sigma <= 0 or spot <= 0 or strike <= 0:
        return 0.0
    sqrt_t = math.sqrt(t)
    d1 = (math.log(spot / strike) + (r - q + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    return spot * math.exp(-q * t) * _phi(d1) * sqrt_t


def implied_vol(market_price: float, spot: float, strike: float, t: float, r: float,
                is_call: bool, q: float = 0.0) -> Optional[float]:
    """Solve for implied volatility from observed market price.

    Strategy: Newton-Raphson with a sane starting guess (sigma=0.30). Fall back to
    bisection on [1e-4, 5.0] if Newton diverges or vega vanishes.

    Returns None if the market price is implausible (below intrinsic, etc.).
    """
    if t <= 0 or spot <= 0 or strike <= 0 or market_price is None or market_price < 0:
        return None
    # Reject prices below intrinsic (arbitrage) — there's no real IV
    intrinsic = max(spot - strike, 0.0) if is_call else max(strike - spot, 0.0)
    if market_price < intrinsic - 1e-4:
        return None

    # Newton-Raphson
    sigma = 0.30
    for _ in range(50):
        try:
            price = bs_price(spot, strike, t, r, sigma, is_call, q)
            vega = bs_vega(spot, strike, t, r, sigma, q)
            diff = price - market_price
            if abs(diff) < 1e-5:
                return sigma
            if vega < 1e-8:
                break  # fall through to bisection
            sigma_new = sigma - diff / vega
            if sigma_new <= 0 or sigma_new > 10:
                break
            sigma = sigma_new
        except (ValueError, ZeroDivisionError, OverflowError):
            break

    # Bisection fallback — guaranteed convergence on [low, high] if a root exists
    lo, hi = 1e-4, 5.0
    for _ in range(80):
        mid = 0.5 * (lo + hi)
        try:
            price = bs_price(spot, strike, t, r, mid, is_call, q)
        except (ValueError, OverflowError):
            return None
        if abs(price - market_price) < 1e-5:
            return mid
        if price < market_price:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


def bs_gamma(spot: float, strike: float, t: float, r: float, sigma: float,
             q: float = 0.0) -> float:
    """Gamma = ∂²Price/∂spot² — needed for the PMCC §12 scorecard
    (low / moderate / high classification).

    Symmetric for calls and puts (Black-Scholes property)."""
    if t <= 0 or sigma <= 0 or spot <= 0 or strike <= 0:
        return 0.0
    sqrt_t = math.sqrt(t)
    d1 = (math.log(spot / strike) + (r - q + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    return math.exp(-q * t) * _phi(d1) / (spot * sigma * sqrt_t)


def bs_delta_theta(spot: float, strike: float, t: float, r: float, sigma: float,
                   is_call: bool, q: float = 0.0) -> tuple:
    """Returns (delta, theta_per_year)."""
    if t <= 0 or sigma <= 0 or spot <= 0 or strike <= 0:
        # At expiry → delta is 0 or ±1, theta is 0
        if is_call:
            return (1.0 if spot > strike else 0.0, 0.0)
        return (-1.0 if spot < strike else 0.0, 0.0)
    sqrt_t = math.sqrt(t)
    d1 = (math.log(spot / strike) + (r - q + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t

    # Delta
    if is_call:
        delta = math.exp(-q * t) * _Phi(d1)
    else:
        delta = math.exp(-q * t) * (_Phi(d1) - 1.0)

    # Theta (annual; per-share)
    term1 = -(spot * math.exp(-q * t) * _phi(d1) * sigma) / (2 * sqrt_t)
    if is_call:
        term2 = -r * strike * math.exp(-r * t) * _Phi(d2)
        term3 = q * spot * math.exp(-q * t) * _Phi(d1)
        theta = term1 + term2 + term3
    else:
        term2 = r * strike * math.exp(-r * t) * _Phi(-d2)
        term3 = -q * spot * math.exp(-q * t) * _Phi(-d1)
        theta = term1 + term2 + term3
    return delta, theta


def compute_greeks(spot: float, strike: float, dte_days: float, market_price: float,
                   is_call: bool, is_long: bool = True,
                   r: float = 0.045, q: float = 0.0) -> dict:
    """Compute Delta + Theta for a single option position.

    Args:
        spot          Underlying spot price.
        strike        Option strike.
        dte_days      Days to expiry (calendar).
        market_price  Current option market price (per share).
        is_call       True=call, False=put.
        is_long       True for long (BTO), False for short (STO). Sign-flips delta + theta.
        r             Risk-free rate (default 4.5%).
        q             Continuous dividend yield (default 0).

    Returns:
        {'delta': float, 'theta_per_day': float, 'iv': float}
        — all None if inputs invalid or IV solve fails.
    """
    out = {"delta": None, "theta_per_day": None, "iv": None}
    try:
        spot = float(spot); strike = float(strike); dte_days = float(dte_days)
        market_price = float(market_price)
    except (TypeError, ValueError):
        return out
    if spot <= 0 or strike <= 0 or dte_days <= 0 or market_price <= 0:
        return out

    t = dte_days / 365.0
    iv = implied_vol(market_price, spot, strike, t, r, is_call, q)
    if iv is None or iv <= 0:
        return out
    delta, theta_per_year = bs_delta_theta(spot, strike, t, r, iv, is_call, q)
    theta_per_day = theta_per_year / 365.0

    sign = 1.0 if is_long else -1.0
    return {
        "delta": delta * sign,
        "theta_per_day": theta_per_day * sign,
        "iv": iv,
    }
