"""Math layer — HV30, theta hurdle, yield ratio, extrinsic, theta/delta, book greeks.

Pure functions. No streamlit, no I/O. All numbers in dollars and decimal fractions.
Doctrine reference: §2 (theta hurdle), §13 (theta_per_delta).
"""
from __future__ import annotations

import math
from typing import Iterable, Mapping, Optional, Sequence

from . import doctrine


def compute_hv30(daily_closes: Sequence[float]) -> Optional[float]:
    """Annualized 30-day historical volatility from daily closes.

    Args:
        daily_closes: chronological closes. Need ≥31 to compute 30 returns.

    Returns:
        Annualized vol as a decimal (0.17 = 17%). None if not enough data.
    """
    closes = [float(c) for c in daily_closes if c is not None]
    if len(closes) < 31:
        return None
    log_returns = []
    for i in range(1, len(closes)):
        if closes[i - 1] <= 0 or closes[i] <= 0:
            continue
        log_returns.append(math.log(closes[i] / closes[i - 1]))
    last_30 = log_returns[-30:]
    if len(last_30) < 30:
        return None
    mean = sum(last_30) / 30.0
    variance = sum((r - mean) ** 2 for r in last_30) / 29.0   # ddof=1
    return math.sqrt(variance) * math.sqrt(doctrine.TRADING_DAYS_PER_YEAR)


def daily_one_sigma_move(strike: float, hv30: float) -> float:
    """Expected daily 1σ dollar move at a given strike under HV30.

    Used as the input to the dynamic theta hurdle.
    """
    if strike <= 0 or hv30 <= 0:
        return 0.0
    return strike * hv30 / math.sqrt(doctrine.TRADING_DAYS_PER_YEAR)


def theta_hurdle(
    strike: float,
    hv30: float,
    capture_rate: float = doctrine.HURDLE_CAPTURE_RATE,
) -> float:
    """Dollar/day floor a short must produce to clear the dynamic theta hurdle.

    hurdle = strike × HV30 / √252 × capture_rate
    """
    return daily_one_sigma_move(strike, hv30) * capture_rate


def yield_ratio(shorts: Iterable[Mapping], hv30: float,
                capture_rate: float = doctrine.HURDLE_CAPTURE_RATE) -> float:
    """Book-level yield ratio: actual theta / total hurdle.

    shorts: iterable of dicts with keys ``strike`` and ``theta_per_day``.
            theta_per_day is the absolute daily theta the short produces
            (positive number — premium decaying to the short seller's benefit).

    Returns:
        Ratio. ≥ 1.0 means earning at-or-above spec.
    """
    actual = 0.0
    hurdle = 0.0
    for s in shorts:
        try:
            actual += abs(float(s.get("theta_per_day", 0.0)))
            hurdle += theta_hurdle(float(s["strike"]), hv30, capture_rate)
        except (KeyError, TypeError, ValueError):
            continue
    if hurdle <= 0:
        return 0.0
    return actual / hurdle


def extrinsic(mark: float, spot: float, strike: float, is_call: bool = True) -> float:
    """Time value remaining in an option mark.

    For calls: mark − max(0, spot − strike)
    For puts:  mark − max(0, strike − spot)
    """
    try:
        mark = float(mark); spot = float(spot); strike = float(strike)
    except (TypeError, ValueError):
        return 0.0
    if is_call:
        intrinsic = max(0.0, spot - strike)
    else:
        intrinsic = max(0.0, strike - spot)
    return mark - intrinsic


def book_greeks(longs: Iterable[Mapping], shorts: Iterable[Mapping]) -> dict:
    """Aggregate Greeks across longs and shorts.

    Each leg: {qty, delta, theta} where delta + theta are PER-SHARE
    (Alpaca/BS convention — multiply by 100 for per-contract).

    Returns delta in $ exposure per $1 underlying move, theta in $/day.
    """
    long_delta = 0.0
    short_delta = 0.0
    long_theta = 0.0
    short_theta = 0.0
    for l in longs:
        try:
            long_delta += float(l["qty"]) * float(l["delta"]) * 100.0
            long_theta += float(l["qty"]) * float(l["theta"]) * 100.0  # already negative
        except (KeyError, TypeError, ValueError):
            continue
    for s in shorts:
        try:
            # Short delta is positive in the source dict — we want absolute exposure on the short side
            short_delta += float(s["qty"]) * float(s["delta"]) * 100.0
            short_theta += float(s["qty"]) * abs(float(s["theta"])) * 100.0
        except (KeyError, TypeError, ValueError):
            continue
    return {
        "long_delta": long_delta,
        "short_delta": short_delta,
        "net_delta": long_delta - short_delta,
        "long_theta": long_theta,    # negative (decay against us)
        "short_theta": short_theta,  # positive (decay for us)
        "net_theta": short_theta + long_theta,
    }


def theta_per_delta(net_theta: float, net_delta: float) -> float:
    """Income efficiency: dollar daily theta per dollar of directional risk.

    Returns 0 if net_delta is non-positive (no directional risk to compare).
    """
    if net_delta is None or net_delta <= 0:
        return 0.0
    return float(net_theta) / float(net_delta)


def theta_per_delta_rating(tpd: float) -> str:
    """Map theta_per_delta to {Optimal, Acceptable, Suboptimal} per §13."""
    if tpd >= doctrine.THETA_PER_DELTA_OPTIMAL:
        return "Optimal"
    if tpd >= doctrine.THETA_PER_DELTA_ACCEPTABLE:
        return "Acceptable"
    return "Suboptimal"


def daily_risk_one_sigma(net_delta: float, spot: float, hv30: float) -> float:
    """Dollar-equivalent 1σ daily P&L risk at the book level.

    net_delta is shares-equivalent ($ P&L per $1 underlying move).
    Daily 1σ underlying $-move = spot × hv30 / √252.
    So daily 1σ portfolio risk = |net_delta| × (spot × hv30 / √252).
    """
    if spot <= 0 or hv30 <= 0:
        return 0.0
    daily_1sigma_underlying = spot * hv30 / math.sqrt(doctrine.TRADING_DAYS_PER_YEAR)
    return abs(float(net_delta)) * daily_1sigma_underlying


def theta_coverage(net_theta: float, daily_risk: float) -> float:
    """Theta as a fraction of 1σ daily risk. ≥1.0 means a 1σ down day is covered by theta."""
    if daily_risk <= 0:
        return 0.0
    return float(net_theta) / float(daily_risk)
