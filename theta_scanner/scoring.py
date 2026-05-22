"""Scoring math for the Scanner — pure functions, no I/O, no Streamlit.

Handles both wheel legs:
  - PUT  → cash-secured put: collateral is strike × 100.
  - CALL → covered call: basis is the 100 shares you hold (≈ spot × 100).

All percentage outputs are in display units (2.3 means 2.3%), so the
composite-score scaling constants below are also in percent.
"""
from __future__ import annotations

from typing import Optional

# ─── Composite-score scaling targets (percent units) ──────────────
ANN_YIELD_FULL_MARKS = 35.0    # 35%+ annualized yield → 100 on the yield axis
DISTANCE_FULL_MARKS = 10.0     # 10%+ OTM → 100 on the safety axis
DELTA_TARGET = 0.25            # delta sweet spot
DELTA_BANDWIDTH = 0.25         # delta score decays to 0 at |delta| 0.00 or 0.50

W_YIELD = 0.40
W_DISTANCE = 0.30
W_DELTA = 0.30

# ─── Liquidity floors ──────────────────────────────────────────────
MIN_OPEN_INTEREST = 100
MAX_SPREAD_PCT = 8.0           # bid/ask spread ≤ 8% of mid

# ─── Verdict cutoffs (option score 0-100) ──────────────────────────
VERDICT_STRONG = 75.0
VERDICT_GOOD = 60.0
VERDICT_MARGINAL = 45.0


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def _is_put(option_type: str) -> bool:
    return str(option_type).upper().startswith("P")


# ─── Per-contract economics ────────────────────────────────────────


def option_economics(option_type: str, spot: float, strike: float,
                      premium: float, dte, delta) -> dict:
    """Return on capital, annualized yield, %OTM, breakeven, PoP.

    Percentages are in display units. `dte`/`delta` may be None.
    """
    spot = float(spot or 0)
    strike = float(strike or 0)
    premium = float(premium or 0)
    is_put = _is_put(option_type)

    # Collateral basis: strike for a CSP, spot for a covered call.
    basis = strike if is_put else spot
    roc = (premium / basis * 100.0) if basis > 0 else 0.0

    try:
        d = float(dte)
    except (TypeError, ValueError):
        d = 0.0
    annual_yield = roc * (365.0 / d) if d > 0 else 0.0

    if spot > 0:
        # Positive %OTM = the safe side for either leg.
        pct_otm = ((spot - strike) / spot * 100.0) if is_put else ((strike - spot) / spot * 100.0)
    else:
        pct_otm = 0.0

    breakeven = (strike - premium) if is_put else (spot - premium)

    delta_abs = abs(float(delta)) if delta is not None else None
    pop = (1.0 - delta_abs) * 100.0 if delta_abs is not None else None

    return {
        "roc": roc,
        "annual_yield": annual_yield,
        "pct_otm": pct_otm,
        "breakeven": breakeven,
        "pop": pop,
    }


# ─── Composite axis scores (0-100) ─────────────────────────────────


def yield_score(annual_yield_pct: float) -> float:
    return _clamp(annual_yield_pct / ANN_YIELD_FULL_MARKS) * 100.0


def distance_score(pct_otm: float) -> float:
    return _clamp(pct_otm / DISTANCE_FULL_MARKS) * 100.0


def delta_score(delta_abs: Optional[float]) -> float:
    """Tent function — peaks at DELTA_TARGET, 0 at the band edges."""
    if delta_abs is None:
        return 0.0
    return _clamp(1.0 - abs(abs(float(delta_abs)) - DELTA_TARGET) / DELTA_BANDWIDTH) * 100.0


def option_score(annual_yield_pct: float, pct_otm: float,
                 delta_abs: Optional[float]) -> float:
    """Blended 0-100 desirability of a contract to sell."""
    return (
        W_YIELD * yield_score(annual_yield_pct)
        + W_DISTANCE * distance_score(pct_otm)
        + W_DELTA * delta_score(delta_abs)
    )


def verdict(score: float) -> str:
    if score >= VERDICT_STRONG:
        return "Strong"
    if score >= VERDICT_GOOD:
        return "Good"
    if score >= VERDICT_MARGINAL:
        return "Marginal"
    return "Weak"


# ─── Underlying-level scores ───────────────────────────────────────


def stock_rating(price: Optional[float], ma20: Optional[float],
                 ma50: Optional[float], ma200: Optional[float],
                 rsi: Optional[float], perf_quarter: Optional[float]) -> Optional[float]:
    """0-100 technical health of the underlying.

    Blend of trend (price vs moving averages), momentum (quarterly
    performance) and an RSI-health tent. Returns None if nothing is known.
    """
    parts: list = []
    weights: list = []

    if price and price > 0 and any(m for m in (ma20, ma50, ma200)):
        hits = sum(1 for m in (ma20, ma50, ma200) if m and price > m)
        seen = sum(1 for m in (ma20, ma50, ma200) if m)
        parts.append((hits / seen) * 100.0)
        weights.append(0.45)

    if perf_quarter is not None:
        # -20%..+20% quarterly → 0..100
        parts.append(_clamp((float(perf_quarter) + 20.0) / 40.0) * 100.0)
        weights.append(0.30)

    if rsi is not None:
        # Health tent: peaks at RSI 55, 0 at 25 or 85.
        parts.append(_clamp(1.0 - abs(float(rsi) - 55.0) / 30.0) * 100.0)
        weights.append(0.25)

    if not parts:
        return None
    total_w = sum(weights)
    return sum(p * w for p, w in zip(parts, weights)) / total_w


def rel_strength(perf_year: Optional[float],
                 benchmark_perf_year: Optional[float]) -> Optional[float]:
    """0-100 relative strength vs a benchmark. 50 = matches the benchmark."""
    if perf_year is None or benchmark_perf_year is None:
        return None
    rel = float(perf_year) - float(benchmark_perf_year)
    # -50pp..+50pp relative → 0..100
    return _clamp((rel + 50.0) / 100.0) * 100.0


# ─── Liquidity gate ────────────────────────────────────────────────


def liquidity_ok(open_interest: Optional[float], bid: Optional[float],
                 ask: Optional[float], mid: Optional[float]) -> bool:
    """Open-interest floor + bid/ask spread ceiling.

    A missing quote is a data gap, not a defect — only an explicitly-wide
    spread rejects.
    """
    try:
        if open_interest is not None and float(open_interest) < MIN_OPEN_INTEREST:
            return False
    except (TypeError, ValueError):
        pass
    try:
        if bid is None or ask is None or not mid or float(mid) <= 0:
            return True
        return (float(ask) - float(bid)) / float(mid) * 100.0 <= MAX_SPREAD_PCT
    except (TypeError, ValueError):
        return True


def spread_pct(bid: Optional[float], ask: Optional[float],
               mid: Optional[float]) -> Optional[float]:
    """Bid/ask spread as a percent of mid, or None if not computable."""
    try:
        if bid is None or ask is None or not mid or float(mid) <= 0:
            return None
        return (float(ask) - float(bid)) / float(mid) * 100.0
    except (TypeError, ValueError):
        return None
