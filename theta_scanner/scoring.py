"""CSP candidate scoring — pure functions, no I/O, no Streamlit.

A cash-secured put ties up `strike × 100` in collateral and collects
`premium × 100`. The scanner scores each candidate on four axes the operator
named — yield, distance, delta — and blends them into a 0-100 composite.
(Yield captures both 'juicy premium' and 'RoR' — for a CSP they are the same
dimension: premium relative to collateral.)
"""
from __future__ import annotations

from typing import Mapping, Optional

# ─── Composite-score scaling targets ───────────────────────────────
# Each axis is scaled to 0-100, then blended by the weights below.
ANN_ROR_FULL_MARKS = 0.35      # 35%+ annualized return on collateral → 100 on the yield axis
DISTANCE_FULL_MARKS = 0.10     # 10%+ OTM → 100 on the safety axis
DELTA_TARGET = 0.25            # delta sweet spot for a CSP
DELTA_BANDWIDTH = 0.25         # delta score decays to 0 at |delta| 0.00 or 0.50

W_YIELD = 0.40
W_DISTANCE = 0.30
W_DELTA = 0.30

# ─── Liquidity floors ──────────────────────────────────────────────
MIN_OPEN_INTEREST = 100
MAX_SPREAD_PCT = 0.08          # bid/ask spread ≤ 8% of mid

# ─── Verdict cutoffs (composite 0-100) ─────────────────────────────
VERDICT_STRONG = 75.0
VERDICT_GOOD = 60.0
VERDICT_MARGINAL = 45.0


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


# ─── Core CSP math ─────────────────────────────────────────────────


def csp_collateral(strike: float) -> float:
    """Cash secured per contract = strike × 100."""
    return float(strike) * 100.0


def csp_ror(premium: float, strike: float) -> float:
    """Single-cycle return on collateral (decimal). premium/strike."""
    if strike <= 0:
        return 0.0
    return float(premium) / float(strike)


def csp_annualized_ror(premium: float, strike: float, dte: float) -> float:
    """Annualized return on collateral (decimal)."""
    if strike <= 0 or dte is None or dte <= 0:
        return 0.0
    return (float(premium) / float(strike)) * (365.0 / float(dte))


def distance_to_spot_pct(spot: float, strike: float) -> float:
    """Fraction the strike sits below spot. Positive = OTM put (the safe side)."""
    if spot is None or spot <= 0:
        return 0.0
    return (float(spot) - float(strike)) / float(spot)


def pop_from_delta(delta_abs: float) -> float:
    """Probability of profit for a short option ≈ 1 − |delta|."""
    if delta_abs is None:
        return 0.0
    return _clamp(1.0 - abs(float(delta_abs)))


def breakeven(strike: float, premium: float) -> float:
    """Effective cost basis if assigned: strike − premium."""
    return float(strike) - float(premium)


# ─── Per-axis scores (0-100) ───────────────────────────────────────


def yield_score(annualized_ror: float) -> float:
    return _clamp(annualized_ror / ANN_ROR_FULL_MARKS) * 100.0


def distance_score(distance_pct: float) -> float:
    return _clamp(distance_pct / DISTANCE_FULL_MARKS) * 100.0


def delta_score(delta_abs: float) -> float:
    """Tent function — peaks at DELTA_TARGET, 0 at the band edges.

    A CSP with delta near 0 has no premium; near 0.50 is a coin-flip on
    assignment. The sweet spot is the middle.
    """
    if delta_abs is None:
        return 0.0
    return _clamp(1.0 - abs(abs(float(delta_abs)) - DELTA_TARGET) / DELTA_BANDWIDTH) * 100.0


def composite_score(annualized_ror: float, distance_pct: float, delta_abs: float,
                     w_yield: float = W_YIELD, w_distance: float = W_DISTANCE,
                     w_delta: float = W_DELTA) -> float:
    """Weighted 0-100 blend of the three scoring axes."""
    return (
        w_yield * yield_score(annualized_ror)
        + w_distance * distance_score(distance_pct)
        + w_delta * delta_score(delta_abs)
    )


def verdict(composite: float) -> str:
    """Map a composite score to a verdict label."""
    if composite >= VERDICT_STRONG:
        return "Strong"
    if composite >= VERDICT_GOOD:
        return "Good"
    if composite >= VERDICT_MARGINAL:
        return "Marginal"
    return "Weak"


def liquidity_ok(open_interest: Optional[float], bid: Optional[float],
                 ask: Optional[float], mid: Optional[float]) -> bool:
    """Open-interest floor + bid/ask spread ceiling.

    Missing bid/ask is not treated as a fail (data gap, not a real defect) —
    only an explicitly-wide spread rejects.
    """
    try:
        if open_interest is not None and float(open_interest) < MIN_OPEN_INTEREST:
            return False
    except (TypeError, ValueError):
        pass
    try:
        if bid is None or ask is None or mid in (None, 0):
            return True
        if float(mid) <= 0:
            return True
        return (float(ask) - float(bid)) / float(mid) <= MAX_SPREAD_PCT
    except (TypeError, ValueError):
        return True


def score_csp_candidate(spot: float, row: Mapping) -> dict:
    """Score one put-chain row as a CSP candidate.

    `row` needs: strike, mid (or bid/ask), dte, delta. Optional: bid, ask,
    open_interest, iv, expiry, symbol.

    Returns a dict with every metric the UI shows plus the composite + verdict.
    """
    strike = float(row.get("strike", 0) or 0)
    mid = row.get("mid")
    bid = row.get("bid")
    ask = row.get("ask")
    if mid is None and bid is not None and ask is not None:
        mid = (float(bid) + float(ask)) / 2.0
    mid = float(mid or 0.0)
    dte = row.get("dte")
    delta_raw = row.get("delta")
    delta_abs = abs(float(delta_raw)) if delta_raw is not None else None
    oi = row.get("open_interest")

    ror = csp_ror(mid, strike)
    ann_ror = csp_annualized_ror(mid, strike, dte)
    dist = distance_to_spot_pct(spot, strike)
    pop = pop_from_delta(delta_abs) if delta_abs is not None else None
    comp = composite_score(ann_ror, dist, delta_abs if delta_abs is not None else 0.0)
    liq = liquidity_ok(oi, bid, ask, mid)

    return {
        "symbol": row.get("symbol"),
        "expiry": row.get("expiry"),
        "strike": strike,
        "dte": dte,
        "premium": mid,
        "bid": bid,
        "ask": ask,
        "collateral": csp_collateral(strike),
        "ror_pct": ror * 100.0,
        "annualized_ror_pct": ann_ror * 100.0,
        "distance_pct": dist * 100.0,
        "delta": delta_abs,
        "pop_pct": pop * 100.0 if pop is not None else None,
        "breakeven": breakeven(strike, mid),
        "open_interest": oi,
        "iv": row.get("iv"),
        "yield_score": yield_score(ann_ror),
        "distance_score": distance_score(dist),
        "delta_score": delta_score(delta_abs if delta_abs is not None else 0.0),
        "composite": comp,
        "verdict": verdict(comp),
        "liquidity_ok": liq,
    }
