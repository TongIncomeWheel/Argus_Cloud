"""Posture optimization — array layout, theta/delta, capital efficiency, ex-div.

Doctrine §4 (capital efficiency), §8 (defensive flip), §13 (income optimization).
"""
from __future__ import annotations

from typing import Iterable, Mapping, Optional, Sequence

from . import doctrine
from .theta_math import theta_per_delta, theta_per_delta_rating


def array_layout(spot: float, shorts: Sequence[Mapping]) -> dict:
    """Describe the current array layout for visualization.

    Splits shorts into ITM and OTM relative to spot, returns sorted strike lists
    and the absolute and percentage distance from spot.
    """
    itm = []
    otm = []
    for s in shorts:
        try:
            strike = float(s["strike"])
        except (KeyError, TypeError, ValueError):
            continue
        delta = (strike - spot) / spot if spot else 0.0
        entry = {**dict(s), "strike": strike, "pct_from_spot": delta}
        if strike < spot:
            itm.append(entry)
        else:
            otm.append(entry)
    itm.sort(key=lambda x: x["strike"])
    otm.sort(key=lambda x: x["strike"])
    return {
        "itm": itm,
        "otm": otm,
        "itm_count": len(itm),
        "otm_count": len(otm),
        "is_2_2": len(itm) == 2 and len(otm) == 2,
        "is_3_3": len(itm) == 3 and len(otm) == 3,
        "is_all_itm": len(itm) > 0 and len(otm) == 0,
        "is_all_otm": len(itm) == 0 and len(otm) > 0,
    }


def coverage_ratios(longs: Sequence[Mapping], shorts: Sequence[Mapping]) -> dict:
    """Both coverage metrics from §4: raw contract vs chassis-only."""
    short_total = sum(int(s.get("qty", 1)) for s in shorts)
    long_total = sum(int(l.get("qty", 1)) for l in longs)
    chassis = [l for l in longs if doctrine.CHASSIS_DELTA_BASELINE_MIN
               <= float(l.get("delta", 0.0)) < doctrine.BRICK_DELTA_THRESHOLD]
    bricks = [l for l in longs if float(l.get("delta", 0.0)) >= doctrine.BRICK_DELTA_THRESHOLD]
    chassis_qty = sum(int(c.get("qty", 1)) for c in chassis)
    return {
        "long_total": long_total,
        "short_total": short_total,
        "contract_ratio_long_short": long_total / short_total if short_total > 0 else float("inf"),
        "chassis_qty": chassis_qty,
        "bricks_qty": sum(int(b.get("qty", 1)) for b in bricks),
        "chassis_ratio_long_short": chassis_qty / short_total if short_total > 0 else float("inf"),
        "naked_shorts": max(0, short_total - long_total),
    }


def evaluate_brick_extraction(brick: Mapping, replacement_delta: float = 0.80,
                              margin_rate: float = 0.065) -> dict:
    """Run the §4 extraction math on a brick LEAPS.

    A brick (delta ≥ 0.95) has near-zero extrinsic decay relative to its cost.
    Extract if:
      - implied financing cost (extrinsic decay rate) > margin rate, OR
      - 0.80Δ replacement available at materially lower capital outlay, OR
      - extracted credit funds ≥1 incremental 0.80Δ chassis.

    The function flags the conditions; the operator confirms the trade.
    """
    try:
        brick_mark = float(brick.get("mark", 0.0))
        brick_extrinsic = float(brick.get("extrinsic", 0.0))
        brick_dte = int(brick.get("dte", 365))
        brick_delta = float(brick.get("delta", 0.95))
    except (TypeError, ValueError):
        return {"extract": False, "reason": "incomplete brick data"}

    if brick_delta < doctrine.BRICK_DELTA_THRESHOLD:
        return {"extract": False, "reason": "not a brick (delta < 0.95)"}

    # Implied financing rate: extrinsic decay / (capital deployed) annualized
    # Treat the brick as locking up brick_mark × 100 of capital
    # and giving back brick_extrinsic over brick_dte
    capital_locked = brick_mark * 100.0
    if capital_locked <= 0 or brick_dte <= 0:
        return {"extract": False, "reason": "zero capital or DTE"}
    extrinsic_decay_pa = (brick_extrinsic * 100.0 / capital_locked) * (365.0 / brick_dte)

    flags = []
    if extrinsic_decay_pa > margin_rate:
        flags.append(f"financing cost {extrinsic_decay_pa*100:.1f}% > margin {margin_rate*100:.1f}%")
    flags.append(f"capital locked ${capital_locked:,.0f} (consider 0.80Δ replacement)")

    return {
        "extract": len(flags) > 0,
        "capital_locked": capital_locked,
        "extrinsic_decay_pa": extrinsic_decay_pa,
        "implied_financing_cost": extrinsic_decay_pa,
        "flags": flags,
        "reason": "; ".join(flags),
    }


def defensive_flip_compliance(shorts: Sequence[Mapping], spot: float) -> dict:
    """When the regime mandates defensive flip, all shorts must be ≤ 0.97 × spot."""
    if spot is None or spot <= 0:
        return {"compliant": False, "violators": [], "reason": "no spot"}
    threshold = spot * doctrine.DEFENSIVE_FLIP_PCT_BELOW_SPOT
    violators = []
    for s in shorts:
        try:
            strike = float(s["strike"])
            if strike > threshold:
                violators.append({"strike": strike, "label": s.get("label", "")})
        except (KeyError, TypeError, ValueError):
            continue
    return {
        "compliant": len(violators) == 0,
        "threshold": threshold,
        "violators": violators,
        "reason": "all shorts ≤ 0.97×spot" if not violators else f"{len(violators)} short(s) above defensive threshold",
    }


def dead_weight_shorts(shorts: Sequence[Mapping]) -> list:
    """List shorts where extrinsic has decayed below the dead-weight floor."""
    out = []
    for s in shorts:
        try:
            ext = float(s.get("extrinsic", 0.0))
            if ext < doctrine.ITM_DEAD_WEIGHT_EXTRINSIC_FLOOR:
                out.append({
                    "strike": float(s.get("strike", 0.0)),
                    "extrinsic": ext,
                    "label": s.get("label", ""),
                })
        except (TypeError, ValueError):
            continue
    return out


def reoptimization_check(net_theta: float, net_delta: float,
                        shorts: Sequence[Mapping], spot: float,
                        array_center_strike: Optional[float] = None) -> dict:
    """Decide whether the array needs re-centering per §13 triggers.

    Returns a dict with the failing condition (or all-clear) so the UI can
    surface what's actionable.
    """
    reasons = []

    tpd = theta_per_delta(net_theta, net_delta)
    if tpd < doctrine.THETA_PER_DELTA_ACCEPTABLE:
        reasons.append(f"theta/delta {tpd:.2f} < {doctrine.THETA_PER_DELTA_ACCEPTABLE} (suboptimal)")

    dead = dead_weight_shorts(shorts)
    if dead:
        reasons.append(f"{len(dead)} dead-weight short(s) (extrinsic < ${doctrine.ITM_DEAD_WEIGHT_EXTRINSIC_FLOOR:.2f})")

    if array_center_strike and spot:
        drift = abs(spot - array_center_strike) / array_center_strike
        if drift > doctrine.ARRAY_RECENTER_SPOT_DRIFT:
            reasons.append(
                f"spot drifted {drift*100:.1f}% from array center "
                f"({array_center_strike:.0f} → {spot:.2f})"
            )

    return {
        "reoptimize": len(reasons) > 0,
        "theta_per_delta": tpd,
        "theta_per_delta_rating": theta_per_delta_rating(tpd),
        "dead_weight_count": len(dead),
        "dead_weight_legs": dead,
        "reasons": reasons,
    }
