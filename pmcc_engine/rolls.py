"""Roll mechanics — decomposition + stagger rules.

Doctrine §5: every roll proposal is reported with intrinsic uncapped, extrinsic
captured, theta runway gained, and the Greek transfer block. Rejected unless at
least two of {intrinsic uncap, theta gained, gamma reduced} are positive.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Sequence

from . import doctrine
from .theta_math import extrinsic


@dataclass
class RollDecomposition:
    old_leg: dict
    new_leg: dict
    spot: float

    # Cash flow
    btc_cost: float = 0.0           # premium paid to close (debit)
    sto_received: float = 0.0       # premium received to open (credit)
    net_cash: float = 0.0           # positive = credit, negative = debit

    # Value transfer
    intrinsic_uncapped: float = 0.0  # change in intrinsic at the new strike vs old (capped at 0)
    extrinsic_captured: float = 0.0  # new extrinsic - old extrinsic
    theta_runway_gained: float = 0.0 # daily theta delta × dte_gained

    # Greek transfer
    delta_change: float = 0.0
    gamma_change: float = 0.0
    vega_change: float = 0.0
    theta_change: float = 0.0       # $/day delta

    # Derived structure
    strike_lift: float = 0.0
    dte_gained: int = 0
    expectancy: float = 0.0

    # Verdict
    positive_metrics: list = field(default_factory=list)
    verdict: str = "fail"           # "pass" | "fail" | "conditional"
    rejection_reason: str = ""

    def as_dict(self) -> dict:
        d = self.__dict__.copy()
        d["old_leg"] = dict(self.old_leg)
        d["new_leg"] = dict(self.new_leg)
        return d


def roll_decomposition(old_leg: Mapping, new_leg: Mapping, spot: float,
                       is_call: bool = True) -> RollDecomposition:
    """Compute the §5 roll decomposition.

    Leg dicts require keys: mark, strike, dte. Optional: delta, gamma, vega, theta.
    All Greeks are PER-SHARE; the function returns delta in shares-equivalent.
    """
    decomp = RollDecomposition(old_leg=dict(old_leg), new_leg=dict(new_leg), spot=spot)

    old_mark = float(old_leg.get("mark", 0.0))
    new_mark = float(new_leg.get("mark", 0.0))
    old_strike = float(old_leg.get("strike", 0.0))
    new_strike = float(new_leg.get("strike", 0.0))
    old_dte = int(old_leg.get("dte", 0))
    new_dte = int(new_leg.get("dte", 0))

    decomp.btc_cost = old_mark * 100.0
    decomp.sto_received = new_mark * 100.0
    decomp.net_cash = (new_mark - old_mark) * 100.0   # +credit / -debit

    old_ext = extrinsic(old_mark, spot, old_strike, is_call=is_call)
    new_ext = extrinsic(new_mark, spot, new_strike, is_call=is_call)
    decomp.extrinsic_captured = (new_ext - old_ext) * 100.0

    # Intrinsic uncap: how much directional headroom the new strike gives back.
    # For a covered call roll-up: strike lift adds intrinsic back to the long.
    decomp.strike_lift = new_strike - old_strike
    if is_call:
        decomp.intrinsic_uncapped = decomp.strike_lift * 100.0
    else:
        decomp.intrinsic_uncapped = -decomp.strike_lift * 100.0

    decomp.dte_gained = new_dte - old_dte

    old_theta = abs(float(old_leg.get("theta", 0.0)))
    new_theta = abs(float(new_leg.get("theta", 0.0)))
    theta_delta_per_share = new_theta - old_theta
    decomp.theta_change = theta_delta_per_share * 100.0
    decomp.theta_runway_gained = theta_delta_per_share * 100.0 * max(decomp.dte_gained, 0)

    decomp.delta_change = (float(new_leg.get("delta", 0.0)) - float(old_leg.get("delta", 0.0))) * 100.0
    decomp.gamma_change = (float(new_leg.get("gamma", 0.0)) - float(old_leg.get("gamma", 0.0))) * 100.0
    decomp.vega_change = (float(new_leg.get("vega", 0.0)) - float(old_leg.get("vega", 0.0))) * 100.0

    # Doctrine expectancy formula
    gamma_cost = max(0.0, decomp.gamma_change)  # rising gamma is cost to the short
    decomp.expectancy = (
        decomp.intrinsic_uncapped
        + decomp.theta_runway_gained
        - max(0.0, -decomp.net_cash)              # debit reduces expectancy
        - gamma_cost
    )

    # Verdict — need ≥2 positives from {intrinsic uncap, theta gained, gamma reduced}
    positives = []
    if decomp.intrinsic_uncapped > 0:
        positives.append("intrinsic_uncap")
    if decomp.theta_runway_gained > 0:
        positives.append("theta_gained")
    if decomp.gamma_change < 0:
        positives.append("gamma_reduced")
    decomp.positive_metrics = positives

    if len(positives) >= 2 and decomp.expectancy > 0:
        decomp.verdict = "pass"
    elif len(positives) >= 2:
        decomp.verdict = "conditional"
        decomp.rejection_reason = "expectancy not positive despite 2+ favorable metrics"
    else:
        decomp.verdict = "fail"
        missing = [m for m in ("intrinsic_uncap", "theta_gained", "gamma_reduced") if m not in positives]
        decomp.rejection_reason = f"only {len(positives)} positive metric(s); missing: {', '.join(missing)}"

    return decomp


def check_stagger(shorts: Iterable[Mapping]) -> dict:
    """Stagger rule check: no 2+ shorts share the same expiry.

    Returns:
        {ok: bool, violations: [(expiry, count)], expiries: Counter}.
    """
    expiries = []
    for s in shorts:
        e = s.get("expiry") or s.get("expiry_date") or s.get("Expiry_Date")
        if e is None:
            continue
        expiries.append(str(e))
    counts = Counter(expiries)
    violations = [(e, c) for e, c in counts.items() if c >= 2]
    return {"ok": len(violations) == 0, "violations": violations, "expiries": counts}


def estimated_roll_cost_rally(shorts: Sequence[Mapping], spot: float,
                              rally_amount: float) -> float:
    """Rough cost-to-roll all shorts under a spot rally scenario.

    For each short, approximate the new mark = intrinsic + 30% of current extrinsic,
    buy-to-close at that price, sell new short at spot×0.97 strike. Sum the debits.

    Matches the handoff spec's `estimated_roll_cost_rally` formulation.
    """
    total = 0.0
    if spot is None or spot <= 0:
        return 0.0
    new_spot = spot + rally_amount
    for s in shorts:
        try:
            strike = float(s["strike"])
            current_ext = float(s.get("extrinsic", 0.0))
            new_intrinsic = max(0.0, new_spot - strike)
            estimated_mark = new_intrinsic + max(0.50, current_ext * 0.30)
            btc_cost = estimated_mark * 100.0
            new_strike = new_spot * 0.97
            new_sto_extrinsic_est = new_spot * 0.03 * 0.40   # rough extrinsic estimate
            sto_received = (max(0.0, new_spot - new_strike) + new_sto_extrinsic_est) * 100.0
            roll_debit = btc_cost - sto_received
            total += max(0.0, roll_debit)
        except (KeyError, TypeError, ValueError):
            continue
    return total
