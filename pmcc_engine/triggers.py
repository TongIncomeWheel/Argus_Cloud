"""Triggers — tripwires, short roll triggers, LEAPS refresh triggers.

Pure functions. Each returns either a bool flag or a structured result;
none mutate state. Calendar checks accept date objects from the caller.

Doctrine §4 (capital efficiency), §6 (ex-div), §9 (LEAPS maintenance).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Iterable, Mapping, Optional, Sequence

from . import doctrine
from .theta_math import extrinsic


# ─── Tripwires ─────────────────────────────────────────────────────


@dataclass
class TripwireResult:
    name: str
    triggered: bool
    detail: str = ""

    def __bool__(self) -> bool:
        return self.triggered


def _business_days_between(start: date, end: date) -> int:
    """Trading-day count between two dates (inclusive of end, exclusive of start).

    Naive Mon-Fri calendar — exchange holidays are ignored. For the ex-div
    window check (within 2 TD) this is conservative-enough.
    """
    if start is None or end is None:
        return 9999
    if end < start:
        return -1
    days = 0
    cur = start
    while cur < end:
        # Advance one calendar day
        from datetime import timedelta
        cur = cur + timedelta(days=1)
        if cur.weekday() < 5:   # Mon=0, Fri=4
            days += 1
    return days


def check_upper_breach(spot: float, upper: float) -> TripwireResult:
    """SPY (or ticker) ≥ upper tripwire → evaluate OTM runner rolls."""
    triggered = spot is not None and upper is not None and spot >= upper
    return TripwireResult(
        name="Upper",
        triggered=bool(triggered),
        detail=f"spot {spot:.2f} ≥ {upper:.2f}" if triggered else f"spot {spot:.2f} < upper {upper:.2f}" if spot and upper else "n/a",
    )


def check_lower_breach(spot: float, lower: float) -> TripwireResult:
    """Spot ≤ lower tripwire → evaluate harvesting short profits."""
    triggered = spot is not None and lower is not None and spot <= lower
    return TripwireResult(
        name="Lower",
        triggered=bool(triggered),
        detail=f"spot {spot:.2f} ≤ {lower:.2f}" if triggered else f"spot {spot:.2f} > lower {lower:.2f}" if spot and lower else "n/a",
    )


def check_vix_shock(vix: float, vix_shock_level: float) -> TripwireResult:
    """VIX ≥ vix_shock → lock array, harvest IV crush."""
    triggered = vix is not None and vix_shock_level is not None and vix >= vix_shock_level
    return TripwireResult(
        name="VIX shock",
        triggered=bool(triggered),
        detail=f"VIX {vix:.2f} ≥ {vix_shock_level:.2f}" if triggered else (f"VIX {vix:.2f}" if vix else "n/a"),
    )


def check_disorderly(spot: float, vix: float, spot_floor: float, vix_floor: float) -> TripwireResult:
    """Spot AND vix both bad simultaneously → defensive protocol."""
    triggered = (
        spot is not None and vix is not None
        and spot_floor is not None and vix_floor is not None
        and spot <= spot_floor and vix > vix_floor
    )
    return TripwireResult(
        name="Disorderly",
        triggered=bool(triggered),
        detail=f"spot {spot:.2f} ≤ {spot_floor:.2f} AND VIX {vix:.2f} > {vix_floor:.2f}" if triggered else "ok",
    )


def check_dte_profit(shorts: Iterable[Mapping]) -> TripwireResult:
    """Any short with DTE ≤ 10 sitting below 50% profit capture."""
    triggered = False
    breached_legs = []
    for s in shorts:
        try:
            dte = int(s.get("dte", 99))
            if dte > 10:
                continue
            premium = float(s.get("premium_received", 0.0))
            mark = float(s.get("mark", 0.0))
            if premium <= 0:
                continue
            profit_pct = (premium - mark) / premium
            if profit_pct < 0.50:
                triggered = True
                breached_legs.append(
                    f"{s.get('label', s.get('strike'))} DTE={dte} profit={profit_pct*100:.0f}%"
                )
        except (TypeError, ValueError, ZeroDivisionError):
            continue
    return TripwireResult(
        name="DTE/profit",
        triggered=triggered,
        detail="; ".join(breached_legs) if breached_legs else "ok",
    )


def check_ex_div_window(
    shorts: Iterable[Mapping],
    ex_div_date: Optional[date],
    today: date,
    projected_dividend: float,
) -> TripwireResult:
    """Within 2 TD of ex-div, any ITM short whose extrinsic falls below 1.25 × dividend."""
    if ex_div_date is None or today is None:
        return TripwireResult(name="Ex-div", triggered=False, detail="no ex-div date configured")
    days_out = _business_days_between(today, ex_div_date)
    if days_out < 0 or days_out > doctrine.EX_DIV_WINDOW_TRADING_DAYS:
        return TripwireResult(name="Ex-div", triggered=False, detail=f"{days_out} TD to ex-div ({ex_div_date})")
    breached = []
    for s in shorts:
        try:
            spot = float(s["spot"])
            strike = float(s["strike"])
            if spot <= strike:
                continue   # OTM — no early-assignment risk
            mark = float(s["mark"])
            ext = extrinsic(mark, spot, strike, is_call=True)
            trigger = doctrine.EX_DIV_TRIGGER_MULTIPLIER * float(projected_dividend or 0.0)
            if ext < trigger:
                breached.append(
                    f"strike {strike:.2f} ext ${ext:.2f} < ${trigger:.2f} (1.25×div)"
                )
        except (KeyError, TypeError, ValueError):
            continue
    return TripwireResult(
        name="Ex-div",
        triggered=len(breached) > 0,
        detail="; ".join(breached) if breached else f"within window ({days_out} TD) — all shorts safe",
    )


def check_all_tripwires(
    spot: float,
    vix: float,
    shorts: Sequence[Mapping],
    state: Mapping,
    today: Optional[date] = None,
) -> list:
    """Run all 6 tripwire checks against current state.

    Args:
        spot: live ticker spot.
        vix: live VIX (or ticker IV30 if state['vol_axis'] != 'VIX').
        shorts: list of short legs with at minimum {strike, mark, spot, dte, premium_received}.
        state: per-ticker engine state. Expects keys:
            tripwires.upper, tripwires.lower, tripwires.vix_shock,
            tripwires.disorderly.{price,vix}, ex_div_calendar (list of {date, est_dividend}).
        today: date for ex-div check (default = today).

    Returns:
        List of TripwireResult. Truthy iff that tripwire fired.
    """
    today = today or date.today()
    trip = (state or {}).get("tripwires", {}) or {}
    cal = (state or {}).get("ex_div_calendar", []) or []
    next_div = _next_ex_div(cal, today)
    disorderly = trip.get("disorderly", {}) or {}
    return [
        check_upper_breach(spot, trip.get("upper")),
        check_lower_breach(spot, trip.get("lower")),
        check_vix_shock(vix, trip.get("vix_shock")),
        check_disorderly(spot, vix, disorderly.get("price"), disorderly.get("vix")),
        check_dte_profit(shorts),
        check_ex_div_window(
            shorts=shorts,
            ex_div_date=next_div["date"] if next_div else None,
            today=today,
            projected_dividend=next_div["est_dividend"] if next_div else 0.0,
        ),
    ]


def _next_ex_div(cal: Sequence[Mapping], today: date) -> Optional[dict]:
    """Find the next upcoming ex-div date in the calendar."""
    upcoming = []
    for entry in cal:
        d = entry.get("date")
        if isinstance(d, str):
            try:
                from datetime import datetime
                d = datetime.fromisoformat(d).date()
            except (ValueError, TypeError):
                continue
        if d and d >= today:
            upcoming.append({"date": d, "est_dividend": float(entry.get("est_dividend", 0.0))})
    upcoming.sort(key=lambda e: e["date"])
    return upcoming[0] if upcoming else None


# ─── Short-leg roll triggers ───────────────────────────────────────


@dataclass
class RollTrigger:
    triggered: bool
    reason: str
    urgency: str = "normal"   # "normal" | "high"

    def __bool__(self) -> bool:
        return self.triggered


def short_roll_trigger(leg: Mapping, spot: float) -> RollTrigger:
    """Per-leg roll trigger for a short call/put.

    Required keys on leg: mark, strike, dte, premium_received.
    Optional: is_call (default True).
    """
    try:
        mark = float(leg["mark"])
        strike = float(leg["strike"])
        dte = int(leg["dte"])
        premium = float(leg["premium_received"])
        is_call = bool(leg.get("is_call", True))
    except (KeyError, TypeError, ValueError):
        return RollTrigger(False, "leg data incomplete")

    ext = extrinsic(mark, spot, strike, is_call=is_call)
    profit_pct = (premium - mark) / premium if premium > 0 else 0.0

    if profit_pct >= 0.80:
        return RollTrigger(True, f"profit {profit_pct*100:.0f}% ≥ 80% — close", urgency="high")
    if profit_pct >= 0.50:
        return RollTrigger(True, f"profit {profit_pct*100:.0f}% ≥ 50% — harvest")
    if ext < 1.0:
        return RollTrigger(True, f"extrinsic ${ext:.2f} < $1.00", urgency="high")
    if dte <= 10 and profit_pct < 0.50:
        return RollTrigger(True, f"DTE {dte} ≤ 10 with profit {profit_pct*100:.0f}% (<50%)", urgency="high")
    return RollTrigger(False, "hold")


# ─── LEAPS refresh triggers ────────────────────────────────────────


@dataclass
class RefreshTrigger:
    triggered: bool
    reason: str
    urgency: str = "none"   # "none" | "schedule" | "evaluate" | "immediate" | "forced"

    def __bool__(self) -> bool:
        return self.triggered


def leaps_refresh_trigger(leg: Mapping) -> RefreshTrigger:
    """Per-LEAPS refresh decision. Required keys: delta, dte."""
    try:
        delta = float(leg["delta"])
        dte = int(leg["dte"])
    except (KeyError, TypeError, ValueError):
        return RefreshTrigger(False, "leg data incomplete")

    # Survival floor — overrides everything
    if dte < doctrine.LEAPS_SURVIVAL_FLOOR_DTE:
        return RefreshTrigger(True, f"DTE {dte} < {doctrine.LEAPS_SURVIVAL_FLOOR_DTE} — survival floor",
                              urgency="forced")
    # Delta drift
    if delta < doctrine.CHASSIS_DELTA_DRIFT_FLOOR:
        return RefreshTrigger(True, f"delta {delta:.2f} < {doctrine.CHASSIS_DELTA_DRIFT_FLOOR:.2f} — chassis degradation",
                              urgency="immediate")
    # Brick
    if delta >= doctrine.BRICK_DELTA_THRESHOLD:
        return RefreshTrigger(True, f"delta {delta:.2f} ≥ {doctrine.BRICK_DELTA_THRESHOLD:.2f} — brick, run extraction math",
                              urgency="evaluate")
    # Efficiency boundary
    if dte < doctrine.LEAPS_REFRESH_TARGET_DTE_MIN:
        return RefreshTrigger(True, f"DTE {dte} < {doctrine.LEAPS_REFRESH_TARGET_DTE_MIN} — efficiency trigger",
                              urgency="schedule")
    return RefreshTrigger(False, "hold")
