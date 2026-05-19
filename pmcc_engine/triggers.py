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


# ─── Per-leg status commentary (advisor-style) ─────────────────────


def short_status_label(leg: Mapping, spot: float) -> dict:
    """Nuanced per-leg status — replaces the binary 'YES/hold' trigger output.

    Returns dict with:
      label    — short text (e.g. "Sit", "Approaching 50% harvest")
      tier     — "triggered" | "watch" | "ok"
      reason   — longer-form explanation

    Categories mirror what an experienced PMCC operator would write in their
    own daily review (e.g. "Paper red — SPY ran through strike",
    "Extrinsic declining. Not triggered.", "Approaching 50% harvest").
    """
    try:
        mark = float(leg.get("mark", 0.0))
        strike = float(leg.get("strike", 0.0))
        dte = int(leg.get("dte", 99))
        premium = float(leg.get("premium_received", 0.0)) or mark
        is_call = bool(leg.get("is_call", True))
    except (TypeError, ValueError):
        return {"label": "—", "tier": "ok", "reason": "leg data incomplete"}

    ext = extrinsic(mark, spot, strike, is_call=is_call)
    profit_pct = (premium - mark) / premium if premium > 0 else 0.0

    # ── Triggered (action required) ────────────────────────────
    if profit_pct >= 0.80:
        return {"label": "Close — profit ≥80%", "tier": "triggered",
                "reason": f"profit {profit_pct*100:.0f}% — close, max harvest reached"}
    if ext < 1.0:
        return {"label": "Extrinsic critical — roll", "tier": "triggered",
                "reason": f"extrinsic ${ext:.2f} < $1.00"}
    if dte <= 10 and profit_pct < 0.50:
        return {"label": "DTE ≤10, profit <50% — forced roll", "tier": "triggered",
                "reason": f"DTE {dte} with profit {profit_pct*100:.0f}%"}
    if profit_pct >= 0.50:
        return {"label": "Harvest — profit ≥50%", "tier": "triggered",
                "reason": f"profit {profit_pct*100:.0f}% ≥ 50%"}

    # ── Watch (not triggered, but worth noticing) ─────────────
    if profit_pct >= 0.25:
        return {"label": "Approaching 50% harvest", "tier": "watch",
                "reason": f"profit {profit_pct*100:.0f}%, on track for 50% harvest trigger"}
    if 1.0 <= ext < 3.0:
        return {"label": "Extrinsic declining. Not triggered.", "tier": "watch",
                "reason": f"extrinsic ${ext:.2f} — approaching $1 floor"}
    if dte <= 14:
        return {"label": "DTE approaching 10", "tier": "watch",
                "reason": f"DTE {dte} — within 4 sessions of forced-roll window"}
    # Paper-red short: mark > premium received AND strike is near/inside spot
    if profit_pct < 0 and spot > 0 and strike <= spot * 1.02:
        return {"label": "Paper red — SPY ran through strike", "tier": "watch",
                "reason": f"loss {profit_pct*100:.0f}%, strike threatened by spot"}

    return {"label": "Sit", "tier": "ok", "reason": "no trigger near"}


def leaps_status_label(leg: Mapping) -> dict:
    """LEAPS status — usually 'Sit' unless a §9 refresh trigger fires."""
    rt = leaps_refresh_trigger(leg)
    if not rt.triggered:
        return {"label": "Sit", "tier": "ok", "reason": "chassis healthy"}
    urgency_to_tier = {"forced": "triggered", "immediate": "triggered",
                       "evaluate": "watch", "schedule": "watch"}
    return {"label": rt.reason, "tier": urgency_to_tier.get(rt.urgency, "watch"),
            "reason": rt.reason}


# ─── Items on Watch (advisor-style proactive surfacing) ────────────


def _est_extrinsic_floor_date(current_ext: float, theta_per_day: float, today: date) -> str:
    """Linear estimate: how many sessions until extrinsic hits ~$1?"""
    from datetime import timedelta
    if theta_per_day is None or theta_per_day <= 0 or current_ext <= 1.0:
        return "—"
    sessions = max(1, int((current_ext - 1.0) / theta_per_day))
    # Approximate to calendar days × 7/5
    est_date = today + timedelta(days=int(sessions * 7 / 5))
    return f"~{est_date.strftime('%b %-d')} if SPY stays flat"


def _est_mark_target_date(current_mark: float, target_mark: float,
                          theta_per_day: float, today: date) -> str:
    """Estimate when mark drops to target via theta decay."""
    from datetime import timedelta
    if theta_per_day is None or theta_per_day <= 0 or current_mark <= target_mark:
        return "—"
    sessions = max(1, int((current_mark - target_mark) / theta_per_day))
    est_date = today + timedelta(days=int(sessions * 7 / 5))
    return f"~{est_date.strftime('%b %-d')} if theta works"


def items_on_watch(
    shorts: Sequence[Mapping],
    state: Mapping,
    today: Optional[date] = None,
) -> list:
    """Surface non-triggered-but-close items. Advisor-style proactive section.

    Returns a list of dicts with: item, current, trigger, est, leg_label.
    Sorted by estimated fire date (soonest first).
    """
    today = today or date.today()
    items = []

    for s in shorts:
        try:
            strike = float(s.get("strike", 0.0))
            ext = float(s.get("extrinsic", 0.0))
            mark = float(s.get("mark", 0.0))
            premium = float(s.get("premium_received", 0.0)) or mark
            theta = abs(float(s.get("theta_per_day", 0.0)))
            dte = int(s.get("dte", 99))
            label = s.get("label") or f"${strike:.0f}"
        except (TypeError, ValueError):
            continue

        profit_pct = (premium - mark) / premium if premium > 0 else 0.0

        # Extrinsic approaching $1 floor
        if 1.0 <= ext < 5.0:
            items.append({
                "item": f"{label} extrinsic",
                "current": f"${ext:.2f}",
                "trigger": "< $1.00",
                "est": _est_extrinsic_floor_date(ext, theta, today),
                "_priority": 1 if ext < 3.0 else 2,
            })

        # Profit approaching 50% harvest
        if 0.25 <= profit_pct < 0.50:
            target_mark = premium * 0.50
            items.append({
                "item": f"{label} profit",
                "current": f"{profit_pct*100:.0f}%",
                "trigger": f"≥50% (mark drops to ${target_mark:.2f})",
                "est": _est_mark_target_date(mark, target_mark, theta, today),
                "_priority": 2 if profit_pct >= 0.35 else 3,
            })

        # DTE approaching 10
        if 11 <= dte <= 14:
            items.append({
                "item": f"{label} DTE",
                "current": f"{dte}",
                "trigger": "≤10 with profit <50%",
                "est": f"~{dte - 10} sessions",
                "_priority": 2,
            })

    # Ex-div approaching
    cal = (state or {}).get("ex_div_calendar", []) or []
    next_div = _next_ex_div(cal, today)
    if next_div:
        days_to = _business_days_between(today, next_div["date"])
        if 3 <= days_to <= 30:
            items.append({
                "item": f"Ex-div {next_div['date']}",
                "current": f"T-{days_to}",
                "trigger": f"T-2 with ITM ext < 1.25×div (${doctrine.EX_DIV_TRIGGER_MULTIPLIER * float(next_div['est_dividend']):.2f})",
                "est": "Screen at T-5",
                "_priority": 1 if days_to <= 7 else 3,
            })

    items.sort(key=lambda x: x.get("_priority", 99))
    for i in items:
        i.pop("_priority", None)
    return items[:8]
