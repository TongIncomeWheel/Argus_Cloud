"""Per-ticker engine state — load/save against ARGUS settings.

Doctrine §1 says calibration is per-ticker (vol_median, ex-div calendar,
tripwires). The engine math is universal. We store state inside the existing
ARGUS settings blob under the key:

    settings["pmcc_engine_state"][TICKER] = {
        "vol_median_5yr": float,
        "vol_axis": "VIX" | "IV30",
        "quarterly_dividend": float,
        "tripwires": {
            "upper": float,
            "lower": float,
            "vix_shock": float,
            "disorderly": {"price": float, "vix": float},
        },
        "ex_div_calendar": [{"date": "YYYY-MM-DD", "est_dividend": float}, ...],
        "array_center_strike": float | None,
        "notes": str,
    }

This module exposes pure accessors + merge helpers. Persistence is delegated
back to ARGUS' existing settings save flow.
"""
from __future__ import annotations

from datetime import date
from typing import Dict, Optional

from . import doctrine


STATE_KEY = "pmcc_engine_state"


def get_ticker_state(settings: Dict, ticker: str) -> Dict:
    """Return the engine state dict for `ticker`, merging defaults for missing fields.

    User-stored values always win when present. The default seed in
    doctrine.DEFAULT_TICKER_STATE fills any gaps — including the ex-dividend
    calendar, so a fresh install of ARGUS on SPY/QQQ/IWM gets known ex-div
    dates without manual entry. Edits in the State Editor overwrite the seed.
    """
    ticker = ticker.upper()
    all_state = (settings or {}).get(STATE_KEY, {}) or {}
    user_state = all_state.get(ticker, {}) or {}
    seed = doctrine.DEFAULT_TICKER_STATE.get(ticker, {})

    # ex_div_calendar: user wins if they've explicitly stored any entries.
    # Empty list from user is interpreted as "user has not configured" → use seed.
    user_cal = user_state.get("ex_div_calendar")
    if user_cal:
        ex_div_calendar = user_cal
    else:
        ex_div_calendar = list(seed.get("ex_div_calendar", []))

    merged = {
        "vol_median_5yr": user_state.get("vol_median_5yr", seed.get("vol_median_5yr", 20.0)),
        "vol_axis": user_state.get("vol_axis", seed.get("vol_axis", "IV30")),
        "quarterly_dividend": user_state.get("quarterly_dividend", seed.get("quarterly_dividend", 0.0)),
        "tripwires": user_state.get("tripwires", {}) or {},
        "ex_div_calendar": ex_div_calendar,
        "array_center_strike": user_state.get("array_center_strike"),
        "notes": user_state.get("notes", ""),
    }
    return merged


def upsert_ticker_state(settings: Dict, ticker: str, patch: Dict) -> Dict:
    """Merge `patch` into the ticker state and return the updated settings dict.

    The settings dict is mutated in place AND returned for chain-friendliness.
    Caller is responsible for persisting via ARGUS' save_settings().
    """
    ticker = ticker.upper()
    if STATE_KEY not in settings or not isinstance(settings.get(STATE_KEY), dict):
        settings[STATE_KEY] = {}
    current = settings[STATE_KEY].get(ticker, {}) or {}
    current.update(patch or {})
    settings[STATE_KEY][ticker] = current
    return settings


def list_configured_tickers(settings: Dict) -> list:
    """All tickers that have engine state configured. Sorted."""
    return sorted((settings or {}).get(STATE_KEY, {}).keys())


def suggest_tripwires(spot: float, shorts: list, vix_shock: float = 24.5) -> dict:
    """Suggest tripwires from current array + a hard-coded vix shock floor.

    Logic from app handoff §4.4 — picks lower = min ITM strike, upper = above
    highest OTM strike. Caller can override.
    """
    if not shorts:
        return {
            "upper": round(spot * 1.05, 2) if spot else None,
            "lower": round(spot * 0.93, 2) if spot else None,
            "vix_shock": vix_shock,
            "disorderly": {"price": round(spot * 0.93, 2) if spot else None, "vix": 22.0},
        }
    itm = [float(s["strike"]) for s in shorts if "strike" in s and float(s["strike"]) < spot]
    otm = [float(s["strike"]) for s in shorts if "strike" in s and float(s["strike"]) >= spot]
    lower = min(itm) if itm else spot * 0.95
    upper = max(otm) - 5 if otm else spot * 1.02
    return {
        "upper": round(upper, 2),
        "lower": round(lower, 2),
        "vix_shock": vix_shock,
        "disorderly": {"price": round(min(spot * 0.93, lower), 2), "vix": 22.0},
    }


def add_ex_div_entry(settings: Dict, ticker: str, ex_div: date,
                     est_dividend: float) -> Dict:
    """Append an ex-div date to the ticker's calendar (idempotent on date)."""
    state = get_ticker_state(settings, ticker)
    cal = list(state.get("ex_div_calendar", []) or [])
    iso = ex_div.isoformat() if isinstance(ex_div, date) else str(ex_div)
    if any(e.get("date") == iso for e in cal):
        # Update existing
        for e in cal:
            if e.get("date") == iso:
                e["est_dividend"] = float(est_dividend)
    else:
        cal.append({"date": iso, "est_dividend": float(est_dividend)})
    cal.sort(key=lambda e: e["date"])
    return upsert_ticker_state(settings, ticker, {"ex_div_calendar": cal})
