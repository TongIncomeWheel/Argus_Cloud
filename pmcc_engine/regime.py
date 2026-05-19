"""Regime classifier — vol band × IVR → posture.

Doctrine §1: state the regime cell explicitly on every review. This module is
the single source of truth for that classification.
"""
from __future__ import annotations

from typing import Optional, Sequence

from . import doctrine


def vol_band(current_vol: float, median_vol: float) -> str:
    """Map current vol to one of {L, M, H, X} relative to ticker's median.

    Args:
        current_vol: VIX for SPY/QQQ/IWM, ticker IV30 (or HV30 fallback) for stocks.
        median_vol:  5-year median for that ticker (state).

    Returns:
        'L' (<1×), 'M' (1.0–1.4×), 'H' (1.4–2.0×), 'X' (>2.0×).
    """
    if median_vol is None or median_vol <= 0:
        return "M"   # default to base case if we have no anchor
    if current_vol is None or current_vol <= 0:
        return "M"
    ratio = float(current_vol) / float(median_vol)
    if ratio < doctrine.VOL_BAND_L_MAX:
        return "L"
    if ratio < doctrine.VOL_BAND_M_MAX:
        return "M"
    if ratio < doctrine.VOL_BAND_H_MAX:
        return "H"
    return "X"


def ivr_band(ivr: float) -> str:
    """Map IVR (0–100) to {cheap, neutral, rich, extreme}."""
    if ivr is None:
        return "neutral"
    if ivr < doctrine.IVR_CHEAP_MAX:
        return "cheap"
    if ivr < doctrine.IVR_NEUTRAL_MAX:
        return "neutral"
    if ivr < doctrine.IVR_RICH_MAX:
        return "rich"
    return "extreme"


def compute_ivr_52w(current_iv: float, iv_history: Sequence[float]) -> Optional[float]:
    """52-week IV Rank: 0 = at 52w low, 100 = at 52w high.

    Args:
        current_iv: current IV (or RV30 proxy) for the ticker.
        iv_history: trailing 52w of IV/RV30 observations.

    Returns:
        IVR in [0, 100], or None if not enough history.
    """
    series = [float(x) for x in iv_history if x is not None]
    if len(series) < 30 or current_iv is None:
        return None
    hi = max(series)
    lo = min(series)
    if hi == lo:
        return 50.0
    return max(0.0, min(100.0, (float(current_iv) - lo) / (hi - lo) * 100.0))


def regime_cell(current_vol: float, median_vol: float,
                ivr: float) -> dict:
    """Look up the (vol_band, ivr_band) cell in the regime grid.

    Returns a dict with keys:
        vol_band, ivr_band, posture, dte_weeks, shape, description, cell_label.

    `shape` is the LEAN direction the regime calls for (centered / lean_itm /
    lean_otm / all_itm / all_otm / all_otm_half / stand_down). The count of
    shorts is operator-determined — typically = number of LEAPS to maintain
    100% PMCC coverage.

    The legacy key `array` is kept as an alias of `shape` for backwards-compat.
    """
    vb = vol_band(current_vol, median_vol)
    ib = ivr_band(ivr)
    grid_entry = doctrine.REGIME_GRID.get((vb, ib), {})
    posture = grid_entry.get("posture")
    shape = grid_entry.get("shape")
    return {
        "vol_band": vb,
        "ivr_band": ib,
        "cell_label": f"Band {vb} × IVR {ib}",
        "posture": posture,
        "dte_weeks": grid_entry.get("dte_weeks"),
        "shape": shape,
        "array": shape,   # legacy alias
        "description": doctrine.POSTURE_DESCRIPTIONS.get(posture, ""),
        "ivr": float(ivr) if ivr is not None else None,
        "current_vol": float(current_vol) if current_vol else None,
        "median_vol": float(median_vol) if median_vol else None,
    }


def is_base_case(cell: dict) -> bool:
    """True if the cell is the doctrine 'base case' (Band M × IVR neutral)."""
    return cell.get("vol_band") == "M" and cell.get("ivr_band") == "neutral"


def is_stand_down(cell: dict) -> bool:
    """True if the regime calls for standing down (no new short deployment)."""
    return cell.get("posture", "").startswith("stand_down")
