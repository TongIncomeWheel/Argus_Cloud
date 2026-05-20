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


def band_boundary_proximity(
    current_vol: float,
    median_vol: float,
    ivr: float,
    vol_pct_threshold: float = 0.03,
    ivr_pt_threshold: float = 5.0,
) -> dict:
    """Detect when the regime classification sits near a band boundary.

    At a boundary, a tiny data wobble (e.g. VIX 17.99 vs 18.02, or a stale vs
    live print) can flip the regime cell and change the target shape. This
    helper surfaces that fragility so the operator watches it deliberately.

    Vol-band boundaries live in ratio space at 1.0, 1.4, 2.0 × median.
    IVR-band boundaries are at 25, 50, 75.

    Returns:
        {
          vol_near (bool), vol_detail (str),
          ivr_near (bool), ivr_detail (str),
          any_near (bool),
        }
    """
    result = {
        "vol_near": False, "vol_detail": "",
        "ivr_near": False, "ivr_detail": "",
        "any_near": False,
    }

    # ── Vol axis ──────────────────────────────────────────────
    if current_vol and median_vol and median_vol > 0 and current_vol > 0:
        boundaries = [
            (doctrine.VOL_BAND_L_MAX, "L", "M"),   # ratio 1.0
            (doctrine.VOL_BAND_M_MAX, "M", "H"),   # ratio 1.4
            (doctrine.VOL_BAND_H_MAX, "H", "X"),   # ratio 2.0
        ]
        for b_ratio, lo_band, hi_band in boundaries:
            b_vol = median_vol * b_ratio
            dist = abs(current_vol - b_vol)
            dist_pct = dist / current_vol
            if dist_pct <= vol_pct_threshold:
                side = "below" if current_vol < b_vol else "at/above"
                result["vol_near"] = True
                result["vol_detail"] = (
                    f"Vol {current_vol:.2f} is {dist:.2f} ({dist_pct*100:.1f}%) {side} the "
                    f"Band {lo_band}/{hi_band} boundary ({b_vol:.2f}). "
                    f"A small move flips the band — verify the print against a live source."
                )
                break

    # ── IVR axis ──────────────────────────────────────────────
    if ivr is not None:
        ivr_boundaries = [
            (doctrine.IVR_CHEAP_MAX, "cheap", "neutral"),     # 25
            (doctrine.IVR_NEUTRAL_MAX, "neutral", "rich"),    # 50
            (doctrine.IVR_RICH_MAX, "rich", "extreme"),       # 75
        ]
        for b_ivr, lo_band, hi_band in ivr_boundaries:
            dist = abs(ivr - b_ivr)
            if dist <= ivr_pt_threshold:
                side = "below" if ivr < b_ivr else "at/above"
                result["ivr_near"] = True
                result["ivr_detail"] = (
                    f"IVR {ivr:.0f} is {dist:.0f} pt(s) {side} the "
                    f"{lo_band}/{hi_band} boundary ({b_ivr:.0f}). A small move flips the IVR band."
                )
                break

    result["any_near"] = result["vol_near"] or result["ivr_near"]
    return result
