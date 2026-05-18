"""Strike candidate filters — ITM and OTM zones around spot.

Doctrine §3 (Strike Selection) and §13 (3% band as starting point).
The scorecard in §12 is run on top candidates by the caller.
"""
from __future__ import annotations

from typing import Iterable, Mapping, Optional, Sequence

from . import doctrine
from .theta_math import extrinsic, theta_hurdle


def _chain_row_get(row: Mapping, *keys, default=None):
    """Resolve a chain row across alternate key spellings (Alpaca / Tiger / custom)."""
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return default


def itm_candidates(
    spot: float,
    chain: Iterable[Mapping],
    hv30: float,
    target_pct: float = doctrine.STRIKE_TARGET_PCT_BELOW_SPOT,
    band_min: float = doctrine.STRIKE_BAND_MIN,
    band_max: float = doctrine.STRIKE_BAND_MAX,
) -> list:
    """Return ITM call candidates within the 1–5% below-spot band.

    Each row of `chain` should expose at minimum: strike, mid (or price/last/mark),
    theta, dte. Optional: open_interest, bid, ask, delta.

    Candidates are sorted by closeness to the target_pct band center.
    Caller is expected to feed only call-side chain rows.
    """
    if spot <= 0:
        return []
    out = []
    for c in chain:
        try:
            strike = float(_chain_row_get(c, "strike"))
            if strike <= 0:
                continue
            pct_below = (spot - strike) / spot
            if not (band_min <= pct_below <= band_max):
                continue
            mid = float(_chain_row_get(c, "mid", "price", "last", "mark", default=0.0) or 0.0)
            theta = abs(float(_chain_row_get(c, "theta", default=0.0) or 0.0))
            ext = extrinsic(mid, spot, strike, is_call=True)
            hurdle = theta_hurdle(strike, hv30)
            out.append({
                **dict(c),
                "strike": strike,
                "mid": mid,
                "theta": theta,
                "extrinsic": ext,
                "hurdle": hurdle,
                "hurdle_pass": theta >= hurdle,
                "tv_pass": ext >= doctrine.MIN_TIME_VALUE_FLOOR,
                "pct_below_spot": pct_below,
                "side": "ITM",
            })
        except (KeyError, TypeError, ValueError):
            continue
    out.sort(key=lambda x: abs(x["pct_below_spot"] - target_pct))
    return out


def otm_candidates(
    spot: float,
    chain: Iterable[Mapping],
    hv30: float,
    band_min: float = doctrine.STRIKE_BAND_MIN,
    band_max: float = doctrine.STRIKE_BAND_MAX,
) -> list:
    """OTM call candidates within the 1–5% above-spot band.

    For OTM calls, mid IS the extrinsic — there's no intrinsic to subtract.
    """
    if spot <= 0:
        return []
    out = []
    for c in chain:
        try:
            strike = float(_chain_row_get(c, "strike"))
            if strike <= 0:
                continue
            pct_above = (strike - spot) / spot
            if not (band_min <= pct_above <= band_max):
                continue
            mid = float(_chain_row_get(c, "mid", "price", "last", "mark", default=0.0) or 0.0)
            theta = abs(float(_chain_row_get(c, "theta", default=0.0) or 0.0))
            hurdle = theta_hurdle(strike, hv30)
            out.append({
                **dict(c),
                "strike": strike,
                "mid": mid,
                "theta": theta,
                "extrinsic": mid,    # all extrinsic for OTM
                "hurdle": hurdle,
                "hurdle_pass": theta >= hurdle,
                "tv_pass": mid >= doctrine.MIN_TIME_VALUE_FLOOR,
                "pct_above_spot": pct_above,
                "side": "OTM",
            })
        except (KeyError, TypeError, ValueError):
            continue
    out.sort(key=lambda x: x["pct_above_spot"])
    return out


def dte_in_band(dte: int, min_weeks: int, max_weeks: int) -> bool:
    """True if DTE falls within (min_weeks*7, max_weeks*7) inclusive."""
    if dte is None:
        return False
    return min_weeks * 7 <= int(dte) <= max_weeks * 7


def liquidity_ok(row: Mapping) -> bool:
    """Open interest and bid/ask spread pass §3 floors."""
    oi = float(_chain_row_get(row, "open_interest", "oi", default=0) or 0)
    if oi < doctrine.MIN_OPEN_INTEREST:
        return False
    bid = _chain_row_get(row, "bid")
    ask = _chain_row_get(row, "ask")
    mid = _chain_row_get(row, "mid", default=None)
    try:
        if bid is None or ask is None or mid in (None, 0):
            return True   # if we don't have b/a, don't reject (data gap, not a real fail)
        spread = float(ask) - float(bid)
        if float(mid) <= 0:
            return True
        return (spread / float(mid)) <= doctrine.MAX_BID_ASK_SPREAD_PCT
    except (TypeError, ValueError):
        return True


def filter_by_doctrine(candidates: Sequence[Mapping], regime_cell: Mapping) -> list:
    """Apply doctrine-level rejects: TV floor, OI floor, spread, regime DTE band.

    Caller should already have filtered chain to call options; this adds the
    quality filters that come from doctrine §3 + §11.
    """
    dte_window = (regime_cell or {}).get("dte_weeks")
    out = []
    for c in candidates:
        if not c.get("tv_pass", True):
            continue
        if not liquidity_ok(c):
            continue
        if dte_window:
            try:
                dte = int(c.get("dte", -1))
            except (TypeError, ValueError):
                continue
            if not dte_in_band(dte, dte_window[0], dte_window[1]):
                continue
        out.append(c)
    return out


def split_chain_calls_puts(chain: Iterable[Mapping]) -> tuple:
    """Partition a chain into (calls, puts) for downstream filtering.

    Recognizes 'put_call', 'right', 'type' keys with values C/CALL or P/PUT.
    """
    calls = []
    puts = []
    for row in chain:
        right = _chain_row_get(row, "put_call", "right", "type", default="C")
        if str(right).upper().startswith("C"):
            calls.append(row)
        else:
            puts.append(row)
    return calls, puts
