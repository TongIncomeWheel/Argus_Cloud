"""Roll-candidate engine — structural anchor finder + strike placement.

Single source of truth for the "where should the new short live" question.
All logic is PURE (no I/O, no Tiger client, no MCP context) so the engine
is unit-testable and the MCP server can wrap it without leaking concerns.

Design split:
  • Pure compute (this module): given daily OHLC bars + spot + side,
    return a structural anchor, an ATR-derived buffer, and a target
    strike. Pure functions — easy to test with stub bar data.
  • I/O wrappers (`fetch_fmp_bars` here, MCP tool in server.py): pull
    the bars from FMP REST, then call the pure functions.

Decision hierarchy for the structural anchor (per the v3 build spec):

  PUTS (support below spot)
    1. Most recent swing low in last 20 bars, must be below spot
       — confidence: HIGH
    2. Round number ($5 or $10 level) below spot, only if no swing low
       within 15% of spot — confidence: MEDIUM
    3. 50-day or 200-day MA confluence (spot within 3% of MA)
       — confidence: HIGH if within 1%, else MEDIUM
    4. Consolidation base (3+ consecutive bars in a 2% range below spot)
       — confidence: MEDIUM

  CALLS (resistance above spot)
    Mirror image — swing high / round number above spot / MA / distribution
    zone. Cost-basis constraint applies for Core Pot calls: chosen anchor
    MUST be above cost_basis; otherwise pick the next valid one.

  No anchor found within 20% of spot → `anchor_type="none"`, the MCP
  tool flags degraded selection to the operator.

ATR-14 buffer sizing (from the bracket skill, Step 3B):
    atr/spot < 1.5%  → 0.3% buffer
    atr/spot < 3.0%  → 0.5% buffer
    otherwise        → 0.7% buffer

Strike placement:
    For puts:  target = anchor × (1 − buffer);   pick highest strike ≤ target
    For calls: target = anchor × (1 + buffer);   pick lowest  strike ≥ target

The roll math (net credit/debit, mid, extrinsic) lives in pure helpers
at the bottom so the MCP tool can compose them with Tiger chain data.
"""
from __future__ import annotations

import json
import logging
import math
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, asdict
from datetime import date, datetime
from typing import Iterable, List, Optional

logger = logging.getLogger("tiger-mcp.roll_engine")


# ── Data types ───────────────────────────────────────────────────────────────


@dataclass
class Bar:
    """Single daily OHLC bar. Date is ISO YYYY-MM-DD."""
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class Anchor:
    """Output of the structural anchor finder."""
    anchor_price: Optional[float]
    anchor_type: str   # "swing_low" | "swing_high" | "round_number" | "ma_50" | "ma_200" | "consolidation" | "distribution_zone" | "none"
    anchor_confidence: str  # "high" | "medium" | "low" | "none"
    atr_14: Optional[float]
    buffer_pct: Optional[float]
    target_strike_price: Optional[float]
    source: str
    note: str = ""


# ── Bars / coercion ──────────────────────────────────────────────────────────


def bars_from_fmp_payload(payload: dict) -> List[Bar]:
    """Convert an FMP historical-price-full response into our Bar dataclass list.

    FMP returns newest-first under `historical`. We reverse to oldest-first
    so indexing is naturally chronological.
    """
    raw = payload.get("historical") or []
    out: List[Bar] = []
    for row in raw:
        try:
            out.append(Bar(
                date=str(row.get("date", "")),
                open=float(row.get("open", 0) or 0),
                high=float(row.get("high", 0) or 0),
                low=float(row.get("low", 0) or 0),
                close=float(row.get("close", 0) or 0),
                volume=float(row.get("volume", 0) or 0),
            ))
        except (TypeError, ValueError):
            continue
    out.reverse()
    return out


# ── ATR + SMA ────────────────────────────────────────────────────────────────


def _true_range(bar: Bar, prev_close: Optional[float]) -> float:
    if prev_close is None:
        return bar.high - bar.low
    return max(
        bar.high - bar.low,
        abs(bar.high - prev_close),
        abs(bar.low - prev_close),
    )


def atr(bars: List[Bar], period: int = 14) -> Optional[float]:
    """Simple ATR (Wilder's smoothing skipped — uses straight mean of last N TRs).

    Good enough for the buffer-sizing decision; Wilder smoothing matters
    more for trailing stops than for strike placement."""
    if not bars or len(bars) < period + 1:
        return None
    trs: List[float] = []
    prev_close: Optional[float] = None
    for bar in bars:
        trs.append(_true_range(bar, prev_close))
        prev_close = bar.close
    return sum(trs[-period:]) / period


def sma(bars: List[Bar], period: int) -> Optional[float]:
    if not bars or len(bars) < period:
        return None
    return sum(b.close for b in bars[-period:]) / period


def compute_buffer_pct(atr_14: Optional[float], spot: float) -> float:
    """Buffer percentage from the ATR/spot ratio. Doctrine §3 buffer table."""
    if not atr_14 or spot <= 0:
        return 0.005  # safe default
    ratio = atr_14 / spot
    if ratio < 0.015:
        return 0.003
    if ratio < 0.030:
        return 0.005
    return 0.007


# ── Swing detection ──────────────────────────────────────────────────────────


def _swing_lows(bars: List[Bar]) -> List[tuple[int, float]]:
    """Indices and prices of swing lows (low < both neighbors). 1..n-2 only."""
    swings = []
    for i in range(1, len(bars) - 1):
        if bars[i].low < bars[i - 1].low and bars[i].low < bars[i + 1].low:
            swings.append((i, bars[i].low))
    return swings


def _swing_highs(bars: List[Bar]) -> List[tuple[int, float]]:
    swings = []
    for i in range(1, len(bars) - 1):
        if bars[i].high > bars[i - 1].high and bars[i].high > bars[i + 1].high:
            swings.append((i, bars[i].high))
    return swings


# ── Round-number candidates ──────────────────────────────────────────────────


def _round_numbers_below(spot: float) -> List[float]:
    """$5 and $10 levels strictly below spot, descending. Cap at 10 candidates."""
    if spot <= 0:
        return []
    levels = set()
    # $5 levels
    n = int(math.floor(spot / 5.0))
    for i in range(n, max(n - 10, -1), -1):
        if i * 5.0 < spot:
            levels.add(i * 5.0)
    # $10 levels (already a subset of $5; kept explicit for symmetry)
    n10 = int(math.floor(spot / 10.0))
    for i in range(n10, max(n10 - 10, -1), -1):
        if i * 10.0 < spot:
            levels.add(i * 10.0)
    return sorted(levels, reverse=True)


def _round_numbers_above(spot: float) -> List[float]:
    if spot <= 0:
        return []
    levels = set()
    n = int(math.ceil(spot / 5.0))
    for i in range(n, n + 10):
        if i * 5.0 > spot:
            levels.add(i * 5.0)
    n10 = int(math.ceil(spot / 10.0))
    for i in range(n10, n10 + 10):
        if i * 10.0 > spot:
            levels.add(i * 10.0)
    return sorted(levels)


# ── Consolidation / distribution zones ───────────────────────────────────────


def _consolidation_base_below(bars: List[Bar], spot: float, range_pct: float = 0.02,
                              min_bars: int = 3) -> Optional[float]:
    """Find a tight consolidation (3+ bars in 2% range) below spot.

    Walks the most recent 20 bars looking for the longest run that:
      - sits below spot
      - has (max(high) - min(low)) / avg_low <= range_pct
    Returns the low of that range as the anchor.
    """
    recent = bars[-20:]
    best_low: Optional[float] = None
    n = len(recent)
    for start in range(n - min_bars + 1):
        for end in range(start + min_bars, n + 1):
            window = recent[start:end]
            highs = [b.high for b in window]
            lows = [b.low for b in window]
            if max(highs) >= spot:
                continue
            avg_low = sum(lows) / len(lows)
            if avg_low <= 0:
                continue
            width = max(highs) - min(lows)
            if width / avg_low <= range_pct:
                # candidate — take the LOW of the consolidation
                cand = min(lows)
                if best_low is None or cand > best_low:
                    best_low = cand
    return best_low


def _distribution_zone_above(bars: List[Bar], spot: float, range_pct: float = 0.02,
                             min_bars: int = 3) -> Optional[float]:
    """Mirror of consolidation_base_below — 3+ bars failing to close above a
    level (wicks but no body above). Returns the HIGH of the zone as anchor.
    """
    recent = bars[-20:]
    best_high: Optional[float] = None
    n = len(recent)
    for start in range(n - min_bars + 1):
        for end in range(start + min_bars, n + 1):
            window = recent[start:end]
            closes = [b.close for b in window]
            highs = [b.high for b in window]
            if min(closes) <= spot:
                continue
            avg_high = sum(highs) / len(highs)
            if avg_high <= 0:
                continue
            width = max(highs) - min(closes)
            if width / avg_high <= range_pct:
                cand = max(highs)
                if best_high is None or cand < best_high:
                    best_high = cand
    return best_high


# ── Public anchor finders ────────────────────────────────────────────────────


def find_support(bars: List[Bar], spot: float, atr_14: Optional[float]) -> Anchor:
    """Pick the best PUT-side anchor (support below spot)."""
    if not bars or spot <= 0:
        return Anchor(None, "none", "none", atr_14, None, None,
                      source="empty bars / invalid spot",
                      note="No bars or invalid spot — cannot anchor")

    buffer_pct = compute_buffer_pct(atr_14, spot)
    note = ""

    # 1. Swing low within 20-bar lookback, below spot, take HIGHEST valid
    recent20 = bars[-20:]
    swing_candidates = [
        price for _, price in _swing_lows(recent20)
        if price < spot
    ]
    if swing_candidates:
        anchor_price = max(swing_candidates)
        within_15pct = (spot - anchor_price) / spot <= 0.15
        if within_15pct:
            target = anchor_price * (1 - buffer_pct)
            return Anchor(
                anchor_price=round(anchor_price, 4),
                anchor_type="swing_low",
                anchor_confidence="high",
                atr_14=round(atr_14, 4) if atr_14 else None,
                buffer_pct=buffer_pct,
                target_strike_price=round(target, 4),
                source=f"swing low @ {anchor_price:.2f} in last 20 bars",
            )
        note = (
            f"Best swing low @ {anchor_price:.2f} is "
            f"{(spot - anchor_price) / spot:.1%} below spot — outside 15% gate. "
            "Falling through to round-number / MA / consolidation."
        )

    # 2. Round number below spot
    for level in _round_numbers_below(spot):
        if (spot - level) / spot <= 0.15:
            target = level * (1 - buffer_pct)
            return Anchor(
                anchor_price=level,
                anchor_type="round_number",
                anchor_confidence="medium",
                atr_14=round(atr_14, 4) if atr_14 else None,
                buffer_pct=buffer_pct,
                target_strike_price=round(target, 4),
                source=f"round number @ {level:.2f}",
                note=note,
            )

    # 3. MA confluence (within 3% of spot)
    ma50 = sma(bars, 50)
    ma200 = sma(bars, 200)
    ma_candidates: List[tuple[str, float]] = []
    if ma50 and abs(ma50 - spot) / spot <= 0.03 and ma50 < spot:
        ma_candidates.append(("ma_50", ma50))
    if ma200 and abs(ma200 - spot) / spot <= 0.03 and ma200 < spot:
        ma_candidates.append(("ma_200", ma200))
    if ma_candidates:
        # Pick the CLOSER MA
        ma_candidates.sort(key=lambda x: abs(x[1] - spot))
        kind, ma_val = ma_candidates[0]
        conf = "high" if abs(ma_val - spot) / spot <= 0.01 else "medium"
        target = ma_val * (1 - buffer_pct)
        return Anchor(
            anchor_price=round(ma_val, 4),
            anchor_type=kind,
            anchor_confidence=conf,
            atr_14=round(atr_14, 4) if atr_14 else None,
            buffer_pct=buffer_pct,
            target_strike_price=round(target, 4),
            source=f"{kind} @ {ma_val:.2f}",
            note=note,
        )

    # 4. Consolidation base
    cons = _consolidation_base_below(bars, spot)
    if cons and (spot - cons) / spot <= 0.20:
        target = cons * (1 - buffer_pct)
        return Anchor(
            anchor_price=round(cons, 4),
            anchor_type="consolidation",
            anchor_confidence="medium",
            atr_14=round(atr_14, 4) if atr_14 else None,
            buffer_pct=buffer_pct,
            target_strike_price=round(target, 4),
            source=f"consolidation base @ {cons:.2f}",
            note=note,
        )

    return Anchor(
        anchor_price=None,
        anchor_type="none",
        anchor_confidence="none",
        atr_14=round(atr_14, 4) if atr_14 else None,
        buffer_pct=buffer_pct,
        target_strike_price=None,
        source="exhausted all priority levels",
        note=(note + " " if note else "") +
             "No clean structural anchor within 20% of spot — strike selection degraded.",
    )


def find_resistance(bars: List[Bar], spot: float, atr_14: Optional[float],
                    cost_basis: Optional[float] = None) -> Anchor:
    """Pick the best CALL-side anchor (resistance above spot).

    Core Pot rule: when cost_basis is provided, the chosen anchor MUST be
    >= cost_basis. If the natural Priority-1 anchor sits below cost_basis,
    we annotate and search upward for the next valid level.
    """
    if not bars or spot <= 0:
        return Anchor(None, "none", "none", atr_14, None, None,
                      source="empty bars / invalid spot",
                      note="No bars or invalid spot — cannot anchor")

    buffer_pct = compute_buffer_pct(atr_14, spot)
    note = ""
    min_price = max(spot, cost_basis or 0.0)

    # 1. Swing high — LOWEST valid above spot (closest = most relevant)
    recent20 = bars[-20:]
    swing_candidates = [
        price for _, price in _swing_highs(recent20)
        if price > min_price
    ]
    if swing_candidates:
        anchor_price = min(swing_candidates)
        within_15pct = (anchor_price - spot) / spot <= 0.15
        if within_15pct:
            target = anchor_price * (1 + buffer_pct)
            extra = ""
            if cost_basis and anchor_price < cost_basis:
                extra = f" (uplifted from below cost basis {cost_basis:.2f})"
            return Anchor(
                anchor_price=round(anchor_price, 4),
                anchor_type="swing_high",
                anchor_confidence="high",
                atr_14=round(atr_14, 4) if atr_14 else None,
                buffer_pct=buffer_pct,
                target_strike_price=round(target, 4),
                source=f"swing high @ {anchor_price:.2f}{extra}",
            )
        note = (
            f"Best swing high @ {anchor_price:.2f} is "
            f"{(anchor_price - spot) / spot:.1%} above spot — outside 15% gate."
        )

    # 2. Round number above min_price
    for level in _round_numbers_above(spot):
        if level < min_price:
            continue
        if (level - spot) / spot <= 0.15:
            target = level * (1 + buffer_pct)
            return Anchor(
                anchor_price=level,
                anchor_type="round_number",
                anchor_confidence="medium",
                atr_14=round(atr_14, 4) if atr_14 else None,
                buffer_pct=buffer_pct,
                target_strike_price=round(target, 4),
                source=f"round number @ {level:.2f}",
                note=note,
            )

    # 3. MA confluence
    ma50 = sma(bars, 50)
    ma200 = sma(bars, 200)
    ma_candidates: List[tuple[str, float]] = []
    if ma50 and abs(ma50 - spot) / spot <= 0.03 and ma50 >= min_price:
        ma_candidates.append(("ma_50", ma50))
    if ma200 and abs(ma200 - spot) / spot <= 0.03 and ma200 >= min_price:
        ma_candidates.append(("ma_200", ma200))
    if ma_candidates:
        ma_candidates.sort(key=lambda x: abs(x[1] - spot))
        kind, ma_val = ma_candidates[0]
        conf = "high" if abs(ma_val - spot) / spot <= 0.01 else "medium"
        target = ma_val * (1 + buffer_pct)
        return Anchor(
            anchor_price=round(ma_val, 4),
            anchor_type=kind,
            anchor_confidence=conf,
            atr_14=round(atr_14, 4) if atr_14 else None,
            buffer_pct=buffer_pct,
            target_strike_price=round(target, 4),
            source=f"{kind} @ {ma_val:.2f}",
            note=note,
        )

    # 4. Distribution zone
    dist = _distribution_zone_above(bars, spot)
    if dist and dist >= min_price and (dist - spot) / spot <= 0.20:
        target = dist * (1 + buffer_pct)
        return Anchor(
            anchor_price=round(dist, 4),
            anchor_type="distribution_zone",
            anchor_confidence="medium",
            atr_14=round(atr_14, 4) if atr_14 else None,
            buffer_pct=buffer_pct,
            target_strike_price=round(target, 4),
            source=f"distribution zone @ {dist:.2f}",
            note=note,
        )

    return Anchor(
        anchor_price=None,
        anchor_type="none",
        anchor_confidence="none",
        atr_14=round(atr_14, 4) if atr_14 else None,
        buffer_pct=buffer_pct,
        target_strike_price=None,
        source="exhausted all priority levels",
        note=(note + " " if note else "") +
             "No clean structural anchor within 20% of spot — strike selection degraded.",
    )


# ── Strike placement against an available-strikes ladder ─────────────────────


def place_strike(target_price: float, available_strikes: Iterable[float],
                 side: str) -> Optional[float]:
    """Pick the strike from the chain that lines up with the anchor target.

      side="PUT":  return the HIGHEST strike <= target  (just below structural support)
      side="CALL": return the LOWEST  strike >= target  (just above resistance)
    """
    strikes = sorted({float(s) for s in available_strikes})
    if not strikes:
        return None
    if side.upper() == "PUT":
        below = [s for s in strikes if s <= target_price]
        return max(below) if below else None
    # CALL
    above = [s for s in strikes if s >= target_price]
    return min(above) if above else None


# ── Roll math — pure, no Tiger objects ───────────────────────────────────────


def roll_math(btc_mid: float, sto_mid: float, qty: int) -> dict:
    """Net cash for the BTC + STO pair, sign convention: positive = credit.

      btc_cost_total = btc_mid × 100 × |qty|
      sto_recv_total = sto_mid × 100 × |qty|
      net_credit     = sto_recv_total − btc_cost_total
      net_credit_per_lot = net_credit / |qty|
    """
    q = abs(int(qty))
    btc_cost_total = btc_mid * 100.0 * q
    sto_recv_total = sto_mid * 100.0 * q
    net_credit = sto_recv_total - btc_cost_total
    return {
        "btc_cost_total": round(btc_cost_total, 2),
        "sto_recv_total": round(sto_recv_total, 2),
        "net_credit": round(net_credit, 2),
        "net_credit_per_lot": round(net_credit / q, 2) if q else 0.0,
    }


# ── FMP HTTP client (slim) ──────────────────────────────────────────────────


_FMP_BASE = "https://financialmodelingprep.com/api/v3"


class FMPError(RuntimeError):
    pass


def fetch_fmp_bars(symbol: str, api_key: Optional[str] = None,
                   max_bars: int = 250, timeout: float = 10.0) -> List[Bar]:
    """Pull daily OHLC bars from FMP. Raises FMPError on failure — caller
    decides whether to fail the whole tool call or downgrade gracefully.

    `max_bars` trims to the most recent N (enough for 200-day MA + buffer).
    """
    key = (api_key or os.environ.get("MCP_FMP_API_KEY") or
           os.environ.get("FMP_API_KEY") or "").strip()
    if not key or key.upper() == "NOT_SET":
        raise FMPError(
            "FMP_API_KEY not configured. Set MCP_FMP_API_KEY env var on the "
            "Cloud Run service, or sync the FMP_API_KEY GitHub secret via "
            "the deploy workflow."
        )
    url = (
        f"{_FMP_BASE}/historical-price-full/"
        f"{urllib.parse.quote(symbol.upper())}?apikey={urllib.parse.quote(key)}"
    )
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        raise FMPError(f"FMP HTTP {e.code} fetching {symbol}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise FMPError(f"FMP network error fetching {symbol}: {e.reason}") from e

    try:
        payload = json.loads(raw)
    except ValueError as e:
        raise FMPError(f"FMP returned non-JSON for {symbol}: {e}") from e

    bars = bars_from_fmp_payload(payload)
    if not bars:
        # FMP sometimes returns an error object instead of `historical`
        msg = payload.get("Error Message") or payload.get("error") or "no bars in response"
        raise FMPError(f"FMP returned no bars for {symbol}: {msg}")

    return bars[-max_bars:]
