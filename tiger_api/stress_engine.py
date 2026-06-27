"""Stress-test engine — Scenarios A / B / D / B+D against a live book.

Single source of truth for the morning Margin Health Block. All
position-classification + math is pure; the MCP wrapper feeds it live
data from Tiger + computed LEAPS Greeks.

Doctrine (per ARGUS E1 BACKLOG v3.0, Priority 3):

  Scenario A — moderate equity drawdown
    Core stocks (MARA + CRCL) marked down 15%.
    Short puts assumed to stay OTM (no assignment).
    No PMCC impact (SPY flat).

  Scenario B — heavy equity drawdown
    Core stocks marked down 30%.
    Short puts marked to the shocked spot — assignment loss = (strike − shocked_spot) × 100
    when shocked_spot < strike.
    Short calls collect ~100% of original premium as offset (deep OTM, expire worthless).

  Scenario D — SPY crash on PMCC chassis
    SPY down 20%. LEAPS revalue using their delta:
      pmcc_loss = sum(delta × spy_move_$ × 100 × |qty|)
    Short calls against the LEAPS collect ~50–100% premium offset as the
    underlying drops away (per call delta × 100 × |qty| × spy_move_frac, capped at
    the original premium). v1 uses a conservative 50% of original premium.

  Scenario B+D — combined
    Sum of Scenario B equity / put / call deltas + Scenario D LEAPS/call deltas.

Zone classification — five bands keyed on `excess_liquidity_after_shock`:
  > $60K     → safe
  $40–60K   → watch
  $20–40K   → reduce
  $0–20K    → critical
  ≤ $0      → insolvent

MARA reduction schedule fires when zone in {reduce, critical, insolvent}.
Steps reduce toward floors {13000, 10000, 8000}; margin relief estimated at
share_count × current_price × (1 − margin_req_pct).

PMCC hard stop: SPY close below $650 → trigger. Status reported alongside.

What's NOT here (intentional):
  - Tiger client / MCP context / FMP fetches — those live in server.py.
  - Implied vol shocks — current spec is spot-only. Vol-shock variant
    can land later as a second `run_stress_test_vol(...)` tool.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import List, Optional

logger = logging.getLogger("tiger-mcp.stress_engine")


# ── Zone bands (operator-visible — bump in one place) ────────────────────────

ZONE_SAFE_FLOOR = 60_000
ZONE_WATCH_FLOOR = 40_000
ZONE_REDUCE_FLOOR = 20_000
ZONE_CRITICAL_FLOOR = 0

PMCC_HARD_STOP_SPY = 650.0
MARGIN_REQ_PCT = 0.20  # initial margin requirement for stock collateral
MARA_REDUCTION_TARGETS = [13_000, 10_000, 8_000]


def classify_zone(excess_liquidity: float) -> str:
    if excess_liquidity > ZONE_SAFE_FLOOR:
        return "safe"
    if excess_liquidity > ZONE_WATCH_FLOOR:
        return "watch"
    if excess_liquidity > ZONE_REDUCE_FLOOR:
        return "reduce"
    if excess_liquidity > ZONE_CRITICAL_FLOOR:
        return "critical"
    return "insolvent"


# ── Position abstractions (pure dicts; matches _position_to_dict shape) ──────

CORE_TICKERS = {"MARA", "CRCL"}


@dataclass
class _ShortOption:
    symbol: str
    right: str          # "PUT" or "CALL"
    strike: float
    qty: int            # absolute count of contracts
    premium_received: float  # avg_cost per share — short = credit collected

    @property
    def total_premium(self) -> float:
        return self.premium_received * 100.0 * self.qty


@dataclass
class _Leap:
    symbol: str
    strike: float
    qty: int            # absolute count, long calls = qty > 0
    delta: float        # current per-share delta (LEAPS typically 0.70–0.95)
    market_price: float


# ── Position classification ──────────────────────────────────────────────────


def classify_positions(stock_positions: List[dict], option_positions: List[dict],
                        leaps_greeks_by_key: Optional[dict] = None) -> dict:
    """Bucketize live positions into {mara_value, crcl_value, short_puts,
    short_calls, leaps}. `leaps_greeks_by_key` maps a key string to {delta}
    so the stress engine can value LEAPS deltas without re-solving here.
    """
    leaps_greeks_by_key = leaps_greeks_by_key or {}

    mara_value = 0.0
    crcl_value = 0.0
    for p in stock_positions:
        sym = (p.get("symbol") or "").upper()
        try:
            val = float(p.get("market_value") or 0)
        except (TypeError, ValueError):
            val = 0.0
        if sym == "MARA":
            mara_value += val
        elif sym == "CRCL":
            crcl_value += val

    short_puts: List[_ShortOption] = []
    short_calls: List[_ShortOption] = []
    leaps: List[_Leap] = []

    for p in option_positions:
        sym = (p.get("symbol") or "").upper()
        right = (p.get("right") or "").upper()
        try:
            qty = float(p.get("quantity") or 0)
            strike = float(p.get("strike") or 0)
            avg_cost = float(p.get("avg_cost") or 0)
            market_price = float(p.get("market_price") or 0)
        except (TypeError, ValueError):
            continue
        if right not in ("PUT", "CALL") or qty == 0 or strike <= 0:
            continue

        if qty < 0:
            opt = _ShortOption(
                symbol=sym, right=right, strike=strike, qty=int(abs(qty)),
                premium_received=avg_cost,
            )
            (short_puts if right == "PUT" else short_calls).append(opt)
        else:
            # Long option — treat long calls as PMCC LEAPS chassis when
            # delta data is available. Long puts (protective) aren't part
            # of the stress test in v1.
            if right != "CALL":
                continue
            key = f"{sym}|{strike}|{p.get('expiry')}|{right}"
            delta = float(leaps_greeks_by_key.get(key, {}).get("delta") or 0.80)
            leaps.append(_Leap(
                symbol=sym, strike=strike, qty=int(abs(qty)),
                delta=delta, market_price=market_price,
            ))

    return {
        "mara_value": round(mara_value, 2),
        "crcl_value": round(crcl_value, 2),
        "short_puts": short_puts,
        "short_calls": short_calls,
        "leaps": leaps,
    }


# ── Spot-shock pricing helpers ───────────────────────────────────────────────


def _put_assignment_loss(short_puts: List[_ShortOption], shock_pct: float,
                         core_spots: dict) -> float:
    """Total $ loss if shocked spot pushes puts ITM.

    Loss per assigned put = (strike − shocked_spot) × 100 × qty. Floored at 0.
    """
    loss = 0.0
    for p in short_puts:
        spot = float(core_spots.get(p.symbol) or 0)
        if spot <= 0:
            continue
        shocked = spot * (1 + shock_pct)
        if shocked < p.strike:
            loss += (p.strike - shocked) * 100.0 * p.qty
    return loss


def _call_premium_offset(short_calls: List[_ShortOption], offset_frac: float) -> float:
    """Fraction of original short-call premium that returns as 'safe' P&L
    in a down move. `offset_frac` = 1.0 for "expire worthless" assumption.
    """
    return sum(c.total_premium * offset_frac for c in short_calls)


def _pmcc_loss(leaps: List[_Leap], spy_spot: float, spy_shock_pct: float) -> float:
    """LEAPS dollar loss from a parallel SPY-style shock applied to each
    underlying. v1 approximation: apply spy_shock_pct to each LEAPS' own
    spot (we don't have per-ticker betas to SPY); LEAPS_delta × shock × 100 × qty.
    """
    spy_move = spy_spot * spy_shock_pct
    loss = 0.0
    for L in leaps:
        # Shock is negative — delta * negative = negative price move per share
        per_share_move = L.delta * spy_move
        loss += abs(per_share_move) * 100.0 * L.qty
    return loss


# ── MARA reduction schedule ──────────────────────────────────────────────────


def reduction_schedule_mara(current_shares: int, mara_price: float,
                            margin_req_pct: float = MARGIN_REQ_PCT) -> List[dict]:
    """Stepped MARA share-reduction schedule. Each step closes down toward
    the next floor in MARA_REDUCTION_TARGETS; margin relief is an estimate
    based on the freed collateral.
    """
    if current_shares <= 0 or mara_price <= 0:
        return []
    steps: List[dict] = []
    prev = current_shares
    # Closing N shares at $X frees the maintenance-margin portion of the
    # collateral that was tied up — operationally `(1 − margin_req_pct)`
    # of the closed value (the rest was already required as initial margin).
    # Per the BACKLOG v3.0 spec example: 2900 MARA at $14.5 → ~$34K relief.
    relief_fraction = 1.0 - margin_req_pct
    for i, target in enumerate(MARA_REDUCTION_TARGETS, 1):
        if prev <= target:
            continue
        to_close = prev - target
        margin_relief = to_close * mara_price * relief_fraction
        steps.append({
            "step": i,
            "from_shares": int(prev),
            "to_shares": int(target),
            "close_shares": int(to_close),
            "margin_relief_est": round(margin_relief),
        })
        prev = target
    return steps


# ── Scenario assembly ────────────────────────────────────────────────────────


def run_scenarios(
    nav: float,
    excess_liquidity: float,
    margin_debit: float,
    maintain_margin: float,
    classified: dict,
    spy_spot: float,
    core_spots: Optional[dict] = None,
) -> dict:
    """Compute the four scenario blocks. All numbers in $.

    `core_spots` maps symbol → current spot for MARA / CRCL etc. (used for
    put assignment math). If absent, falls back to inferring spot from
    market_value / quantity in the stock positions (handled upstream by
    the MCP wrapper).
    """
    core_spots = core_spots or {}
    mara_value = classified["mara_value"]
    crcl_value = classified["crcl_value"]
    short_puts = classified["short_puts"]
    short_calls = classified["short_calls"]
    leaps = classified["leaps"]
    core_total = mara_value + crcl_value
    leaps_value = sum(L.market_price * 100.0 * L.qty for L in leaps)

    # Scenario A — −15% equity, no put assignment
    a_equity_loss = core_total * 0.15
    a_total = a_equity_loss
    a_buffer = excess_liquidity - a_total

    # Scenario B — −30% equity + put assignment + call premium offset
    b_equity_loss = core_total * 0.30
    b_put_loss = _put_assignment_loss(short_puts, -0.30, core_spots)
    b_call_offset = _call_premium_offset(short_calls, offset_frac=1.0)
    b_total = b_equity_loss + b_put_loss - b_call_offset
    b_buffer = excess_liquidity - b_total

    # Scenario D — SPY −20% on LEAPS
    d_pmcc_loss = _pmcc_loss(leaps, spy_spot, -0.20)
    d_call_offset = _call_premium_offset(short_calls, offset_frac=0.5)
    d_total = d_pmcc_loss - d_call_offset
    d_buffer = excess_liquidity - d_total

    # Scenario B+D — combined
    bd_call_offset = _call_premium_offset(short_calls, offset_frac=1.0)
    bd_total = b_equity_loss + b_put_loss + d_pmcc_loss - bd_call_offset
    bd_buffer = excess_liquidity - bd_total

    pmcc_hard_stop_triggered = spy_spot < PMCC_HARD_STOP_SPY

    return {
        "baseline": {
            "nav": round(nav, 2),
            "excess_liquidity": round(excess_liquidity, 2),
            "margin_debit": round(margin_debit, 2),
            "maintain_margin": round(maintain_margin, 2),
            "mara_value": round(mara_value, 2),
            "crcl_value": round(crcl_value, 2),
            "leaps_value": round(leaps_value, 2),
            "spy_spot": round(spy_spot, 2),
        },
        "scenarios": {
            "A": {
                "equity_loss": round(a_equity_loss, 2),
                "put_assignment_loss": 0.0,
                "pmcc_loss": 0.0,
                "call_premium_offset": 0.0,
                "total_loss": round(a_total, 2),
                "buffer_after": round(a_buffer, 2),
                "zone": classify_zone(a_buffer),
            },
            "B": {
                "equity_loss": round(b_equity_loss, 2),
                "put_assignment_loss": round(b_put_loss, 2),
                "pmcc_loss": 0.0,
                "call_premium_offset": round(b_call_offset, 2),
                "total_loss": round(b_total, 2),
                "buffer_after": round(b_buffer, 2),
                "zone": classify_zone(b_buffer),
            },
            "D": {
                "equity_loss": 0.0,
                "put_assignment_loss": 0.0,
                "pmcc_loss": round(d_pmcc_loss, 2),
                "call_premium_offset": round(d_call_offset, 2),
                "total_loss": round(d_total, 2),
                "buffer_after": round(d_buffer, 2),
                "zone": classify_zone(d_buffer),
            },
            "BD": {
                "equity_loss": round(b_equity_loss, 2),
                "put_assignment_loss": round(b_put_loss, 2),
                "pmcc_loss": round(d_pmcc_loss, 2),
                "call_premium_offset": round(bd_call_offset, 2),
                "total_loss": round(bd_total, 2),
                "buffer_after": round(bd_buffer, 2),
                "zone": classify_zone(bd_buffer),
            },
        },
        "current_zone": classify_zone(excess_liquidity),
        "pmcc_hard_stop": {
            "spy_close_below": PMCC_HARD_STOP_SPY,
            "status": "triggered" if pmcc_hard_stop_triggered else "not triggered",
            "spy_spot": round(spy_spot, 2),
        },
    }
