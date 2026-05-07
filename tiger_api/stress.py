"""Stress testing — what happens to the book if the market moves N%?

For each open position, repricing at shocked spots:
  • Stock: linear MTM impact
  • Long LEAP: deep ITM call → close to delta=1; use BS to reprice
  • Short CSP: shocked spot can move it ITM (assignment risk + MTM loss)
  • Short CC: shocked spot can move it ITM (called-away risk + MTM loss)

Reprices with Black-Scholes using the IV solved from current market price.
For Δ-only fast path: use shock × current Delta to estimate MTM change.
For accurate path: re-solve at new spot. We use BS reprice for accuracy.

Output rows:
  shock_pct · NAV impact $ · NAV impact % · stock impact · option impact ·
  ITM count after shock · margin call risk flag
"""
from __future__ import annotations

import logging
from typing import Dict, List

import pandas as pd

logger = logging.getLogger(__name__)


def _bs_price_call(spot, strike, t, r, sigma):
    import math
    if t <= 0 or sigma <= 0:
        return max(spot - strike, 0.0)
    sqrt_t = math.sqrt(t)
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma ** 2) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    from math import erf
    Phi = lambda x: 0.5 * (1.0 + erf(x / math.sqrt(2.0)))
    return spot * Phi(d1) - strike * math.exp(-r * t) * Phi(d2)


def _bs_price_put(spot, strike, t, r, sigma):
    import math
    if t <= 0 or sigma <= 0:
        return max(strike - spot, 0.0)
    sqrt_t = math.sqrt(t)
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma ** 2) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    from math import erf
    Phi = lambda x: 0.5 * (1.0 + erf(x / math.sqrt(2.0)))
    return strike * math.exp(-r * t) * Phi(-d2) - spot * Phi(-d1)


def stress_book(df_open: pd.DataFrame, spot_prices: Dict[str, float],
                 shocks_pct: List[float] = (-30, -20, -10, -5, 0, 5, 10),
                 r: float = 0.045) -> pd.DataFrame:
    """Reprice the book at each spot shock; return per-shock NAV impact.

    For options, we use Black-Scholes with IV solved from current market price
    (uses tiger_api.greeks.implied_vol). At shocked spot, reprice with same IV.
    Rough but directional.
    """
    if df_open is None or df_open.empty:
        return pd.DataFrame()
    from tiger_api.greeks import implied_vol, bs_price

    rows = []
    d = df_open.copy()
    d["q"] = pd.to_numeric(d.get("Quantity", 0), errors="coerce").fillna(0)
    d["k"] = pd.to_numeric(d.get("Option_Strike_Price_(USD)", 0), errors="coerce").fillna(0)
    d["mp"] = pd.to_numeric(d.get("_market_price", 0), errors="coerce").fillna(0)
    d["mv"] = pd.to_numeric(d.get("_market_value", 0), errors="coerce").fillna(0)
    d["expiry"] = pd.to_datetime(d.get("Expiry_Date"), errors="coerce")
    today = pd.Timestamp.now().normalize()
    d["dte"] = (d["expiry"] - today).dt.days.clip(lower=0)

    for shock in shocks_pct:
        shock_factor = 1 + shock / 100.0
        total_stock_impact = 0.0
        total_option_impact = 0.0
        itm_after = 0
        positions_marked = 0

        for _, row in d.iterrows():
            ttype = row.get("TradeType", "")
            tkr = str(row.get("Ticker", "")).upper()
            spot = float(spot_prices.get(tkr, 0) or 0)
            qty = float(row["q"])
            mv = float(row["mv"])
            if spot <= 0:
                continue
            shocked_spot = spot * shock_factor

            if ttype == "STOCK":
                # Linear: shock × current value
                stock_change = (shocked_spot / spot - 1) * mv
                total_stock_impact += stock_change
                positions_marked += 1
                continue

            # Option leg
            strike = float(row["k"])
            mkt_px = float(row["mp"])
            dte = float(row["dte"])
            if strike <= 0 or mkt_px <= 0 or dte <= 0:
                continue
            t = dte / 365.0
            is_call = ttype in ("CC", "LEAP")
            iv = implied_vol(mkt_px, spot, strike, t, r, is_call)
            if iv is None or iv <= 0:
                continue
            try:
                new_px = bs_price(shocked_spot, strike, t, r, iv, is_call)
            except Exception:
                continue
            # Mark-to-market change per share × 100 × signed qty
            # qty is negative for shorts (Tiger convention), positive for longs
            old_value = mkt_px * 100 * qty
            new_value = new_px * 100 * qty
            option_change = new_value - old_value
            total_option_impact += option_change
            positions_marked += 1
            # ITM check after shock
            if (is_call and shocked_spot > strike) or (not is_call and shocked_spot < strike):
                itm_after += abs(int(qty))

        rows.append({
            "Shock %": shock,
            "Stock impact $": total_stock_impact,
            "Option impact $": total_option_impact,
            "Total NAV change $": total_stock_impact + total_option_impact,
            "Positions ITM after": itm_after,
        })
    return pd.DataFrame(rows)
