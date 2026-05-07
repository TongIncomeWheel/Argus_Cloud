"""Wheel Cycle Tracker — classify per-ticker fills into wheel cycles.

A WHEEL CYCLE = state machine per ticker:
  STATE_CASH  — no shares held; selling CSPs
    → on CSP assignment → STATE_STOCK
    → on CSP buy-to-close (BTC) or expire-worthless → stay in STATE_CASH (collect premium)
  STATE_STOCK — shares held; selling CCs
    → on CC assignment (called away) → STATE_CASH (cycle complete)
    → on CC BTC or expire-worthless → stay in STATE_STOCK (collect premium)

A "cycle" begins when entering STATE_CASH (start fresh) and ends when
returning to STATE_CASH after at least one full revolution (CSP→stock→CC→sale).

Reality is messier:
  • Multiple CSP rolls before assignment → all attribute to same cycle
  • Stock bought directly (not via assignment) → cycle starts mid-stream
  • Partial assignments (rare) → split tracking
  • LEAPs are PMCC-cycle territory — separate handling

For the typical wheel:
  cycle_pnl = Σ(CSP premium received) − Σ(CSP buy-back cost)
            + (sale_price − assigned_price) × shares
            + Σ(CC premium received) − Σ(CC buy-back cost)
            − fees

This module provides build_cycles(df_orders) → DataFrame of cycle records.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def _is_assignment(row: dict) -> bool:
    """Detect CSP/CC assignment from order fields."""
    event = str(row.get("Event", "")).upper()
    return event in ("ASSIGNED", "EXERCISED")


def _is_expired(row: dict) -> bool:
    event = str(row.get("Event", "")).upper()
    return event == "EXPIRED"


def build_cycles(df_orders: pd.DataFrame) -> pd.DataFrame:
    """Walk fills per ticker, classify into wheel cycles.

    Returns DataFrame with columns:
      Ticker · Cycle # · Start Date · End Date · Duration (days)
      CSP Premium · CSP Buyback · CC Premium · CC Buyback
      Stock Bought $ · Stock Sold $ · Realized $ · Status (open/closed)
      Trades · CSP fills · CC fills · Outcome (worthless | assigned | called | open)
    """
    if df_orders is None or df_orders.empty:
        return pd.DataFrame()

    df = df_orders.copy()
    df["TradeDateTime"] = pd.to_datetime(df.get("TradeDateTime", df.get("TradeDate")), errors="coerce")
    df = df.sort_values("TradeDateTime").reset_index(drop=True)

    cycles_out: List[dict] = []

    for ticker, sub in df.groupby("Ticker"):
        if not isinstance(ticker, str) or not ticker:
            continue
        sub = sub.sort_values("TradeDateTime").reset_index(drop=True)

        cycle_idx = 0
        state = "CASH"  # CASH or STOCK
        share_qty = 0
        share_cost_total = 0.0
        cycle_open: Optional[dict] = None  # active cycle dict

        def _start_cycle(start_dt, reason: str):
            nonlocal cycle_open, cycle_idx
            cycle_idx += 1
            cycle_open = {
                "Ticker": ticker,
                "Cycle #": cycle_idx,
                "Start": start_dt,
                "End": None,
                "CSP Premium": 0.0,
                "CSP Buyback": 0.0,
                "CC Premium": 0.0,
                "CC Buyback": 0.0,
                "Stock Bought $": 0.0,
                "Stock Sold $": 0.0,
                "CSP fills": 0,
                "CC fills": 0,
                "Stock fills": 0,
                "Total fills": 0,
                "Status": "open",
                "Outcome": "open",
                "Start Reason": reason,
            }

        def _close_cycle(end_dt, outcome: str):
            nonlocal cycle_open, state
            if cycle_open is None:
                return
            cycle_open["End"] = end_dt
            cycle_open["Status"] = "closed"
            cycle_open["Outcome"] = outcome
            # Premium P&L (cash from options) — always realized
            premium_pnl = (
                cycle_open["CSP Premium"] - cycle_open["CSP Buyback"]
                + cycle_open["CC Premium"] - cycle_open["CC Buyback"]
            )
            # Stock P&L — only realized when stock is SOLD (cycle closed)
            stock_pnl = cycle_open["Stock Sold $"] - cycle_open["Stock Bought $"]
            cycle_open["Premium P&L"] = premium_pnl
            cycle_open["Stock P&L"] = stock_pnl
            cycle_open["Realized $"] = premium_pnl + stock_pnl
            duration = (end_dt - cycle_open["Start"]).days if end_dt and cycle_open["Start"] else 0
            cycle_open["Duration (days)"] = max(duration, 0)
            cycles_out.append(dict(cycle_open))
            cycle_open = None

        for _, r in sub.iterrows():
            ttype = str(r.get("TradeType", "")).upper()
            action = str(r.get("Action", r.get("Direction", ""))).upper()
            event = str(r.get("Event", "")).upper()
            dt = r.get("TradeDateTime")
            qty = abs(float(r.get("Quantity", 0) or 0))
            cash = float(r.get("FilledCashAmount", 0) or 0)  # signed: + for sells, − for buys
            fill_price = float(r.get("FillPrice", 0) or 0)

            # Open a cycle if none active
            if cycle_open is None and dt is not None:
                _start_cycle(dt, f"first {ttype} fill")

            if cycle_open is None:
                continue
            cycle_open["Total fills"] += 1

            # CSP — short put
            if ttype == "CSP":
                cycle_open["CSP fills"] += 1
                if action == "SELL":  # STO
                    cycle_open["CSP Premium"] += abs(cash)
                else:  # BUY (BTC)
                    cycle_open["CSP Buyback"] += abs(cash)
                # Assignment of CSP → stock acquired at strike
                if _is_assignment(r):
                    state = "STOCK"
                    strike = float(r.get("Option_Strike_Price_(USD)", 0) or 0)
                    contracts = qty
                    shares_acquired = contracts * 100
                    share_qty += shares_acquired
                    share_cost_total += strike * shares_acquired
                    cycle_open["Stock Bought $"] += strike * shares_acquired

            # CC — short call
            elif ttype == "CC":
                cycle_open["CC fills"] += 1
                if action == "SELL":  # STO
                    cycle_open["CC Premium"] += abs(cash)
                else:  # BUY (BTC)
                    cycle_open["CC Buyback"] += abs(cash)
                # Assignment of CC → shares called away at strike
                if _is_assignment(r):
                    strike = float(r.get("Option_Strike_Price_(USD)", 0) or 0)
                    contracts = qty
                    shares_sold = min(contracts * 100, share_qty)
                    sale_proceeds = strike * shares_sold
                    share_qty -= shares_sold
                    share_cost_total -= (share_cost_total / max(share_qty + shares_sold, 1)) * shares_sold
                    cycle_open["Stock Sold $"] += sale_proceeds
                    if share_qty <= 0:
                        # Cycle complete via called-away
                        state = "CASH"
                        _close_cycle(dt, "called_away")

            # STOCK — direct stock buy/sell (manual or via assignment side)
            elif ttype == "STOCK":
                cycle_open["Stock fills"] += 1
                if action in ("BUY", "BTO"):
                    state = "STOCK"
                    share_qty += qty
                    share_cost_total += abs(cash)
                    cycle_open["Stock Bought $"] += abs(cash)
                elif action in ("SELL", "STC"):
                    sale_proceeds = abs(cash)
                    cycle_open["Stock Sold $"] += sale_proceeds
                    share_qty -= qty
                    if share_qty <= 0:
                        state = "CASH"
                        _close_cycle(dt, "stock_sold")

            # LEAP — long-term call, treat as part of cycle but no state change
            elif ttype == "LEAP":
                if action in ("BUY", "BTO"):
                    cycle_open["Stock Bought $"] += abs(cash)
                else:
                    cycle_open["Stock Sold $"] += abs(cash)

        # End of loop — emit lingering OPEN cycle (status stays 'open')
        if cycle_open is not None:
            premium_pnl = (
                cycle_open["CSP Premium"] - cycle_open["CSP Buyback"]
                + cycle_open["CC Premium"] - cycle_open["CC Buyback"]
            )
            # For OPEN cycles, stock P&L is unrealized — don't include in Realized $
            cycle_open["Premium P&L"] = premium_pnl
            cycle_open["Stock P&L"] = 0.0  # unrealized while open
            cycle_open["Realized $"] = premium_pnl  # only premium realized so far
            cycle_open["Open Stock Cost"] = cycle_open["Stock Bought $"] - cycle_open["Stock Sold $"]
            duration = (sub["TradeDateTime"].max() - cycle_open["Start"]).days if cycle_open["Start"] else 0
            cycle_open["Duration (days)"] = max(duration, 0)
            cycle_open["End"] = sub["TradeDateTime"].max()
            cycles_out.append(dict(cycle_open))
            cycle_open = None

    if not cycles_out:
        return pd.DataFrame()
    out = pd.DataFrame(cycles_out)
    return out


def cycle_summary(cycles_df: pd.DataFrame) -> dict:
    """Aggregate cycle stats: avg duration, win rate, total realized."""
    out = {
        "total_cycles": 0,
        "closed_cycles": 0,
        "open_cycles": 0,
        "avg_duration_days": 0.0,
        "win_rate_pct": 0.0,
        "total_realized": 0.0,
        "avg_realized_per_cycle": 0.0,
        "best_cycle_pnl": 0.0,
        "worst_cycle_pnl": 0.0,
    }
    if cycles_df is None or cycles_df.empty:
        return out
    out["total_cycles"] = len(cycles_df)
    closed = cycles_df[cycles_df["Status"] == "closed"]
    out["closed_cycles"] = len(closed)
    out["open_cycles"] = len(cycles_df) - len(closed)
    if not closed.empty:
        out["avg_duration_days"] = float(closed["Duration (days)"].mean() or 0)
        wins = (closed["Realized $"] > 0).sum()
        out["win_rate_pct"] = float(wins / len(closed) * 100) if len(closed) else 0
        out["total_realized"] = float(closed["Realized $"].sum())
        out["avg_realized_per_cycle"] = float(closed["Realized $"].mean() or 0)
        out["best_cycle_pnl"] = float(closed["Realized $"].max() or 0)
        out["worst_cycle_pnl"] = float(closed["Realized $"].min() or 0)
    return out
