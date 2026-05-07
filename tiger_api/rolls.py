"""Roll Tracker — analyze quality of every roll in the archive.

A "roll" = closing an existing short option AND opening a new one same day,
typically same ticker & same right (PUT or CALL). Tiger records it as a MLEG
combo we expand into 2 legs:
  - BTC leg (Buy-To-Close the old position) — pay the buyback
  - STO leg (Sell-To-Open the new position) — collect new premium

Roll Quality dimensions:
  • Net credit/debit:    new STO premium − BTC cost  (positive = collected credit)
  • Strike movement:     for CSPs, lower strike = improved (less assignment risk)
                          for CCs, higher strike = improved (more upside)
  • DTE extension:       new expiry − old expiry (bought time)
  • Defensive vs offensive: if old position was deep ITM, it's defensive (locked loss)

Approach: walk archive grouped by date+ticker. Find pairs of opposite-action
fills on same option leg key. Match BTC against STO of same right (P/P or C/C).
"""
from __future__ import annotations

import logging
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


def build_rolls(df_orders: pd.DataFrame, ticker_filter=None, pot_tickers=None,
                start_date=None, end_date=None) -> pd.DataFrame:
    """Identify roll pairs (BTC + STO) and analyze each.

    Args:
      ticker_filter: optional set/list of tickers to include
      pot_tickers:   optional set of tickers in selected pot(s)
      start_date, end_date: filter by roll Date

    Returns DataFrame with columns:
      Date · Ticker · Type · Right (P/C) ·
      Old Strike · Old Expiry · Old Premium (BTC paid) ·
      New Strike · New Expiry · New Premium (STO received) ·
      Net Credit · Strike Δ · DTE Δ · Roll Quality
    """
    if df_orders is None or df_orders.empty:
        return pd.DataFrame()

    df = df_orders.copy()
    df["TradeDateTime"] = pd.to_datetime(df.get("TradeDateTime", df.get("TradeDate")), errors="coerce")
    df["Expiry_Date"] = pd.to_datetime(df.get("Expiry_Date"), errors="coerce")
    df["TradeDate"] = df["TradeDateTime"].dt.date

    # Filters before grouping
    if ticker_filter:
        df = df[df["Ticker"].isin(ticker_filter)]
    if pot_tickers:
        df = df[df["Ticker"].isin(pot_tickers)]

    # Only options
    df = df[df["TradeType"].isin(["CSP", "CC"])].copy()
    if df.empty:
        return pd.DataFrame()

    df["right"] = df["TradeType"].map({"CSP": "P", "CC": "C"})
    df["Action"] = df.get("Action", df.get("Direction", "")).astype(str).str.upper()
    df["FillPrice"] = pd.to_numeric(df.get("FillPrice", 0), errors="coerce").fillna(0)
    df["Quantity"] = pd.to_numeric(df.get("Quantity", 0), errors="coerce").fillna(0).abs()
    df["Strike"] = pd.to_numeric(df.get("Option_Strike_Price_(USD)", 0), errors="coerce").fillna(0)
    df["FilledCashAmount"] = pd.to_numeric(df.get("FilledCashAmount", 0), errors="coerce").fillna(0)

    rolls_out: List[dict] = []
    # Group by trade-day + ticker + right
    grouper = df.groupby(["TradeDate", "Ticker", "right"], dropna=False)
    for (date_d, tkr, right), group in grouper:
        # Need at least one BTC and one STO same day
        btc_legs = group[group["Action"].str.contains("BUY", na=False)]
        sto_legs = group[group["Action"].str.contains("SELL", na=False)]
        if btc_legs.empty or sto_legs.empty:
            continue
        # Match by quantity — for simple rolls qty matches 1:1
        # (More complex rolls split-fills handled by aggregating amounts)
        btc_premium = float(btc_legs["FillPrice"].mean() or 0)  # avg buyback price/share
        sto_premium = float(sto_legs["FillPrice"].mean() or 0)  # avg new premium/share
        btc_qty = int(btc_legs["Quantity"].sum())
        sto_qty = int(sto_legs["Quantity"].sum())
        if btc_qty != sto_qty or btc_qty == 0:
            # Mixed roll or partial — log but skip detailed analysis
            continue
        # Old leg detail
        old_strike = float(btc_legs["Strike"].iloc[0])
        old_expiry = btc_legs["Expiry_Date"].iloc[0]
        # New leg detail
        new_strike = float(sto_legs["Strike"].iloc[0])
        new_expiry = sto_legs["Expiry_Date"].iloc[0]

        # Net cash impact: STO cash (positive) + BTC cash (negative) per share × 100 × qty
        net_cash = float(sto_legs["FilledCashAmount"].sum() + btc_legs["FilledCashAmount"].sum())

        strike_delta = new_strike - old_strike  # for CSPs: negative = improved (lower strike)
                                                  # for CCs: positive = improved (higher strike)
        dte_delta = (new_expiry - old_expiry).days if pd.notna(old_expiry) and pd.notna(new_expiry) else 0

        # Roll Quality classification
        if right == "P":  # CSP
            improved_strike = strike_delta < 0
        else:  # CC
            improved_strike = strike_delta > 0

        if net_cash > 0 and improved_strike:
            quality = "✅ Strong (credit + better strike)"
        elif net_cash > 0:
            quality = "🟡 OK (credit, same/worse strike)"
        elif net_cash >= -5 and improved_strike:
            quality = "🟡 Defensive (small debit, better strike)"
        else:
            quality = "🔴 Defensive (debit + worse strike)"

        rolls_out.append({
            "Date": date_d,
            "Ticker": tkr,
            "Type": "CSP" if right == "P" else "CC",
            "Qty": btc_qty,
            "Old Strike": old_strike,
            "Old Expiry": old_expiry,
            "Old Premium": btc_premium,
            "New Strike": new_strike,
            "New Expiry": new_expiry,
            "New Premium": sto_premium,
            "Net Cash $": net_cash,
            "Strike Δ": strike_delta,
            "DTE Δ": dte_delta,
            "Roll Quality": quality,
        })

    if not rolls_out:
        return pd.DataFrame()
    rdf = pd.DataFrame(rolls_out)

    # Apply date filter
    if start_date is not None or end_date is not None:
        rdf["_date_dt"] = pd.to_datetime(rdf["Date"], errors="coerce")
        if start_date is not None:
            rdf = rdf[rdf["_date_dt"] >= pd.Timestamp(start_date)]
        if end_date is not None:
            # Include the end date by adding 1 day (since rolls are date-only)
            rdf = rdf[rdf["_date_dt"] <= pd.Timestamp(end_date) + pd.Timedelta(days=1)]
        rdf = rdf.drop(columns=["_date_dt"])

    return rdf.sort_values("Date", ascending=False).reset_index(drop=True)


def roll_summary(rolls_df: pd.DataFrame) -> dict:
    out = {
        "total_rolls": 0,
        "credit_rolls": 0,
        "debit_rolls": 0,
        "total_net_credit": 0.0,
        "avg_credit_per_roll": 0.0,
        "best_roll": 0.0,
        "worst_roll": 0.0,
    }
    if rolls_df is None or rolls_df.empty:
        return out
    out["total_rolls"] = len(rolls_df)
    credit = rolls_df[rolls_df["Net Cash $"] > 0]
    debit = rolls_df[rolls_df["Net Cash $"] <= 0]
    out["credit_rolls"] = len(credit)
    out["debit_rolls"] = len(debit)
    out["total_net_credit"] = float(rolls_df["Net Cash $"].sum())
    out["avg_credit_per_roll"] = float(rolls_df["Net Cash $"].mean() or 0)
    out["best_roll"] = float(rolls_df["Net Cash $"].max() or 0)
    out["worst_roll"] = float(rolls_df["Net Cash $"].min() or 0)
    return out
