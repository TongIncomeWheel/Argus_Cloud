"""Win-rate analysis — classify closed trades by setup, compute win % per bucket.

For each CLOSED short option (CSP/CC) trade:
  - Bucket by Type × DTE-at-open × Premium-to-strike ratio (delta proxy)
  - Mark as Win if Actual_Profit_(USD) > 0, Loss otherwise
  - Aggregate to: count, win count, win %, avg profit per trade

Premium-to-strike ratio buckets approximate Delta at open:
  • premium/strike < 1%   → ~5-15Δ  (very OTM)
  • premium/strike < 2.5% → ~15-30Δ (OTM)
  • premium/strike < 5%   → ~30-45Δ (near ATM)
  • premium/strike >= 5%  → 45+ Δ   (ATM/ITM)

DTE buckets:
  • 0-7d (weeklies)
  • 8-21d (short)
  • 22-45d (Tasty sweet spot)
  • 46+d (long)
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def _dte_bucket(dte: float) -> str:
    if dte <= 7: return "0-7d"
    if dte <= 21: return "8-21d"
    if dte <= 45: return "22-45d"
    return "46+d"


def _moneyness_bucket(prem: float, strike: float) -> str:
    """Approximates delta at open by premium-to-strike ratio."""
    if strike <= 0 or prem <= 0:
        return "—"
    ratio = prem / strike
    if ratio < 0.01: return "Far OTM (~5-15Δ)"
    if ratio < 0.025: return "OTM (~15-30Δ)"
    if ratio < 0.05: return "Near ATM (~30-45Δ)"
    return "ATM/ITM (~45+Δ)"


def build_win_rate_table(df_orders: pd.DataFrame, ticker_filter: Optional[set] = None,
                         pot_tickers: Optional[set] = None,
                         start_date=None, end_date=None) -> dict:
    """Walk closed CSP/CC trades, attribute outcomes to setup buckets.

    Strategy:
      1. For each CLOSED leg (is_opening==False), find its OPENING leg via
         matching strike+expiry+ticker+type. Take DTE and premium from open.
      2. Bucket by Type × DTE × Moneyness.
      3. Win if Actual_Profit_(USD) > 0.

    Returns dict with:
      'by_type'  → DataFrame[Type, Trades, Wins, Win %, Avg P&L]
      'by_dte'   → DataFrame[DTE Bucket, Trades, Wins, Win %, Avg P&L]
      'by_money' → DataFrame[Moneyness Bucket, Trades, Wins, Win %, Avg P&L]
      'pivot'    → DataFrame[Type × DTE × Moneyness pivot of Win %]
    """
    out = {"by_type": pd.DataFrame(), "by_dte": pd.DataFrame(),
           "by_money": pd.DataFrame(), "pivot": pd.DataFrame()}
    if df_orders is None or df_orders.empty:
        return out

    df = df_orders.copy()
    df["TradeDateTime"] = pd.to_datetime(df.get("TradeDateTime", df.get("TradeDate")), errors="coerce")
    df["Expiry_Date"] = pd.to_datetime(df.get("Expiry_Date"), errors="coerce")

    if ticker_filter:
        df = df[df["Ticker"].isin(ticker_filter)]
    if pot_tickers:
        df = df[df["Ticker"].isin(pot_tickers)]

    # Only CSP/CC fills; LEAP/STOCK skipped
    df = df[df["TradeType"].isin(["CSP", "CC"])].copy()

    # Period filter: applied to CLOSING fills (when the trade was actually realized)
    if start_date is not None or end_date is not None:
        # Note: opens may fall before the period; what matters is when it closed
        # to count as a realized trade in the period. We DON'T filter df itself
        # (we still need opens for the matching join). Instead we filter the
        # `closes` set after the open lookup is built.
        pass
    if df.empty:
        return out

    # Closed = not opening. Each closed trade had an opening trade matching
    # same Ticker+Type+Strike+Expiry. We use those for setup attribution.
    df["is_opening"] = df["is_opening"].fillna(False)
    closes = df[~df["is_opening"].astype(bool)].copy()
    opens = df[df["is_opening"].astype(bool)].copy()

    # Apply period filter to CLOSES only — what matters is when the trade
    # realized P&L within the user's selected period.
    if start_date is not None:
        closes = closes[closes["TradeDateTime"] >= pd.Timestamp(start_date)]
    if end_date is not None:
        closes = closes[closes["TradeDateTime"] <= pd.Timestamp(end_date) + pd.Timedelta(days=1)]

    if closes.empty:
        return out

    # Index opens by (Ticker, Type, Strike, Expiry) → first matching open's DTE & premium
    open_idx = {}
    for _, r in opens.iterrows():
        try:
            k = (
                str(r["Ticker"]).upper(),
                r["TradeType"],
                round(float(r["Option_Strike_Price_(USD)"] or 0), 2),
                pd.to_datetime(r["Expiry_Date"], errors="coerce"),
            )
            if k in open_idx:
                continue
            open_dt = r["TradeDateTime"]
            exp_dt = r["Expiry_Date"]
            dte_at_open = (exp_dt - open_dt).days if (pd.notna(open_dt) and pd.notna(exp_dt)) else 0
            premium = float(r.get("FillPrice", 0) or 0)
            open_idx[k] = {"dte_at_open": max(dte_at_open, 0), "premium": abs(premium)}
        except (TypeError, ValueError, KeyError):
            continue

    # Classify each close
    rows = []
    for _, r in closes.iterrows():
        try:
            k = (
                str(r["Ticker"]).upper(),
                r["TradeType"],
                round(float(r["Option_Strike_Price_(USD)"] or 0), 2),
                pd.to_datetime(r["Expiry_Date"], errors="coerce"),
            )
            setup = open_idx.get(k)
            if not setup:
                continue
            dte = setup["dte_at_open"]
            prem = setup["premium"]
            strike = float(r["Option_Strike_Price_(USD)"] or 0)
            pnl = float(r.get("Actual_Profit_(USD)", 0) or 0)
            rows.append({
                "Ticker": k[0],
                "Type": k[1],
                "DTE Bucket": _dte_bucket(dte),
                "Moneyness Bucket": _moneyness_bucket(prem, strike),
                "P&L $": pnl,
                "Win": 1 if pnl > 0 else 0,
                "Trade": 1,
            })
        except (TypeError, ValueError, KeyError):
            continue
    if not rows:
        return out
    rdf = pd.DataFrame(rows)

    def _agg(group_col):
        g = rdf.groupby(group_col).agg(
            Trades=("Trade", "sum"),
            Wins=("Win", "sum"),
            **{"Total P&L": ("P&L $", "sum")},
        ).reset_index()
        g["Win %"] = (g["Wins"] / g["Trades"] * 100).round(0).astype(int)
        g["Avg per trade"] = (g["Total P&L"] / g["Trades"]).round(2)
        return g.sort_values("Trades", ascending=False)

    out["by_type"] = _agg("Type")
    out["by_dte"] = _agg("DTE Bucket")
    out["by_money"] = _agg("Moneyness Bucket")

    # Cross pivot: Type × DTE Bucket → Win %
    try:
        pvt = rdf.pivot_table(
            index="DTE Bucket", columns="Type",
            values=["Trades", "Wins"], aggfunc="sum", fill_value=0,
        )
        # Compute Win % from Trades + Wins
        if isinstance(pvt.columns, pd.MultiIndex):
            for ttype in ["CSP", "CC"]:
                if ("Trades", ttype) in pvt.columns and ("Wins", ttype) in pvt.columns:
                    pvt[("Win %", ttype)] = (
                        (pvt[("Wins", ttype)] / pvt[("Trades", ttype)] * 100)
                        .replace([float("inf"), float("-inf")], 0).fillna(0).round(0).astype(int)
                    )
        out["pivot"] = pvt
    except Exception as e:
        logger.debug("Pivot failed: %s", e)

    out["raw"] = rdf
    return out
