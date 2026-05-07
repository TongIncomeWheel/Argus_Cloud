"""IV Rank / Percentile scanner — proxy via realized vol.

Tasty/wheel literature: sell premium when IV is HIGH relative to its own
recent history (IV Rank > 50). Don't sell when IV is depressed (IV Rank < 30).

We don't have a clean historical-IV feed (Alpaca's option snapshot has CURRENT
IV but not 52-week IV history per ticker). Workaround: use realized
volatility from daily price returns as a proxy. The two are highly correlated;
RV-rank tracks IV-rank closely on liquid underlyings.

Computation per ticker:
  1. Pull 2 years of daily closes from yfinance
  2. Daily returns → rolling 30-day std × √252 = annualized RV30
  3. Last 252 sessions: high, low, current
  4. IV Rank = (current − 52w low) / (52w high − 52w low) × 100
  5. IV Percentile = % of 52w sessions where RV30 was below current
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)


def _compute_iv_proxy(ticker: str) -> Optional[dict]:
    """Compute IV Rank/Percentile proxy for one ticker via realized vol."""
    try:
        import yfinance as yf
    except ImportError:
        return None
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="2y")
        if hist.empty or "Close" not in hist.columns:
            return None
        hist = hist.copy()
        hist["ret"] = hist["Close"].pct_change()
        # Annualized 30-day rolling realized vol
        hist["rv30"] = hist["ret"].rolling(30).std() * (252 ** 0.5) * 100
        last252 = hist["rv30"].tail(252).dropna()
        if len(last252) < 30:
            return None
        current = float(last252.iloc[-1])
        hi = float(last252.max())
        lo = float(last252.min())
        rv_rank = (current - lo) / (hi - lo) * 100 if hi > lo else 50.0
        rv_pct = float((last252 < current).sum() / len(last252) * 100)
        return {
            "ticker": ticker.upper(),
            "rv30_current": current,
            "rv30_52w_high": hi,
            "rv30_52w_low": lo,
            "iv_rank_proxy": float(rv_rank),
            "iv_percentile_proxy": rv_pct,
            "spot": float(hist["Close"].iloc[-1]),
        }
    except Exception as e:
        logger.debug("IV proxy failed for %s: %s", ticker, e)
        return None


@st.cache_data(ttl=3600, show_spinner="📊 Computing IV Rank proxy via realized vol…")
def build_iv_scanner(tickers_tuple: tuple) -> pd.DataFrame:
    """Build IV Rank/Percentile table for the given tickers.

    Cached 1hr — IV doesn't move minute-by-minute and yfinance is rate-limited.
    """
    if not tickers_tuple:
        return pd.DataFrame()
    rows = []
    for tkr in tickers_tuple:
        r = _compute_iv_proxy(str(tkr).upper())
        if r:
            rows.append(r)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df.sort_values("iv_rank_proxy", ascending=False).reset_index(drop=True)


def regime_label(iv_rank: float) -> str:
    """Map IV Rank to a wheel-trading regime label."""
    if iv_rank is None: return "—"
    if iv_rank >= 70: return "🟢 SELL aggressively (high IV)"
    if iv_rank >= 50: return "🟡 OK to sell (mid IV)"
    if iv_rank >= 30: return "⚪ Neutral (avoid full size)"
    return "🔴 AVOID selling premium (low IV)"
