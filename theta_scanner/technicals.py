"""Technical indicators for the Scanner — computed from daily OHLCV.

ARGUS's shared `daily_closes` loader returns closes only; ATR needs highs and
lows, so this module pulls full OHLCV via yfinance and caches per ticker.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Trading-day lookbacks for performance windows.
_WEEK, _MONTH, _QUARTER, _YEAR = 5, 21, 63, 252


# ─── Streamlit cache shim (importable without a Streamlit runtime) ──
try:
    import streamlit as st
    _cache_data = st.cache_data
except Exception:  # pragma: no cover
    def _cache_data(*args, **kwargs):
        def deco(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return deco


def _rsi(closes, period: int = 14) -> Optional[float]:
    """Wilder's RSI of the latest bar."""
    import pandas as pd
    s = pd.Series(closes, dtype="float64")
    if len(s) < period + 1:
        return None
    delta = s.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    last_gain = avg_gain.iloc[-1]
    last_loss = avg_loss.iloc[-1]
    if last_loss == 0:
        return 100.0
    rs = last_gain / last_loss
    return float(100.0 - 100.0 / (1.0 + rs))


def _atr(high, low, close, period: int = 14) -> Optional[float]:
    """Wilder's Average True Range of the latest bar."""
    import pandas as pd
    h, lo, c = (pd.Series(x, dtype="float64") for x in (high, low, close))
    if len(c) < period + 1:
        return None
    prev_close = c.shift(1)
    tr = pd.concat([
        h - lo,
        (h - prev_close).abs(),
        (lo - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    val = atr.iloc[-1]
    return float(val) if val == val else None  # NaN guard


def _perf(closes, lookback: int) -> Optional[float]:
    """Percent change over `lookback` trading days (clamped to history)."""
    if len(closes) < 2:
        return None
    n = min(lookback, len(closes) - 1)
    past = closes[-1 - n]
    if not past:
        return None
    return float((closes[-1] / past - 1.0) * 100.0)


@_cache_data(ttl=3600, show_spinner=False)
def compute_technicals(ticker: str) -> dict:
    """RSI, ATR, moving averages, performance and volume for one ticker.

    Returns a dict; any field may be None when history is too short or the
    fetch fails.
    """
    blank = {k: None for k in (
        "price", "pct_change", "rsi", "atr", "ma20", "ma50", "ma200",
        "perf_week", "perf_month", "perf_quarter", "perf_year",
        "avg_vol", "day_vol",
    )}
    try:
        import yfinance as yf
        hist = yf.Ticker(ticker).history(period="1y", auto_adjust=False)
    except Exception as e:
        logger.warning("technicals fetch failed for %s: %s", ticker, e)
        return blank
    if hist is None or hist.empty or "Close" not in hist:
        return blank

    closes = [float(x) for x in hist["Close"].dropna().tolist()]
    if len(closes) < 2:
        return blank
    highs = [float(x) for x in hist["High"].tolist()]
    lows = [float(x) for x in hist["Low"].tolist()]
    vols = [float(x) for x in hist["Volume"].fillna(0).tolist()]

    import pandas as pd
    cs = pd.Series(closes, dtype="float64")

    def _ma(n):
        return float(cs.tail(n).mean()) if len(cs) >= n else None

    return {
        "price": closes[-1],
        "pct_change": float((closes[-1] / closes[-2] - 1.0) * 100.0) if closes[-2] else None,
        "rsi": _rsi(closes),
        "atr": _atr(highs, lows, closes),
        "ma20": _ma(20),
        "ma50": _ma(50),
        "ma200": _ma(200),
        "perf_week": _perf(closes, _WEEK),
        "perf_month": _perf(closes, _MONTH),
        "perf_quarter": _perf(closes, _QUARTER),
        "perf_year": _perf(closes, _YEAR),
        "avg_vol": (sum(vols[-30:]) / len(vols[-30:]) / 1e6) if vols else None,
        "day_vol": (vols[-1] / 1e6) if vols else None,
    }


def load_technicals(tickers_tuple: tuple, progress_cb=None) -> dict:
    """Technicals for many tickers: {ticker: technicals dict}."""
    out: dict = {}
    total = len(tickers_tuple)
    for i, t in enumerate(tickers_tuple, start=1):
        out[t] = compute_technicals(t)
        if progress_cb:
            progress_cb(i, total, t)
    return out


def clear_caches() -> None:
    """Drop every cached entry — called by the UI's 'Load new data' button."""
    try:
        compute_technicals.clear()
    except Exception:
        pass
