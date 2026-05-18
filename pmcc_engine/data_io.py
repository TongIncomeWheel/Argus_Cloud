"""Data I/O — daily bars, VIX, and option chain wrappers.

Sits on top of ARGUS' existing data clients (yfinance / Alpaca / Tiger) so the
engine never reaches out to a network directly. All loaders are Streamlit-cached
where it matters (daily bars, VIX) but cache hooks are kept optional so the
modules remain importable in non-Streamlit contexts (tests).
"""
from __future__ import annotations

import logging
import math
from typing import Iterable, Optional, Sequence

logger = logging.getLogger(__name__)


# ─── Caching shim ──────────────────────────────────────────────────
# In a Streamlit context we want @st.cache_data on the heavy fetchers, but the
# math modules need to be testable without streamlit installed. The shim below
# falls back to a no-op decorator if streamlit is unavailable.
try:
    import streamlit as st
    _cache_data = st.cache_data
except Exception:  # pragma: no cover - non-Streamlit environment
    def _cache_data(*args, **kwargs):
        def deco(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return deco


# ─── Daily bars (for HV30, MAs, history) ───────────────────────────


def _yfinance_daily_closes(ticker: str, period: str = "120d") -> list:
    """Pull daily closes via yfinance. ≥31 closes needed for HV30."""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not installed — HV30 unavailable")
        return []
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period=period, auto_adjust=False)
        if hist.empty or "Close" not in hist.columns:
            return []
        return [float(x) for x in hist["Close"].dropna().tolist()]
    except Exception as e:
        logger.warning("yfinance daily fetch failed for %s: %s", ticker, e)
        return []


@_cache_data(ttl=3600, show_spinner=False)
def daily_closes(ticker: str, period: str = "120d") -> list:
    """Cached daily closes for a ticker (1-hour TTL).

    Returns a list of floats, oldest → newest. Empty if fetch fails.
    """
    return _yfinance_daily_closes(ticker.upper(), period=period)


def hv30_from_ticker(ticker: str) -> Optional[float]:
    """Compute HV30 for a ticker via yfinance. Returns annualized decimal or None."""
    from .theta_math import compute_hv30
    closes = daily_closes(ticker, period="120d")
    return compute_hv30(closes)


def moving_average(ticker: str, window: int = 20) -> Optional[float]:
    """Simple N-day moving average of close."""
    closes = daily_closes(ticker, period="120d")
    if len(closes) < window:
        return None
    return sum(closes[-window:]) / float(window)


# ─── VIX ───────────────────────────────────────────────────────────


@_cache_data(ttl=300, show_spinner=False)
def get_vix() -> Optional[float]:
    """Current VIX level via yfinance (^VIX). 5-minute cache."""
    try:
        import yfinance as yf
    except ImportError:
        return None
    try:
        v = yf.Ticker("^VIX")
        hist = v.history(period="5d")
        if hist.empty:
            return None
        return float(hist["Close"].iloc[-1])
    except Exception as e:
        logger.warning("VIX fetch failed: %s", e)
        return None


@_cache_data(ttl=3600, show_spinner=False)
def vix_history(period: str = "1y") -> list:
    """VIX daily closes for the trailing period. Used by IVR proxy when needed."""
    try:
        import yfinance as yf
    except ImportError:
        return []
    try:
        v = yf.Ticker("^VIX")
        hist = v.history(period=period)
        return [float(x) for x in hist["Close"].dropna().tolist()]
    except Exception as e:
        logger.warning("VIX history fetch failed: %s", e)
        return []


# ─── IV30 / RV30 history (for IVR) ─────────────────────────────────


@_cache_data(ttl=3600, show_spinner=False)
def rv30_history(ticker: str, lookback_days: int = 252) -> list:
    """Trailing rolling 30-day annualized realized vol series — used as IV proxy.

    We compute this on the fly from daily closes since Alpaca's free tier doesn't
    expose 52-week IV history per ticker. Same approach as ARGUS' existing
    iv_scanner module — kept consistent.
    """
    closes = daily_closes(ticker, period="2y")
    if len(closes) < 60:
        return []
    # rolling 30-day std of log returns × √252
    import math
    log_returns = []
    for i in range(1, len(closes)):
        if closes[i - 1] <= 0 or closes[i] <= 0:
            continue
        log_returns.append(math.log(closes[i] / closes[i - 1]))
    rv = []
    for i in range(30, len(log_returns) + 1):
        window = log_returns[i - 30:i]
        mean = sum(window) / 30.0
        variance = sum((r - mean) ** 2 for r in window) / 29.0
        rv.append(math.sqrt(variance) * math.sqrt(252) * 100)
    return rv[-lookback_days:]


def current_iv_signal(ticker: str, vol_axis: str = "VIX") -> Optional[float]:
    """Return the current vol level appropriate for the regime grid's vol axis.

    For ETFs that track the broad market, VIX is the doctrine-prescribed axis.
    For single stocks, use the ticker's RV30 (HV30 ×100) as the IV30 proxy
    (Alpaca free tier doesn't give 52w IV history).
    """
    axis = (vol_axis or "VIX").upper()
    if axis == "VIX":
        return get_vix()
    hv = hv30_from_ticker(ticker)
    return hv * 100.0 if hv else None


def ivr_for_ticker(ticker: str, vol_axis: str = "VIX") -> Optional[float]:
    """52-week IVR (or RV-rank proxy) for a ticker."""
    from .regime import compute_ivr_52w
    axis = (vol_axis or "VIX").upper()
    if axis == "VIX":
        series = vix_history(period="1y")
        current = get_vix()
    else:
        series = rv30_history(ticker, lookback_days=252)
        current = series[-1] if series else None
    return compute_ivr_52w(current, series) if current is not None else None


# ─── Option chain pull (Alpaca-backed) ─────────────────────────────


@_cache_data(ttl=120, show_spinner="📈 Fetching option chain (Alpaca)…")
def load_chain(ticker: str, expiry_from: str, expiry_to: str,
               strike_min: float, strike_max: float,
               option_type: str = "call") -> list:
    """Fetch an option chain from Alpaca within a strike + expiry window.

    Returns list of rows: {symbol, strike, expiry, type, mid, bid, ask, last,
    iv, delta, gamma, theta, vega, open_interest, dte}.
    """
    import os
    from datetime import datetime
    api_key = os.getenv("ALPACA_API_KEY")
    secret = os.getenv("ALPACA_SECRET_KEY")
    if not api_key or not secret:
        return []
    try:
        from alpaca.data.historical import OptionHistoricalDataClient
        from alpaca.data.requests import OptionChainRequest
        client = OptionHistoricalDataClient(api_key, secret)
        # Alpaca expects ContractType enum for some clients; the param name varies
        # by SDK version. We pass type as kwarg defensively.
        req = OptionChainRequest(
            underlying_symbol=ticker.upper(),
            expiration_date_gte=expiry_from,
            expiration_date_lte=expiry_to,
            strike_price_gte=str(strike_min),
            strike_price_lte=str(strike_max),
        )
        chain = client.get_option_chain(req)
    except Exception as e:
        logger.warning("Alpaca chain fetch failed for %s: %s", ticker, e)
        return []

    today = datetime.utcnow().date()
    rows = []
    for occ_sym, snap in (chain or {}).items():
        # Filter by option type
        try:
            cp = occ_sym[-9]  # 'C' or 'P' before strike digits
            if option_type and option_type.lower() == "call" and cp.upper() != "C":
                continue
            if option_type and option_type.lower() == "put" and cp.upper() != "P":
                continue
            strike_raw = int(occ_sym[-8:])
            strike = strike_raw / 1000.0
            exp_raw = occ_sym[-15:-9]   # YYMMDD
            exp_date = datetime.strptime("20" + exp_raw, "%Y%m%d").date()
        except (ValueError, IndexError):
            continue

        lq = getattr(snap, "latest_quote", None)
        lt = getattr(snap, "latest_trade", None)
        greeks = getattr(snap, "greeks", None)
        bid = float(lq.bid_price) if lq and lq.bid_price else None
        ask = float(lq.ask_price) if lq and lq.ask_price else None
        last = float(lt.price) if lt and lt.price else None
        mid = (bid + ask) / 2.0 if bid and ask else (last or 0.0)
        iv = float(snap.implied_volatility) if getattr(snap, "implied_volatility", None) else None
        oi = float(snap.open_interest) if getattr(snap, "open_interest", None) else None

        rows.append({
            "symbol": occ_sym,
            "strike": strike,
            "expiry": exp_date.isoformat(),
            "type": cp.upper(),
            "mid": mid,
            "bid": bid,
            "ask": ask,
            "last": last,
            "iv": iv,
            "delta": float(greeks.delta) if greeks and greeks.delta is not None else None,
            "gamma": float(greeks.gamma) if greeks and greeks.gamma is not None else None,
            "theta": float(greeks.theta) if greeks and greeks.theta is not None else None,
            "vega": float(greeks.vega) if greeks and greeks.vega is not None else None,
            "open_interest": oi,
            "dte": (exp_date - today).days,
        })
    return rows
