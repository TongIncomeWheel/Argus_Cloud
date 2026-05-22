"""Data layer for the Theta Scanner — Alpaca put chains + batch spot.

Self-contained: reuses ARGUS' shared spot loader (tiger_api.tiger_data) but
keeps its own focused Alpaca put-chain fetcher so the module has no dependency
on pmcc_engine.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import List, Optional

logger = logging.getLogger(__name__)


# ─── Streamlit cache shim (importable without Streamlit, e.g. tests) ──
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


@_cache_data(ttl=120, show_spinner=False)
def batch_spot_prices(tickers_tuple: tuple) -> dict:
    """Spot prices for many tickers in one batched call.

    Delegates to ARGUS' shared multi-source loader (yfinance → Alpaca → Tiger).
    """
    if not tickers_tuple:
        return {}
    try:
        from tiger_api import tiger_data
        return tiger_data.load_spot_prices(tuple(tickers_tuple))
    except Exception as e:
        logger.warning("batch_spot_prices failed: %s", e)
        return {}


@_cache_data(ttl=120, show_spinner=False)
def load_put_chain(ticker: str, expiry_from: str, expiry_to: str,
                   strike_min: float, strike_max: float) -> list:
    """Fetch a put-option chain from Alpaca within a strike + expiry window.

    Returns list of rows: {symbol, strike, expiry, dte, mid, bid, ask, last,
    iv, delta, open_interest}. Empty list on failure / missing credentials.
    """
    api_key = os.getenv("ALPACA_API_KEY")
    secret = os.getenv("ALPACA_SECRET_KEY")
    if not api_key or not secret:
        logger.warning("Alpaca credentials missing — put chain unavailable")
        return []
    try:
        from alpaca.data.historical import OptionHistoricalDataClient
        from alpaca.data.requests import OptionChainRequest
        client = OptionHistoricalDataClient(api_key, secret)
        req = OptionChainRequest(
            underlying_symbol=ticker.upper(),
            expiration_date_gte=expiry_from,
            expiration_date_lte=expiry_to,
            strike_price_gte=str(strike_min),
            strike_price_lte=str(strike_max),
        )
        chain = client.get_option_chain(req)
    except Exception as e:
        logger.warning("Alpaca put-chain fetch failed for %s: %s", ticker, e)
        return []

    today = datetime.utcnow().date()
    rows = []
    for occ_sym, snap in (chain or {}).items():
        try:
            cp = occ_sym[-9]                       # 'C' or 'P'
            if cp.upper() != "P":
                continue
            strike = int(occ_sym[-8:]) / 1000.0
            exp_date = datetime.strptime("20" + occ_sym[-15:-9], "%Y%m%d").date()
        except (ValueError, IndexError):
            continue

        lq = getattr(snap, "latest_quote", None)
        lt = getattr(snap, "latest_trade", None)
        greeks = getattr(snap, "greeks", None)
        bid = float(lq.bid_price) if lq and lq.bid_price else None
        ask = float(lq.ask_price) if lq and lq.ask_price else None
        last = float(lt.price) if lt and lt.price else None
        mid = (bid + ask) / 2.0 if (bid and ask) else (last or None)
        iv = getattr(snap, "implied_volatility", None)
        oi = getattr(snap, "open_interest", None)

        rows.append({
            "symbol": occ_sym,
            "strike": strike,
            "expiry": exp_date.isoformat(),
            "dte": (exp_date - today).days,
            "mid": mid,
            "bid": bid,
            "ask": ask,
            "last": last,
            "iv": float(iv) if iv else None,
            "delta": float(greeks.delta) if greeks and greeks.delta is not None else None,
            "open_interest": float(oi) if oi else None,
        })
    return rows


def alpaca_configured() -> bool:
    """True if Alpaca credentials are present (chain pulls will work)."""
    return bool(os.getenv("ALPACA_API_KEY") and os.getenv("ALPACA_SECRET_KEY"))
