"""Underlying fundamentals for the Scanner.

Two paths, picked automatically:
  - FMP_API_KEY configured → two batched FMP calls (quote + profile). Fast.
  - otherwise              → yfinance `.info` per ticker. Slower, but covers
                             more fields (forward P/E, short float, analyst
                             rating) and needs no key.

Every field is optional — a missing value surfaces as None and the matching
column / filter degrades gracefully rather than dropping rows.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

from .universe import _fmp_api_key, fmp_configured

logger = logging.getLogger(__name__)

_FMP_BASE = "https://financialmodelingprep.com/api/v3"
_CHUNK = 50  # symbols per batched FMP request

_BLANK_KEYS = (
    "market_cap", "sector", "is_etf", "pe", "fwd_pe", "eps_ttm", "beta",
    "short_float", "analyst_rating", "dividend", "div_yield", "ex_div_date",
    "earnings_date",
)


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


def _blank() -> dict:
    return {k: None for k in _BLANK_KEYS}


def _parse_date(value) -> Optional[date]:
    """Parse a date from an ISO string or epoch seconds."""
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.utcfromtimestamp(float(value)).date()
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except (ValueError, OSError, OverflowError):
        return None


def _num(value) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


# ─── FMP path ──────────────────────────────────────────────────────


def _fmp_batch(endpoint: str, symbols: list, key: str) -> dict:
    """Call a batched FMP endpoint, return {symbol: record}."""
    import requests
    out: dict = {}
    for i in range(0, len(symbols), _CHUNK):
        chunk = symbols[i:i + _CHUNK]
        try:
            resp = requests.get(
                f"{_FMP_BASE}/{endpoint}/{','.join(chunk)}",
                params={"apikey": key}, timeout=20,
            )
            resp.raise_for_status()
            for row in resp.json() or []:
                sym = str(row.get("symbol", "")).upper()
                if sym:
                    out[sym] = row
        except Exception as e:
            logger.warning("FMP %s batch failed: %s", endpoint, e)
    return out


@_cache_data(ttl=3600, show_spinner=False)
def _fmp_fundamentals(tickers_tuple: tuple) -> dict:
    key = _fmp_api_key()
    if not key:
        return {}
    symbols = [t.upper() for t in tickers_tuple]
    quotes = _fmp_batch("quote", symbols, key)
    profiles = _fmp_batch("profile", symbols, key)

    out: dict = {}
    for sym in symbols:
        rec = _blank()
        q = quotes.get(sym, {})
        p = profiles.get(sym, {})
        mc = _num(q.get("marketCap"))
        rec["market_cap"] = mc / 1e9 if mc else None
        rec["pe"] = _num(q.get("pe"))
        rec["eps_ttm"] = _num(q.get("eps"))
        rec["earnings_date"] = _parse_date(q.get("earningsAnnouncement"))
        rec["sector"] = p.get("sector") or None
        rec["beta"] = _num(p.get("beta"))
        rec["is_etf"] = bool(p.get("isEtf")) if p else None
        last_div = _num(p.get("lastDiv"))
        rec["dividend"] = last_div
        price = _num(q.get("price")) or _num(p.get("price"))
        if last_div and price:
            rec["div_yield"] = last_div / price * 100.0
        out[sym] = rec
    return out


# ─── yfinance fallback path ────────────────────────────────────────


@_cache_data(ttl=3600, show_spinner=False)
def _yf_fundamentals(ticker: str) -> dict:
    rec = _blank()
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info or {}
    except Exception as e:
        logger.warning("yfinance info failed for %s: %s", ticker, e)
        return rec
    mc = _num(info.get("marketCap"))
    rec["market_cap"] = mc / 1e9 if mc else None
    rec["sector"] = info.get("sector") or None
    rec["is_etf"] = str(info.get("quoteType", "")).upper() == "ETF"
    rec["pe"] = _num(info.get("trailingPE"))
    rec["fwd_pe"] = _num(info.get("forwardPE"))
    rec["eps_ttm"] = _num(info.get("trailingEps"))
    rec["beta"] = _num(info.get("beta"))
    sf = _num(info.get("shortPercentOfFloat"))
    rec["short_float"] = sf * 100.0 if sf is not None else None
    rec["analyst_rating"] = _num(info.get("recommendationMean"))
    rec["dividend"] = _num(info.get("dividendRate"))
    dy = _num(info.get("dividendYield"))
    # yfinance reports dividendYield as a fraction (0.012) — normalize to %.
    if dy is not None:
        rec["div_yield"] = dy * 100.0 if dy < 1 else dy
    rec["ex_div_date"] = _parse_date(info.get("exDividendDate"))
    rec["earnings_date"] = _parse_date(info.get("earningsTimestamp"))
    return rec


# ─── Public entry point ────────────────────────────────────────────


def load_fundamentals(tickers_tuple: tuple, progress_cb=None) -> dict:
    """Fundamentals for many tickers: {ticker: fundamentals dict}.

    Uses the batched FMP path when a key is configured, else yfinance.
    """
    symbols = [t.upper() for t in tickers_tuple]
    if fmp_configured():
        if progress_cb:
            progress_cb(0, len(symbols), "FMP batch")
        data = _fmp_fundamentals(tuple(symbols))
        return {s: data.get(s, _blank()) for s in symbols}

    out: dict = {}
    total = len(symbols)
    for i, sym in enumerate(symbols, start=1):
        out[sym] = _yf_fundamentals(sym)
        if progress_cb:
            progress_cb(i, total, sym)
    return out


def source_label() -> str:
    """Human-readable description of the active fundamentals source."""
    return "FMP" if fmp_configured() else "yfinance"
