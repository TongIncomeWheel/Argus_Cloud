"""Underlying fundamentals for the Scanner.

Two paths, picked automatically:

  - FMP_API_KEY configured → batched `/quote` + `/profile` for the bulk,
    plus a concurrent per-symbol enrichment pass for the three fields
    those batched endpoints don't carry (forward P/E, short float,
    analyst rating). Forward P/E uses the nearest forward fiscal-year
    EPS from FMP's analyst-estimates; analyst rating uses FMP's
    ratings-snapshot; short float falls back to yfinance because FMP
    doesn't expose it cleanly on v3.

  - otherwise → yfinance `.info` per ticker. Slower, but covers every
    field in one shot.

Every field is optional — a missing value surfaces as None and the
matching column / filter degrades gracefully rather than dropping rows.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import Optional

from .universe import _fmp_api_key, fmp_configured

logger = logging.getLogger(__name__)

_FMP_BASE = "https://financialmodelingprep.com/api/v3"
_CHUNK = 50           # symbols per batched FMP request
_ENRICH_WORKERS = 8   # concurrent per-symbol enrichment requests

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


def _num(value) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


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


# ─── Pure parsers (unit-testable) ──────────────────────────────────


def _pick_forward_eps(rows: list, today: date) -> Optional[float]:
    """Nearest forward-FY consensus EPS from an analyst-estimates response.

    Accepts both the v3 (`estimatedEpsAvg`) and stable (`epsAvg`) field names.
    """
    candidates: list = []
    for row in rows or []:
        try:
            rdate = date.fromisoformat(str(row.get("date", ""))[:10])
        except (ValueError, TypeError):
            continue
        if rdate <= today:
            continue
        eps = _num(row.get("estimatedEpsAvg") or row.get("epsAvg"))
        if eps and eps > 0:
            candidates.append((rdate, eps))
    if not candidates:
        return None
    candidates.sort()
    return candidates[0][1]


def _normalize_yf_rating(val) -> Optional[float]:
    """yfinance recommendationMean: 1=Strong Buy, 5=Sell. Invert so 5=best."""
    v = _num(val)
    if v is None or v < 1 or v > 5:
        return None
    return 6.0 - v


# ─── FMP batched calls (quote + profile) ───────────────────────────


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
def _fmp_quote(tickers_tuple: tuple) -> dict:
    key = _fmp_api_key()
    if not key:
        return {}
    return _fmp_batch("quote", [t.upper() for t in tickers_tuple], key)


@_cache_data(ttl=3600, show_spinner=False)
def _fmp_profile(tickers_tuple: tuple) -> dict:
    key = _fmp_api_key()
    if not key:
        return {}
    return _fmp_batch("profile", [t.upper() for t in tickers_tuple], key)


@_cache_data(ttl=3600, show_spinner=False)
def _fmp_fundamentals(tickers_tuple: tuple) -> dict:
    if not _fmp_api_key():
        return {}
    quotes = _fmp_quote(tickers_tuple)
    profiles = _fmp_profile(tickers_tuple)

    out: dict = {}
    for sym in (t.upper() for t in tickers_tuple):
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


# ─── Per-symbol enrichment (forward P/E, rating, short float) ──────


_FMP_STABLE = "https://financialmodelingprep.com/stable"


@_cache_data(ttl=3600, show_spinner=False)
def _fmp_forward_eps(symbol: str) -> Optional[float]:
    """Nearest forward-FY consensus EPS — tries v3, then the stable endpoint."""
    key = _fmp_api_key()
    if not key:
        return None
    import requests
    today = date.today()
    attempts = [
        (f"{_FMP_BASE}/analyst-estimates/{symbol}",
         {"apikey": key, "period": "annual", "limit": 8}),
        (f"{_FMP_STABLE}/financial-estimates",
         {"apikey": key, "symbol": symbol, "period": "annual", "limit": 8}),
    ]
    for url, params in attempts:
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            eps = _pick_forward_eps(resp.json() or [], today)
            if eps is not None:
                return eps
        except Exception as e:
            logger.debug("FMP estimates %s failed: %s", url, e)
    return None


@_cache_data(ttl=3600, show_spinner=False)
def _fmp_rating_score(symbol: str) -> Optional[float]:
    """FMP rating, 1-5 scale (higher = stronger). Tries v3 then stable."""
    key = _fmp_api_key()
    if not key:
        return None
    import requests
    attempts = [
        (f"{_FMP_BASE}/rating/{symbol}", {"apikey": key}),
        (f"{_FMP_STABLE}/ratings-snapshot", {"apikey": key, "symbol": symbol}),
    ]
    for url, params in attempts:
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json() or []
            if data:
                # v3: ratingScore · stable: overallScore — accept either.
                v = _num(data[0].get("ratingScore") or data[0].get("overallScore"))
                if v is not None:
                    return v
        except Exception as e:
            logger.debug("FMP rating %s failed: %s", url, e)
    return None


@_cache_data(ttl=3600, show_spinner=False)
def _yf_short_pct(symbol: str) -> Optional[float]:
    """Short interest as a percent of float, via yfinance (FMP gap)."""
    try:
        import yfinance as yf
        sf = (yf.Ticker(symbol).info or {}).get("shortPercentOfFloat")
        sf = _num(sf)
        if sf is None:
            return None
        return sf * 100.0 if sf < 1 else sf
    except Exception as e:
        logger.debug("yfinance short_float failed for %s: %s", symbol, e)
        return None


@_cache_data(ttl=3600, show_spinner=False)
def _fmp_enrichment(tickers_tuple: tuple) -> dict:
    """Per-symbol enrichment running concurrently across tickers."""
    if not _fmp_api_key():
        return {}
    symbols = [t.upper() for t in tickers_tuple]
    prices = _fmp_quote(tickers_tuple)  # cache-hit when called after _fmp_fundamentals

    def _one(sym: str) -> tuple:
        fwd_eps = _fmp_forward_eps(sym)
        price = _num((prices.get(sym) or {}).get("price"))
        fwd_pe = (price / fwd_eps) if (price and fwd_eps and fwd_eps > 0) else None
        return sym, {
            "fwd_pe": fwd_pe,
            "analyst_rating": _fmp_rating_score(sym),
            "short_float": _yf_short_pct(sym),
        }

    out: dict = {}
    with ThreadPoolExecutor(max_workers=_ENRICH_WORKERS) as ex:
        for sym, rec in ex.map(_one, symbols):
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
    rec["short_float"] = sf * 100.0 if sf is not None and sf < 1 else sf
    rec["analyst_rating"] = _normalize_yf_rating(info.get("recommendationMean"))
    rec["dividend"] = _num(info.get("dividendRate"))
    dy = _num(info.get("dividendYield"))
    if dy is not None:
        rec["div_yield"] = dy * 100.0 if dy < 1 else dy
    rec["ex_div_date"] = _parse_date(info.get("exDividendDate"))
    rec["earnings_date"] = _parse_date(info.get("earningsTimestamp"))
    return rec


# ─── Public entry point ────────────────────────────────────────────


def load_fundamentals(tickers_tuple: tuple, progress_cb=None) -> dict:
    """Fundamentals for many tickers: {ticker: fundamentals dict}.

    Uses the batched FMP path + concurrent enrichment when a key is
    configured, else loops yfinance.
    """
    symbols = [t.upper() for t in tickers_tuple]
    if fmp_configured():
        if progress_cb:
            progress_cb(0, len(symbols), "FMP quote + profile")
        base = _fmp_fundamentals(tuple(symbols))
        if progress_cb:
            progress_cb(len(symbols) // 2, len(symbols), "FMP enrichment")
        enrich = _fmp_enrichment(tuple(symbols))
        for sym in symbols:
            rec = base.get(sym, _blank())
            e = enrich.get(sym, {})
            if e.get("fwd_pe") is not None:
                rec["fwd_pe"] = e["fwd_pe"]
            if e.get("analyst_rating") is not None:
                rec["analyst_rating"] = e["analyst_rating"]
            if e.get("short_float") is not None:
                rec["short_float"] = e["short_float"]
            base[sym] = rec
        if progress_cb:
            progress_cb(len(symbols), len(symbols), "FMP complete")
        return {s: base.get(s, _blank()) for s in symbols}

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


def clear_caches() -> None:
    """Drop every cached entry — called by the UI's 'Load new data' button."""
    for fn in (_fmp_quote, _fmp_profile, _fmp_fundamentals,
               _fmp_forward_eps, _fmp_rating_score, _yf_short_pct,
               _fmp_enrichment, _yf_fundamentals):
        try:
            fn.clear()
        except Exception:
            pass
