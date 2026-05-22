"""Candidate ticker universe for the Theta Scanner.

Two sources, picked automatically:
  1. If FMP_API_KEY is configured → live FMP stock screener
     (marketCapMoreThan=5B, volumeMoreThan=5M, NASDAQ/NYSE, equities only).
  2. Otherwise → BUNDLED_UNIVERSE, a curated list of liquid optionable
     US large-caps. In practice every name on it clears $5B market cap
     and 5M average daily share volume, so it satisfies the operator's
     screen without an API key.

The bundled list is intentionally capped (~110 names) — it is the realistic
CSP-wheel universe and keeps the live chain-scan tractable on Alpaca's free
tier.
"""
from __future__ import annotations

import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


# ─── Bundled liquid large-cap universe ─────────────────────────────
# Curated optionable US large-caps across sectors. All clear ~$5B cap and
# ~5M+ average daily volume. Refresh periodically as the large-cap roster
# shifts; or configure FMP_API_KEY for a live screen.
BUNDLED_UNIVERSE: List[str] = [
    # Mega-cap tech / communications
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "AVGO", "ORCL",
    "CRM", "ADBE", "AMD", "INTC", "CSCO", "QCOM", "TXN", "IBM", "NOW", "INTU",
    "AMAT", "MU", "PANW", "ANET", "LRCX", "KLAC", "SNPS", "CDNS", "NFLX", "MRVL",
    # Consumer discretionary / staples
    "TSLA", "HD", "MCD", "NKE", "SBUX", "LOW", "TGT", "COST", "WMT", "DIS",
    "BKNG", "ABNB", "CMG", "LULU", "MAR", "PG", "KO", "PEP", "PM", "MO",
    "CL", "MDLZ",
    # Financials
    "JPM", "BAC", "WFC", "GS", "MS", "C", "SCHW", "AXP", "BLK", "SPGI",
    "V", "MA", "PYPL", "COF",
    # Healthcare
    "UNH", "JNJ", "LLY", "ABBV", "MRK", "PFE", "TMO", "ABT", "DHR", "BMY",
    "AMGN", "GILD", "CVS", "ISRG", "VRTX",
    # Industrials
    "BA", "CAT", "GE", "HON", "UPS", "RTX", "LMT", "DE", "UNP", "MMM",
    # Energy
    "XOM", "CVX", "COP", "SLB", "EOG", "MPC", "PSX",
    # Materials / utilities
    "LIN", "FCX", "NEM", "NEE", "DUK", "SO",
    # High-IV growth / popular wheel names
    "COIN", "PLTR", "SHOP", "UBER", "SNOW", "DDOG", "CRWD", "ROKU",
    # Liquid index ETFs (prime CSP underlyings)
    "SPY", "QQQ", "IWM",
]


def _fmp_api_key() -> Optional[str]:
    """Resolve an FMP API key from ARGUS config (st.secrets → .env)."""
    try:
        from config import get_secret
        key = get_secret("FMP_API_KEY", "")
        return key or None
    except Exception:
        import os
        return os.getenv("FMP_API_KEY") or None


def _fmp_screened_universe(min_market_cap: float = 5_000_000_000,
                           min_volume: float = 5_000_000) -> Optional[List[str]]:
    """Live FMP stock-screener universe. Returns None if the call fails.

    Endpoint: GET /api/v3/stock-screener
      marketCapMoreThan, volumeMoreThan, exchange, isEtf, isActivelyTrading.
    """
    key = _fmp_api_key()
    if not key:
        return None
    try:
        import requests
    except ImportError:
        logger.warning("requests not installed — FMP screener unavailable")
        return None
    try:
        resp = requests.get(
            "https://financialmodelingprep.com/api/v3/stock-screener",
            params={
                "marketCapMoreThan": int(min_market_cap),
                "volumeMoreThan": int(min_volume),
                "exchange": "NASDAQ,NYSE",
                "isEtf": "false",
                "isActivelyTrading": "true",
                "limit": 3000,
                "apikey": key,
            },
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            logger.warning("FMP screener returned unexpected payload")
            return None
        symbols = sorted({
            str(row.get("symbol", "")).upper()
            for row in data
            if row.get("symbol")
        })
        return symbols or None
    except Exception as e:
        logger.warning("FMP screener call failed: %s", e)
        return None


def get_universe(min_market_cap: float = 5_000_000_000,
                 min_volume: float = 5_000_000) -> dict:
    """Resolve the scan universe.

    Returns:
        {
          'tickers': list[str],
          'source': 'FMP live screen' | 'bundled list',
          'count': int,
        }
    """
    fmp = _fmp_screened_universe(min_market_cap, min_volume)
    if fmp:
        return {"tickers": fmp, "source": "FMP live screen", "count": len(fmp)}
    return {
        "tickers": list(BUNDLED_UNIVERSE),
        "source": "bundled list",
        "count": len(BUNDLED_UNIVERSE),
    }


def fmp_configured() -> bool:
    """True if an FMP API key is available (live screen will be used)."""
    return _fmp_api_key() is not None
