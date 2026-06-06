"""Data layer for the Scanner — option chains + batch spot prices.

Thin delegation layer: option chains reuse ARGUS's Alpaca-backed
`pmcc_engine.data_io.load_chain` (calls + puts, full greeks), and spot prices
reuse the shared multi-source `tiger_data.load_spot_prices`. Both are already
cached upstream.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def alpaca_configured() -> bool:
    """True if Alpaca credentials are present (chain pulls will work)."""
    return bool(os.getenv("ALPACA_API_KEY") and os.getenv("ALPACA_SECRET_KEY"))


def batch_spot_prices(tickers_tuple: tuple) -> dict:
    """Spot prices for many tickers via ARGUS's shared loader."""
    if not tickers_tuple:
        return {}
    try:
        from tiger_api import tiger_data
        return tiger_data.load_spot_prices(tuple(tickers_tuple))
    except Exception as e:
        logger.warning("batch_spot_prices failed: %s", e)
        return {}


def load_option_chain(ticker: str, option_type: str, expiry_from: str,
                      expiry_to: str, strike_min: float, strike_max: float) -> list:
    """Fetch a put or call option chain within a strike + expiry window.

    Delegates to pmcc_engine.data_io.load_chain. Returns list of contract
    dicts: {symbol, strike, expiry, type, mid, bid, ask, last, iv, delta,
    gamma, theta, vega, open_interest, dte}.
    """
    try:
        from pmcc_engine.data_io import load_chain
    except Exception as e:
        logger.warning("load_chain import failed: %s", e)
        return []
    leg = "put" if str(option_type).lower().startswith("p") else "call"
    try:
        return load_chain(
            ticker=ticker.upper(),
            expiry_from=expiry_from, expiry_to=expiry_to,
            strike_min=strike_min, strike_max=strike_max,
            option_type=leg,
        )
    except Exception as e:
        logger.warning("option chain fetch failed for %s: %s", ticker, e)
        return []


def load_earnings(tickers_tuple: tuple) -> dict:
    """Next-earnings dates via ARGUS's shared loader: {ticker: date|None}."""
    if not tickers_tuple:
        return {}
    try:
        from tiger_api import tiger_data
        return tiger_data.load_earnings_calendar(tuple(tickers_tuple))
    except Exception as e:
        logger.warning("load_earnings failed: %s", e)
        return {}
