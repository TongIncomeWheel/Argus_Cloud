"""
price_feed.py — legacy entry point (thin adapter).

All market data logic now lives in market_data/service.py.
This file exists solely for backward compatibility — existing imports in
app.py (get_cached_prices, PriceFeed, display_price_status) continue to
work without any changes.

To access options data or historical OHLCV, import MarketDataService directly:
    from market_data import MarketDataService
"""
import logging
import streamlit as st
from typing import Dict, Optional

from market_data import MarketDataService

logger = logging.getLogger(__name__)

# Single shared instance — avoids re-initialising Alpaca client on every call
_service = MarketDataService()


# ---------------------------------------------------------------------------
# Drop-in replacement for get_cached_prices()
# ---------------------------------------------------------------------------

def get_cached_prices(tickers) -> Dict[str, Optional[float]]:
    """
    Returns Dict[ticker → price | None].
    Same signature and output as the original implementation.
    Now backed by MarketDataService (yfinance + 5-min TTL cache).
    """
    return _service.get_price_dict(list(tickers))


# ---------------------------------------------------------------------------
# Status display (Streamlit — only called from app.py UI)
# ---------------------------------------------------------------------------

def display_price_status(is_connected: bool = True) -> None:
    """Display market data service status in Streamlit sidebar."""
    alpaca_ok = _service.alpaca_available
    greeks_status = "Greeks: enabled" if alpaca_ok else "Greeks: add Alpaca keys to .env"
    st.success(f"Market Data Service — Active (15 min delay)")
    st.caption(f"yfinance: equity + options chain  |  {greeks_status}")


# ---------------------------------------------------------------------------
# PriceFeed class — kept for import compatibility
# ---------------------------------------------------------------------------

class PriceFeed:
    """
    Kept for backward compatibility with existing imports.
    All methods delegate to MarketDataService.
    For options data + Greeks use MarketDataService.get_open_positions_data() directly.
    """

    def connect(self) -> bool:
        return True

    def is_connected(self) -> bool:
        return True

    def disconnect(self) -> None:
        pass

    def get_live_prices(self, tickers: list, contract_fetcher=None) -> Dict[str, Optional[float]]:
        return _service.get_price_dict(tickers)

    def get_option_prices(self, contract_fetcher) -> dict:
        # Options data now via MarketDataService.get_open_positions_data()
        return {}

    def get_single_price(self, ticker: str) -> Optional[float]:
        return _service.get_price_dict([ticker]).get(ticker)
