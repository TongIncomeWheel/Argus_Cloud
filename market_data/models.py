"""
Data models for the ARGUS Market Data Service.
Pure Python dataclasses — no Streamlit, no UI dependencies.
"""
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


@dataclass
class EquityQuote:
    """Current equity price snapshot."""
    ticker: str
    price: float
    prev_close: float
    timestamp: datetime


@dataclass
class OptionsContract:
    """
    Options contract data for a single open position.
    Greeks are Optional — populated by Alpaca if keys are configured,
    None otherwise. All other fields always populated by yfinance.
    """
    contract_symbol: str       # e.g. "MARA250321C00020000"
    underlying: str            # e.g. "MARA"
    strike: float
    expiry: date
    right: str                 # "C" (call / CC) or "P" (put / CSP)
    bid: float
    ask: float
    last_price: float
    implied_volatility: float
    delta: Optional[float]     # None if Alpaca unavailable
    gamma: Optional[float]
    theta: Optional[float]
    timestamp: datetime


@dataclass
class OHLCVBar:
    """
    Single OHLCV bar for historical data queries.
    Used by LLM context and standalone Market Data panel.
    """
    ticker: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    frequency: str             # "daily" or "monthly"
