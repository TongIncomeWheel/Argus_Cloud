"""
ARGUS Market Data Service.

Usage:
    from market_data import MarketDataService
    service = MarketDataService()

    # Equity prices
    prices = service.get_equity_prices(["MARA", "SPY"])

    # Open positions with options data + Greeks
    contracts = service.get_open_positions_data(df_open)

    # Historical OHLCV for LLM / chart queries
    bars = service.get_historical_ohlcv("MARA", period_days=60, frequency="daily")

    # Drop-in replacement for legacy get_cached_prices()
    price_dict = service.get_price_dict(["MARA", "SPY"])
"""
from .service import MarketDataService

__all__ = ["MarketDataService"]
