"""
MarketDataService — single entry point for all market data in ARGUS.

Three public methods:
  get_equity_prices()       → Dict[str, EquityQuote]
  get_open_positions_data() → List[OptionsContract]  (bid/ask/last/IV + Greeks)
  get_historical_ohlcv()    → List[OHLCVBar]

  get_price_dict()          → Dict[str, float]  compatibility shim for price_feed.py

Pure Python — no Streamlit imports. Reusable by any app.
"""
import logging
from typing import Dict, List, Optional

import pandas as pd

from .cache import TTLCache
from .config import CACHE_TTL_SECONDS
from .models import EquityQuote, OHLCVBar, OptionsContract
from .providers.alpaca_provider import AlpacaProvider
from .providers.stooq_provider import StooqProvider
from .providers.yfinance_provider import YFinanceProvider

logger = logging.getLogger(__name__)


class MarketDataService:
    """
    Unified market data service for ARGUS.

    Instantiate once at app startup and share the instance.
    Internal TTL cache prevents redundant API calls across page renders.
    """

    def __init__(self):
        self._cache = TTLCache(ttl_seconds=CACHE_TTL_SECONDS)
        self._yfinance = YFinanceProvider()
        self._alpaca = AlpacaProvider()
        self._stooq = StooqProvider()

    @property
    def alpaca_available(self) -> bool:
        """True if Alpaca keys are configured and client initialised."""
        return self._alpaca.is_available

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_equity_prices(self, tickers: List[str]) -> Dict[str, EquityQuote]:
        """
        Fetch current equity prices for a list of tickers.
        Results cached for CACHE_TTL_SECONDS (default 5 min).

        Returns Dict[ticker → EquityQuote]. Missing tickers are omitted.
        """
        if not tickers:
            return {}

        cache_key = "equity_" + "_".join(sorted(tickers))
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        result = self._yfinance.get_equity_prices(tickers)
        self._cache.set(cache_key, result)
        return result

    def get_open_positions_data(
        self, positions_df: pd.DataFrame
    ) -> List[OptionsContract]:
        """
        Fetch options data (bid/ask/last/IV) + Greeks for all open positions.

        positions_df: DataFrame with columns Ticker, Option_Strike_Price_(USD),
                      Expiry_Date, TradeType (CC or CSP), Status == Open.

        Returns List[OptionsContract]. Greeks are None if Alpaca unavailable.
        Cached per unique set of open positions.
        """
        if positions_df is None or positions_df.empty:
            return []

        required_cols = [
            "Ticker", "Option_Strike_Price_(USD)", "Expiry_Date", "TradeType"
        ]
        if not all(c in positions_df.columns for c in required_cols):
            logger.warning("get_open_positions_data: missing required columns")
            return []

        # Cache key based on position fingerprint
        try:
            fingerprint = str(
                positions_df[required_cols].sort_values(required_cols).values.tolist()
            )
            cache_key = f"options_{hash(fingerprint)}"
        except Exception:
            cache_key = "options_all"

        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        # Build position list for provider
        positions = []
        for _, row in positions_df.iterrows():
            try:
                expiry = pd.to_datetime(row["Expiry_Date"]).strftime("%Y-%m-%d")
                positions.append(
                    {
                        "underlying": str(row["Ticker"]).upper(),
                        "strike": float(row["Option_Strike_Price_(USD)"]),
                        "right": "C" if row["TradeType"] == "CC" else "P",
                        "expiry": expiry,
                    }
                )
            except Exception as exc:
                logger.warning(f"Skipping position row: {exc}")

        if not positions:
            return []

        # Fetch chain from yfinance, then enrich Greeks via Alpaca
        contracts = self._yfinance.get_contracts_for_positions(positions)
        contracts = self._alpaca.enrich_with_greeks(contracts)

        self._cache.set(cache_key, contracts)
        return contracts

    def get_historical_ohlcv(
        self,
        ticker: str,
        period_days: int = 90,
        frequency: str = "daily",
    ) -> List[OHLCVBar]:
        """
        Fetch historical OHLCV bars for a ticker via Stooq.

        Args:
            ticker:      Equity ticker (e.g. "MARA", "SPY")
            period_days: Calendar days of history to fetch
            frequency:   "daily" or "monthly"

        Returns List[OHLCVBar] sorted oldest-first. Empty on failure.
        """
        cache_key = f"ohlcv_{ticker}_{period_days}_{frequency}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        result = self._stooq.get_historical_ohlcv(ticker, period_days, frequency)
        self._cache.set(cache_key, result)
        return result

    # ------------------------------------------------------------------
    # Compatibility shim — drop-in replacement for get_cached_prices()
    # ------------------------------------------------------------------

    def get_price_dict(self, tickers: List[str]) -> Dict[str, Optional[float]]:
        """
        Returns Dict[ticker → price | None].
        Identical output format to the legacy price_feed.get_cached_prices().
        Used by price_feed.py adapter and any code that needs raw prices.
        """
        quotes = self.get_equity_prices(tickers)
        return {t: (quotes[t].price if t in quotes else None) for t in tickers}

    def refresh_cache(self) -> None:
        """Force-clear the cache so next call fetches fresh data."""
        self._cache.clear()
        logger.info("MarketDataService: cache cleared.")
