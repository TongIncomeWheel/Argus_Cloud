"""
Stooq provider — historical OHLCV data via pandas-datareader.
Source: stooq.com (free, no API key, daily and monthly frequency).
Used by: LLM historical price queries, standalone Market Data panel.
"""
import logging
from datetime import date, timedelta
from typing import List

from ..models import OHLCVBar

logger = logging.getLogger(__name__)

_FREQ_MAP = {
    "daily": "d",
    "monthly": "m",
}


class StooqProvider:
    """
    Fetches historical OHLCV bars from Stooq via pandas-datareader.
    No authentication required.
    """

    def get_historical_ohlcv(
        self,
        ticker: str,
        period_days: int = 90,
        frequency: str = "daily",
    ) -> List[OHLCVBar]:
        """
        Fetch historical OHLCV bars for a ticker.

        Args:
            ticker:      Equity ticker symbol (e.g. "MARA", "SPY")
            period_days: How many calendar days back to fetch
            frequency:   "daily" or "monthly"

        Returns:
            List of OHLCVBar sorted oldest-first. Empty list on failure.
        """
        try:
            import pandas_datareader.data as web
        except ImportError:
            logger.warning(
                "Stooq: pandas-datareader not installed. "
                "Run: pip install pandas-datareader"
            )
            return []

        freq = _FREQ_MAP.get(frequency, "d")
        end = date.today()
        start = end - timedelta(days=period_days)

        try:
            df = web.DataReader(ticker, "stooq", start=start, end=end)

            if df.empty:
                logger.warning(f"Stooq: no data returned for {ticker}")
                return []

            # Stooq returns newest-first — sort ascending
            df = df.sort_index()

            bars: List[OHLCVBar] = []
            for dt, row in df.iterrows():
                bar_date = dt.date() if hasattr(dt, "date") else dt
                bars.append(
                    OHLCVBar(
                        ticker=ticker,
                        date=bar_date,
                        open=float(row["Open"]),
                        high=float(row["High"]),
                        low=float(row["Low"]),
                        close=float(row["Close"]),
                        volume=int(row.get("Volume", 0)),
                        frequency=frequency,
                    )
                )
            return bars

        except Exception as exc:
            logger.warning(f"Stooq: failed to get OHLCV for {ticker}: {exc}")
            return []
