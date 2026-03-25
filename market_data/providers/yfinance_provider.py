"""
yfinance provider — equity prices and options chain data.
Source: Yahoo Finance (15-min delayed, free, no auth required).
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf

from ..models import EquityQuote, OptionsContract

logger = logging.getLogger(__name__)


class YFinanceProvider:
    """
    Fetches equity quotes and options chain data from Yahoo Finance.
    No authentication required. Data is 10-15 minutes delayed.
    """

    def get_equity_prices(self, tickers: List[str]) -> Dict[str, EquityQuote]:
        """
        Fetch current equity prices for a list of tickers.
        Returns only tickers where price was successfully retrieved.
        """
        result: Dict[str, EquityQuote] = {}

        for ticker in tickers:
            # Normalise composite tickers (e.g. CRCL/ETHA/SOL → CRCL)
            symbol = ticker.split("/")[0] if "/" in ticker else ticker

            try:
                t = yf.Ticker(symbol)
                info = t.info

                price = (
                    info.get("currentPrice")
                    or info.get("regularMarketPrice")
                    or info.get("previousClose")
                )

                if price is None:
                    hist = t.history(period="1d", interval="1m")
                    price = float(hist["Close"].iloc[-1]) if not hist.empty else None

                if price is not None:
                    result[ticker] = EquityQuote(
                        ticker=ticker,
                        price=float(price),
                        prev_close=float(info.get("previousClose") or price),
                        timestamp=datetime.now(),
                    )
                else:
                    logger.warning(f"yfinance: no price available for {ticker}")

            except Exception as exc:
                logger.warning(f"yfinance: failed to get price for {ticker}: {exc}")

        return result

    def get_contracts_for_positions(
        self, positions: List[dict]
    ) -> List[OptionsContract]:
        """
        Fetch options chain data for a list of open position dicts.
        Each position dict must have keys: underlying, strike, right, expiry (YYYY-MM-DD).
        Fetches chain per (ticker, expiry) pair to minimise API calls.
        Returns only contracts matching the requested positions.
        """
        if not positions:
            return []

        # Group positions by (underlying, expiry) to batch chain fetches
        pairs: Dict[tuple, List[dict]] = {}
        for pos in positions:
            key = (pos["underlying"], pos["expiry"])
            pairs.setdefault(key, []).append(pos)

        contracts: List[OptionsContract] = []

        for (underlying, expiry), pos_group in pairs.items():
            try:
                chain = yf.Ticker(underlying).option_chain(expiry)
                expiry_date = pd.to_datetime(expiry).date()

                for df, right in [(chain.calls, "C"), (chain.puts, "P")]:
                    # Only keep rows matching a requested position
                    wanted_strikes = {
                        float(p["strike"]) for p in pos_group if p["right"] == right
                    }
                    if not wanted_strikes:
                        continue

                    for _, row in df.iterrows():
                        row_strike = float(row.get("strike", 0))
                        if row_strike not in wanted_strikes:
                            continue

                        contracts.append(
                            OptionsContract(
                                contract_symbol=str(row.get("contractSymbol", "")),
                                underlying=underlying,
                                strike=row_strike,
                                expiry=expiry_date,
                                right=right,
                                bid=float(row.get("bid") or 0),
                                ask=float(row.get("ask") or 0),
                                last_price=float(row.get("lastPrice") or 0),
                                implied_volatility=float(
                                    row.get("impliedVolatility") or 0
                                ),
                                delta=None,
                                gamma=None,
                                theta=None,
                                timestamp=datetime.now(),
                            )
                        )

            except Exception as exc:
                logger.warning(
                    f"yfinance: failed to get options chain for "
                    f"{underlying}/{expiry}: {exc}"
                )

        return contracts
