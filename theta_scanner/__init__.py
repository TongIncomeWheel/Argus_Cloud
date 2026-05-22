"""ARGUS Theta Scanner — an options screener for wheel traders.

Scans an options universe for cash-secured puts and covered calls, scores
every contract, and presents a filterable, sortable, configurable table.

Modules:
  universe      candidate ticker universe — bundled liquid large-cap list,
                auto-upgrades to a live FMP market-cap/volume screen
  scoring       pure scoring math — option economics (RoR, annualized yield,
                %OTM, PoP), composite Option Score, Stock Rating, Rel Strength
  technicals    RSI / ATR / moving averages / performance from daily OHLCV
  fundamentals  underlying fundamentals — batched FMP, or yfinance fallback
  data          option chains (Alpaca) + batch spot prices
  scan          scan orchestration — builds the per-contract DataFrame
  filters       ~35-filter catalog + apply logic
  columns       table column catalog (labels, categories, formats)
  presets       saved filter presets / column layouts / watchlist
  ui            Streamlit Scanner UI (rendered inside the Lookup tab)
"""
from __future__ import annotations

__version__ = "2.0.0"
