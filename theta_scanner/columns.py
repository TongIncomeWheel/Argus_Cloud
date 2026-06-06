"""Column catalog for the Scanner table.

One source of truth for every column the table can show — its label, category,
default visibility, and number format. Drives the "Columns" toggle panel, saved
layouts, and the st.dataframe column_config.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import streamlit as st


@dataclass(frozen=True)
class ColumnDef:
    key: str            # DataFrame column name
    label: str          # header shown in the table
    category: str       # grouping for the Columns panel
    default: bool       # part of the default layout
    fmt: Optional[str]  # printf format for NumberColumn, or None for text
    help: str = ""


# ─── The catalog ───────────────────────────────────────────────────
CATALOG: List[ColumnDef] = [
    # Basic
    ColumnDef("type", "Type", "Basic", True, None, "Put or Call"),
    ColumnDef("symbol", "Symbol", "Basic", True, None, "Underlying ticker"),
    ColumnDef("strike", "Strike", "Basic", True, "$%.2f"),
    ColumnDef("expiration", "Expiration", "Basic", True, None),
    ColumnDef("dte", "DTE", "Basic", True, "%d", "Days to expiration"),
    ColumnDef("days_to_er", "Days to ER", "Basic", True, "%d", "Days to next earnings"),
    ColumnDef("sector", "Sector", "Basic", True, None),
    ColumnDef("market_cap", "Market Cap", "Basic", True, "$%.1fB"),
    # Price
    ColumnDef("last_price", "Last Price", "Price", True, "$%.2f", "Option last trade"),
    ColumnDef("pct_change", "% Change", "Price", True, "%.2f%%", "Underlying daily change"),
    ColumnDef("mark", "Mark", "Price", True, "$%.2f", "Option mid price"),
    ColumnDef("bid", "Bid", "Price", False, "$%.2f"),
    ColumnDef("ask", "Ask", "Price", False, "$%.2f"),
    ColumnDef("roc", "ROC %", "Price", True, "%.2f%%", "Return on capital, one cycle"),
    ColumnDef("annual_yield", "Annual Yield %", "Price", True, "%.1f%%", "Annualized ROC"),
    ColumnDef("underlying_price", "Underlying", "Price", False, "$%.2f"),
    ColumnDef("breakeven", "Breakeven", "Price", False, "$%.2f"),
    # Greeks
    ColumnDef("delta", "Delta", "Greeks", True, "%.3f"),
    ColumnDef("theta", "Theta", "Greeks", True, "%.3f"),
    ColumnDef("gamma", "Gamma", "Greeks", False, "%.4f"),
    ColumnDef("pct_otm", "% OTM", "Greeks", True, "%.1f%%", "Percent out of the money"),
    # Liquidity
    ColumnDef("volume", "Volume", "Liquidity", False, "%d", "Option contract volume"),
    ColumnDef("open_interest", "Open Interest", "Liquidity", False, "%d"),
    ColumnDef("avg_vol", "Avg Vol", "Liquidity", True, "%.1fM", "Stock 30d avg volume"),
    ColumnDef("day_vol", "Day Vol", "Liquidity", False, "%.1fM", "Stock latest day volume"),
    ColumnDef("spread_pct", "Spread %", "Liquidity", False, "%.1f%%", "Bid/ask spread"),
    # Fundamentals
    ColumnDef("pe", "P/E", "Fundamentals", False, "%.1f"),
    ColumnDef("fwd_pe", "Fwd P/E", "Fundamentals", False, "%.1f"),
    ColumnDef("eps_ttm", "EPS (TTM)", "Fundamentals", False, "$%.2f"),
    ColumnDef("beta", "Beta", "Fundamentals", False, "%.2f"),
    ColumnDef("short_float", "Short Float %", "Fundamentals", False, "%.1f%%"),
    ColumnDef("analyst_rating", "Analyst Rating", "Fundamentals", False, "%.1f", "1-5 scale"),
    # Technicals
    ColumnDef("rsi", "RSI", "Technicals", False, "%.0f"),
    ColumnDef("atr", "ATR", "Technicals", False, "$%.2f", "Average true range, 14d"),
    ColumnDef("ma20", "MA20", "Technicals", True, "$%.2f"),
    ColumnDef("ma50", "MA50", "Technicals", False, "$%.2f"),
    ColumnDef("ma200", "MA200", "Technicals", False, "$%.2f"),
    ColumnDef("perf_week", "Perf Week", "Technicals", False, "%.1f%%"),
    ColumnDef("perf_month", "Perf Month", "Technicals", False, "%.1f%%"),
    ColumnDef("perf_quarter", "Perf Qtr", "Technicals", False, "%.1f%%"),
    ColumnDef("perf_year", "Perf Year", "Technicals", False, "%.1f%%"),
    # Dividend
    ColumnDef("dividend", "Dividend", "Dividend", False, "$%.2f", "Annual dividend/share"),
    ColumnDef("div_yield", "Div Yield %", "Dividend", False, "%.2f%%"),
    ColumnDef("days_to_div", "Days to Div", "Dividend", False, "%d", "Days to ex-dividend"),
    # ARGUS proprietary scores
    ColumnDef("option_score", "Option Score", "Scores", True, "%.0f",
              "0-100 blend: yield, distance OTM, delta"),
    ColumnDef("stock_rating", "Stock Rating", "Scores", True, "%.0f",
              "0-100 technical health of the underlying"),
    ColumnDef("rel_strength", "Rel Strength", "Scores", False, "%.0f",
              "0-100 performance vs SPY"),
    ColumnDef("stock_iv", "Stock IV", "Scores", True, "%.0f%%",
              "Median IV across the ticker's contracts"),
    ColumnDef("iv_pct", "IV %", "Scores", True, "%.0f%%", "This contract's implied vol"),
    ColumnDef("pop", "PoP %", "Scores", False, "%.0f%%", "Probability of profit ≈ 1−|Δ|"),
    ColumnDef("verdict", "Verdict", "Scores", False, None),
]

_BY_KEY = {c.key: c for c in CATALOG}

# Default visible columns, in the spec's display order.
DEFAULT_LAYOUT: List[str] = [
    "symbol", "option_score", "strike", "expiration", "pct_change", "dte",
    "days_to_er", "last_price", "annual_yield", "pct_otm", "sector", "delta",
    "roc", "theta", "market_cap", "stock_iv", "iv_pct", "stock_rating", "mark",
    "type", "avg_vol", "ma20",
]

CATEGORY_ORDER = ["Basic", "Price", "Greeks", "Liquidity", "Fundamentals",
                  "Technicals", "Dividend", "Scores"]


def all_keys() -> List[str]:
    """Every column key, grouped by category order."""
    out: List[str] = []
    for cat in CATEGORY_ORDER:
        out.extend(c.key for c in CATALOG if c.category == cat)
    return out


def get(key: str) -> Optional[ColumnDef]:
    return _BY_KEY.get(key)


def label(key: str) -> str:
    c = _BY_KEY.get(key)
    return c.label if c else key


def keys_by_category() -> dict:
    """{category: [keys]} — drives the grouped Columns toggle panel."""
    out: dict = {cat: [] for cat in CATEGORY_ORDER}
    for c in CATALOG:
        out[c.category].append(c.key)
    return out


def column_config(visible_keys: List[str]) -> dict:
    """Build the st.dataframe column_config for the given visible columns."""
    cfg: dict = {}
    for key in visible_keys:
        c = _BY_KEY.get(key)
        if c is None:
            continue
        if c.fmt is None:
            cfg[key] = st.column_config.TextColumn(label=c.label, help=c.help or None)
        else:
            cfg[key] = st.column_config.NumberColumn(
                label=c.label, format=c.fmt, help=c.help or None,
            )
    return cfg
