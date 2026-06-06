"""Filter catalog + apply logic for the Scanner.

FILTER_DEFS is the single source of truth for every filter: which section it
lives in, its kind, and the DataFrame column it acts on. `apply_filters`
consumes a flat state dict and returns the filtered DataFrame.

Graceful degradation: a filter whose target column is entirely empty (e.g.
fundamentals with no FMP key) is skipped rather than dropping every row.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import List

import pandas as pd

SECTIONS = ["Options", "Fundamentals", "Technicals", "Dividends", "Global"]
DIRECTION_CHOICES = ("Any", "Price Above", "Price Below")
ANY_SECTOR = "Any Sector"


@dataclass(frozen=True)
class FilterDef:
    key: str
    label: str
    section: str
    kind: str                       # range | max | toggle | choice | direction
    column: str = ""
    help: str = ""
    step: float = 1.0
    default_on: bool = False
    choices: tuple = ()
    default_choice: str = ""


# ─── The catalog ───────────────────────────────────────────────────
FILTER_DEFS: List[FilterDef] = [
    # Options
    FilterDef("strike", "Strike Price", "Options", "range", "strike", step=1.0),
    FilterDef("dte", "Days to Expiration", "Options", "range", "dte", step=1.0),
    FilterDef("annual_yield", "Annual Yield %", "Options", "range", "annual_yield", step=1.0),
    FilterDef("roc", "ROC %", "Options", "range", "roc", step=0.5,
              help="Return on capital, one cycle"),
    FilterDef("delta", "Delta (|Δ|)", "Options", "range", "delta", step=0.05),
    FilterDef("iv", "IV %", "Options", "range", "iv_pct", step=1.0,
              help="Target set by the IV basis toggle"),
    FilterDef("max_spread", "Max Bid/Ask Spread %", "Options", "max", "spread_pct", step=0.5),
    FilterDef("volume", "Option Volume", "Options", "range", "volume", step=10.0),
    FilterDef("open_interest", "Open Interest", "Options", "range", "open_interest", step=50.0),
    FilterDef("pct_otm", "% OTM", "Options", "range", "pct_otm", step=1.0),
    FilterDef("days_to_earnings", "Days to Earnings", "Options", "range", "days_to_er", step=1.0),
    FilterDef("only_before_earnings", "Only Before Earnings", "Options", "toggle",
              help="Contract expires before the next earnings date"),
    FilterDef("show_only_upcoming_earnings", "Show Only Upcoming Earnings", "Options", "toggle",
              help="Underlying has a known upcoming earnings date"),
    # Fundamentals
    FilterDef("market_cap", "Market Cap ($B)", "Fundamentals", "range", "market_cap", step=1.0),
    FilterDef("pe", "P/E Ratio", "Fundamentals", "range", "pe", step=1.0),
    FilterDef("fwd_pe", "Forward P/E", "Fundamentals", "range", "fwd_pe", step=1.0),
    FilterDef("eps_ttm", "EPS (TTM)", "Fundamentals", "range", "eps_ttm", step=0.5),
    FilterDef("short_float", "Short Float %", "Fundamentals", "range", "short_float", step=0.5),
    FilterDef("beta", "Beta", "Fundamentals", "range", "beta", step=0.1),
    FilterDef("analyst_rating", "Analyst Rating (1-5)", "Fundamentals", "range",
              "analyst_rating", step=0.5),
    FilterDef("ts_rating", "Stock Rating (0-100)", "Fundamentals", "range",
              "stock_rating", step=5.0),
    FilterDef("ts_options_score", "Option Score (0-100)", "Fundamentals", "range",
              "option_score", step=5.0),
    FilterDef("ts_rel_strength", "Rel Strength (0-100)", "Fundamentals", "range",
              "rel_strength", step=5.0),
    FilterDef("sector", "Sector", "Fundamentals", "choice", "sector",
              default_choice=ANY_SECTOR),
    # Technicals
    FilterDef("rsi", "RSI", "Technicals", "range", "rsi", step=1.0),
    FilterDef("ma20", "MA 20", "Technicals", "direction", "ma20",
              choices=DIRECTION_CHOICES, default_choice="Any"),
    FilterDef("ma50", "MA 50", "Technicals", "direction", "ma50",
              choices=DIRECTION_CHOICES, default_choice="Any"),
    FilterDef("ma200", "MA 200", "Technicals", "direction", "ma200",
              choices=DIRECTION_CHOICES, default_choice="Any"),
    FilterDef("atr", "ATR", "Technicals", "range", "atr", step=0.5),
    FilterDef("perf_week", "Performance Week %", "Technicals", "range", "perf_week", step=1.0),
    FilterDef("perf_month", "Performance Month %", "Technicals", "range", "perf_month", step=1.0),
    FilterDef("perf_quarter", "Performance Quarter %", "Technicals", "range",
              "perf_quarter", step=1.0),
    FilterDef("perf_year", "Performance Year %", "Technicals", "range", "perf_year", step=1.0),
    # Dividends
    FilterDef("div_yield", "Dividend Yield %", "Dividends", "range", "div_yield", step=0.5),
    FilterDef("days_to_div", "Days to Ex-Dividend", "Dividends", "range", "days_to_div", step=1.0),
    FilterDef("only_dividend_payers", "Only Dividend Payers", "Dividends", "toggle"),
    FilterDef("expiry_before_exdiv", "Expiry Before Ex-Div", "Dividends", "toggle"),
    # Global
    FilterDef("hide_etfs", "Hide ETFs", "Global", "toggle"),
    FilterDef("cleanup_illiquid", "Cleanup Illiquid", "Global", "toggle", default_on=True,
              help="Drop contracts that fail the liquidity gate"),
    FilterDef("watchlist_only", "Watchlist Only", "Global", "toggle"),
]

_BY_KEY = {f.key: f for f in FILTER_DEFS}


def get(key: str) -> FilterDef:
    return _BY_KEY[key]


def defs_for_section(section: str) -> List[FilterDef]:
    return [f for f in FILTER_DEFS if f.section == section]


def default_filter_state() -> dict:
    """Fresh state dict — every filter inactive (cleanup_illiquid on)."""
    state: dict = {"iv_basis": "Option IV"}
    for f in FILTER_DEFS:
        if f.kind in ("range", "max"):
            if f.kind == "range":
                state[f"{f.key}_min"] = None
            state[f"{f.key}_max"] = None
        elif f.kind == "toggle":
            state[f.key] = f.default_on
        elif f.kind in ("choice", "direction"):
            state[f.key] = f.default_choice
    return state


def count_active(state: dict) -> int:
    """How many filters are currently doing something."""
    n = 0
    for f in FILTER_DEFS:
        if f.kind == "range":
            if state.get(f"{f.key}_min") is not None or state.get(f"{f.key}_max") is not None:
                n += 1
        elif f.kind == "max":
            if state.get(f"{f.key}_max") is not None:
                n += 1
        elif f.kind == "toggle":
            if bool(state.get(f.key)) != f.default_on:
                n += 1
        elif f.kind == "choice":
            if state.get(f.key) not in (None, "", f.default_choice):
                n += 1
        elif f.kind == "direction":
            if state.get(f.key) not in (None, "", "Any"):
                n += 1
    return n


# ─── Apply ─────────────────────────────────────────────────────────


def _has_data(df: pd.DataFrame, col: str) -> bool:
    return col in df.columns and df[col].notna().sum() > 0


def _apply_range(df: pd.DataFrame, col: str, lo, hi) -> pd.DataFrame:
    if not _has_data(df, col):
        return df
    out = df
    if lo is not None:
        out = out[out[col] >= lo]
    if hi is not None:
        out = out[out[col] <= hi]
    return out


def apply_filters(df: pd.DataFrame, state: dict, watchlist=None) -> pd.DataFrame:
    """Return the rows of `df` that pass every active filter."""
    if df is None or df.empty:
        return df
    out = df
    watchlist = {str(t).upper() for t in (watchlist or [])}

    for f in FILTER_DEFS:
        if f.kind == "range":
            lo = state.get(f"{f.key}_min")
            hi = state.get(f"{f.key}_max")
            if lo is None and hi is None:
                continue
            col = f.column
            if f.key == "iv":  # basis toggle picks the column
                col = "stock_iv" if state.get("iv_basis") == "Stock IV" else "iv_pct"
            out = _apply_range(out, col, lo, hi)

        elif f.kind == "max":
            hi = state.get(f"{f.key}_max")
            if hi is not None:
                out = _apply_range(out, f.column, None, hi)

        elif f.kind == "choice":
            val = state.get(f.key)
            if val not in (None, "", f.default_choice) and _has_data(out, f.column):
                out = out[out[f.column] == val]

        elif f.kind == "direction":
            val = state.get(f.key)
            if val in (None, "", "Any"):
                continue
            if _has_data(out, f.column) and _has_data(out, "underlying_price"):
                if "Above" in val:
                    out = out[out["underlying_price"] > out[f.column]]
                elif "Below" in val:
                    out = out[out["underlying_price"] < out[f.column]]

        elif f.kind == "toggle":
            active = bool(state.get(f.key))
            if not active:
                continue
            out = _apply_toggle(out, f.key, watchlist)

    return out


def _apply_toggle(df: pd.DataFrame, key: str, watchlist: set) -> pd.DataFrame:
    today = date.today()

    if key == "cleanup_illiquid":
        if "liquidity_ok" in df.columns:
            return df[df["liquidity_ok"] == True]  # noqa: E712
        return df

    if key == "hide_etfs":
        if _has_data(df, "is_etf"):
            return df[df["is_etf"] != True]  # noqa: E712 — keeps None/False
        return df

    if key == "watchlist_only":
        if watchlist and "symbol" in df.columns:
            return df[df["symbol"].str.upper().isin(watchlist)]
        return df

    if key == "only_dividend_payers":
        if _has_data(df, "div_yield"):
            return df[df["div_yield"].fillna(0) > 0]
        return df

    if key == "show_only_upcoming_earnings":
        if _has_data(df, "earnings_date"):
            return df[df["earnings_date"].apply(
                lambda d: isinstance(d, date) and d >= today)]
        return df

    if key == "only_before_earnings":
        # Keep rows expiring before earnings; unknown-earnings rows are kept.
        if _has_data(df, "earnings_date") and "expiration" in df.columns:
            def _ok(row):
                ed = row.get("earnings_date")
                if not isinstance(ed, date):
                    return True
                try:
                    exp = date.fromisoformat(str(row.get("expiration")))
                except (ValueError, TypeError):
                    return True
                return exp < ed
            return df[df.apply(_ok, axis=1)]
        return df

    if key == "expiry_before_exdiv":
        if _has_data(df, "ex_div_date") and "expiration" in df.columns:
            def _ok(row):
                xd = row.get("ex_div_date")
                if not isinstance(xd, date):
                    return True
                try:
                    exp = date.fromisoformat(str(row.get("expiration")))
                except (ValueError, TypeError):
                    return True
                return exp < xd
            return df[df.apply(_ok, axis=1)]
        return df

    return df
