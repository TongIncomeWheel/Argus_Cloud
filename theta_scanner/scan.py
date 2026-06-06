"""Scan orchestration — builds the full per-contract DataFrame.

`run_scan` pulls spot, fundamentals, technicals and option chains for the
requested tickers, scores every contract, and returns one wide DataFrame the
UI can filter and display. The scan is unfiltered — the UI applies the user's
filters afterwards, so re-filtering never triggers a re-pull.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable, List, Optional

import pandas as pd

from . import data as data_mod
from . import fundamentals as fund_mod
from . import scoring
from . import technicals as tech_mod
from .universe import fmp_configured

logger = logging.getLogger(__name__)

# Strike window pulled per underlying, as a fraction of spot.
_STRIKE_LO, _STRIKE_HI = 0.75, 1.25


@dataclass
class ScanResult:
    df: pd.DataFrame
    errors: List[str] = field(default_factory=list)
    scanned_at: Optional[datetime] = None
    fundamentals_source: str = ""
    n_tickers_ok: int = 0


def _days_between(target: Optional[date]) -> Optional[int]:
    if not isinstance(target, date):
        return None
    return (target - date.today()).days


def run_scan(tickers: List[str], option_type: str, dte_min: int, dte_max: int,
             progress: Optional[Callable[[float, str], None]] = None) -> ScanResult:
    """Pull + score every contract for `tickers` in the DTE window."""
    tickers = sorted({str(t).strip().upper() for t in tickers if t and str(t).strip()})
    if not tickers:
        return ScanResult(df=pd.DataFrame(), scanned_at=datetime.now())

    is_put = str(option_type).lower().startswith("p")
    leg = "Put" if is_put else "Call"
    today = date.today()
    expiry_from = (today + timedelta(days=int(dte_min))).isoformat()
    expiry_to = (today + timedelta(days=int(dte_max))).isoformat()

    def _tick(phase_base: float, phase_span: float):
        def cb(done, total, label):
            frac = phase_base + phase_span * (done / max(total, 1))
            if progress:
                progress(min(frac, 0.99), f"{label}…")
        return cb

    # ── Phase 1: spot prices (fast, batched) ──────────────────────
    if progress:
        progress(0.02, "Fetching spot prices…")
    spots = data_mod.batch_spot_prices(tuple(tickers))

    # ── Phase 2: fundamentals (FMP batch, or yfinance per ticker) ─
    fundamentals = fund_mod.load_fundamentals(tuple(tickers), progress_cb=_tick(0.05, 0.20))

    # ── Phase 3: technicals (per ticker; SPY added for benchmark) ─
    tech_tickers = tuple(sorted(set(tickers) | {"SPY"}))
    technicals = tech_mod.load_technicals(tech_tickers, progress_cb=_tick(0.25, 0.30))
    benchmark_perf = (technicals.get("SPY") or {}).get("perf_year")

    # ── Earnings: FMP gives it free in fundamentals; else dedicated ─
    earnings: dict = {}
    if not fmp_configured():
        earnings = data_mod.load_earnings(tuple(tickers))

    # ── Phase 4: option chains + scoring (the bulk) ───────────────
    rows: List[dict] = []
    errors: List[str] = []
    iv_by_ticker: dict = {}
    total = len(tickers)

    for i, ticker in enumerate(tickers, start=1):
        if progress:
            progress(0.55 + 0.44 * (i / total), f"Scanning {ticker} ({i}/{total})…")
        spot = float(spots.get(ticker, 0) or 0)
        if spot <= 0:
            errors.append(ticker)
            continue

        chain = data_mod.load_option_chain(
            ticker=ticker, option_type=leg,
            expiry_from=expiry_from, expiry_to=expiry_to,
            strike_min=spot * _STRIKE_LO, strike_max=spot * _STRIKE_HI,
        )
        if not chain:
            errors.append(ticker)
            continue

        fund = fundamentals.get(ticker, {})
        tech = technicals.get(ticker, {})
        earn_date = fund.get("earnings_date") or earnings.get(ticker)
        exdiv_date = fund.get("ex_div_date")
        ticker_ivs: List[float] = []

        stock_rating = scoring.stock_rating(
            price=tech.get("price"), ma20=tech.get("ma20"),
            ma50=tech.get("ma50"), ma200=tech.get("ma200"),
            rsi=tech.get("rsi"), perf_quarter=tech.get("perf_quarter"),
        )
        rel_str = scoring.rel_strength(tech.get("perf_year"), benchmark_perf)

        for c in chain:
            mid = c.get("mid")
            delta_raw = c.get("delta")
            delta_abs = abs(float(delta_raw)) if delta_raw is not None else None
            econ = scoring.option_economics(
                option_type=leg, spot=spot, strike=c.get("strike", 0),
                premium=mid or 0, dte=c.get("dte"), delta=delta_raw,
            )
            iv = c.get("iv")
            iv_pct = float(iv) * 100.0 if iv is not None else None
            if iv_pct is not None:
                ticker_ivs.append(iv_pct)
            score = scoring.option_score(econ["annual_yield"], econ["pct_otm"], delta_abs)

            rows.append({
                "type": leg,
                "symbol": ticker,
                "occ_symbol": c.get("symbol"),
                "strike": c.get("strike"),
                "expiration": c.get("expiry"),
                "dte": c.get("dte"),
                "days_to_er": _days_between(earn_date),
                "sector": fund.get("sector"),
                "market_cap": fund.get("market_cap"),
                "last_price": c.get("last"),
                "pct_change": tech.get("pct_change"),
                "mark": mid,
                "bid": c.get("bid"),
                "ask": c.get("ask"),
                "roc": econ["roc"],
                "annual_yield": econ["annual_yield"],
                "underlying_price": spot,
                "breakeven": econ["breakeven"],
                "delta": delta_abs,
                "theta": c.get("theta"),
                "gamma": c.get("gamma"),
                "pct_otm": econ["pct_otm"],
                "volume": None,  # not exposed by Alpaca's chain snapshot
                "open_interest": c.get("open_interest"),
                "avg_vol": tech.get("avg_vol"),
                "day_vol": tech.get("day_vol"),
                "spread_pct": scoring.spread_pct(c.get("bid"), c.get("ask"), mid),
                "pe": fund.get("pe"),
                "fwd_pe": fund.get("fwd_pe"),
                "eps_ttm": fund.get("eps_ttm"),
                "beta": fund.get("beta"),
                "short_float": fund.get("short_float"),
                "analyst_rating": fund.get("analyst_rating"),
                "rsi": tech.get("rsi"),
                "atr": tech.get("atr"),
                "ma20": tech.get("ma20"),
                "ma50": tech.get("ma50"),
                "ma200": tech.get("ma200"),
                "perf_week": tech.get("perf_week"),
                "perf_month": tech.get("perf_month"),
                "perf_quarter": tech.get("perf_quarter"),
                "perf_year": tech.get("perf_year"),
                "dividend": fund.get("dividend"),
                "div_yield": fund.get("div_yield"),
                "days_to_div": _days_between(exdiv_date),
                "option_score": score,
                "stock_rating": stock_rating,
                "rel_strength": rel_str,
                "stock_iv": None,  # filled below from the ticker median
                "iv_pct": iv_pct,
                "pop": econ["pop"],
                "verdict": scoring.verdict(score),
                "liquidity_ok": scoring.liquidity_ok(
                    c.get("open_interest"), c.get("bid"), c.get("ask"), mid),
                "earnings_date": earn_date,
                "ex_div_date": exdiv_date,
                "is_etf": fund.get("is_etf"),
            })

        if ticker_ivs:
            iv_by_ticker[ticker] = sorted(ticker_ivs)[len(ticker_ivs) // 2]

    df = pd.DataFrame(rows)
    if not df.empty:
        df["stock_iv"] = df["symbol"].map(iv_by_ticker)

    return ScanResult(
        df=df,
        errors=errors,
        scanned_at=datetime.now(),
        fundamentals_source=fund_mod.source_label(),
        n_tickers_ok=len(tickers) - len(errors),
    )
