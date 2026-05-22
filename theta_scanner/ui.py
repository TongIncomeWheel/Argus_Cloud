"""Streamlit UI for the Theta Scanner — rendered as a sub-tab inside Lookup.

Entry point: render_theta_scanner(df_open, settings).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import streamlit as st

from . import data as data_mod
from . import scoring
from . import universe as universe_mod

logger = logging.getLogger(__name__)


def render_theta_scanner(df_open: Optional[pd.DataFrame] = None,
                         settings: Optional[dict] = None) -> None:
    """Render the Theta Scanner — CSP candidate finder."""
    settings = settings or {}

    st.markdown("### θ Theta Scanner")
    st.caption(
        "Finds cash-secured-put candidates worth selling. Scores every OTM put on "
        "four axes — juicy premium, return on collateral, safe distance to spot, and "
        "delta — then blends them into a 0-100 composite and ranks."
    )

    # ── Universe ──────────────────────────────────────────────────
    uni = universe_mod.get_universe()
    tickers_all = uni["tickers"]
    src_note = (
        f"**Universe:** {uni['count']} tickers · source: {uni['source']}"
    )
    if uni["source"] == "bundled list":
        src_note += "  — add an `FMP_API_KEY` to Streamlit secrets for a live market-cap/volume screen."
    st.caption(src_note)

    if not data_mod.alpaca_configured():
        st.error("Alpaca credentials missing — the scanner needs Alpaca for option chains.")
        return

    # ── Filters ───────────────────────────────────────────────────
    st.markdown("#### Filters")
    f1, f2, f3 = st.columns(3)
    with f1:
        dte_min, dte_max = st.slider(
            "DTE window", min_value=1, max_value=120, value=(25, 45), step=1,
            key="ts_dte", help="Days to expiry. 30-45 is the classic wheel CSP window.",
        )
    with f2:
        delta_min, delta_max = st.slider(
            "Delta band (|Δ|)", min_value=0.05, max_value=0.50, value=(0.15, 0.35), step=0.01,
            key="ts_delta", help="Absolute put delta. ~0.15-0.35 is the usual CSP range.",
        )
    with f3:
        min_distance = st.slider(
            "Min distance OTM (%)", min_value=0.0, max_value=25.0, value=4.0, step=0.5,
            key="ts_dist", help="How far below spot the strike must sit. Higher = safer.",
        )
    g1, g2, g3 = st.columns(3)
    with g1:
        min_ann_ror = st.slider(
            "Min annualized RoR (%)", min_value=0.0, max_value=80.0, value=15.0, step=1.0,
            key="ts_ror", help="Annualized return on the cash collateral.",
        )
    with g2:
        min_composite = st.slider(
            "Min composite score", min_value=0, max_value=100, value=55, step=5,
            key="ts_comp", help="Blended 0-100 score. Strong ≥75 · Good ≥60 · Marginal ≥45.",
        )
    with g3:
        require_liquidity = st.checkbox(
            "Require liquidity (OI ≥ 100, spread ≤ 8%)", value=True, key="ts_liq",
        )

    # ── Ticker selection ──────────────────────────────────────────
    st.markdown("#### Tickers to scan")
    scan_tickers = st.multiselect(
        "Universe tickers (deselect to scan a faster subset)",
        options=tickers_all, default=tickers_all, key="ts_tickers",
    )
    extra_csv = st.text_input(
        "Add ad-hoc tickers (comma-separated)", value="", key="ts_extra",
        help="Names outside the universe — e.g. a ticker you're specifically eyeing.",
    )
    extras = [t.strip().upper() for t in extra_csv.split(",") if t.strip()]
    scan_list = sorted(set(scan_tickers) | set(extras))

    if not scan_list:
        st.info("Select at least one ticker to scan.")
        return

    est_sec = len(scan_list)  # ~1 chain call/ticker; cached calls are instant
    run = st.button(
        f"🔍 Run scan — {len(scan_list)} ticker(s)  (~{est_sec}s first run, cached after)",
        key="ts_run", type="primary",
    )
    if run:
        st.session_state["ts_results"] = _run_scan(
            scan_list, dte_min, dte_max,
        )

    results = st.session_state.get("ts_results")
    if results is None:
        st.info("Set filters, pick tickers, and click **Run scan**.")
        return

    raw_rows, scan_errors = results
    if not raw_rows:
        st.warning(
            "No put candidates returned. Check the DTE window and Alpaca chain availability."
        )
        if scan_errors:
            st.caption(f"{len(scan_errors)} ticker(s) errored: {', '.join(scan_errors[:15])}")
        return

    # ── Apply filters ─────────────────────────────────────────────
    held = set()
    if df_open is not None and not df_open.empty and "Ticker" in df_open.columns:
        held = set(df_open["Ticker"].dropna().str.upper().unique())

    filtered = []
    for r in raw_rows:
        dte = r.get("dte")
        if dte is None or not (dte_min <= dte <= dte_max):
            continue
        d = r.get("delta")
        if d is None or not (delta_min <= d <= delta_max):
            continue
        if r["distance_pct"] < min_distance:
            continue
        if r["annualized_ror_pct"] < min_ann_ror:
            continue
        if r["composite"] < min_composite:
            continue
        if require_liquidity and not r["liquidity_ok"]:
            continue
        filtered.append(r)

    filtered.sort(key=lambda x: x["composite"], reverse=True)

    if not filtered:
        st.warning(
            f"Scanned {len(raw_rows)} puts across {len(scan_list)} ticker(s) — "
            "none passed the filters. Loosen the thresholds above."
        )
        return

    # ── Summary counters ──────────────────────────────────────────
    st.markdown("#### Results")
    n = len(filtered)
    strong = sum(1 for r in filtered if r["verdict"] == "Strong")
    good = sum(1 for r in filtered if r["verdict"] == "Good")
    marginal = sum(1 for r in filtered if r["verdict"] == "Marginal")
    distinct = len({r["ticker"] for r in filtered})
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Candidates", n)
    s2.metric("Strong / Good", f"{strong} / {good}")
    s3.metric("Marginal", marginal)
    s4.metric("Distinct tickers", distinct)

    # ── Results table ─────────────────────────────────────────────
    table_rows = []
    for r in filtered:
        table_rows.append({
            "Ticker": r["ticker"] + (" ✓" if r["ticker"] in held else ""),
            "Strike": r["strike"],
            "Expiry": r["expiry"],
            "DTE": r["dte"],
            "Premium": r["premium"],
            "RoR %": r["ror_pct"],
            "Ann.RoR %": r["annualized_ror_pct"],
            "Dist %": r["distance_pct"],
            "Δ": r["delta"],
            "PoP %": r["pop_pct"],
            "Breakeven": r["breakeven"],
            "OI": r["open_interest"],
            "Composite": r["composite"],
            "Verdict": r["verdict"],
        })
    df = pd.DataFrame(table_rows)
    st.dataframe(
        df, use_container_width=True, hide_index=True,
        column_config={
            "Strike":    st.column_config.NumberColumn(format="$%.2f"),
            "Premium":   st.column_config.NumberColumn(format="$%.2f",
                             help="Put mid price — premium collected per share."),
            "RoR %":     st.column_config.NumberColumn(format="%.2f%%",
                             help="Single-cycle return on collateral (premium / strike)."),
            "Ann.RoR %": st.column_config.NumberColumn(format="%.1f%%",
                             help="Annualized return on the cash secured."),
            "Dist %":    st.column_config.NumberColumn(format="%.1f%%",
                             help="How far the strike sits below spot. Higher = safer."),
            "Δ":         st.column_config.NumberColumn(format="%.3f",
                             help="Absolute put delta — assignment-probability proxy."),
            "PoP %":     st.column_config.NumberColumn(format="%.0f%%",
                             help="Probability of profit ≈ 1 − |delta|."),
            "Breakeven": st.column_config.NumberColumn(format="$%.2f",
                             help="Effective cost basis if assigned (strike − premium)."),
            "Composite": st.column_config.NumberColumn(format="%.0f",
                             help="Blended 0-100: 40% yield, 30% distance, 30% delta."),
        },
    )
    st.caption(
        "✓ = you already hold this ticker. Composite blend: 40% annualized yield · "
        "30% distance OTM · 30% delta (sweet spot ≈ 0.25). Verdict: Strong ≥75 · "
        "Good ≥60 · Marginal ≥45."
    )
    if scan_errors:
        st.caption(f"⚠️ {len(scan_errors)} ticker(s) returned no chain: {', '.join(scan_errors[:15])}")

    # ── Sector-style summary: candidate count per ticker ──────────
    with st.expander("📊 Candidate count by ticker"):
        by_ticker = (
            df.assign(_t=df["Ticker"].str.replace(" ✓", "", regex=False))
            .groupby("_t").size().sort_values(ascending=False)
        )
        st.bar_chart(by_ticker)


def _run_scan(scan_list, dte_min, dte_max) -> tuple:
    """Pull spot + put chains for every ticker, score all OTM puts.

    Returns (scored_rows, error_tickers). Scoring is unfiltered here — the UI
    applies the user's thresholds afterward so re-filtering needs no re-pull.
    """
    today = date.today()
    expiry_from = (today + timedelta(days=int(dte_min))).isoformat()
    expiry_to = (today + timedelta(days=int(dte_max))).isoformat()

    spots = data_mod.batch_spot_prices(tuple(scan_list))
    scored = []
    errors = []
    progress = st.progress(0.0, text="Scanning…")
    total = len(scan_list)

    for i, ticker in enumerate(scan_list, start=1):
        progress.progress(i / total, text=f"Scanning {ticker} ({i}/{total})…")
        spot = float(spots.get(ticker, 0) or 0)
        if spot <= 0:
            errors.append(ticker)
            continue
        try:
            chain = data_mod.load_put_chain(
                ticker=ticker,
                expiry_from=expiry_from, expiry_to=expiry_to,
                strike_min=spot * 0.75, strike_max=spot * 1.00,
            )
        except Exception as e:
            logger.debug("Theta scan chain error %s: %s", ticker, e)
            errors.append(ticker)
            continue
        if not chain:
            errors.append(ticker)
            continue
        for row in chain:
            sc = scoring.score_csp_candidate(spot, row)
            sc["ticker"] = ticker
            sc["spot"] = spot
            scored.append(sc)

    progress.empty()
    return scored, errors
