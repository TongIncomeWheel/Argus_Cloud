"""Streamlit UI for the PMCC Engine.

Renders the 4-block doctrine review plus interactive tools (strike scanner,
roll simulator, Monte Carlo scorecard, per-ticker settings editor).

Top-level entry: render_pmcc_engine(df_open, settings, spot_prices, save_settings_fn).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Dict, Optional

import pandas as pd
import streamlit as st

from . import doctrine
from . import data_io
from . import posture as posture_mod
from . import regime as regime_mod
from . import review as review_mod
from . import rolls as rolls_mod
from . import scorecard as scorecard_mod
from . import state as state_mod
from . import strikes as strikes_mod
from . import theta_math
from . import triggers as triggers_mod

logger = logging.getLogger(__name__)


# ─── Top-level render ──────────────────────────────────────────────


def render_pmcc_engine(df_open, settings: dict, spot_prices: Optional[dict] = None,
                        save_settings_fn=None) -> None:
    """Render the full PMCC Engine tab."""
    spot_prices = spot_prices or {}

    st.markdown("### 🧠 PMCC Engine")
    st.caption(
        "Operationalizes the PMCC Operating Doctrine (v2). Regime classification, "
        "dynamic theta hurdle, tripwires, strike scanner, roll decomposer, and Monte Carlo "
        "scorecard — ticker-portable across SPY, MSFT, GOOG and other liquid underlyings."
    )

    # ── Ticker selector ───────────────────────────────────────────
    pmcc_tickers = sorted(set(settings.get("pmcc_tickers", ["SPY"]) or ["SPY"]))
    if df_open is not None and not df_open.empty and "Ticker" in df_open.columns:
        # Surface any ticker with LEAP+CC structure even if not flagged
        all_tickers_in_book = set(df_open["Ticker"].dropna().str.upper().unique())
        for t in pmcc_tickers:
            if t in all_tickers_in_book:
                continue
        pmcc_tickers = sorted(set(pmcc_tickers) | (all_tickers_in_book & set(pmcc_tickers)))

    if not pmcc_tickers:
        st.warning("No PMCC tickers configured. Add one in Config → PMCC Tickers.")
        return

    sel_col, ref_col = st.columns([3, 1])
    with sel_col:
        ticker = st.selectbox(
            "Ticker",
            pmcc_tickers,
            key="pmcc_engine_ticker",
            help="Engine state is per-ticker (vol_median, tripwires, ex-div calendar).",
        )
    with ref_col:
        st.write("")  # spacer
        if st.button("🔄 Refresh data", key="pmcc_refresh"):
            data_io.daily_closes.clear()
            data_io.get_vix.clear()
            data_io.vix_history.clear()
            data_io.rv30_history.clear()
            st.rerun()

    ticker = ticker.upper()
    ticker_state = state_mod.get_ticker_state(settings, ticker)

    # ── Sub-tabs for the 4-block review + tools ───────────────────
    tab_review, tab_scanner, tab_roll, tab_score, tab_state = st.tabs([
        "📋 4-Block Review",
        "🔎 Strike Scanner",
        "🔁 Roll Simulator",
        "🎲 Trade Scorecard",
        "⚙️ Engine State",
    ])

    with tab_review:
        _render_review_block(ticker, df_open, settings, ticker_state, spot_prices)
    with tab_scanner:
        _render_strike_scanner(ticker, df_open, settings, ticker_state, spot_prices)
    with tab_roll:
        _render_roll_simulator(ticker, df_open, settings, ticker_state, spot_prices)
    with tab_score:
        _render_scorecard_panel(ticker, ticker_state, spot_prices)
    with tab_state:
        _render_state_editor(ticker, settings, ticker_state, save_settings_fn)


# ─── Block 1-4 review ──────────────────────────────────────────────


def _render_review_block(ticker, df_open, settings, ticker_state, spot_prices):
    """The four-block review per Doctrine §10."""

    # ── Pull live market state ────────────────────────────────────
    with st.spinner("📊 Loading market state…"):
        spot = float(spot_prices.get(ticker, 0) or 0)
        if spot <= 0:
            recent = data_io.daily_closes(ticker, period="5d")
            spot = float(recent[-1]) if recent else 0.0
        hv30 = data_io.hv30_from_ticker(ticker) or 0.0
        vol_axis = (ticker_state.get("vol_axis") or "VIX").upper()
        current_vol = data_io.current_iv_signal(ticker, vol_axis=vol_axis) or 0.0
        ivr = data_io.ivr_for_ticker(ticker, vol_axis=vol_axis)
        cell = regime_mod.regime_cell(
            current_vol=current_vol,
            median_vol=ticker_state.get("vol_median_5yr", 18.0),
            ivr=ivr if ivr is not None else 50.0,
        )
        cell["vol_axis"] = vol_axis

    # ── BLOCK 1 — MARKET STATE ────────────────────────────────────
    st.markdown("#### Block 1 — Market State")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(f"{ticker} Spot", f"${spot:.2f}" if spot else "—")
    c2.metric(f"{vol_axis}", f"{current_vol:.1f}" if current_vol else "—",
              delta=f"median {ticker_state.get('vol_median_5yr', '—')}",
              delta_color="off")
    c3.metric("HV30", f"{hv30*100:.1f}%" if hv30 else "—")
    c4.metric("IVR (52w)", f"{ivr:.0f}" if ivr is not None else "—")
    c5.metric("Regime cell", cell["cell_label"])

    st.info(
        f"**Posture mandated:** `{cell['posture']}` — {cell.get('description', '')}"
        + (f"  \n**DTE band:** {cell['dte_weeks'][0]}–{cell['dte_weeks'][1]} weeks." if cell.get("dte_weeks") else "")
        + (f"  \n**Doctrine target array:** {doctrine.array_description(cell['array'])} "
           "_(your actual layout shown in Block 2)_"
           if cell.get("array") else "")
    )

    # ── BLOCK 2 — POSITION TABLE ──────────────────────────────────
    st.markdown("#### Block 2 — Position Table")
    longs, shorts = _extract_pmcc_positions(ticker, df_open, spot)
    if not longs and not shorts:
        st.info(f"No PMCC positions found for {ticker}. Add a LEAP + short calls to engage the engine.")
        return

    pos_rows = []
    for leg in longs + shorts:
        pos_rows.append({
            "Side": leg["side"],
            "Type": leg["argus_type"],
            "Qty": leg.get("qty", 1),
            "Strike": leg["strike"],
            "Expiry": leg.get("expiry"),
            "DTE": leg.get("dte"),
            "Mark": leg.get("mark"),
            "Δ/share": leg.get("delta"),
            "Θ/day": leg.get("theta_per_day"),
            "Extrinsic": leg.get("extrinsic"),
            "$ from spot": round(leg["strike"] - spot, 2) if spot else None,
            "Hurdle $/day": theta_math.theta_hurdle(leg["strike"], hv30) if hv30 else None,
            "Hurdle?": _hurdle_flag(leg, spot, hv30),
        })
    pos_df = pd.DataFrame(pos_rows)
    st.dataframe(
        pos_df, use_container_width=True, hide_index=True,
        column_config={
            "Mark":       st.column_config.NumberColumn(format="$%.2f", help="Current option mark (mid-of-market)."),
            "Strike":     st.column_config.NumberColumn(format="$%.2f"),
            "Δ/share":    st.column_config.NumberColumn(format="%.3f",
                              help="Delta per share. Long calls positive; short calls show their per-share delta (book-greek sums use sign)."),
            "Θ/day":      st.column_config.NumberColumn(format="$%.3f",
                              help="Theta per share, per day. For a short, this is the daily premium decay you collect."),
            "Extrinsic":  st.column_config.NumberColumn(format="$%.2f",
                              help="Time value remaining in the mark (mark minus intrinsic). When extrinsic ≈ 0, the short is acting as synthetic stock."),
            "$ from spot": st.column_config.NumberColumn(format="$%+.2f"),
            "Hurdle $/day": st.column_config.NumberColumn(format="$%.3f",
                              help="Minimum daily theta this short must produce to be worth the assignment / gamma risk it carries. Computed dynamically per §2."),
            "Hurdle?":    st.column_config.TextColumn(
                              help="✅ short's |Θ/day| ≥ hurdle (earning at spec). ⚠️ below hurdle — flag for strike re-selection on next roll cycle. — long legs don't have a hurdle."),
        },
    )
    with st.expander("ℹ️ What is the Hurdle? (and why OTM legs often fail it on purpose)"):
        st.markdown(
            "**Hurdle $/day** is the doctrine §2 dynamic theta floor — the minimum daily theta a "
            "short option must produce to earn its keep relative to the directional risk it carries.\n\n"
            "**Formula:** `Hurdle $/day = strike × HV30 / √252 × 0.04`\n\n"
            "- `strike × HV30 / √252` is the **expected 1σ daily $ move** of the underlying at that strike\n"
            "- `× 0.04` captures **4%** of that move as time premium per day — the doctrine's calibration rate\n\n"
            "**Why it's dynamic:** when realized volatility expands, the daily expected move rises and so does "
            "the hurdle. A short that cleared the floor at 12% HV may underearn at 25% HV without the operator "
            "changing anything. Static dollar floors break in regime transitions.\n\n"
            "### Flag interpretation (it depends on ITM vs OTM)\n\n"
            "| Leg          | Below hurdle | Above hurdle |\n"
            "|--------------|--------------|--------------|\n"
            "| **ITM short**| ⚠️ ITM — real concern. ITM legs exist to harvest extrinsic; if they're below the floor the strike/DTE/vol mix needs re-selection on the next roll cycle. | ✅ Earning at spec. |\n"
            "| **OTM short**| ℹ️ OTM — usually expected. OTM legs are mostly directional headroom + LEAP-cap; their theta capture is typically modest. The doctrine §2 regime caveat explicitly says: *\"in sustained low-vol regimes, even the dynamic hurdle may be unreachable across the OTM surface.\"* Not a roll signal on its own. | ✅ Earning at spec **and** providing growth participation — best of both. |\n\n"
            "### Why the universal hurdle still matters even with the OTM caveat\n\n"
            "The hurdle is a uniform yardstick so the **book-level** Yield Ratio in Block 3 has meaning. "
            "If every OTM leg sits at ℹ️ but every ITM leg sits at ✅, the book can still clear ≥1.0 yield ratio "
            "because ITM legs carry the income work. If the whole array goes below 1.0, the doctrine offers two responses:\n\n"
            "1. Accept yield ratio 0.80–0.95 in a low-vol regime and lean OTM for defensive posture\n"
            "2. Skew ATM / slightly ITM for richer extrinsic, accepting more gamma exposure\n\n"
            "**Operator decision, not engine override.** State the chosen response in the §10 review when the regime forces it."
        )

    # ── Array layout visualization ────────────────────────────────
    short_dicts = [{"strike": s["strike"], "label": s.get("expiry", ""), "extrinsic": s.get("extrinsic", 0.0)} for s in shorts]
    layout = posture_mod.array_layout(spot, short_dicts)
    current_array_label = f"{layout['itm_count']}_{layout['otm_count']}"
    doctrine_array = cell.get("array") or "—"
    array_match = current_array_label == doctrine_array

    st.markdown(
        f"**Your shorts:** {layout['itm_count']} ITM + {layout['otm_count']} OTM  ·  "
        f"**Doctrine target** ({cell['cell_label']}): {doctrine.array_description(doctrine_array)}  ·  "
        f"{'✅ on-doctrine' if array_match else '⚠️ off-doctrine'}"
    )
    st.caption(
        "Compares your current short-call placement (how many sit ITM vs OTM of spot) against the doctrine §1 "
        "target for your regime cell. LEAP-vs-CC ratio is the **Coverage** metric in Block 3 — different thing."
    )
    layout_line = _format_array_line(layout, spot)
    st.code(layout_line, language="text")

    # ── BLOCK 3 — AGGREGATE ───────────────────────────────────────
    st.markdown("#### Block 3 — Aggregate")
    longs_for_book = [{"qty": l.get("qty", 1), "delta": l.get("delta", 0.0), "theta": l.get("theta_per_day", 0.0)} for l in longs]
    shorts_for_book = [{"qty": s.get("qty", 1), "delta": s.get("delta", 0.0), "theta": s.get("theta_per_day", 0.0)} for s in shorts]
    greeks = theta_math.book_greeks(longs_for_book, shorts_for_book)

    yr = theta_math.yield_ratio(
        [{"strike": s["strike"], "theta_per_day": s.get("theta_per_day", 0.0)} for s in shorts],
        hv30,
    ) if hv30 else 0.0
    tpd = theta_math.theta_per_delta(greeks["net_theta"], greeks["net_delta"])
    rating = theta_math.theta_per_delta_rating(tpd)

    daily_risk = theta_math.daily_risk_one_sigma(greeks["net_delta"], spot, hv30) if hv30 else 0.0
    theta_cov = theta_math.theta_coverage(greeks["net_theta"], daily_risk) if daily_risk else 0.0

    coverage = posture_mod.coverage_ratios(longs_for_book, shorts_for_book)

    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Net Δ ($/$1)", f"${greeks['net_delta']:,.1f}")
    a2.metric("Net Θ ($/day)", f"${greeks['net_theta']:,.2f}")
    a3.metric("Θ/Δ", f"${tpd:.2f}",
              delta=rating,
              delta_color="normal" if rating == "Optimal" else ("off" if rating == "Acceptable" else "inverse"))
    a4.metric("Yield ratio", f"{yr:.2f}",
              delta="above hurdle" if yr >= doctrine.YIELD_RATIO_PASS else "below hurdle",
              delta_color="normal" if yr >= doctrine.YIELD_RATIO_PASS else "inverse")

    b1, b2, b3, b4 = st.columns(4)
    b1.metric("1σ daily risk", f"${daily_risk:,.2f}")
    b2.metric("Θ coverage", f"{theta_cov*100:.0f}%")
    b3.metric("Coverage (contract)", f"{coverage['long_total']}:{coverage['short_total']}")
    b4.metric("Coverage (chassis)", f"{coverage['chassis_qty']}:{coverage['short_total']}")

    # ── Tripwire status ───────────────────────────────────────────
    today = date.today()
    shorts_for_tripwires = [
        {
            "strike": s["strike"],
            "spot": spot,
            "mark": s.get("mark", 0.0),
            "dte": s.get("dte", 99),
            "premium_received": s.get("premium_received", 0.0) or s.get("mark", 0.0),
            "is_call": s["argus_type"] == "CC",
        }
        for s in shorts
    ]
    trip_results = triggers_mod.check_all_tripwires(
        spot=spot, vix=current_vol if vol_axis == "VIX" else current_vol,
        shorts=shorts_for_tripwires, state=ticker_state, today=today,
    )

    st.markdown("**Tripwire status:**")
    trip_cols = st.columns(len(trip_results))
    for col, t in zip(trip_cols, trip_results):
        if t.triggered:
            col.error(f"🔴 {t.name}")
            col.caption(t.detail)
        else:
            col.success(f"✅ {t.name}")
            col.caption(t.detail)

    # Re-optimization check (§13)
    reopt = posture_mod.reoptimization_check(
        net_theta=greeks["net_theta"],
        net_delta=greeks["net_delta"],
        shorts=[{"strike": s["strike"], "extrinsic": s.get("extrinsic", 0.0)} for s in shorts],
        spot=spot,
        array_center_strike=ticker_state.get("array_center_strike"),
    )
    if reopt["reoptimize"]:
        st.warning("**§13 Re-optimization triggers fired:**\n" + "\n".join(f"  - {r}" for r in reopt["reasons"]))

    # LEAPS refresh triggers (§9)
    for l in longs:
        rt = triggers_mod.leaps_refresh_trigger({"delta": l.get("delta", 0.0), "dte": l.get("dte", 365)})
        if rt:
            icon = {"forced": "🔴", "immediate": "🔴", "evaluate": "🟡", "schedule": "🟡"}.get(rt.urgency, "ℹ️")
            st.warning(f"{icon} **LEAPS @ {l['strike']:.2f} ({l.get('expiry')})** — {rt.reason} (urgency: `{rt.urgency}`)")

    # ── BLOCK 4 — ACTION ──────────────────────────────────────────
    st.markdown("#### Block 4 — Action")

    # Compute per-leg roll triggers regardless — used to distinguish book-level
    # tripwire breaches from leg-level forced rolls.
    action_rows = []
    any_leg_triggered = False
    for s in shorts:
        rt = triggers_mod.short_roll_trigger({
            "mark": s.get("mark", 0.0),
            "strike": s["strike"],
            "dte": s.get("dte", 99),
            "premium_received": s.get("premium_received", 0.0) or s.get("mark", 0.0),
            "is_call": s["argus_type"] == "CC",
        }, spot=spot)
        if rt.triggered:
            any_leg_triggered = True
        action_rows.append({
            "Leg": f"{s['argus_type']} {s['strike']:.2f} {s.get('expiry')}",
            "DTE": s.get("dte"),
            "Trigger?": "🔴 YES" if rt.triggered else "✅ hold",
            "Reason": rt.reason,
            "Urgency": rt.urgency,
        })

    breached_tripwires = [t for t in trip_results if bool(t)]
    any_book_signal = bool(breached_tripwires) or reopt["reoptimize"]

    if not any_book_signal and not any_leg_triggered:
        st.success("✅ No tripwires breached. No per-leg roll triggers. Silent days are good days. Hold.")
    else:
        # Surface what fired and at which level (book vs leg).
        if breached_tripwires:
            st.error(
                "🔴 **Book-level tripwire(s) breached:**  \n"
                + "  \n".join(f"  - **{t.name}** — {t.detail}" for t in breached_tripwires)
            )
        if reopt["reoptimize"]:
            st.warning(
                "🟡 **§13 Re-optimization triggers:**  \n"
                + "  \n".join(f"  - {r}" for r in reopt["reasons"])
            )

        if any_leg_triggered:
            st.markdown("**Per-leg roll evaluation** — at least one leg has its own roll trigger:")
        else:
            st.info(
                "ℹ️ **Per-leg roll evaluation — no individual leg has fired a roll trigger.**  \n"
                "The book-level signal above is informational: a tripwire flags a regime shift "
                "or array drift, but the legs themselves don't yet meet the §4 thresholds (profit ≥ 50%, "
                "DTE ≤ 10 with profit < 50%, or extrinsic < $1.00). "
                "Options: (a) hold and re-check next session, "
                "(b) manually evaluate a defensive roll via the **🔁 Roll Simulator**, "
                "or (c) update tripwire levels in **⚙️ Engine State** if the array has been re-centered."
            )
        st.dataframe(pd.DataFrame(action_rows), use_container_width=True, hide_index=True)
        st.caption(
            "Per-leg triggers: profit ≥ 50% (harvest) · profit ≥ 80% (close) · extrinsic < $1 · "
            "DTE ≤ 10 with profit < 50%. Use the **🔁 Roll Simulator** tab to decompose any proposed roll."
        )

    # ── Text-form report (collapsible) ────────────────────────────
    with st.expander("📄 Plain-text 4-block report (copy/paste)"):
        agg_for_report = {
            **greeks,
            "theta_per_delta": tpd,
            "theta_per_delta_rating": rating,
            "coverage": coverage,
        }
        positions_for_report = [
            {
                "type": l["argus_type"], "strike": l["strike"], "dte": l.get("dte"),
                "mark": l.get("mark"), "delta": l.get("delta"),
                "theta_per_day": l.get("theta_per_day"), "extrinsic": l.get("extrinsic", 0.0),
            }
            for l in longs + shorts
        ]
        text_report = review_mod.render_review(
            ticker=ticker, spot=spot,
            cell={**cell, "ivr": ivr},
            aggregate=agg_for_report,
            positions=positions_for_report,
            tripwires=trip_results,
            yield_ratio=yr,
        )
        st.code(text_report, language="text")


# ─── Strike scanner sub-tab ────────────────────────────────────────


def _render_strike_scanner(ticker, df_open, settings, ticker_state, spot_prices):
    """Pull a chain, filter to ITM + OTM candidates within the doctrine band."""
    st.markdown("#### 🔎 Strike Scanner")
    st.caption(
        "Pulls live option chain via Alpaca, filters to the 1–5% bands around spot "
        "(§3 starting point), and tests each candidate against the §2 dynamic theta hurdle "
        "and §3 liquidity floors. Sort by closeness to 3% or by hurdle pass."
    )

    spot = float(spot_prices.get(ticker, 0)) or 0.0
    hv30 = data_io.hv30_from_ticker(ticker) or 0.0
    if spot <= 0 or hv30 <= 0:
        st.warning("Spot or HV30 unavailable — scanner needs both.")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        dte_min = st.number_input("DTE min (days)", min_value=1, max_value=180, value=21, step=1, key="pmcc_scan_dte_min")
    with c2:
        dte_max = st.number_input("DTE max (days)", min_value=2, max_value=180, value=45, step=1, key="pmcc_scan_dte_max")
    with c3:
        st.write("")
        run = st.button("🔄 Pull chain", key="pmcc_scan_run", type="primary")

    if not run and "_pmcc_chain_cache" not in st.session_state:
        st.info("Pick DTE range and click **Pull chain**. Alpaca chain fetch + scoring takes ~5–15s.")
        return

    if run:
        expiry_from = (date.today() + timedelta(days=int(dte_min))).isoformat()
        expiry_to = (date.today() + timedelta(days=int(dte_max))).isoformat()
        # Strike window 10% around spot for filtering speed
        strike_min = spot * 0.90
        strike_max = spot * 1.10
        with st.spinner(f"Fetching {ticker} chain from Alpaca ({dte_min}-{dte_max} DTE)…"):
            chain = data_io.load_chain(
                ticker=ticker,
                expiry_from=expiry_from, expiry_to=expiry_to,
                strike_min=strike_min, strike_max=strike_max,
                option_type="call",
            )
        st.session_state["_pmcc_chain_cache"] = chain
        st.session_state["_pmcc_chain_meta"] = {
            "ticker": ticker, "dte_min": int(dte_min), "dte_max": int(dte_max),
            "spot": spot, "hv30": hv30, "pulled_at": datetime.utcnow().isoformat(),
        }

    chain = st.session_state.get("_pmcc_chain_cache") or []
    meta = st.session_state.get("_pmcc_chain_meta", {})
    if not chain:
        st.warning("No chain rows returned. Check Alpaca creds + DTE window.")
        return
    st.caption(f"Chain: {len(chain)} call rows · spot ${meta.get('spot', 0):.2f} · HV30 {meta.get('hv30', 0)*100:.1f}% · pulled {meta.get('pulled_at', '')}")

    itm = strikes_mod.itm_candidates(spot=meta["spot"], chain=chain, hv30=meta["hv30"])
    otm = strikes_mod.otm_candidates(spot=meta["spot"], chain=chain, hv30=meta["hv30"])

    def _to_df(candidates, side_label):
        if not candidates:
            return pd.DataFrame()
        rows = []
        for c in candidates:
            rows.append({
                "Side": side_label,
                "Strike": c["strike"],
                "Expiry": c.get("expiry"),
                "DTE": c.get("dte"),
                "Mid": c.get("mid"),
                "Δ": c.get("delta"),
                "Θ": c.get("theta"),
                "Extrinsic": c.get("extrinsic"),
                "Hurdle": c.get("hurdle"),
                "Hurdle?": "✅" if c.get("hurdle_pass") else "❌",
                "TV?": "✅" if c.get("tv_pass") else "❌",
                "OI": c.get("open_interest"),
                "Spread%": (
                    (float(c["ask"]) - float(c["bid"])) / float(c["mid"]) * 100
                    if c.get("bid") and c.get("ask") and c.get("mid") else None
                ),
                "%_from_spot": c.get("pct_below_spot") or c.get("pct_above_spot"),
            })
        return pd.DataFrame(rows)

    st.markdown("**ITM candidates (1–5% below spot):**")
    itm_df = _to_df(itm, "ITM")
    if itm_df.empty:
        st.write("_(none in band)_")
    else:
        st.dataframe(itm_df, use_container_width=True, hide_index=True, column_config={
            "Strike":    st.column_config.NumberColumn(format="$%.2f"),
            "Mid":       st.column_config.NumberColumn(format="$%.2f"),
            "Δ":         st.column_config.NumberColumn(format="%.3f"),
            "Θ":         st.column_config.NumberColumn(format="$%.3f"),
            "Extrinsic": st.column_config.NumberColumn(format="$%.2f"),
            "Hurdle":    st.column_config.NumberColumn(format="$%.3f"),
            "Spread%":   st.column_config.NumberColumn(format="%.1f%%"),
            "%_from_spot": st.column_config.NumberColumn(format="%.2f%%"),
        })

    st.markdown("**OTM candidates (1–5% above spot):**")
    otm_df = _to_df(otm, "OTM")
    if otm_df.empty:
        st.write("_(none in band)_")
    else:
        st.dataframe(otm_df, use_container_width=True, hide_index=True, column_config={
            "Strike":    st.column_config.NumberColumn(format="$%.2f"),
            "Mid":       st.column_config.NumberColumn(format="$%.2f"),
            "Δ":         st.column_config.NumberColumn(format="%.3f"),
            "Θ":         st.column_config.NumberColumn(format="$%.3f"),
            "Extrinsic": st.column_config.NumberColumn(format="$%.2f"),
            "Hurdle":    st.column_config.NumberColumn(format="$%.3f"),
            "Spread%":   st.column_config.NumberColumn(format="%.1f%%"),
            "%_from_spot": st.column_config.NumberColumn(format="%.2f%%"),
        })


# ─── Roll simulator sub-tab ────────────────────────────────────────


def _render_roll_simulator(ticker, df_open, settings, ticker_state, spot_prices):
    """Decompose a hypothetical roll. Inputs: BTC leg + STO leg."""
    st.markdown("#### 🔁 Roll Simulator")
    st.caption(
        "Enter the old short (BTC) and the proposed new short (STO). The engine returns "
        "the §5 decomposition: premium paid, intrinsic uncapped, extrinsic captured, "
        "theta runway gained, and the verdict (need ≥2 of {intrinsic uncap, theta gained, gamma reduced})."
    )

    spot = float(spot_prices.get(ticker, 0)) or 0.0
    st.metric(f"{ticker} spot", f"${spot:.2f}")

    left, right = st.columns(2)
    with left:
        st.markdown("**BTC (close)**")
        old_strike = st.number_input("Strike", value=735.0, step=1.0, key="pmcc_roll_old_strike")
        old_mark = st.number_input("Mark $", value=8.00, step=0.05, key="pmcc_roll_old_mark")
        old_dte = st.number_input("DTE", min_value=0, max_value=400, value=14, step=1, key="pmcc_roll_old_dte")
        old_delta = st.number_input("Δ/share", value=0.55, step=0.01, format="%.3f", key="pmcc_roll_old_delta")
        old_gamma = st.number_input("Γ/share", value=0.02, step=0.001, format="%.4f", key="pmcc_roll_old_gamma")
        old_theta = st.number_input("Θ/day (negative = decay)", value=-0.12, step=0.01, format="%.3f", key="pmcc_roll_old_theta")
        old_vega = st.number_input("Vega", value=0.10, step=0.01, format="%.3f", key="pmcc_roll_old_vega")
    with right:
        st.markdown("**STO (open)**")
        new_strike = st.number_input("Strike", value=740.0, step=1.0, key="pmcc_roll_new_strike")
        new_mark = st.number_input("Mark $", value=9.00, step=0.05, key="pmcc_roll_new_mark")
        new_dte = st.number_input("DTE", min_value=0, max_value=400, value=30, step=1, key="pmcc_roll_new_dte")
        new_delta = st.number_input("Δ/share", value=0.50, step=0.01, format="%.3f", key="pmcc_roll_new_delta")
        new_gamma = st.number_input("Γ/share", value=0.015, step=0.001, format="%.4f", key="pmcc_roll_new_gamma")
        new_theta = st.number_input("Θ/day (negative = decay)", value=-0.10, step=0.01, format="%.3f", key="pmcc_roll_new_theta")
        new_vega = st.number_input("Vega", value=0.12, step=0.01, format="%.3f", key="pmcc_roll_new_vega")

    old_leg = dict(mark=old_mark, strike=old_strike, dte=old_dte, delta=old_delta,
                   gamma=old_gamma, theta=old_theta, vega=old_vega)
    new_leg = dict(mark=new_mark, strike=new_strike, dte=new_dte, delta=new_delta,
                   gamma=new_gamma, theta=new_theta, vega=new_vega)
    decomp = rolls_mod.roll_decomposition(old_leg, new_leg, spot=spot, is_call=True)

    st.markdown("**§5 Decomposition:**")
    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Net cash", f"${decomp.net_cash:+,.2f}", delta="credit" if decomp.net_cash > 0 else "debit", delta_color="normal" if decomp.net_cash > 0 else "inverse")
    d2.metric("Intrinsic uncap", f"${decomp.intrinsic_uncapped:+,.2f}", delta=f"strike lift {decomp.strike_lift:+.2f}")
    d3.metric("Extrinsic captured", f"${decomp.extrinsic_captured:+,.2f}")
    d4.metric("Theta runway gained", f"${decomp.theta_runway_gained:+,.2f}", delta=f"+{decomp.dte_gained} days")

    g1, g2, g3, g4 = st.columns(4)
    g1.metric("Δ change", f"{decomp.delta_change:+.1f} sh")
    g2.metric("Γ change", f"{decomp.gamma_change:+.3f}", delta="↓ better" if decomp.gamma_change < 0 else "↑ worse",
              delta_color="normal" if decomp.gamma_change < 0 else "inverse")
    g3.metric("Vega change", f"{decomp.vega_change:+.2f}")
    g4.metric("Θ change", f"${decomp.theta_change:+.2f}/d")

    e1, e2 = st.columns(2)
    e1.metric("Expectancy", f"${decomp.expectancy:+,.2f}")
    color = {"pass": "success", "conditional": "warning", "fail": "error"}[decomp.verdict]
    getattr(e2, color)(f"**§5 Verdict: {decomp.verdict.upper()}**  \n"
                      f"Positive metrics: {', '.join(decomp.positive_metrics) or 'none'}"
                      + (f"  \n_{decomp.rejection_reason}_" if decomp.rejection_reason else ""))


# ─── Scorecard sub-tab ─────────────────────────────────────────────


def _render_scorecard_panel(ticker, ticker_state, spot_prices):
    """Monte Carlo §12 scorecard."""
    st.markdown("#### 🎲 Trade Scorecard (§12 Monte Carlo)")
    st.caption(
        "5,000-path GBM Monte Carlo under the ticker's HV30. Returns P&L distribution, "
        "assignment probability, Sharpe-equivalent, and CVaR. Verdict applies §12 cutoffs."
    )

    spot = float(spot_prices.get(ticker, 0)) or 0.0
    hv30 = data_io.hv30_from_ticker(ticker) or 0.17

    side = st.radio("Side", ["Short Call (CC)", "Short Put (CSP)"], horizontal=True, key="pmcc_sc_side")

    c1, c2, c3 = st.columns(3)
    with c1:
        sc_strike = st.number_input("Strike $", value=spot * (1.02 if side.startswith("Short Call") else 0.97) if spot else 100.0, step=1.0, key="pmcc_sc_strike")
    with c2:
        sc_premium = st.number_input("Premium $/share", value=5.0, step=0.05, key="pmcc_sc_premium")
    with c3:
        sc_dte = st.number_input("DTE", min_value=1, max_value=400, value=30, step=1, key="pmcc_sc_dte")

    c4, c5, c6 = st.columns(3)
    with c4:
        sc_hv = st.number_input("HV30 / σ", value=float(hv30), step=0.01, format="%.4f", key="pmcc_sc_hv")
    with c5:
        sc_rfr = st.number_input("Risk-free rate", value=doctrine.MC_DEFAULT_RISK_FREE_RATE, step=0.001, format="%.4f", key="pmcc_sc_rfr")
    with c6:
        sc_paths = st.number_input("Paths", min_value=100, max_value=20000, value=doctrine.MC_DEFAULT_PATHS, step=1000, key="pmcc_sc_paths")

    if st.button("🎲 Run scorecard", key="pmcc_sc_run", type="primary"):
        if side.startswith("Short Call"):
            sc = scorecard_mod.short_call_scorecard(
                spot=spot, strike=sc_strike, premium=sc_premium,
                dte_days=int(sc_dte), hv30=sc_hv, rfr=sc_rfr, paths=int(sc_paths),
            )
        else:
            sc = scorecard_mod.short_put_scorecard(
                spot=spot, strike=sc_strike, premium=sc_premium,
                dte_days=int(sc_dte), hv30=sc_hv, rfr=sc_rfr, paths=int(sc_paths),
            )
        verdict, reasons = scorecard_mod.verdict(sc)

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Expected P&L", f"${sc['mean_pnl']:+,.0f}", delta=f"σ ${sc['stdev']:,.0f}")
        m2.metric("P(profit ≥50%)", f"{sc['p_profit_50']*100:.0f}%")
        m3.metric("P(loss)", f"{sc['p_loss']*100:.0f}%")
        m4.metric("P(assignment)", f"{sc['p_assignment']*100:.0f}%")

        n1, n2, n3, n4 = st.columns(4)
        n1.metric("CVaR 5%", f"${sc['cvar_5']:+,.0f}")
        n2.metric("Sharpe-eq", f"{sc['sharpe']:.2f}")
        n3.metric("Ann return", f"{sc['ann_return']*100:.1f}%")
        n4.metric("Ann vol", f"{sc['ann_vol']*100:.1f}%")

        color = {"pass": "success", "conditional": "warning", "fail": "error"}[verdict]
        body = f"**§12 Verdict: {verdict.upper()}**"
        if reasons:
            body += "  \n" + "\n".join(f"  - {r}" for r in reasons)
        getattr(st, color)(body)


# ─── State editor sub-tab ──────────────────────────────────────────


def _render_state_editor(ticker, settings, ticker_state, save_settings_fn):
    """Edit per-ticker engine state (vol median, tripwires, ex-div calendar)."""
    st.markdown("#### ⚙️ Engine State")
    st.caption(
        f"Per-ticker state for **{ticker}**. Doctrine math is universal; calibration is per-ticker. "
        "Saved to ARGUS settings (gSheet + local)."
    )

    with st.form("pmcc_state_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            vol_median = st.number_input(
                "5-year median vol (VIX or IV30 %)",
                value=float(ticker_state.get("vol_median_5yr", 18.0)), step=0.5,
                help="The benchmark for vol band classification. For SPY/QQQ/IWM use the VIX median (~18). For single stocks, IV30 5-year median.",
            )
        with c2:
            vol_axis = st.selectbox(
                "Vol axis",
                ["VIX", "IV30"],
                index=0 if ticker_state.get("vol_axis", "VIX").upper() == "VIX" else 1,
                help="Index ETFs use VIX. Single stocks use their own IV30/HV30.",
            )
        with c3:
            qtr_div = st.number_input(
                "Quarterly dividend $",
                value=float(ticker_state.get("quarterly_dividend", 0.0)), step=0.05,
                help="Used by §6 ex-div trigger: extrinsic < 1.25 × this on ITM short → mandatory roll.",
            )

        st.markdown("**Tripwires (§4)**")
        t = ticker_state.get("tripwires", {}) or {}
        d = t.get("disorderly", {}) or {}
        t1, t2, t3, t4, t5 = st.columns(5)
        with t1:
            upper = st.number_input("Upper", value=float(t.get("upper") or 0.0), step=1.0, format="%.2f")
        with t2:
            lower = st.number_input("Lower", value=float(t.get("lower") or 0.0), step=1.0, format="%.2f")
        with t3:
            vix_shock = st.number_input("VIX shock", value=float(t.get("vix_shock") or 24.5), step=0.1, format="%.2f")
        with t4:
            dis_price = st.number_input("Disorderly price", value=float(d.get("price") or 0.0), step=1.0, format="%.2f")
        with t5:
            dis_vix = st.number_input("Disorderly VIX", value=float(d.get("vix") or 22.0), step=0.1, format="%.2f")

        st.markdown("**Ex-Dividend Calendar (§6)** — one per line: `YYYY-MM-DD, dividend_estimate`")
        cal_default = "\n".join(
            f"{e.get('date')}, {e.get('est_dividend')}"
            for e in (ticker_state.get("ex_div_calendar") or [])
        )
        cal_text = st.text_area("Ex-div entries", value=cal_default, height=120,
                                placeholder="2026-06-19, 1.85\n2026-09-18, 1.85")

        notes = st.text_area("Notes", value=ticker_state.get("notes", ""), height=80)

        submitted = st.form_submit_button("💾 Save engine state", type="primary")
        if submitted and save_settings_fn:
            cal = []
            for line in (cal_text or "").splitlines():
                line = line.strip()
                if not line:
                    continue
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 2:
                    try:
                        cal.append({"date": parts[0], "est_dividend": float(parts[1])})
                    except (ValueError, TypeError):
                        continue
            patch = {
                "vol_median_5yr": float(vol_median),
                "vol_axis": vol_axis,
                "quarterly_dividend": float(qtr_div),
                "tripwires": {
                    "upper": float(upper) if upper else None,
                    "lower": float(lower) if lower else None,
                    "vix_shock": float(vix_shock),
                    "disorderly": {"price": float(dis_price) if dis_price else None, "vix": float(dis_vix)},
                },
                "ex_div_calendar": cal,
                "notes": notes,
            }
            state_mod.upsert_ticker_state(settings, ticker, patch)
            save_settings_fn(settings)
            st.success(f"✅ Saved engine state for {ticker}.")
            st.rerun()
        elif submitted and not save_settings_fn:
            st.error("save_settings function not provided — cannot persist.")


# ─── Helpers ───────────────────────────────────────────────────────


def _extract_pmcc_positions(ticker: str, df_open, spot: float) -> tuple:
    """Pull longs (LEAP) and shorts (CC) for a ticker from df_open.

    Enriches each leg with live Alpaca quotes + locally-computed Greeks where possible.
    Returns (longs, shorts) lists of dicts. Each dict carries:
        side ("LONG"/"SHORT"), argus_type ("LEAP"/"CC"/...), qty, strike, expiry,
        dte, mark, premium_received, delta, theta_per_day, extrinsic.
    """
    longs = []
    shorts = []
    if df_open is None or df_open.empty:
        return longs, shorts

    df_t = df_open[df_open["Ticker"].str.upper() == ticker.upper()].copy()
    if df_t.empty:
        return longs, shorts

    # Build option_quotes key — fetch all in one batched call
    quote_key = []
    for _, r in df_t.iterrows():
        ttype = r.get("TradeType")
        if ttype not in ("LEAP", "CC"):
            continue
        try:
            exp = str(r.get("Expiry_Date"))
            strike = float(r.get("Option_Strike_Price_(USD)"))
            pc = "C"
            quote_key.append((ticker.upper(), exp, strike, pc))
        except (TypeError, ValueError):
            continue
    quotes = {}
    if quote_key:
        try:
            from tiger_api import tiger_data as _td
            quotes = _td.load_option_quotes(tuple(quote_key))
        except Exception as e:
            logger.debug("PMCC option quote fetch failed: %s", e)

    today = pd.Timestamp.now().normalize()
    for _, r in df_t.iterrows():
        ttype = r.get("TradeType")
        if ttype not in ("LEAP", "CC"):
            continue
        try:
            strike = float(r.get("Option_Strike_Price_(USD)"))
            exp = str(r.get("Expiry_Date"))
            qty = int(abs(float(r.get("Quantity", 1))))
            entry_premium = float(r.get("OptPremium", 0.0) or 0.0)
            dte_dt = pd.to_datetime(exp, errors="coerce")
            dte = int((dte_dt - today).days) if pd.notna(dte_dt) else None
        except (TypeError, ValueError):
            continue

        q = quotes.get((ticker.upper(), exp, strike, "C"), {})
        mark = q.get("mid") or q.get("last") or entry_premium
        delta = q.get("delta_alpaca")
        theta = q.get("theta_alpaca")    # already per-day from Alpaca

        # Local fallback Greeks if Alpaca didn't give us live ones
        if (delta is None or theta is None) and mark and spot:
            try:
                from tiger_api.greeks import compute_greeks
                g = compute_greeks(
                    spot=spot, strike=strike, dte_days=max(dte or 1, 1),
                    market_price=float(mark), is_call=True, is_long=(ttype == "LEAP"),
                )
                if delta is None:
                    delta = g.get("delta")
                if theta is None:
                    theta = g.get("theta_per_day")
            except Exception:
                pass

        leg = {
            "argus_type": ttype,
            "side": "LONG" if ttype == "LEAP" else "SHORT",
            "qty": qty,
            "strike": strike,
            "expiry": exp,
            "dte": dte,
            "mark": float(mark) if mark else None,
            "premium_received": entry_premium if ttype == "CC" else None,
            "delta": float(delta) if delta is not None else None,
            "theta_per_day": float(theta) if theta is not None else None,
            "extrinsic": theta_math.extrinsic(mark or 0, spot, strike, is_call=True) if mark and spot else 0.0,
        }
        if ttype == "LEAP":
            longs.append(leg)
        else:
            shorts.append(leg)

    return longs, shorts


def _hurdle_flag(leg: dict, spot: float, hv30: float) -> str:
    """Per-leg hurdle pass/fail with ITM-vs-OTM context.

    Doctrine §2 hurdle is uniform, but the §2 regime caveat acknowledges that OTM
    shorts may sit below hurdle in low-vol regimes. The caveat is intentional: OTM
    legs aren't primarily there to harvest theta — they uncap LEAPS growth and
    provide directional headroom (§3, §13). So we surface different flags:

      Long leg    →  —      (hurdle doesn't apply)
      ITM short:
        above    →  ✅
        below    →  ⚠️ ITM   (re-evaluate strike on next roll cycle)
      OTM short:
        above    →  ✅
        below    →  ℹ️ OTM   (expected per §2 caveat — growth-participation tradeoff)
    """
    if leg.get("side") == "LONG":
        return "—"
    theta = leg.get("theta_per_day")
    strike = leg.get("strike")
    if theta is None or strike is None or hv30 is None or hv30 <= 0:
        return "—"
    hurdle = theta_math.theta_hurdle(strike, hv30)
    if abs(theta) >= hurdle:
        return "✅"
    is_otm = spot is not None and strike >= spot
    return "ℹ️ OTM" if is_otm else "⚠️ ITM"


def _format_array_line(layout: dict, spot: float) -> str:
    """Build a one-line ASCII array visualization centered on spot."""
    itm_strikes = [f"${l['strike']:.0f}" for l in layout["itm"]]
    otm_strikes = [f"${l['strike']:.0f}" for l in layout["otm"]]
    left = "─".join(itm_strikes) if itm_strikes else "  (no ITM shorts)  "
    right = "─".join(otm_strikes) if otm_strikes else "  (no OTM shorts)  "
    return f"{left}  ◄─[ SPOT ${spot:.2f} ]─►  {right}"
