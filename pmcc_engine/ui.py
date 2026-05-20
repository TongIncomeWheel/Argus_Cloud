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
        _render_review_block(ticker, df_open, settings, ticker_state, spot_prices, save_settings_fn)
    with tab_scanner:
        _render_strike_scanner(ticker, df_open, settings, ticker_state, spot_prices)
    with tab_roll:
        _render_roll_simulator(ticker, df_open, settings, ticker_state, spot_prices)
    with tab_score:
        _render_scorecard_panel(ticker, ticker_state, spot_prices)
    with tab_state:
        _render_state_editor(ticker, settings, ticker_state, save_settings_fn)


# ─── Block 1-4 review ──────────────────────────────────────────────


def _render_review_block(ticker, df_open, settings, ticker_state, spot_prices,
                         save_settings_fn=None):
    """Daily review — advisor-style format: summary → market → array → unified
    position table → Δ/Θ summary → tripwires → items on watch → action."""

    # ── Pull live market state ────────────────────────────────────
    with st.spinner("📊 Loading market state…"):
        spot = float(spot_prices.get(ticker, 0) or 0)
        if spot <= 0:
            recent = data_io.daily_closes(ticker, period="5d")
            spot = float(recent[-1]) if recent else 0.0
        hv30 = data_io.hv30_from_ticker(ticker) or 0.0
        vol_axis = (ticker_state.get("vol_axis") or "VIX").upper()
        current_vol, vol_source = data_io.current_iv_signal_with_source(ticker, vol_axis=vol_axis)
        current_vol = current_vol or 0.0
        ivr = data_io.ivr_for_ticker(ticker, vol_axis=vol_axis)
        median_vol = ticker_state.get("vol_median_5yr", 18.0)
        cell = regime_mod.regime_cell(
            current_vol=current_vol,
            median_vol=median_vol,
            ivr=ivr if ivr is not None else 50.0,
        )
        cell["vol_axis"] = vol_axis
        boundary = regime_mod.band_boundary_proximity(
            current_vol=current_vol, median_vol=median_vol,
            ivr=ivr if ivr is not None else 50.0,
        )

    longs, shorts = _extract_pmcc_positions(ticker, df_open, spot)
    if not longs and not shorts:
        st.info(f"No PMCC positions found for {ticker}. Add a LEAP + short calls to engage the engine.")
        return

    # ── Aggregate book greeks (need these for the summary) ────────
    longs_for_book = [{"qty": l.get("qty", 1), "delta": l.get("delta", 0.0), "theta": l.get("theta_per_day", 0.0)} for l in longs]
    shorts_for_book = [{"qty": s.get("qty", 1), "delta": s.get("delta", 0.0), "theta": s.get("theta_per_day", 0.0)} for s in shorts]
    greeks = theta_math.book_greeks(longs_for_book, shorts_for_book)
    tpd = theta_math.theta_per_delta(greeks["net_theta"], greeks["net_delta"])
    rating = theta_math.theta_per_delta_rating(tpd)
    yr = theta_math.yield_ratio(
        [{"strike": s["strike"], "theta_per_day": s.get("theta_per_day", 0.0)} for s in shorts],
        hv30,
    ) if hv30 else 0.0

    # ── Tripwires + watch ─────────────────────────────────────────
    today = date.today()
    shorts_for_tripwires = [
        {
            "strike": s["strike"], "spot": spot,
            "mark": s.get("mark", 0.0), "dte": s.get("dte", 99),
            "premium_received": s.get("premium_received", 0.0) or s.get("mark", 0.0),
            "is_call": s["argus_type"] == "CC",
            "extrinsic": s.get("extrinsic", 0.0),
            "theta_per_day": s.get("theta_per_day", 0.0),
            "label": f"${s['strike']:.0f}",
        }
        for s in shorts
    ]
    trip_results = triggers_mod.check_all_tripwires(
        spot=spot, vix=current_vol, shorts=shorts_for_tripwires,
        state=ticker_state, today=today,
    )
    watch_items = triggers_mod.items_on_watch(shorts_for_tripwires, ticker_state, today=today)
    reopt = posture_mod.reoptimization_check(
        net_theta=greeks["net_theta"], net_delta=greeks["net_delta"],
        shorts=[{"strike": s["strike"], "extrinsic": s.get("extrinsic", 0.0)} for s in shorts],
        spot=spot, array_center_strike=ticker_state.get("array_center_strike"),
    )
    breached = [t for t in trip_results if bool(t)]
    any_leg_triggered = False
    for s in shorts:
        st_lbl = triggers_mod.short_status_label({
            "mark": s.get("mark", 0.0), "strike": s["strike"], "dte": s.get("dte", 99),
            "premium_received": s.get("premium_received", 0.0) or s.get("mark", 0.0),
            "is_call": s["argus_type"] == "CC",
        }, spot=spot)
        if st_lbl["tier"] == "triggered":
            any_leg_triggered = True

    # ── PLAIN-ENGLISH SUMMARY (top of report) ─────────────────────
    closest_watch = watch_items[0] if watch_items else None
    summary_bits = [
        f"**{ticker} ${spot:.2f}.**",
        f"**{vol_axis} {current_vol:.2f}.**",
        f"Regime: **{cell['cell_label']} → {cell.get('posture', '—')}**.",
        f"Earning **${greeks['net_theta']:.0f}/day**.",
    ]
    if breached:
        summary_bits.append(f"⚠️ **{len(breached)} tripwire(s) breached.**")
    elif any_leg_triggered:
        summary_bits.append("⚠️ Per-leg trigger active.")
    else:
        summary_bits.append("**No trigger fired.**")
    if closest_watch:
        summary_bits.append(
            f"_Closest watch: {closest_watch['item']} at {closest_watch['current']} "
            f"(triggers {closest_watch['trigger']})._"
        )
    summary_bits.append("**Hold.**" if not breached and not any_leg_triggered else "**Review action below.**")
    ts_local = datetime.now()
    st.markdown(f"_{ts_local.strftime('%a %b %d, %H:%M')} — review for {ticker}_")
    st.markdown(" ".join(summary_bits))

    # ── REGIME-CHANGE BANNER ──────────────────────────────────────
    # Compare against last acknowledged review for this ticker. Surfaces
    # the change once; user clicks "Acknowledge" to update the baseline.
    # current_shape is computed below in the Array section, but we
    # build the snapshot here so the banner can fire before the user
    # scrolls. Recompute current_shape inline:
    short_dicts_for_snap = [{"strike": s["strike"]} for s in shorts]
    layout_for_snap = posture_mod.array_layout(spot, short_dicts_for_snap)
    target_shape_for_snap = cell.get("shape") or cell.get("array")
    current_shape_for_snap = doctrine.classify_shape(
        layout_for_snap["itm_count"], layout_for_snap["otm_count"]
    )
    current_snapshot = {
        "timestamp": ts_local.isoformat(),
        "vol_band": cell.get("vol_band"),
        "ivr_band": cell.get("ivr_band"),
        "posture": cell.get("posture"),
        "target_shape": target_shape_for_snap,
        "current_shape": current_shape_for_snap,
    }
    last_snapshot = state_mod.get_last_review_snapshot(settings, ticker)
    diff = state_mod.regime_changed_since(last_snapshot, current_snapshot)

    if last_snapshot is None:
        # First-ever review for this ticker — silently bootstrap.
        state_mod.save_last_review_snapshot(settings, ticker, current_snapshot)
        if save_settings_fn:
            try:
                save_settings_fn(settings)
            except Exception:
                pass   # local file may be read-only; in-memory state still updated
        st.caption(
            f"_First review for {ticker} — baseline regime snapshot saved. "
            "Future regime shifts will be flagged here._"
        )
    elif diff["changed"]:
        ack_key = f"pmcc_regime_ack_{ticker}"
        last_ts = last_snapshot.get("timestamp", "previous review")
        try:
            last_ts_short = datetime.fromisoformat(last_ts).strftime("%a %b %d, %H:%M")
        except (ValueError, TypeError):
            last_ts_short = last_ts
        change_bullets = []
        for f in diff["fields"]:
            change_bullets.append(f"  - **{f}**: `{last_snapshot.get(f)}` → `{current_snapshot.get(f)}`")
        banner_text = (
            f"🔔 **Regime change since {last_ts_short}**  \n"
            + "  \n".join(change_bullets)
        )
        if diff["shape_changed"]:
            # Target shape changed — surface implied action
            new_target_desc = doctrine.shape_description(current_snapshot["target_shape"])
            old_target_desc = doctrine.shape_description(last_snapshot.get("target_shape"))
            same_shape_match = current_snapshot["current_shape"] == current_snapshot["target_shape"]
            if same_shape_match:
                banner_text += (
                    f"  \n\n✅ Target shape changed (**{old_target_desc} → {new_target_desc}**) "
                    "but your current array still matches the new target — no action needed."
                )
            else:
                banner_text += (
                    f"  \n\n⚠️ **Target shape changed (`{old_target_desc} → {new_target_desc}`) "
                    f"and your array no longer matches.** See Array section below for roll suggestions."
                )
            st.error(banner_text)
        else:
            # Only vol_band or ivr_band shifted; shape unchanged
            st.warning(banner_text + "  \n\n_Target shape unchanged — no action needed._")

        ack_col, _ = st.columns([1, 4])
        with ack_col:
            if st.button("✓ Acknowledge regime change", key=ack_key, type="primary"):
                state_mod.save_last_review_snapshot(settings, ticker, current_snapshot)
                if save_settings_fn:
                    try:
                        save_settings_fn(settings)
                    except Exception:
                        pass
                st.rerun()

    # ── MARKET ────────────────────────────────────────────────────
    st.markdown("#### Market")
    _vol_source_label = {
        "fast_info": "yfinance live tick",
        "intraday": "yfinance 1-min bar",
        "daily_close": "yfinance daily close (lagging — may be prior session)",
        "HV30": "computed HV30",
    }.get(vol_source, "yfinance")
    m_rows = [
        {"": f"{ticker}", " ": f"**${spot:.2f}**"},
        {"": vol_axis, " ": f"**{current_vol:.2f}** (median {median_vol}) · src: {_vol_source_label}"},
        {"": "HV30", " ": f"{hv30*100:.1f}%"},
        {"": "IVR (52w)", " ": f"{ivr:.0f}" if ivr is not None else "—"},
        {"": "Regime", " ": f"**{cell['cell_label']} → {cell.get('posture', '—')}**"},
    ]
    st.table(pd.DataFrame(m_rows))
    if vol_source == "daily_close":
        st.caption(
            "⚠️ Vol axis is on **yfinance daily close** — fast_info and 1-min intraday were "
            "unavailable. This can lag a full session. Treat the regime band as provisional "
            "until a live tick is available."
        )

    # ── BAND BOUNDARY PROXIMITY ──────────────────────────────────
    # At a band edge, a tiny vol/IVR wobble flips the regime cell and can
    # change the target shape. Surface that fragility deliberately.
    if boundary["any_near"]:
        warn_lines = ["**⚠️ Regime is near a band boundary — classification is fragile right now.**"]
        if boundary["vol_near"]:
            warn_lines.append(f"- {boundary['vol_detail']}")
        if boundary["ivr_near"]:
            warn_lines.append(f"- {boundary['ivr_detail']}")
        warn_lines.append(
            "_Cross-check the vol print against a second source (CBOE / broker terminal) "
            "before acting on a shape change. yfinance free-tier VIX can be ~15 min delayed._"
        )
        st.warning("  \n".join(warn_lines))

    # ── ARRAY ─────────────────────────────────────────────────────
    st.markdown("#### Array")
    short_dicts = [{"strike": s["strike"], "label": s.get("expiry", ""), "extrinsic": s.get("extrinsic", 0.0)} for s in shorts]
    layout = posture_mod.array_layout(spot, short_dicts)
    target_shape = cell.get("shape") or cell.get("array")   # 'shape' is new, 'array' is legacy alias
    layout_line = _format_array_line(layout, spot)
    st.code(layout_line, language="text")

    coverage = posture_mod.coverage_ratios(longs_for_book, shorts_for_book)
    cov_pct = (coverage["short_total"] / coverage["long_total"] * 100) if coverage["long_total"] else 0
    current_shape = doctrine.classify_shape(layout["itm_count"], layout["otm_count"])
    st.markdown(
        f"**Your array:** {layout['itm_count']} ITM + {layout['otm_count']} OTM "
        f"= **{doctrine.shape_description(current_shape)}**  \n"
        f"**PMCC coverage:** {coverage['short_total']} shorts / {coverage['long_total']} LEAPS "
        f"= **{cov_pct:.0f}% of LEAPS covered**  \n"
        f"**Regime target shape** ({cell['cell_label']}): {doctrine.shape_description(target_shape)}"
    )
    guidance = doctrine.shape_guidance(
        current_itm=layout["itm_count"], current_otm=layout["otm_count"],
        target_shape=target_shape,
    )
    if guidance["match"]:
        st.success(f"✅ {guidance['headline']}")
    else:
        with st.expander(f"⚠️ {guidance['headline']} — options to align"):
            for a in guidance.get("actions", []):
                st.markdown(f"- {a}")
            if guidance.get("tradeoffs"):
                st.markdown("**Tradeoffs:**")
                for t in guidance["tradeoffs"]:
                    st.markdown(f"  - {t}")

    with st.expander("ℹ️ How the array works — shape vs count, and how the regime guides it"):
        st.markdown(
            "**Two separate decisions sit underneath every PMCC array:**\n\n"
            "1. **Count** — how many short calls to run. This is *your* choice based on "
            "how many LEAPS you want to cover. In a strict 100%-covered PMCC, the rule is "
            "**one short per LEAP** — so if you own 6 LEAPS, you'd run 6 shorts; if you own "
            "2 LEAPS, you'd run 2 shorts. **The doctrine doesn't pick this number for you.**\n\n"
            "2. **Shape** — where those shorts sit relative to spot. *This* is what the "
            "regime tells you. Same six shorts can be split 6 ITM (defensive flip), or "
            "3 ITM + 3 OTM (centered), or 0 ITM + 6 OTM (all-OTM). Same count, different shape.\n\n"
            "### The shape vocabulary (no numbers — just direction of lean)\n\n"
            "| Shape | Meaning | Use case |\n"
            "|-------|---------|----------|\n"
            "| **Centered** | Equal ITM and OTM shorts | Standard / base case — balanced theta + growth participation |\n"
            "| **ITM-lean** | More ITM than OTM | Harvest mode — when regime pays for theta capture |\n"
            "| **OTM-lean** | More OTM than ITM | Growth mode — when vol is rising and ITM assignment risk grows |\n"
            "| **All-ITM** | Every short ITM | Defensive flip — low vol, low IVR; only ITM theta worth grabbing |\n"
            "| **All-OTM** | Every short OTM | High vol, high IVR; minimize assignment risk |\n"
            "| **Stand down** | Don't deploy new shorts | Extreme vol regime — wait for re-rank |\n\n"
            "### Regime → shape mapping (what the engine surfaces)\n\n"
            "| Regime cell | Target shape | Why |\n"
            "|-------------|--------------|-----|\n"
            "| Band M × IVR neutral (base case) | Centered | Premium typical → balanced split |\n"
            "| Band L × IVR neutral / rich | Centered | Low vol but still earning — stay balanced |\n"
            "| Band L × IVR cheap | All-ITM | Premium is dead → only ITM has any theta |\n"
            "| Band M × IVR rich | ITM-lean | Premium is rich → harvest extra |\n"
            "| Band M × IVR cheap | OTM-lean | Lean OTM in low-but-rising vol |\n"
            "| Band H × IVR cheap / neutral | OTM-lean | Vol expansion → reduce ITM assignment exposure |\n"
            "| Band H × IVR rich | All-OTM | Pay attention to gamma; defensive only |\n"
            "| Band H × IVR extreme | All-OTM half-size | Half size in shock regime |\n"
            "| Band X (extreme) | Stand down or half-size OTM | Can't trust the regime |\n\n"
            f"**Your book right now:** {coverage['long_total']} LEAPS, {coverage['short_total']} shorts → "
            f"**{cov_pct:.0f}% of LEAPS covered**. Shape = **{doctrine.shape_description(current_shape)}**. "
            f"Regime target shape = **{doctrine.shape_description(target_shape)}**."
        )

    # ── POSITION TABLE (unified, advisor-style) ───────────────────
    st.markdown("#### Position table")
    pos_rows = []
    legs_with_status = []
    for i, leg in enumerate(longs + shorts, start=1):
        if leg["side"] == "LONG":
            status = triggers_mod.leaps_status_label({"delta": leg.get("delta", 0.0), "dte": leg.get("dte", 365)})
            profit_str = "—"
        else:
            status = triggers_mod.short_status_label({
                "mark": leg.get("mark", 0.0), "strike": leg["strike"],
                "dte": leg.get("dte", 99),
                "premium_received": leg.get("premium_received", 0.0) or leg.get("mark", 0.0),
                "is_call": leg["argus_type"] == "CC",
            }, spot=spot)
            premium = leg.get("premium_received", 0.0) or leg.get("mark", 0.0)
            mark = leg.get("mark", 0.0)
            profit_pct = ((premium - mark) / premium * 100) if premium > 0 else 0
            profit_str = f"{profit_pct:+.0f}%"
        legs_with_status.append((leg, status))
        delta_dollars = (leg.get("delta") or 0) * 100 * leg.get("qty", 1) * (1 if leg["side"] == "LONG" else -1)
        theta_dollars = (leg.get("theta_per_day") or 0) * 100 * leg.get("qty", 1)
        if leg["side"] == "SHORT":
            theta_dollars = abs(theta_dollars)   # short theta is positive (collected)
        leg_label = (
            f"{'Long' if leg['side'] == 'LONG' else 'Short'} ${leg['strike']:.0f}C "
            f"{_short_expiry(leg.get('expiry'))}"
        )
        pos_rows.append({
            "#": i,
            "Leg": leg_label,
            "delta per $1": delta_dollars,
            "theta $/day": theta_dollars,
            "Extrinsic": leg.get("extrinsic"),
            "$ from spot": round(leg["strike"] - spot, 2) if spot else None,
            "DTE": leg.get("dte"),
            "Profit %": profit_str,
            "Trigger": status["label"],
        })
    pos_df = pd.DataFrame(pos_rows)
    st.dataframe(
        pos_df, use_container_width=True, hide_index=True,
        column_config={
            "delta per $1":  st.column_config.NumberColumn(
                format="$%+.0f",
                help="Dollar P&L per $1 move in the underlying, per leg. Long = positive, short = negative."),
            "theta $/day":   st.column_config.NumberColumn(
                format="$%+.2f",
                help="Daily theta in dollars. Long = negative (decay against you). Short = positive (decay collected)."),
            "Extrinsic":     st.column_config.NumberColumn(
                format="$%.2f",
                help="Time value remaining in the mark. When extrinsic < $1, the short is acting as synthetic stock."),
            "$ from spot":   st.column_config.NumberColumn(format="$%+.2f"),
            "Trigger":       st.column_config.TextColumn(
                help="Per-leg status. Sit = no signal. Approaching 50% = on track for harvest. "
                     "Harvest = ≥50% profit reached. Close = ≥80% profit. "
                     "Extrinsic declining / critical = nearing $1 floor. "
                     "DTE approaching 10 = forced-roll window. "
                     "Paper red = ITM short at a paper loss."),
        },
    )

    # ── DELTA AND THETA SUMMARY ──────────────────────────────────
    st.markdown("#### Delta and Theta")
    delta_theta_rows = [
        {" ": "Longs",       f"delta per $1 {ticker}": f"+${greeks['long_delta']:,.0f}",        "theta $/day": f"−${abs(greeks['long_theta']):,.2f}"},
        {" ": "Shorts",      f"delta per $1 {ticker}": f"−${greeks['short_delta']:,.0f}",       "theta $/day": f"+${greeks['short_theta']:,.2f}"},
        {" ": "**Net**",     f"delta per $1 {ticker}": f"**${greeks['net_delta']:+,.0f}**",     "theta $/day": f"**${greeks['net_theta']:+,.2f}**"},
        {" ": "**theta/delta**", f"delta per $1 {ticker}": f"**${tpd:.2f}**",                  "theta $/day": f"**{rating}** (≥${doctrine.THETA_PER_DELTA_ACCEPTABLE:.2f})"},
    ]
    st.table(pd.DataFrame(delta_theta_rows))
    st.caption(
        f"Yield ratio: **{yr:.2f}** vs hurdle 1.00 — "
        f"{'✅ above hurdle' if yr >= doctrine.YIELD_RATIO_PASS else '⚠️ below hurdle'}. "
        f"PMCC coverage: **{coverage['long_total']}:{coverage['short_total']}** "
        f"({cov_pct:.0f}% of shorts backed by LEAPS, "
        f"{coverage['chassis_qty']} of those are chassis-grade 0.78-0.95Δ)."
    )

    # ── TRIPWIRES (numbered table) ────────────────────────────────
    st.markdown("#### Tripwires")
    trip_rows = []
    for i, t in enumerate(trip_results, start=1):
        trip_rows.append({
            "#": i,
            "Test": _tripwire_test_description(t.name, ticker_state, current_vol),
            "Status": "🔴 BREACH" if t.triggered else "✅ Pass",
            "Detail": t.detail,
        })
    st.dataframe(pd.DataFrame(trip_rows), use_container_width=True, hide_index=True)
    if breached:
        st.error(f"🔴 **{len(breached)} tripwire(s) breached:** " + ", ".join(t.name for t in breached))
    else:
        st.success("**No trigger fired.**")

    # ── ITEMS ON WATCH ────────────────────────────────────────────
    st.markdown("#### Items on watch")
    if watch_items:
        watch_df = pd.DataFrame([
            {"Item": w["item"], "Current": w["current"], "Trigger": w["trigger"], "Est. fire": w["est"]}
            for w in watch_items
        ])
        st.dataframe(watch_df, use_container_width=True, hide_index=True)
    else:
        st.info("Nothing approaching a trigger.")

    # LEAPS refresh items (separate section if any fire)
    leaps_warnings = []
    for l in longs:
        rt = triggers_mod.leaps_refresh_trigger({"delta": l.get("delta", 0.0), "dte": l.get("dte", 365)})
        if rt:
            icon = {"forced": "🔴", "immediate": "🔴", "evaluate": "🟡", "schedule": "🟡"}.get(rt.urgency, "ℹ️")
            leaps_warnings.append(f"{icon} **LEAPS ${l['strike']:.0f} ({l.get('expiry')})** — {rt.reason} (`{rt.urgency}`)")
    if leaps_warnings:
        st.markdown("**LEAPS refresh triggers (§9):**")
        for w in leaps_warnings:
            st.markdown(f"- {w}")

    # ── ACTION (bottom line) ──────────────────────────────────────
    st.markdown("#### Action")
    if not breached and not any_leg_triggered and not reopt["reoptimize"]:
        action_text = (
            f"**None.** Array {'centered' if guidance['match'] else 'off-doctrine but stable'}, "
            f"theta/delta at **${tpd:.2f}** ({rating.lower()}), "
            f"earning **${greeks['net_theta']:.0f}/day**."
        )
        if watch_items:
            action_text += f" {len(watch_items)} item(s) on watch — let theta do its work. Next checkpoint when any trigger fires."
        action_text += "  \n**Hold.**"
        st.success(action_text)
    else:
        bits = []
        if breached:
            bits.append(f"🔴 {len(breached)} tripwire breach: {', '.join(t.name for t in breached)}.")
        if any_leg_triggered:
            bits.append("Per-leg trigger active — see Position table.")
        if reopt["reoptimize"]:
            bits.append(f"§13 re-optimization: {'; '.join(reopt['reasons'])}.")
        st.error("**Action required.**  \n" + "  \n".join(f"- {b}" for b in bits)
                 + "  \n\nUse the **🔁 Roll Simulator** tab for any proposed roll.")

    # ── Text-form report (collapsible) ────────────────────────────
    with st.expander("📄 Plain-text 4-block report (copy/paste)"):
        agg_for_report = {
            **greeks, "theta_per_delta": tpd,
            "theta_per_delta_rating": rating, "coverage": coverage,
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
            ticker=ticker, spot=spot, cell={**cell, "ivr": ivr},
            aggregate=agg_for_report, positions=positions_for_report,
            tripwires=trip_results, yield_ratio=yr,
        )
        st.code(text_report, language="text")


def _tripwire_test_description(name: str, state: dict, current_vol: float) -> str:
    """Human-readable description of what each tripwire tests."""
    tw = (state or {}).get("tripwires", {}) or {}
    dis = tw.get("disorderly", {}) or {}
    return {
        "Upper":       f"Spot ≥ ${tw.get('upper', '?')}",
        "Lower":       f"Spot ≤ ${tw.get('lower', '?')}",
        "VIX shock":   f"VIX ≥ {tw.get('vix_shock', '?')}",
        "Disorderly":  f"Spot ≤ ${dis.get('price', '?')} AND VIX > {dis.get('vix', '?')}",
        "DTE/profit":  "Any short with DTE ≤ 10, profit <50%",
        "Ex-div":      "Within 2 TD of ex-div, ITM extrinsic < 1.25 × dividend",
    }.get(name, name)


def _short_expiry(exp) -> str:
    """Compact expiry label: '2026-06-19' → 'Jun 19'."""
    if not exp:
        return ""
    try:
        d = datetime.fromisoformat(str(exp).split("T")[0]).date()
        return d.strftime("%b %-d")
    except (ValueError, TypeError):
        return str(exp)


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


def _format_array_line(layout: dict, spot: float) -> str:
    """Build a one-line ASCII array visualization centered on spot."""
    itm_strikes = [f"${l['strike']:.0f}" for l in layout["itm"]]
    otm_strikes = [f"${l['strike']:.0f}" for l in layout["otm"]]
    left = "─".join(itm_strikes) if itm_strikes else "  (no ITM shorts)  "
    right = "─".join(otm_strikes) if otm_strikes else "  (no OTM shorts)  "
    return f"{left}  ◄─[ SPOT ${spot:.2f} ]─►  {right}"
