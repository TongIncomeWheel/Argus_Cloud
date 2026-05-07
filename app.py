"""ARGUS — Income Wheel Command Terminal.

Greenfield rebuild. Tiger Open API is the single source of trade truth;
the only ARGUS-specific data persisted in gSheet's Settings tab is config
(pot deposits, allocation %, FX rate, PMCC ticker flags, ignored tickers).

Six top tabs:
  🎯 Cockpit       — Wheel pacing home (CSP + CC pacing, liquidity)
  📅 Ladder        — Expiry calendar + capital release schedule
  📊 P&L           — YTD/MTD/custom range × type × ticker
  ⚠️ Risk          — Concentration, stress test, wheel cycle status
  📜 Transactions  — Last 14 days of filled orders with filters + summary
  ⚙️ Config        — Pot deposits (SGD-primary), allocation %, PMCC tags, FX

Local dev:
    cd C:\\Users\\ashtz\\ARGUS_Cloud
    python -m streamlit run app_v2.py

Streamlit Cloud deployment:
    Tiger creds live in `.streamlit/tiger_openapi_config.properties`.
    For Cloud, paste the same content into Streamlit Cloud → App settings →
    Secrets, under a `[tiger_api]` section, OR set env var `TIGER_CONFIG_PATH`
    to the secrets-mounted path. The TigerClient auto-discovers the config dir.

    gSheet credentials remain in `gsheet_credentials.json` / `secrets.toml`.

Required env / config:
    TIGER_CONFIG_PATH    optional override for Tiger config dir
    INCOME_WHEEL_SHEET_ID  gSheet sheet id (existing)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, date, timedelta
from typing import Optional

import pandas as pd
import streamlit as st

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ── Page config (must be first Streamlit call) ──────────────────
st.set_page_config(
    page_title="ARGUS · Income Wheel Terminal",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Tighter, terminal-feeling CSS
st.markdown("""
<style>
    /* More breathing room at top so header buttons aren't clipped under Streamlit chrome */
    .block-container { padding-top: 3rem; padding-bottom: 2rem; max-width: 100%; }
    [data-testid="stMetricValue"] { font-size: 1.4rem; font-weight: 600; }
    [data-testid="stMetricLabel"] { font-size: 0.75rem; opacity: 0.8; }
    [data-testid="stMetricDelta"] { font-size: 0.85rem; }
    .stTabs [data-baseweb="tab-list"] { gap: 0.5rem; }
    .stTabs [data-baseweb="tab"] { padding: 0.5rem 1rem; }
    div[data-testid="stHorizontalBlock"] { gap: 1rem; align-items: center; }
    div[data-testid="stHorizontalBlock"] .stButton > button {
        height: auto; padding: 0.5rem 0.75rem;
    }

    /* Custom centered tables (used by center_table helper) */
    table.argus-c {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.92rem;
        margin: 0.4rem 0 0.6rem 0;
    }
    table.argus-c th, table.argus-c td {
        text-align: center !important;
        padding: 0.45rem 0.6rem;
        border-bottom: 1px solid rgba(128, 128, 128, 0.18);
        white-space: nowrap;
    }
    table.argus-c th {
        background: rgba(128, 128, 128, 0.10);
        font-weight: 600;
        border-top: 1px solid rgba(128, 128, 128, 0.18);
    }
    table.argus-c tbody tr:hover { background: rgba(128, 128, 128, 0.05); }

    /* Stateful tab-radio: make st.radio look like st.tabs */
    div[data-testid="stRadio"]:has(input[name="_argus_tab_radio"]) > label { display: none; }
    div[data-testid="stRadio"]:has(input[name="_argus_tab_radio"]) > div {
        gap: 0.4rem !important;
    }
    div[data-testid="stRadio"]:has(input[name="_argus_tab_radio"]) > div > label {
        padding: 0.5rem 1rem;
        border-radius: 0.4rem 0.4rem 0 0;
        border-bottom: 2px solid transparent;
        margin: 0 !important;
        cursor: pointer;
        transition: all 0.15s ease;
    }
    div[data-testid="stRadio"]:has(input[name="_argus_tab_radio"]) > div > label:hover {
        background: rgba(128, 128, 128, 0.08);
    }
    div[data-testid="stRadio"]:has(input[name="_argus_tab_radio"]) > div > label:has(input:checked) {
        border-bottom: 2px solid #ff4b4b;
        font-weight: 600;
    }
    div[data-testid="stRadio"]:has(input[name="_argus_tab_radio"]) input { display: none; }

    /* Data coverage strip (header) */
    .argus-coverage {
        display: flex; gap: 1.2rem; align-items: center;
        padding: 0.4rem 0.7rem;
        background: rgba(128, 128, 128, 0.06);
        border: 1px solid rgba(128, 128, 128, 0.18);
        border-radius: 0.4rem;
        font-size: 0.78rem;
        margin: 0.3rem 0;
    }
    .argus-coverage .pill { display: inline-flex; gap: 0.4rem; align-items: center; }
    .argus-coverage .pill .lbl { opacity: 0.7; font-weight: 500; }
    .argus-coverage .pill .val { font-weight: 600; font-family: monospace; }
    .argus-coverage .ok { color: #2e7d32; }
    .argus-coverage .warn { color: #ed6c02; }
    .argus-coverage .bad { color: #d32f2f; }
</style>
""", unsafe_allow_html=True)


# ────────────────────────────────────────────────────────────────
# Persistent header — single-line status strip (always visible)
# ────────────────────────────────────────────────────────────────
def render_header(summary, settings: dict, df_open, spot_prices: dict, portfolio: str = "Income Wheel"):
    """Persistent top header — single row.

    Layout: [Brand · LIVE · Refresh] · [Portfolio Value (USD/SGD)] · [Spot cards for each open ticker]
    """
    fx = float(settings.get("sgd_usd_fx_rate", 1.35) or 1.35)
    nav_usd = float(summary.nav)
    nav_sgd = nav_usd * fx

    open_tickers = []
    if df_open is not None and not df_open.empty and "Ticker" in df_open.columns:
        open_tickers = sorted(df_open["Ticker"].dropna().unique().tolist())

    # Single-row layout: 1 brand cell + 1 portfolio cell + N spot cells
    n_spots = len(open_tickers)
    widths = [2.2, 2.0] + [1.0] * n_spots
    head = st.columns(widths)

    # ── Brand / status / refresh ─────────────────────────────────
    with head[0]:
        mode_badge = "🟡 SANDBOX" if summary.sandbox else "🟢 LIVE"
        if st.button(f"{mode_badge}  🔄", key="hdr_refresh", use_container_width=True,
                      help="Click to force refresh from Tiger API"):
            from tiger_api import tiger_data
            tiger_data.refresh_all()
            st.rerun()
        st.caption(
            f"**ARGUS** · {portfolio} · acct ...{get_account_short()} · {summary.fetched_at.split('T')[-1]}"
        )

        # First-run prompt: if archive is empty, expose a manual backfill button.
        # After first archive, the smart-detect (auto_archive_if_stale) runs
        # silently in main() — no widget needed here unless the user explicitly
        # wants to force a refresh (Config tab handles that).
        try:
            from tiger_api.archive import archive_summary
            arc = archive_summary()
            gs = arc.get("gsheet", {})
            if not gs.get("exists") or gs.get("rows", 0) == 0:
                if st.button("🔴 No archive — backfill now",
                              key="hdr_first_archive",
                              use_container_width=True, type="primary",
                              help="Pulls full Tiger history (~365d) and saves to gSheet + parquet."):
                    from tiger_api import tiger_data
                    pmcc_tuple_arch = tuple(settings.get("pmcc_tickers", ["SPY"]))
                    with st.spinner("Backfilling archive (this is a one-time operation)…"):
                        result = tiger_data.append_to_archive(pmcc_tuple_arch)
                    if result.get("ok"):
                        msg = f"✅ Archived {result['rows']:,} rows"
                        if result.get("gsheet_ok"):
                            msg += " · gSheet ✓"
                        st.success(msg)
                    else:
                        st.error(f"Archive failed: {result.get('msg', 'unknown')}")
                    tiger_data.load_orders_full.clear()
                    tiger_data.get_data_coverage.clear()
                    st.rerun()
        except Exception as e:
            logger.debug("First-archive widget error: %s", e)

    # ── Portfolio Value (labelled, USD primary, SGD subtext) ─────
    with head[1]:
        st.metric(
            "Portfolio Value",
            f"${nav_usd:,.0f}",
            delta=f"≈ S${nav_sgd:,.0f}",
            delta_color="off",
            help=f"Tiger NAV (deposit + realized + unrealized). FX {fx:.4f}.",
        )

    # ── Spot price cards (one per open-position ticker) ──────────
    for i, t in enumerate(open_tickers):
        with head[2 + i]:
            price = spot_prices.get(t)
            if price:
                st.metric(t, f"${price:,.2f}")
            else:
                st.metric(t, "—", help="Spot price unavailable.")

    # ── Persistent data coverage strip ───────────────────────────
    # At-a-glance assurance: archive range + live (Tiger 90d) range + gap status.
    try:
        from tiger_api import tiger_data
        cov = tiger_data.get_data_coverage()
        arc = cov["archive"]
        live = cov["live"]
        health = cov["health"]
        if arc["exists"]:
            arc_str = f"{arc['earliest']} → {arc['latest']} ({arc['rows']:,} rows)"
        else:
            arc_str = "empty — click 🔴 backfill above"
        live_str = f"{live['earliest']} → {live['latest']} (last {live['days']}d)"
        if health == "OK_OVERLAP":
            cls, icon = "ok", "✅"
        elif health == "GAP":
            cls, icon = "warn", "⚠️"
        elif health == "NO_ARCHIVE":
            cls, icon = "warn", "⚪"
        else:
            cls, icon = "bad", "🔴"
        st.markdown(
            f'<div class="argus-coverage">'
            f'<span class="pill"><span class="lbl">📊 Archive (gSheet):</span>'
            f'<span class="val">{arc_str}</span></span>'
            f'<span class="pill"><span class="lbl">📡 Live (Tiger):</span>'
            f'<span class="val">{live_str}</span></span>'
            f'<span class="pill {cls}">{icon} {cov["msg"]}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    except Exception as e:
        st.caption(f"Coverage check unavailable: {e}")
    st.divider()


def center_table(df: pd.DataFrame) -> None:
    """Render a DataFrame as a centered HTML table.

    Streamlit's `st.dataframe` (Glide canvas) ignores text-align CSS, so we
    fall back to a styled HTML <table> for consistent center alignment.
    The CSS lives in the global style block (class `argus-c`).
    """
    if df is None or df.empty:
        st.caption("_(no data)_")
        return
    # Strip pandas' default `dataframe` class so it can't fight our styling
    html = df.to_html(index=False, border=0, escape=False, classes="argus-c")
    # to_html prepends 'dataframe' to classes — drop it
    html = html.replace('class="dataframe argus-c"', 'class="argus-c"')
    st.markdown(html, unsafe_allow_html=True)


def _color_pnl(v) -> str:
    """Pandas Styler helper — red for negative P&L, green for positive."""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return ""
    if x < 0:
        return "color: #d32f2f; font-weight: 600;"
    if x > 0:
        return "color: #2e7d32; font-weight: 600;"
    return ""


def _fmt_pnl(v) -> str:
    """Pandas Styler format — signed dollar amount with thousand separators."""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "—"
    if x == 0:
        return "$0"
    sign = "+" if x > 0 else "−"
    return f"{sign}${abs(x):,.0f}"


def get_account_short() -> str:
    try:
        from tiger_api import tiger_data
        a = tiger_data.get_account()
        return f"...{a[-4:]}" if len(a) > 4 else a
    except Exception:
        return "?"


# ────────────────────────────────────────────────────────────────
# 🎯 COCKPIT — the home screen
# ────────────────────────────────────────────────────────────────
def render_cockpit(summary, df_open, df_orders, settings, spot_prices: dict):
    """Wheel terminal home — Inventory · CC Coverage · Pacing.

    Liquidity & cash maximization moved to the dedicated 💰 Cash tab.
    """
    # Total capital for pacing math = Tiger NAV (deposit + realized + unrealized)
    total_capital = float(summary.nav) if summary else 0.0

    # Current week range — passed down to pacing for "this week" filtering
    now = datetime.now()
    monday = (now - timedelta(days=now.weekday())).date()
    df_wk = df_orders[pd.to_datetime(df_orders["TradeDate"], errors="coerce") >= pd.Timestamp(monday)] if not df_orders.empty else pd.DataFrame()

    # ── 1. Position Inventory by Ticker ─────────────────────────
    _panel_position_inventory(df_open, settings, spot_prices)

    st.divider()

    # ── 2. CC Coverage Summary (per-ticker × 5-week cohort pivot) ─
    _panel_cc_coverage_summary(df_open, settings)

    st.divider()

    # ── 3. PACING (weeks outer — future first → present last) ───
    _panel_pacing(df_open, df_wk, settings, total_capital, spot_prices)


def render_cash(summary, df_open: pd.DataFrame, settings: dict):
    """💰 Cash & Liquidity — broker-side cash, cash-secured policy, full
    cash-maximization trace including FX history, carry analysis, MMF/Vault.

    Three sections (was Cockpit §4-§6):
      • Portfolio Liquidity (CSP) — your cash-secured policy view (per pot)
      • Portfolio Liquidity (Tiger) — broker-side: settled cash, releasing $, BP
      • Cash Maximization — lifetime trace · carry · FX · MMF · Vault alerts
    """
    st.markdown("## 💰 Cash & Liquidity")
    st.caption(
        "Everything about WHERE your cash is, what it costs (margin interest), "
        "and what it could earn (MMF). Cash-secured policy view + Tiger's broker-side "
        "view + full lifetime cash-flow trace, all in one place."
    )
    st.divider()

    # ── A. Portfolio Liquidity (CSP) — cash-secured policy ──────
    _panel_liquidity_csp(df_open, settings)

    st.divider()

    # ── B. Portfolio Liquidity (Tiger) — broker-side cash + BP ──
    _panel_liquidity_tiger(summary, df_open, settings)

    st.divider()

    # ── C. Cash Maximization — lifetime trace + carry + FX + alerts
    _panel_cash_maximization(summary, df_open, settings)


def _friday_buckets(weeks_out: int) -> list:
    """Generate consecutive Friday dates starting from this week's Friday."""
    today = pd.Timestamp(datetime.now().date())
    fridays = []
    cur = today
    for _ in range(weeks_out):
        fri = cur + pd.Timedelta(days=(4 - cur.weekday()) % 7)
        if fri >= today:
            fridays.append(fri.date())
        cur = fri + pd.Timedelta(days=3)  # next Monday
    return fridays


def _bucket_label(d, fridays: list) -> str:
    """Map a date to its Friday bucket label, or 'Beyond' if past horizon."""
    if pd.isna(d):
        return "Beyond"
    d_actual = d.date() if hasattr(d, "date") else d
    for f in fridays:
        if d_actual <= f:
            return f.strftime("Wk %m-%d")
    return "Beyond"


def _ticker_pot_capital(settings: dict) -> dict:
    """Per-pot deposit (cash basis). Returns {'Core': $, 'Active': $}.

    Uses USD-derived pot deposits from Config (which user enters in SGD).
    """
    return {
        "Core": float(settings.get("base_pot_deposit_usd", 0) or 0),
        "Active": float(settings.get("active_pot_deposit_usd", 0) or 0),
    }


def _build_pacing_per_week(df_open: pd.DataFrame, settings: dict,
                            spot_prices: dict, weeks_out: int = 5) -> dict:
    """For each forward Friday, build a per-ticker pacing DataFrame.

    Returns: {friday_date: DataFrame}
      Each DataFrame rows = one per ticker (allocated and/or with positions in that week)
      Columns: Strategy · Ticker · Spot · Target/wk · Executed · Variance · Pacing % · Strikes

    Math (corrected to account for capital already tied up in stock/LEAP):
      ticker_capital     = pot_capital × allocation_%[T]
      already_committed  = stock_cost_basis + leap_cost   (per ticker)
      csp_capital        = ticker_capital − already_committed
      weekly_capital     = csp_capital ÷ 4   (4-week rotation)
      target_per_week    = weekly_capital ÷ (spot × 100)   in contracts
    """
    pmcc_tickers = set(settings.get("pmcc_tickers", []))
    ticker_pots = settings.get("ticker_pots", {}) or {}
    alloc_pct = settings.get("allocation_pct", {}) or {}
    pot_capital = _ticker_pot_capital(settings)

    fridays = _friday_buckets(weeks_out)

    # Pre-compute open CSPs/stock/LEAP per-ticker reference data
    csps_by_week = {fri: pd.DataFrame() for fri in fridays}
    stock_cost_by_ticker: dict = {}
    stock_avg_buy_by_ticker: dict = {}
    leap_cost_by_ticker: dict = {}
    csp_avg_strike_by_ticker: dict = {}
    if not df_open.empty:
        df_all = df_open.copy()
        df_all["q"] = pd.to_numeric(df_all["Quantity"], errors="coerce").fillna(0).abs()
        df_all["avg_cost"] = pd.to_numeric(df_all.get("_avg_cost", 0), errors="coerce").fillna(0)

        # Stock cost basis per ticker (Σ qty × avg_buy_price) and avg buy price
        stk = df_all[df_all["TradeType"] == "STOCK"]
        if not stk.empty:
            stock_cost_by_ticker = (stk["q"] * stk["avg_cost"]).groupby(stk["Ticker"]).sum().to_dict()
            for tk, sub in stk.groupby("Ticker"):
                tot_q = float(sub["q"].sum())
                if tot_q > 0:
                    stock_avg_buy_by_ticker[tk] = float((sub["q"] * sub["avg_cost"]).sum() / tot_q)

        # LEAP cost per ticker (premium × 100 × qty)
        leap = df_all[df_all["TradeType"] == "LEAP"]
        if not leap.empty:
            leap_cost_by_ticker = ((leap["q"] * leap["avg_cost"] * 100).groupby(leap["Ticker"]).sum().to_dict())

        # Open CSPs: bucket by week + compute weighted avg strike per ticker
        csps = df_all[(df_all["TradeType"] == "CSP") & (~df_all["Ticker"].isin(pmcc_tickers))].copy()
        if not csps.empty:
            csps["q"] = csps["q"].astype(int)
            csps["k"] = pd.to_numeric(csps["Option_Strike_Price_(USD)"], errors="coerce").fillna(0)
            csps["exp"] = pd.to_datetime(csps["Expiry_Date"], errors="coerce")
            csps["week_end"] = csps["exp"].apply(
                lambda d: (d + pd.Timedelta(days=(4 - d.weekday()) % 7)).date() if pd.notna(d) else None
            )
            for fri in fridays:
                csps_by_week[fri] = csps[csps["week_end"] == fri]
            # Weighted avg strike per ticker (across all open CSP cohorts)
            for tk, sub in csps.groupby("Ticker"):
                tot_q = float(sub["q"].sum())
                if tot_q > 0:
                    csp_avg_strike_by_ticker[tk] = float((sub["k"] * sub["q"]).sum() / tot_q)

    # Universe of tickers: anything allocated + anything currently open
    all_tickers = set(ticker_pots.keys()) | set(alloc_pct.keys())
    if not df_open.empty:
        all_tickers |= set(df_open["Ticker"].dropna().tolist())
    all_tickers -= pmcc_tickers
    all_tickers = sorted(all_tickers)

    # Pre-compute per-pot CSP capacity (pot deposit − Σ stock/LEAP cost across ALL pot tickers)
    # Pot-level so an over-committed pot zeros out every ticker in it (vs per-ticker silos).
    pot_csp_capacity: dict = {}
    for pot_name in ("Core", "Active"):
        capital = float(pot_capital.get(pot_name, 0))
        explicit = {t for t, p in ticker_pots.items() if p == pot_name}
        if pot_name == "Core" and not df_open.empty:
            unmapped = set(df_open["Ticker"].dropna().unique()) - set(ticker_pots.keys())
            pot_t = explicit | unmapped
        else:
            pot_t = explicit
        stk_total = sum(float(stock_cost_by_ticker.get(t, 0)) for t in pot_t)
        leap_total = sum(float(leap_cost_by_ticker.get(t, 0)) for t in pot_t)
        pot_csp_capacity[pot_name] = max(0.0, capital - stk_total - leap_total)

    out = {}
    for fri in fridays:
        wk_csps = csps_by_week[fri]
        rows = []
        for t in all_tickers:
            pot = ticker_pots.get(t, "Core")
            pct = float(alloc_pct.get(t, 0))
            # Per-ticker CSP capital = pot's available CSP capacity × ticker's alloc %.
            # If pot is over-committed → pot_csp_capacity = 0 → every ticker target = 0.
            ticker_csp_cap = float(pot_csp_capacity.get(pot, 0)) * pct / 100
            weekly_cap = ticker_csp_cap / 4

            # Contract size = avg_price × 100. Priority order:
            #   1. Weighted avg STRIKE of currently-open CSPs for this ticker (Tiger position data)
            #   2. Stock avg BUY price (if user holds stock — represents their cost-basis target)
            #   3. Current spot (final fallback when no positions yet)
            avg_price = (
                csp_avg_strike_by_ticker.get(t)
                or stock_avg_buy_by_ticker.get(t)
                or float(spot_prices.get(t, 0) or 0)
            )
            spot = float(spot_prices.get(t, 0) or 0)  # kept for display reference
            contract_size = avg_price * 100 if avg_price > 0 else 0
            target = (weekly_cap / contract_size) if contract_size > 0 else 0

            t_csps = wk_csps[wk_csps["Ticker"] == t] if not wk_csps.empty else pd.DataFrame()
            executed = int(t_csps["q"].sum()) if not t_csps.empty else 0
            variance = executed - target
            pacing = (executed / target * 100) if target > 0 else (
                100 if executed > 0 else 0
            )

            if target == 0 and executed == 0:
                # Tickers with no allocation AND no positions in this week — skip unless they show elsewhere
                # (the universe is already filtered to allocated/positioned tickers, so include with ⚪)
                status = "⚪"
            elif pacing >= 100:
                status = "🟢"
            elif pacing >= 60:
                status = "🟡"
            else:
                status = "🔴"

            if t_csps.empty:
                strikes = "—"
            else:
                parts = [f"${r['k']:g} ×{int(r['q'])}" for _, r in t_csps.sort_values("k").iterrows()]
                strikes = "  ·  ".join(parts)

            rows.append({
                "Strategy": pot,
                "Ticker": t,
                "Spot": spot if spot > 0 else None,
                "Alloc %": pct,
                "Target/wk": target,
                "Executed": executed,
                "Variance": variance,
                "Pacing %": pacing,
                "Status": status,
                "Strikes": strikes,
            })

        out[fri] = pd.DataFrame(rows).sort_values(["Strategy", "Ticker"]).reset_index(drop=True)
    return out


def _build_cc_per_week(df_open: pd.DataFrame, settings: dict,
                        spot_prices: dict, weeks_out: int = 5) -> dict:
    """For each forward Friday, build a per-ticker CC pacing DataFrame.

    Math:
      inventory_lots = stock_qty ÷ 100   (or LEAP qty for PMCC tickers)
      target_per_week = inventory_lots ÷ 4
      executed = count of CC contracts expiring in that week

    Returns: {friday_date: DataFrame}.
    """
    pmcc_tickers = set(settings.get("pmcc_tickers", []))
    ticker_pots = settings.get("ticker_pots", {}) or {}
    fridays = _friday_buckets(weeks_out)

    # Inventory per ticker
    inv = {}
    if not df_open.empty:
        df = df_open.copy()
        df["q"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).abs()
        for t, sub in df.groupby("Ticker"):
            if t in pmcc_tickers:
                lots = int(sub[sub["TradeType"] == "LEAP"]["q"].sum())
            else:
                lots = int(sub[sub["TradeType"] == "STOCK"]["q"].sum() // 100)
            if lots > 0:
                inv[t] = lots

    # Open CCs bucketed by week_end
    ccs_by_week = {fri: pd.DataFrame() for fri in fridays}
    if not df_open.empty:
        ccs = df_open[df_open["TradeType"] == "CC"].copy()
        if not ccs.empty:
            ccs["q"] = pd.to_numeric(ccs["Quantity"], errors="coerce").fillna(0).abs().astype(int)
            ccs["k"] = pd.to_numeric(ccs["Option_Strike_Price_(USD)"], errors="coerce").fillna(0)
            ccs["exp"] = pd.to_datetime(ccs["Expiry_Date"], errors="coerce")
            ccs["week_end"] = ccs["exp"].apply(
                lambda d: (d + pd.Timedelta(days=(4 - d.weekday()) % 7)).date() if pd.notna(d) else None
            )
            for fri in fridays:
                ccs_by_week[fri] = ccs[ccs["week_end"] == fri]

    out = {}
    for fri in fridays:
        wk_ccs = ccs_by_week[fri]
        rows = []
        # Universe: any ticker with inventory or CC activity this week
        all_tickers = set(inv.keys())
        if not wk_ccs.empty:
            all_tickers |= set(wk_ccs["Ticker"].dropna().tolist())
        for t in sorted(all_tickers):
            inventory = int(inv.get(t, 0))
            target = inventory / 4 if inventory > 0 else 0
            t_ccs = wk_ccs[wk_ccs["Ticker"] == t] if not wk_ccs.empty else pd.DataFrame()
            executed = int(t_ccs["q"].sum()) if not t_ccs.empty else 0
            variance = executed - target
            pacing = (executed / target * 100) if target > 0 else (
                100 if executed > 0 else 0
            )

            spot = float(spot_prices.get(t, 0) or 0)
            pot = ticker_pots.get(t, "Core")
            is_pmcc = t in pmcc_tickers

            if target == 0 and executed == 0:
                status = "⚪"
            elif pacing >= 100:
                status = "🟢"
            elif pacing >= 60:
                status = "🟡"
            else:
                status = "🔴"

            if t_ccs.empty:
                strikes = "—"
            else:
                parts = [f"${r['k']:g} ×{int(r['q'])}" for _, r in t_ccs.sort_values("k").iterrows()]
                strikes = "  ·  ".join(parts)

            rows.append({
                "Strategy": pot,
                "Ticker": f"{t}{' 🔄' if is_pmcc else ''}",
                "Spot": spot if spot > 0 else None,
                "Inventory": inventory,
                "Target/wk": target,
                "Executed": executed,
                "Variance": variance,
                "Pacing %": pacing,
                "Status": status,
                "Strikes": strikes,
            })
        out[fri] = pd.DataFrame(rows).sort_values(["Strategy", "Ticker"]).reset_index(drop=True)
    return out


def _round_contracts(x) -> int:
    """Round contract counts: anything ≥ 0.5 rounds UP to 1; else floor.
    For larger values, standard half-up rounding (avoids banker's-round surprises).
    """
    import math
    try:
        v = float(x or 0)
    except (TypeError, ValueError):
        return 0
    if v <= 0:
        return 0
    return int(math.floor(v + 0.5))


def _format_pacing_table(df: pd.DataFrame, kind: str) -> pd.DataFrame:
    """Format a pacing DataFrame for display. kind = 'csp' or 'cc'.

    Slim columns only — no Spot, no Alloc %, no Variance (math is implicit).
    Target / Executed are integer contract counts.
    """
    disp = df.copy()
    disp["Target Contracts per week"] = disp["Target/wk"].apply(_round_contracts)
    disp["Executed this week"] = disp["Executed"].apply(_round_contracts)
    disp["Pacing %"] = disp.apply(
        lambda r: f"{r['Status']} {r['Pacing %']:.0f}%" if r["Pacing %"] else f"{r['Status']} —",
        axis=1,
    )

    if kind == "csp":
        cols = ["Strategy", "Ticker", "Target Contracts per week", "Executed this week", "Pacing %", "Strikes"]
    else:  # cc
        cols = ["Strategy", "Ticker", "Inventory", "Target Contracts per week", "Executed this week", "Pacing %", "Strikes"]
    return disp[[c for c in cols if c in disp.columns]]


@st.fragment
def _panel_pacing(df_open, df_wk, settings, total_capital, spot_prices: dict):
    """📈 PACING — weeks as outer grouping, CSP & CC as inner sub-sections.

    Layout:
      📈 Pacing
        📅 Wk ending Fri 05-08
          💰 CSP   [per-ticker table]
          📞 CC    [per-ticker table]
        📅 Wk ending Fri 05-15
          ...
    """
    st.markdown("#### 📈 Pacing")

    pot_caps = _ticker_pot_capital(settings)
    st.caption(
        f"**CSP target/wk** = (pot CSP capacity × ticker alloc%) ÷ 4 ÷ (avg_price × 100).  "
        f"`pot CSP capacity` = pot deposit − Σ(stock cost + LEAP cost) across all tickers in pot — so an over-committed pot zeros every ticker.  "
        f"`avg_price` = weighted avg strike of open CSPs (or stock avg buy price · or spot).  ·  "
        f"**CC target/wk** = inventory lots ÷ 4. "
        f"Pots: Core ${pot_caps['Core']:,.0f} · Active ${pot_caps['Active']:,.0f}."
    )

    csp_weekly = _build_pacing_per_week(df_open, settings, spot_prices, weeks_out=5)
    cc_weekly = _build_cc_per_week(df_open, settings, spot_prices, weeks_out=5)

    # Week selector — default = furthest-out (~4 weeks away)
    fridays = list(csp_weekly.keys())
    if not fridays:
        st.info("No pacing data available.")
        return
    week_labels = [f"Fri {fri.strftime('%m-%d')}" for fri in fridays]
    label_to_fri = dict(zip(week_labels, fridays))
    default_idx = len(week_labels) - 1  # furthest-out week
    selected_label = st.selectbox(
        "Week",
        options=week_labels,
        index=default_idx,
        key="pacing_week_select",
        help="Select which forward week to view. Default = furthest-out cohort.",
    )
    fri = label_to_fri[selected_label]

    st.markdown(f"##### 📅 Wk ending **{selected_label}**")

    # ── 💰 CSP ──────────────────────────────────────────────────
    csp_df = csp_weekly.get(fri, pd.DataFrame())
    csp_show = csp_df[(csp_df["Alloc %"] > 0) | (csp_df["Executed"] > 0)] if not csp_df.empty else pd.DataFrame()
    st.markdown("**💰 CSP**")
    if csp_show.empty:
        st.caption("No CSP allocations or positions for this week.")
    else:
        center_table(_format_pacing_table(csp_show, "csp"))
    # ── 📞 CC ───────────────────────────────────────────────────
    cc_df = cc_weekly.get(fri, pd.DataFrame())
    cc_show = cc_df[(cc_df["Inventory"] > 0) | (cc_df["Executed"] > 0)] if not cc_df.empty else pd.DataFrame()
    st.markdown("**📞 CC**")
    if cc_show.empty:
        st.caption("No CC inventory or positions for this week.")
    else:
        center_table(_format_pacing_table(cc_show, "cc"))
def _panel_position_inventory(df_open: pd.DataFrame, settings: dict, spot_prices: dict):
    """📦 Position Inventory by Ticker — per-ticker summary of open positions.

    Mirrors legacy ARGUS Dashboard's "Position Inventory by Ticker" view.
    Columns: Ticker · Strategy · Stock · LEAPs · CC · CSP · CC Coverage · Notes.
    """
    pmcc_tickers = set(settings.get("pmcc_tickers", []))
    ticker_pots = settings.get("ticker_pots", {}) or {}

    st.markdown("#### 📦 Position Inventory by Ticker")

    if df_open.empty:
        st.info("No open positions.")
        return

    df = df_open.copy()
    df["q"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).abs().astype(int)

    rows = []
    for ticker, sub in df.groupby("Ticker"):
        is_pmcc = ticker in pmcc_tickers
        pot = ticker_pots.get(ticker, "Core")

        stock_shares = int(sub[sub["TradeType"] == "STOCK"]["q"].sum())
        leap_contracts = int(sub[sub["TradeType"] == "LEAP"]["q"].sum())
        cc_contracts = int(sub[sub["TradeType"] == "CC"]["q"].sum())
        csp_contracts = int(sub[sub["TradeType"] == "CSP"]["q"].sum())

        # CSP collateral $
        csps = sub[sub["TradeType"] == "CSP"]
        if not csps.empty:
            k = pd.to_numeric(csps["Option_Strike_Price_(USD)"], errors="coerce").fillna(0)
            qq = pd.to_numeric(csps["Quantity"], errors="coerce").fillna(0).abs()
            csp_reserved = float((k * 100 * qq).sum())
        else:
            csp_reserved = 0.0

        # CC Coverage %
        # PMCC ticker: CC ÷ LEAP × 100
        # Otherwise: (CC × 100) ÷ stock_shares × 100  (= % of stock covered)
        if is_pmcc and leap_contracts > 0:
            cov_pct = (cc_contracts / leap_contracts * 100)
            cov_label = f"{cov_pct:.0f}%"
        elif stock_shares > 0:
            shares_needed = cc_contracts * 100
            cov_pct = (shares_needed / stock_shares * 100) if stock_shares > 0 else 0
            cov_label = f"{cov_pct:.0f}%"
        elif cc_contracts > 0:
            cov_label = "Naked ⚠️"
            cov_pct = 999
        else:
            cov_label = "—"
            cov_pct = 0

        # Notes — naked CSP if no underlying
        notes = []
        if csp_contracts > 0 and stock_shares == 0 and leap_contracts == 0:
            notes.append("CSP-only")
        if cov_pct > 100 and not is_pmcc:
            excess = cc_contracts - (stock_shares // 100)
            notes.append(f"⚠️ {excess} naked")
        if cov_pct > 100 and is_pmcc:
            excess = cc_contracts - leap_contracts
            notes.append(f"⚠️ {excess} naked vs LEAP")

        rows.append({
            "Ticker": f"{ticker}{' 🔄' if is_pmcc else ''}",
            "Strategy": pot,
            "Spot $": float(spot_prices.get(ticker, 0) or 0),
            "Stock (shares)": stock_shares,
            "LEAPs (qty)": leap_contracts,
            "CC (qty)": cc_contracts,
            "CSP (qty)": csp_contracts,
            "CSP Reserved $": csp_reserved,
            "CC Coverage": cov_label,
            "Notes": " · ".join(notes) if notes else "",
        })

    df_disp = pd.DataFrame(rows).sort_values(["Strategy", "Ticker"]).reset_index(drop=True)
    df_disp["Spot $"] = df_disp["Spot $"].apply(lambda v: f"${v:,.2f}" if v else "—")
    df_disp["CSP Reserved $"] = df_disp["CSP Reserved $"].apply(lambda v: f"${v:,.0f}" if v else "—")
    # Format integers with thousands separator
    for c in ("Stock (shares)", "LEAPs (qty)", "CC (qty)", "CSP (qty)"):
        df_disp[c] = df_disp[c].apply(lambda v: f"{int(v):,}" if v else "0")

    center_table(df_disp)
    st.caption(
        "**CC Coverage** — Stock-backed: CC contracts × 100 ÷ stock shares (lower is better, ≤100% means stock not over-committed). "
        "PMCC (🔄): CC contracts ÷ LEAP contracts (1:1 = full coverage). **Naked ⚠️** = CC has no underlying."
    )


def _panel_cc_coverage_summary(df_open: pd.DataFrame, settings: dict):
    """🧭 CC Coverage Summary — per-ticker × next 5 Friday cohorts pivot.

    Mirrors legacy Daily Helper's "CC Coverage Planner". Each row = one ticker;
    each column = one upcoming Friday's existing CC contracts in that cohort.
    Helps see at-a-glance which weeks are full / partial / empty per ticker.
    """
    pmcc_tickers = set(settings.get("pmcc_tickers", []))

    st.markdown("#### 🧭 CC Coverage Summary")
    st.caption(
        "Per-ticker view of CC contracts spread across the next 5 Friday expiries. "
        "**Target/wk** = inventory ÷ 4. **🟢 Full** = at/above target · **🟡 Partial** · **🔴 Empty**."
    )

    if df_open.empty:
        st.info("No open positions.")
        return

    df = df_open.copy()
    df["q"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).abs().astype(int)

    # Per-ticker inventory lots
    inv = {}
    for ticker, sub in df.groupby("Ticker"):
        if ticker in pmcc_tickers:
            lots = int(sub[sub["TradeType"] == "LEAP"]["q"].sum())
        else:
            lots = int(sub[sub["TradeType"] == "STOCK"]["q"].sum() // 100)
        if lots > 0:
            inv[ticker] = lots

    if not inv:
        st.info("No CC-eligible inventory (need stock or LEAPs).")
        return

    # CCs bucketed by ticker × week_end
    fridays = _friday_buckets(5)
    ccs = df[df["TradeType"] == "CC"].copy()
    if not ccs.empty:
        ccs["exp"] = pd.to_datetime(ccs["Expiry_Date"], errors="coerce")
        ccs["week_end"] = ccs["exp"].apply(
            lambda d: (d + pd.Timedelta(days=(4 - d.weekday()) % 7)).date() if pd.notna(d) else None
        )

    rows = []
    for ticker in sorted(inv.keys()):
        inventory = inv[ticker]
        target = _round_contracts(inventory / 4)
        is_pmcc = ticker in pmcc_tickers
        row = {
            "Ticker": f"{ticker}{' 🔄' if is_pmcc else ''}",
            "Inventory": inventory,
            "Target/wk": target,
        }
        for fri in fridays:
            label = f"Wk {fri.strftime('%m-%d')}"
            existing = 0
            if not ccs.empty:
                wk = ccs[(ccs["Ticker"] == ticker) & (ccs["week_end"] == fri)]
                existing = int(wk["q"].sum()) if not wk.empty else 0
            if target > 0:
                if existing >= target:
                    cell = f"🟢 {existing}"
                elif existing > 0:
                    cell = f"🟡 {existing}"
                else:
                    cell = "🔴 0"
            else:
                cell = f"{existing}" if existing > 0 else "—"
            row[label] = cell
        rows.append(row)

    center_table(pd.DataFrame(rows))


def _premium_collected(df_orders: pd.DataFrame) -> float:
    """Sum of premium received from opening short option fills (CSP/CC).
    `filled_cash_amount` already accounts for qty × price × 100.
    """
    if df_orders.empty:
        return 0.0
    mask = (
        (df_orders["is_opening"] == True) &
        (df_orders["Action"] == "SELL") &
        (df_orders["TradeType"].isin(["CSP", "CC"]))
    )
    return float(df_orders.loc[mask, "FilledCashAmount"].sum())


def _fmt_var(deployed: float, planned: float) -> str:
    """Format utilization for display in tables (HTML cells).
    Shows utilization % (e.g. 120% = 20% over) AND the dollar variance.
    e.g. '120% (+$20,000)' or '80% (−$20,000)'. Returns '—' when planned is 0.

    Plain `$` is fine inside center_table HTML cells — they don't go through
    Streamlit's KaTeX math processor (only st.markdown body text does).
    """
    if planned <= 0:
        return "—"
    util_pct = (deployed / planned) * 100
    diff = deployed - planned
    if abs(diff) < 1:
        return f"{util_pct:.0f}% (exact)"
    diff_str = f"+${diff:,.0f}" if diff > 0 else f"−${abs(diff):,.0f}"
    return f"{util_pct:.0f}% ({diff_str})"


def _panel_liquidity_csp(df_open: pd.DataFrame, settings: dict):
    """💵 Portfolio Liquidity (CSP) — broker-agnostic policy view, per pot, per ticker.

    For each pot, shows:
      • Per-ticker rows: stock cost basis, LEAP cost, CSP collateral, deployed,
                         planned allocation (pot_cap × alloc%), variance ($/%)
      • Pot total row with Capital, Deployed, Headroom, Status

    Cash Secured = pot capital ≥ deployed (cost basis stock + LEAP cost + CSP collateral).
    Uses cost basis intentionally — this is your policy floor, not mark-to-market.
    """
    ticker_pots = settings.get("ticker_pots", {}) or {}
    pmcc_tickers = set(settings.get("pmcc_tickers", []))
    pot_caps = _ticker_pot_capital(settings)
    alloc_pct = settings.get("allocation_pct", {}) or {}

    st.markdown("#### 💵 Portfolio Liquidity (CSP) — Cash-Secured Policy by Pot")
    st.caption(
        "**Cash Secured** = pot capital ≥ deployed (stock cost basis + LEAP cost + CSP collateral). "
        "**Var** = deployed vs planned allocation (pot capital × allocation %). "
        "Broker-agnostic. Per-ticker breakdown shows what's contributing to over/under."
    )

    if df_open.empty:
        st.info("No open positions.")
        return

    df = df_open.copy()
    df["q"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).abs()
    df["k"] = pd.to_numeric(df["Option_Strike_Price_(USD)"], errors="coerce").fillna(0)
    df["avg_cost"] = pd.to_numeric(df.get("_avg_cost", 0), errors="coerce").fillna(0)

    portfolio_totals = {"capital": 0.0, "stock": 0.0, "leap": 0.0, "csp": 0.0,
                        "deployed": 0.0, "planned": 0.0}

    for pot_name in ("Core", "Active"):
        capital = float(pot_caps.get(pot_name, 0))
        # Explicit pot membership; Core gets unmapped tickers as fallback
        explicit = {t for t, p in ticker_pots.items() if p == pot_name}
        if pot_name == "Core":
            unmapped = set(df["Ticker"].dropna().unique()) - set(ticker_pots.keys())
            pot_tickers = sorted(explicit | unmapped)
        else:
            pot_tickers = sorted(explicit)

        # Per-ticker rows
        ticker_rows = []
        pot_stock = pot_leap = pot_csp = pot_planned = 0.0
        for t in pot_tickers:
            t_df = df[df["Ticker"] == t]
            if t_df.empty:
                continue
            stk = t_df[t_df["TradeType"] == "STOCK"]
            leap = t_df[t_df["TradeType"] == "LEAP"]
            csp = t_df[t_df["TradeType"] == "CSP"]
            stock_cost = float((stk["q"] * stk["avg_cost"]).sum())
            leap_cost = float((leap["q"] * leap["avg_cost"] * 100).sum())
            csp_coll = float((csp["q"] * csp["k"] * 100).sum())
            deployed = stock_cost + leap_cost + csp_coll
            if deployed == 0:
                continue
            # Planned = pot_capital × allocation_pct[t] / 100
            t_alloc_pct = float(alloc_pct.get(t, 0) or 0)
            planned = capital * t_alloc_pct / 100.0
            pot_planned += planned

            is_pmcc = t in pmcc_tickers
            ticker_rows.append({
                "Ticker": f"{t}{' 🔄' if is_pmcc else ''}",
                "Stock $": f"${stock_cost:,.0f}" if stock_cost else "—",
                "LEAP $": f"${leap_cost:,.0f}" if leap_cost else "—",
                "CSP coll $": f"${csp_coll:,.0f}" if csp_coll else "—",
                "Deployed $": f"${deployed:,.0f}",
                "Planned $": f"${planned:,.0f}" if planned > 0 else "—",
                "Var": _fmt_var(deployed, planned),
            })
            pot_stock += stock_cost
            pot_leap += leap_cost
            pot_csp += csp_coll

        pot_deployed = pot_stock + pot_leap + pot_csp
        headroom = capital - pot_deployed

        # Pot total row appended at the bottom
        ticker_rows.append({
            "Ticker": f"Σ {pot_name}",
            "Stock $": f"${pot_stock:,.0f}" if pot_stock else "—",
            "LEAP $": f"${pot_leap:,.0f}" if pot_leap else "—",
            "CSP coll $": f"${pot_csp:,.0f}" if pot_csp else "—",
            "Deployed $": f"${pot_deployed:,.0f}",
            "Planned $": f"${pot_planned:,.0f}" if pot_planned > 0 else "—",
            "Var": _fmt_var(pot_deployed, pot_planned),
        })

        # Pot heading + status (escape $ to avoid markdown→LaTeX math rendering)
        pot_emoji = "🏛️" if pot_name == "Core" else "⚡"
        if headroom >= 0:
            status_md = f"🟢 **Cash Secured** · Headroom **+\\${headroom:,.0f}**"
        else:
            status_md = f"🔴 **Margin used** · Over by **\\${abs(headroom):,.0f}**"
        st.markdown(
            f"**{pot_emoji} {pot_name} Pot** · Capital **\\${capital:,.0f}** · {status_md}"
        )

        if len(ticker_rows) > 1:
            center_table(pd.DataFrame(ticker_rows))
        else:
            st.caption("_No deployed capital in this pot._")

        portfolio_totals["capital"] += capital
        portfolio_totals["stock"] += pot_stock
        portfolio_totals["leap"] += pot_leap
        portfolio_totals["csp"] += pot_csp
        portfolio_totals["deployed"] += pot_deployed
        portfolio_totals["planned"] += pot_planned

    # Portfolio summary
    p_headroom = portfolio_totals["capital"] - portfolio_totals["deployed"]
    if p_headroom >= 0:
        p_status = f"🟢 **Cash Secured** · Headroom **+\\${p_headroom:,.0f}**"
    else:
        p_status = f"🔴 **Margin used** · Over by **\\${abs(p_headroom):,.0f}**"
    plan_var = _fmt_var(portfolio_totals["deployed"], portfolio_totals["planned"])
    st.markdown(
        f"**Σ Portfolio** · Capital **\\${portfolio_totals['capital']:,.0f}** · "
        f"Deployed **\\${portfolio_totals['deployed']:,.0f}** · "
        f"Planned **\\${portfolio_totals['planned']:,.0f}** · "
        f"Var **{plan_var}** · {p_status}"
    )


def _panel_liquidity_tiger(summary, df_open: pd.DataFrame, settings: dict):
    """💧 Portfolio Liquidity (Tiger) — Broker-side cash/margin view.

    Shows what's actually happening in the Tiger account:
      • Multi-currency cash breakdown vs lifetime deposits
      • USD margin loan + collateral securing it
      • Margin used / BP / excess liquidation
      • MMF holdings (if any)
      • Estimated daily interest cost on margin loan
    """
    st.markdown("#### 💧 Portfolio Liquidity (Tiger) — Broker-Side View")
    st.caption(
        "What's actually at Tiger: cash by currency vs deposits, margin loan + collateral, "
        "buying power and margin headroom."
    )

    fx = float(settings.get("sgd_usd_fx_rate", 1.35) or 1.35)
    margin_rate = float(settings.get("tiger_margin_rate_pct", 5.5) or 5.5) / 100.0

    # ── Section 1: Cash by Currency vs Deposits ──────────────────
    st.markdown("##### 💱 Cash by Currency · vs Deposits")
    cc = summary.currency_cash or {}
    # Lifetime deposits per currency from funding history
    deposits_by_ccy = {}
    try:
        from tiger_api import tiger_data
        funding = tiger_data.load_funding_history()
        if not funding.empty and "currency" in funding.columns and "amount" in funding.columns:
            # Deposits positive, withdrawals negative; if Tiger uses 'type' codes,
            # we use the amount sign which is already correct for net flow.
            net = funding.groupby("currency")["amount"].sum().to_dict()
            deposits_by_ccy = {str(k).upper(): float(v) for k, v in net.items()}
    except Exception as e:
        logger.debug("funding history fetch failed: %s", e)

    # Build the multi-currency table
    ccy_order = ["USD", "SGD", "HKD", "CNH"]
    ccy_rows = []
    total_cash_usd = 0.0
    total_deposit_usd = 0.0
    for ccy in ccy_order + [c for c in cc.keys() if c not in ccy_order]:
        if ccy not in cc and ccy not in deposits_by_ccy:
            continue
        info = cc.get(ccy, {})
        bal = float(info.get("cash_balance", 0))
        rate = float(info.get("forex_rate_to_usd", 0)) or (1.0 if ccy == "USD" else 0)
        bal_usd = bal * rate
        deposit_native = float(deposits_by_ccy.get(ccy, 0))
        deposit_usd = deposit_native * rate if rate else 0
        total_cash_usd += bal_usd
        total_deposit_usd += deposit_usd
        # Note column: flag negative cash as margin loan
        if bal < -0.01:
            note = "🔴 Margin loan"
        elif bal > 0.01:
            note = "Collateral / liquid"
        else:
            note = "—"
        ccy_rows.append({
            "Currency": ccy,
            "Cash Balance": f"{bal:,.0f}" if bal else "0",
            "USD Equiv": f"${bal_usd:,.0f}",
            "Lifetime Deposit": f"{deposit_native:,.0f}" if deposit_native else "—",
            "Deposit USD-eq": f"${deposit_usd:,.0f}" if deposit_usd else "—",
            "Note": note,
        })
    # Total row
    ccy_rows.append({
        "Currency": "Σ Total",
        "Cash Balance": "—",
        "USD Equiv": f"${total_cash_usd:,.0f}",
        "Lifetime Deposit": "—",
        "Deposit USD-eq": f"${total_deposit_usd:,.0f}",
        "Note": "Net cash · USD-equivalent",
    })
    if ccy_rows:
        center_table(pd.DataFrame(ccy_rows))

    # ── Section 2: USD Margin Loan + Cost ────────────────────────
    usd_info = cc.get("USD", {})
    usd_balance = float(usd_info.get("cash_balance", 0))
    sgd_info = cc.get("SGD", {})
    sgd_balance_native = float(sgd_info.get("cash_balance", 0))
    sgd_rate = float(sgd_info.get("forex_rate_to_usd", 0))
    sgd_balance_usd = sgd_balance_native * sgd_rate if sgd_rate else 0

    if usd_balance < -0.01:
        st.markdown("##### 🏦 USD Margin Loan")
        loan = abs(usd_balance)
        daily_cost = loan * margin_rate / 365.0
        annual_cost = loan * margin_rate
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("USD Borrowed", f"${loan:,.0f}",
                  help="Negative USD cash balance = margin loan from Tiger.")
        m2.metric("SGD Collateral", f"S${sgd_balance_native:,.0f}",
                  help=f"≈ ${sgd_balance_usd:,.0f} USD at {sgd_rate:.4f}.")
        m3.metric(f"Margin Rate (est)", f"{margin_rate*100:.2f}%",
                  help="Configurable in Config → Tiger margin rate. "
                       "Tiger TBSG USD margin loan rate (check current schedule).")
        m4.metric("Est. Daily Interest", f"−${daily_cost:,.2f}",
                  delta=f"≈ −${annual_cost:,.0f}/yr",
                  delta_color="off",
                  help="loan × rate ÷ 365. Approximation only — Tiger compounds and rates float.")
    else:
        st.caption("✅ No USD margin loan currently outstanding.")

    # ── Section 3: Margin Used + BP + Excess Liquidation ─────────
    st.markdown("##### 📐 Margin & Buying Power")
    mb1, mb2, mb3, mb4, mb5 = st.columns(5)
    init_m = getattr(summary, "init_margin", 0) or 0
    maint_m = getattr(summary, "maintain_margin", 0) or 0
    excess = getattr(summary, "excess_liquidation", 0) or 0
    lev = getattr(summary, "leverage", 0) or 0
    mb1.metric("Init Margin Used", f"${init_m:,.0f}",
               help="Initial margin Tiger is holding against your current positions.")
    mb2.metric("Maintenance Margin", f"${maint_m:,.0f}",
               help="Minimum equity required to keep positions open.")
    mb3.metric("Excess Liquidation", f"${excess:,.0f}",
               help="NAV − Maintenance Margin. Cushion before forced liquidation.")
    mb4.metric("Buying Power", f"${summary.bp:,.0f}",
               help="Max new exposure Tiger will let you take on (uses leverage).")
    mb5.metric("Leverage", f"{lev:.2f}x",
               help="gross_position_value ÷ equity_with_loan.")

    # ── Section 4: Cash Used to Own Securities (cost basis trace) ─
    # This shows where deployed cash actually WENT — broker-side view.
    # Long positions (stocks, LEAPs) consumed cash. Short positions (CSPs, CCs)
    # generate cash income but Tiger holds margin instead of locking cash.
    st.markdown("##### 💸 Cash Used to Own Securities (cost basis)")
    st.caption(
        "Long positions consumed cash (stock cost + LEAP premium). "
        "Short positions (CSPs/CCs) generate premium income; Tiger holds **margin** "
        "for them, not cash — that's why it offsets your USD loan rather than locking up cash."
    )
    long_stock_cost = 0.0
    long_leap_cost = 0.0
    short_csp_collateral_policy = 0.0
    short_cc_count = 0
    if not df_open.empty:
        d = df_open.copy()
        d["q"] = pd.to_numeric(d["Quantity"], errors="coerce").fillna(0).abs()
        d["k"] = pd.to_numeric(d["Option_Strike_Price_(USD)"], errors="coerce").fillna(0)
        d["avg_cost"] = pd.to_numeric(d.get("_avg_cost", 0), errors="coerce").fillna(0)
        stk = d[d["TradeType"] == "STOCK"]
        leap = d[d["TradeType"] == "LEAP"]
        csp = d[d["TradeType"] == "CSP"]
        cc = d[d["TradeType"] == "CC"]
        long_stock_cost = float((stk["q"] * stk["avg_cost"]).sum())
        long_leap_cost = float((leap["q"] * leap["avg_cost"] * 100).sum())
        short_csp_collateral_policy = float((csp["q"] * csp["k"] * 100).sum())
        short_cc_count = int(cc["q"].sum())

    deploy_rows = [
        {"Position Type": "Long Stocks (cost basis)",
         "Cash Out (cost)": f"${long_stock_cost:,.0f}",
         "Note": "Cash actually paid out of Tiger to settle stock purchases."},
        {"Position Type": "Long LEAPs (premium paid)",
         "Cash Out (cost)": f"${long_leap_cost:,.0f}",
         "Note": "Premium paid on LEAP calls — fully sunk cash."},
        {"Position Type": "Σ Long-Position Cash Deployed",
         "Cash Out (cost)": f"${(long_stock_cost + long_leap_cost):,.0f}",
         "Note": "This is the cash that left Tiger to acquire what you OWN."},
        {"Position Type": "Short CSP collateral (your policy)",
         "Cash Out (cost)": f"${short_csp_collateral_policy:,.0f}",
         "Note": "Tiger does NOT lock this — uses margin (~30%). You earmark it under your cash-secured policy."},
        {"Position Type": "Short CCs",
         "Cash Out (cost)": f"{short_cc_count} contracts",
         "Note": "Covered by stock or LEAP — no extra cash/margin required."},
    ]
    center_table(pd.DataFrame(deploy_rows))

    # Bridge to the USD margin loan
    total_long_deployed = long_stock_cost + long_leap_cost
    nav = float(summary.nav or 0)
    sgd_collateral_usd = sgd_balance_usd  # computed in Section 2
    usd_loan = abs(usd_balance) if usd_balance < 0 else 0.0
    st.caption(
        f"**Bridge:** You own \\${total_long_deployed:,.0f} of long positions (cost basis). "
        f"Your SGD collateral converts to ≈ \\${sgd_collateral_usd:,.0f} USD. "
        f"Tiger lent you \\${usd_loan:,.0f} USD to fund the gap (and absorb realized losses + fees). "
        f"NAV today = \\${nav:,.0f}."
    )

    # ── Section 5: Position Snapshot ─────────────────────────────
    st.markdown("##### 📊 Position Snapshot (mark-to-market)")
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Gross Position Value", f"${summary.gross_position_value:,.0f}",
              help="Current MARKET value of all open positions (vs cost basis above).")
    p2.metric("Equity w/ Loan", f"${summary.equity_with_loan:,.0f}",
              help="Account equity if margin loan was paid off.")
    p3.metric("Today P&L", f"${summary.realized_pnl_today:,.0f}",
              help="Realized P&L locked in today.")
    p4.metric("Unrealized P&L", f"${summary.unrealized_pnl:+,.0f}",
              help="Mark-to-market on open positions.")

    # ── Section 6: Tiger Vault (MMF auto-sweep) history ──────────
    st.markdown("##### 💰 Tiger Vault (MMF) — SGD/USD auto-sweep")
    try:
        from tiger_api import tiger_data as _td_mod
        vault = _td_mod.vault_summary()
        vault_df = _td_mod.load_vault_history()

        # Headline metrics
        v1, v2, v3, v4 = st.columns(4)
        net_held = float(vault.get("current_balance_sgd", 0))
        v1.metric("Current Vault balance",
                  f"S${net_held:,.0f}" if net_held > 0.01 else "S$0",
                  help="Sum of all subscriptions − redemptions. 0 = MMF positions liquidated.")
        v2.metric("Lifetime subscribed",
                  f"S${vault.get('lifetime_buys_sgd', 0):,.0f}",
                  help="Total cash put INTO the Vault over account lifetime.")
        v3.metric("Lifetime redeemed",
                  f"S${vault.get('lifetime_sells_sgd', 0):,.0f}",
                  help="Total cash taken OUT (sold MMF units).")
        v4.metric("Last activity",
                  vault.get("last_activity_date", "—"),
                  help="Most recent fund subscription / redemption / transfer.")

        if vault.get("fund_names"):
            st.caption("**Funds used:** " + " · ".join(vault["fund_names"]))

        # Recent activity table — last 10 events
        if not vault_df.empty:
            with st.expander("📜 Recent Vault activity (last 10 events)", expanded=False):
                show = vault_df.head(10)[["business_date", "type", "currency", "amount", "contract_name", "desc"]].copy()
                show["business_date"] = show["business_date"].dt.strftime("%Y-%m-%d")
                show["amount"] = show["amount"].apply(lambda v: f"{float(v):,.2f}" if pd.notna(v) else "—")
                show.columns = ["Date", "Type", "Ccy", "Amount", "Fund", "Desc"]
                center_table(show)
                st.caption(
                    "Type legend: **Trade** = bought/sold MMF units · "
                    "**Funds Transfer In/Out** = SEC↔FUND segment movement · "
                    "**Fund Subscription** = subscription bookkeeping · "
                    "**Campaign Subsidy** = interest accrual / promo credit."
                )

        # Insight: if balance is 0 but lifetime activity > 0, surface that
        if net_held < 1 and vault.get("lifetime_buys_sgd", 0) > 0:
            st.info(
                f"ℹ️ You PREVIOUSLY held SGD MMF in Tiger Vault "
                f"(S${vault['lifetime_buys_sgd']:,.0f} subscribed lifetime), "
                f"but the positions were liquidated on {vault.get('last_activity_date', '?')}. "
                f"That's why your Vault P&L has been $0 since then. "
                f"Re-enrol via the Tiger app if you want SGD idle cash to earn yield again."
            )

        # MMF/margin clarification — important so user understands what triggers redemption
        st.caption(
            "💡 **How Tiger Vault MMF interacts with margin:** MMF holdings count as "
            "collateral for `equity_with_loan` and `init_margin`. **Selling new options "
            "(CSP/CC) does NOT redeem MMF** — Tiger uses margin against the MMF balance. "
            "Auto-redemption only happens on (a) **stock-buy settlement** when SEC cash "
            "is short by T+1, (b) **CSP assignment** forcing a USD purchase, or "
            "(c) **maintenance margin breach**. So your S$317k can sit in Vault earning "
            "yield while you keep wheeling normally."
        )

        # Margin loan offset opportunity
        if usd_loan > 0 and net_held < 1:
            opportunity = usd_loan * margin_rate / 365.0
            st.caption(
                f"💡 You're paying ≈ −${opportunity:,.2f}/day in USD margin interest. "
                f"USD MMF / Cash Boost+ would help only if you had IDLE USD cash — "
                f"all your USD is on loan, so MMF won't directly offset. "
                f"SGD Vault would earn yield on your S${sgd_balance_native:,.0f} SGD collateral instead."
            )
    except Exception as e:
        st.warning(f"Vault history unavailable: {e}")


# ────────────────────────────────────────────────────────────────
# 💰 Cash Maximization — full picture: deposits, FX, MMF, loan, carry
# ────────────────────────────────────────────────────────────────
def _panel_cash_maximization(summary, df_open: pd.DataFrame, settings: dict):
    """The complete 'where is my cash' panel.

    Five sections:
      A. Lifetime cash flow trace (deposits → FX → MMF → positions → current)
      B. Live carry analysis (USD margin cost vs MMF yield offset)
      C. FX trades history
      D. FX position summary (avg buy rate, unrealized FX P&L)
      E. Vault pull alert (recent FUND→SEC transfers)
    """
    from tiger_api import tiger_data as _td

    st.markdown("### 💰 Cash Maximization — Full Picture")
    st.caption(
        "Every dollar from deposit → conversion → MMF → collateral → position → loan, "
        "in one view. Plus live carry analysis showing the true cost of holding the USD "
        "margin loan and how much MMF yield could offset."
    )

    # ── Settings (with defaults) ────────────────────────────────
    margin_rate_pct = float(settings.get("tiger_margin_rate_pct", 7.0) or 7.0)
    mmf_yield_pct = float(settings.get("mmf_yield_pct", 3.5) or 3.5)
    fx_sgd_per_usd = float(settings.get("sgd_usd_fx_rate", 1.276) or 1.276)

    # FX is loaded on-demand (slow first call due to Tiger rate limit).
    # User toggles via Section C button — flag persists in session state.
    fx_loaded = st.session_state.get("fx_trades_loaded", False)

    # ── A. Lifetime cash flow trace ─────────────────────────────
    st.markdown("##### 🌊 A. Lifetime Cash Flow — where every dollar lives now")
    try:
        pic = _td.compute_cash_flow_picture(summary, df_open, settings,
                                             include_fx=fx_loaded)
    except Exception as e:
        st.warning(f"Cash flow picture unavailable: {e}")
        pic = None

    if pic:
        cur = pic["current"]
        # Inflows row
        ic1, ic2, ic3, ic4 = st.columns(4)
        ic1.metric("Lifetime SGD deposits",
                   f"S${pic['deposits_sgd']:,.0f}",
                   delta=f"≈ ${pic['deposits_usd_eq']:,.0f} USD",
                   delta_color="off",
                   help="Sum of every SGD deposit ever made into this account.")
        fx_c = pic["fx_conversions"]
        if fx_loaded and fx_c["trade_count"] > 0:
            ic2.metric("SGD→USD converted",
                       f"S${fx_c['sgd_spent']:,.0f}",
                       delta=f"got ${fx_c['usd_received']:,.0f} @ {fx_c['avg_rate']:.4f}",
                       delta_color="off",
                       help=f"{fx_c['trade_count']} FX conversion trade(s). "
                            f"Avg rate paid (SGD per USD).")
        elif fx_loaded:
            ic2.metric("SGD→USD converted", "S$0",
                       help="No FX conversion trades found via Tiger API.")
        else:
            ic2.metric("SGD→USD converted", "—",
                       help="Click 'Load FX trades' in Section C to populate this metric. "
                            "Skipped by default to keep cold start fast.")

        mmf = pic["mmf"]
        ic3.metric("MMF yield earned",
                   f"S${mmf['lifetime_yield_sgd']:+,.0f}",
                   delta=f"current S${mmf['current_balance_sgd']:,.0f}",
                   delta_color="off",
                   help="Lifetime: redeemed minus subscribed. Current vault balance shown as delta.")
        ic4.metric("Today's NAV",
                   f"${cur['nav']:,.0f}",
                   delta=f"≈ S${cur['nav'] * fx_sgd_per_usd:,.0f}",
                   delta_color="off",
                   help="Current account value (positions MTM + cash).")

        # Where the cash is now
        st.markdown("**Current Position of Capital (where the cash sits TODAY):**")
        flow_rows = [
            {"Bucket": "💵 SGD idle in SEC (collateral, not earning)",
             "SGD": f"S${cur['sgd_idle_cash']:,.0f}",
             "USD-eq": f"${cur['sgd_idle_cash_usd_eq']:,.0f}",
             "Note": "Sitting as margin collateral. Could be in Vault MMF earning yield."},
            {"Bucket": "📈 MMF (Tiger Vault)",
             "SGD": f"S${mmf['current_balance_sgd']:,.0f}",
             "USD-eq": f"${mmf['current_balance_sgd'] / fx_sgd_per_usd:,.0f}" if fx_sgd_per_usd > 0 else "—",
             "Note": "Earning yield. Auto-redeems if margin pressure hits."},
            {"Bucket": "🏦 USD margin loan from Tiger",
             "SGD": f"−S${cur['usd_loan'] * fx_sgd_per_usd:,.0f}",
             "USD-eq": f"−${cur['usd_loan']:,.0f}",
             "Note": "Borrowed against SGD collateral to fund US positions."},
            {"Bucket": "📊 Long Stock (cost basis)",
             "SGD": f"S${cur['long_stock_cost'] * fx_sgd_per_usd:,.0f}",
             "USD-eq": f"${cur['long_stock_cost']:,.0f}",
             "Note": "Cash spent acquiring stock you currently hold."},
            {"Bucket": "📉 Long LEAPs (premium paid)",
             "SGD": f"S${cur['long_leap_cost'] * fx_sgd_per_usd:,.0f}",
             "USD-eq": f"${cur['long_leap_cost']:,.0f}",
             "Note": "Premium paid for currently-held LEAP calls."},
            {"Bucket": "🔒 CSP collateral (your policy)",
             "SGD": f"S${cur['short_csp_collateral_policy'] * fx_sgd_per_usd:,.0f}",
             "USD-eq": f"${cur['short_csp_collateral_policy']:,.0f}",
             "Note": "Your cash-secured-put policy floor. Tiger holds margin (~30%), not cash."},
            {"Bucket": "🛡️ Tiger init margin (actual broker hold)",
             "SGD": f"S${cur['tiger_init_margin'] * fx_sgd_per_usd:,.0f}",
             "USD-eq": f"${cur['tiger_init_margin']:,.0f}",
             "Note": "What Tiger ACTUALLY locks for your shorts (vs your CSP policy)."},
        ]
        center_table(pd.DataFrame(flow_rows))

        # The reconciliation arithmetic
        outflow_long = cur["long_stock_cost"] + cur["long_leap_cost"]
        st.caption(
            f"**Reconciliation:** S${pic['deposits_sgd']:,.0f} deposited "
            f"(≈ \\${pic['deposits_usd_eq']:,.0f}) → \\${outflow_long:,.0f} long-position cost "
            f"+ \\${cur['sgd_idle_cash_usd_eq']:,.0f} idle SGD collateral "
            f"− \\${cur['usd_loan']:,.0f} USD loan "
            f"= net **\\${cur['nav']:,.0f} NAV** (with unrealized P&L baked in)."
        )

    # ── B. Carry analysis ───────────────────────────────────────
    st.markdown("##### 🧮 B. Carry Analysis — true cost of margin")
    try:
        sgd_idle = pic["current"]["sgd_idle_cash"] if pic else 0
        usd_loan = pic["current"]["usd_loan"] if pic else 0
        carry = _td.compute_carry_analysis(
            usd_loan=usd_loan, sgd_idle=sgd_idle, fx_sgd_per_usd=fx_sgd_per_usd,
            margin_rate_pct=margin_rate_pct, mmf_yield_pct=mmf_yield_pct,
        )
        cc1, cc2, cc3, cc4 = st.columns(4)
        cc1.metric("USD margin annual cost",
                   f"−${carry['annual_interest_cost_usd']:,.0f}/yr",
                   delta=f"−${carry['daily_interest_cost_usd']:.2f}/day",
                   delta_color="off",
                   help=f"USD loan × {margin_rate_pct:.2f}% APR. "
                        f"Configurable in Config → Tiger USD margin rate.")
        cc2.metric("Potential MMF offset",
                   f"+${carry['potential_mmf_offset_usd']:,.0f}/yr",
                   delta=f"+S${carry['potential_mmf_offset_sgd']:,.0f}/yr",
                   delta_color="off",
                   help=f"Idle SGD × {mmf_yield_pct:.2f}% yield. "
                        f"Configurable in Config → SGD MMF expected yield.")
        net_carry = carry["net_annual_carry_usd"]
        cc3.metric("Net annual carry",
                   f"−${net_carry:,.0f}/yr",
                   delta=f"{carry['offset_pct']:.0f}% offset by MMF",
                   delta_color="off",
                   help="True ongoing cost of keeping the loan, AFTER potential MMF income.")
        cc4.metric("Break-even MMF yield",
                   f"{carry['breakeven_mmf_yield_pct']:.2f}%",
                   help="MMF yield % required to fully neutralize the margin cost.")
        if mmf_yield_pct < carry["breakeven_mmf_yield_pct"]:
            shortfall = carry["breakeven_mmf_yield_pct"] - mmf_yield_pct
            st.caption(
                f"⚠️ Current MMF yield estimate ({mmf_yield_pct:.2f}%) is "
                f"**{shortfall:.2f}pp below** break-even — even with full MMF enrolment, "
                f"you'd net −\\${abs(net_carry):,.0f}/yr in financing cost. To eliminate, "
                f"shrink the USD loan."
            )
        else:
            st.caption("✅ MMF yield exceeds break-even — full enrolment would zero out financing cost.")
    except Exception as e:
        st.warning(f"Carry analysis unavailable: {e}")

    # ── C. FX trades history (on-demand) ────────────────────────
    st.markdown("##### 💱 C. FX Trade History — every SGD↔USD conversion")
    if not fx_loaded:
        col_btn, col_caption = st.columns([1, 4])
        with col_btn:
            if st.button("📥 Load FX trades", key="fx_load_btn",
                          help="Pulls FX conversion history from Tiger. Takes ~60-70s "
                               "first time due to Tiger's 10/min rate limit on the "
                               "fund_details endpoint. Cached for 10 min after.",
                          type="primary"):
                with st.spinner("Pulling FX trades from Tiger (paginated, ~60s)…"):
                    _td.load_fx_trades(start_date="2024-01-01")  # warm cache
                st.session_state["fx_trades_loaded"] = True
                st.rerun()
        with col_caption:
            st.caption(
                "FX trades load on-demand to keep cold start fast. Click the button → "
                "Sections A (SGD→USD), C (history), D (position P&L) all populate."
            )
    else:
        try:
            fx_df = _td.load_fx_trades(start_date="2024-01-01")
            if fx_df.empty:
                st.caption("No FX conversion trades on record.")
            else:
                disp = fx_df.copy()
                disp["trade_date"] = disp["trade_date"].dt.strftime("%Y-%m-%d")
                disp["from_amount"] = disp["from_amount"].apply(lambda v: f"{v:,.2f}")
                disp["to_amount"] = disp["to_amount"].apply(lambda v: f"{v:,.2f}")
                disp["rate"] = disp["rate"].apply(lambda v: f"{v:.5f}")
                disp.columns = ["Trade Date", "Pair", "From Ccy", "From Amount",
                                "To Ccy", "To Amount", "Rate", "Tiger Desc"]
                center_table(disp)
                col_cap, col_refresh = st.columns([4, 1])
                col_cap.caption(
                    "Rate = SGD per USD when converting SGD→USD. Lower rate = stronger SGD = "
                    "you got more USD for your SGD."
                )
                with col_refresh:
                    if st.button("🔄 Refresh", key="fx_refresh_btn",
                                  help="Re-pull FX trades from Tiger (clears 10min cache)."):
                        _td.load_fx_trades.clear()
                        st.rerun()
        except Exception as e:
            st.warning(f"FX trades unavailable: {e}")

    # ── D. FX position summary (only when FX loaded) ────────────
    if fx_loaded:
        try:
            fx_pos = _td.fx_position_summary(fx_sgd_per_usd)
            if fx_pos["trade_count"] > 0:
                st.markdown("##### 📐 D. FX Position — unrealized P&L on conversions")
                fp1, fp2, fp3, fp4 = st.columns(4)
                fp1.metric("Avg buy rate",
                           f"{fx_pos['avg_buy_rate']:.5f}",
                           help="Weighted-average SGD per USD across all your FX conversions.")
                fp2.metric("Current rate",
                           f"{fx_pos['current_rate']:.5f}",
                           delta=f"{((fx_pos['current_rate']/fx_pos['avg_buy_rate'])-1)*100:+.2f}%" if fx_pos['avg_buy_rate'] else None,
                           help="Today's spot SGD per USD (from Tiger).")
                fp3.metric("Unrealized FX P&L",
                           f"${fx_pos['unrealized_fx_pnl_usd']:+,.0f}",
                           delta=f"S${fx_pos['unrealized_fx_pnl_sgd']:+,.0f}",
                           delta_color="off",
                           help="(current rate − avg buy rate) × USD bought. "
                                "Positive = SGD weakened since you bought USD = your USD is worth more SGD now.")
                fp4.metric("Last FX trade",
                           fx_pos["last_trade_date"] or "—",
                           help="Most recent SGD↔USD conversion in the account.")
        except Exception as e:
            logger.debug("FX position summary skipped: %s", e)

    # ── E. Vault pull alert (recent activity) ───────────────────
    try:
        alert = _td.detect_vault_pull_alert(window_days=14, min_amount_sgd=5000)
        if alert["alert"]:
            st.markdown("##### ⚠️ E. Recent Vault Redemption (last 14 days)")
            st.warning(
                f"**S${alert['total_sgd']:,.0f} moved FUND → SEC recently.** "
                f"Two common causes: (a) Tiger auto-pulled to cover margin pressure "
                f"(bad — investigate position sizing); (b) Tiger migrated/discontinued "
                f"a fund and force-redeemed (benign — re-subscribe to the new offering). "
                f"Compare the redemption funds vs. what's currently offered in Tiger Vault."
            )
            for ev in alert["events"]:
                st.markdown(
                    f"  • **{ev['date']}** — S${ev['amount_sgd']:,.0f} from FUND → SEC "
                    f"({ev['status']})"
                )
        else:
            st.caption("✅ E. No Vault redemptions in the last 14 days. (Window: any FUND→SEC transfer ≥ S$5,000.)")
    except Exception as e:
        st.caption(f"Vault pull check unavailable: {e}")


@st.fragment
def render_positions(df_open: pd.DataFrame, spot_prices: dict, settings: dict):
    """📦 Open Positions — every currently-open holding pulled live from Tiger.

    One row per position with strike / expiry / DTE / qty / cost / spot / mkt value /
    unrealized P&L. Filterable by Type, Strategy (Core/Active), Ticker.
    """
    st.markdown("### 📦 Open Positions")
    st.caption(
        "Live positions from Tiger. Each row is one open holding. "
        "Filter by Type, Strategy, or Ticker. Updates on 🔄 Refresh."
    )

    if df_open is None or df_open.empty:
        st.info("No open positions.")
        return

    df = df_open.copy()
    ticker_pots = settings.get("ticker_pots", {}) or {}
    df["Strategy"] = df["Ticker"].map(lambda t: ticker_pots.get(t, "Core"))
    df["Spot"] = df["Ticker"].map(lambda t: spot_prices.get(t))

    df["Expiry_dt"] = pd.to_datetime(df["Expiry_Date"], errors="coerce")
    today = pd.Timestamp.now().normalize()
    df["DTE"] = (df["Expiry_dt"] - today).dt.days
    df["Expiry_Month"] = df["Expiry_dt"].dt.strftime("%Y-%m")

    # ── Filter row 1: Type / Strategy / Ticker ──────────────────
    f1 = st.columns([1, 1, 1, 1, 1])
    type_options = ["All"] + sorted(df["TradeType"].dropna().unique().tolist())
    type_filter = f1[0].selectbox("Type", type_options, key="pos_type_filter")

    strat_options = ["All"] + sorted(df["Strategy"].dropna().unique().tolist())
    strat_filter = f1[1].selectbox("Strategy", strat_options, key="pos_strat_filter")

    ticker_options = ["All"] + sorted(df["Ticker"].dropna().unique().tolist())
    ticker_filter = f1[2].selectbox("Ticker", ticker_options, key="pos_ticker_filter")

    month_options = sorted(df["Expiry_Month"].dropna().unique().tolist())
    month_filter = f1[3].multiselect(
        "Expiry Month", month_options, key="pos_exp_month_filter",
        placeholder="All months",
    )

    date_options = sorted(df["Expiry_Date"].dropna().replace("", pd.NA).dropna().unique().tolist())
    date_filter = f1[4].multiselect(
        "Expiry Date", date_options, key="pos_exp_date_filter",
        placeholder="All dates",
    )

    if st.button("Reset filters", key="pos_reset_filters"):
        for k in ("pos_type_filter", "pos_strat_filter", "pos_ticker_filter",
                  "pos_exp_month_filter", "pos_exp_date_filter"):
            st.session_state.pop(k, None)
        try:
            st.rerun(scope="fragment")
        except TypeError:
            st.rerun()

    filt = df.copy()
    if type_filter != "All":
        filt = filt[filt["TradeType"] == type_filter]
    if strat_filter != "All":
        filt = filt[filt["Strategy"] == strat_filter]
    if ticker_filter != "All":
        filt = filt[filt["Ticker"] == ticker_filter]
    if month_filter:  # non-empty list
        filt = filt[filt["Expiry_Month"].isin(month_filter)]
    if date_filter:
        filt = filt[filt["Expiry_Date"].isin(date_filter)]

    if filt.empty:
        st.info("No positions match the current filters.")
        return

    # Build display rows — keep P&L numeric so Styler can color it red/green
    # Greeks: Tiger denies for retail TBSG, so we compute Delta + Theta locally
    # via Black-Scholes (solve IV from market price → plug into BS Greeks).
    from tiger_api.greeks import compute_greeks
    from tiger_api import tiger_data as _td_mod

    # Pre-fetch Alpaca option quotes (last/mid/bid/ask) for ALL option rows in one call.
    # Single OptionSnapshotRequest for up to 100 symbols = ~1s vs N×Tiger calls.
    options_only = filt[filt["TradeType"] != "STOCK"].copy()
    quote_key = []
    for _, r in options_only.iterrows():
        try:
            tkr = str(r["Ticker"]).upper()
            exp = str(r["Expiry_Date"])
            strike = float(r["Option_Strike_Price_(USD)"])
            ttype = r["TradeType"]
            pc = "C" if ttype in ("CC", "LEAP") else "P"
            quote_key.append((tkr, exp, strike, pc))
        except (TypeError, ValueError, KeyError):
            continue
    quotes = {}
    if quote_key:
        try:
            quotes = _td_mod.load_option_quotes(tuple(quote_key))
        except Exception as e:
            logger.debug("Option quote fetch failed: %s", e)

    # Pre-fetch earnings calendar for all unique tickers in filter
    unique_tickers = tuple(sorted(filt["Ticker"].dropna().unique().tolist()))
    earnings = {}
    try:
        earnings = _td_mod.load_earnings_calendar(unique_tickers)
    except Exception as e:
        logger.debug("Earnings calendar fetch failed: %s", e)

    rows = []
    total_theta_dollars = 0.0  # Σ θ/day across all option positions (× 100 × |qty|)
    total_delta_shares = 0.0   # Σ delta-equivalent shares (× 100 × |qty|)
    for _, r in filt.iterrows():
        is_option = r["TradeType"] != "STOCK"
        unrl = float(r.get("_unrealized_pnl") or 0)

        # Compute Delta + Theta for option rows
        delta_val = None
        theta_val = None
        if is_option:
            ttype = r["TradeType"]
            is_call = ttype in ("CC", "LEAP")  # CSP=put, CC/LEAP=call
            # Short: CSP, CC | Long: LEAP, BTO stocks-of-options (rare)
            is_long = ttype == "LEAP"
            try:
                strike_v = float(r["Option_Strike_Price_(USD)"])
                spot_v = float(r["Spot"]) if pd.notna(r.get("Spot")) and r["Spot"] else None
                dte_v = float(r["DTE"]) if pd.notna(r["DTE"]) else None
                mkt_v = float(r["_market_price"]) if r.get("_market_price") else None
                if all(v is not None and v > 0 for v in (strike_v, spot_v, dte_v, mkt_v)):
                    g = compute_greeks(
                        spot=spot_v, strike=strike_v, dte_days=dte_v,
                        market_price=mkt_v, is_call=is_call, is_long=is_long,
                    )
                    delta_val = g["delta"]
                    theta_val = g["theta_per_day"]
                    # Aggregate to portfolio-level: × 100 (contract size) × |qty|
                    qty_abs = abs(int(r["Quantity"])) if r.get("Quantity") else 0
                    if delta_val is not None:
                        total_delta_shares += delta_val * 100 * qty_abs
                    if theta_val is not None:
                        total_theta_dollars += theta_val * 100 * qty_abs
            except (TypeError, ValueError, KeyError):
                pass

        # Annualized yield % — wheel's most-cited efficiency metric.
        # CSP/CC: yield = (premium/share / strike) × (365 / DTE) × 100
        # LEAP: skipped (long-term holding, not yielding premium).
        yield_pct = None
        if is_option and r["TradeType"] in ("CSP", "CC"):
            try:
                prem = float(r.get("_avg_cost") or 0)
                strk = float(r.get("Option_Strike_Price_(USD)") or 0)
                dte = float(r["DTE"]) if pd.notna(r["DTE"]) else 0
                if prem > 0 and strk > 0 and dte > 0:
                    yield_pct = (prem / strk) * (365.0 / dte) * 100.0
            except (TypeError, ValueError, KeyError):
                pass

        # Look up Alpaca quote for this option (Last + Mid, replaces Tiger's market_price)
        last_str = "—"
        mid_str = "—"
        if is_option:
            try:
                tkr = str(r["Ticker"]).upper()
                exp = str(r["Expiry_Date"])
                strike = float(r["Option_Strike_Price_(USD)"])
                ttype = r["TradeType"]
                pc = "C" if ttype in ("CC", "LEAP") else "P"
                q = quotes.get((tkr, exp, strike, pc), {})
                last_v = q.get("last")
                mid_v = q.get("mid")
                if last_v is not None:
                    last_str = f"${last_v:.2f}"
                elif r.get("_market_price"):  # fallback to Tiger's market_price
                    last_str = f"${float(r['_market_price']):.2f}"
                if mid_v is not None:
                    bid_v = q.get("bid")
                    ask_v = q.get("ask")
                    spread = (ask_v - bid_v) if (bid_v and ask_v) else None
                    mid_str = f"${mid_v:.2f}"
                    if spread is not None and spread > 0 and mid_v > 0:
                        # Embed bid/ask spread % in display for tight/wide context
                        mid_str = f"${mid_v:.2f}"
            except (TypeError, ValueError, KeyError):
                pass

        # Earnings warning — flag if earnings fall within DTE window for shorts
        earn_str = "—"
        if is_option:
            earn_date = earnings.get(str(r["Ticker"]).upper())
            if earn_date:
                today_d = pd.Timestamp.now().normalize().date()
                days_to_earnings = (earn_date - today_d).days if hasattr(earn_date, "year") else None
                exp_d = pd.to_datetime(r["Expiry_Date"], errors="coerce")
                if days_to_earnings is not None and days_to_earnings >= 0:
                    if pd.notna(exp_d) and earn_date <= exp_d.date() and r["TradeType"] in ("CSP", "CC"):
                        # Earnings BEFORE expiry on a short = gap risk
                        earn_str = f"⚠️ {earn_date.strftime('%m-%d')} ({days_to_earnings}d)"
                    else:
                        earn_str = f"{earn_date.strftime('%m-%d')} ({days_to_earnings}d)"

        rows.append({
            "Ticker": r["Ticker"],
            "Strategy": r["Strategy"],
            "Type": r["TradeType"],
            "Direction": r["Direction"],
            "Qty": int(r["Quantity"]) if r["Quantity"] else 0,
            "Strike $": f"${float(r['Option_Strike_Price_(USD)']):.2f}" if (is_option and r["Option_Strike_Price_(USD)"]) else "—",
            "Expiry": str(r["Expiry_Date"]) if (is_option and r["Expiry_Date"]) else "—",
            "DTE": int(r["DTE"]) if pd.notna(r["DTE"]) else "—",
            "Earnings": earn_str,
            "Avg / Premium": f"${float(r['_avg_cost']):.4f}" if r.get("_avg_cost") else "—",
            "Yield %/yr": f"{yield_pct:.0f}%" if yield_pct is not None else "—",
            "Spot": f"${float(r['Spot']):.2f}" if pd.notna(r.get("Spot")) and r["Spot"] else "—",
            "Last": last_str,
            "Mid": mid_str,
            "Δ": f"{delta_val:+.2f}" if delta_val is not None else "—",
            "Θ/day": f"${theta_val:+.3f}" if theta_val is not None else "—",
            "Mkt Value": f"${float(r['_market_value']):,.0f}",
            "Unrl P&L": unrl,  # numeric — Styler colors and formats it below
        })

    df_disp = pd.DataFrame(rows).sort_values(["Type", "Ticker", "Expiry"]).reset_index(drop=True)
    styled = df_disp.style.map(_color_pnl, subset=["Unrl P&L"]).format({"Unrl P&L": _fmt_pnl})
    st.dataframe(styled, use_container_width=True, hide_index=True)
    st.caption(
        "**Last** = last traded price (Alpaca). **Mid** = (bid+ask)/2. "
        "Δ (Delta) = sensitivity to $1 spot move per share. "
        "Θ/day (Theta) = daily P&L from time decay per share (× 100 for $-impact). "
        "Greeks computed locally via Black-Scholes; quotes from Alpaca (Tiger denies "
        "option market data for retail TBSG). Short positions show flipped Δ sign."
    )

    # NOTE: Roll / Close Candidates moved to ⚠️ Risk & Rolls tab.

    # Summary strip
    st.divider()
    total_unrl = pd.to_numeric(filt["_unrealized_pnl"], errors="coerce").fillna(0).sum()
    total_mv_abs = pd.to_numeric(filt["_market_value"], errors="coerce").fillna(0).abs().sum()

    type_counts = filt["TradeType"].value_counts().to_dict()
    type_summary = " · ".join(f"{k}: {v}" for k, v in sorted(type_counts.items()))

    cols = st.columns(5)
    cols[0].metric("Σ Open positions", len(filt))
    cols[1].metric("Σ |Market value|", f"${total_mv_abs:,.0f}")
    cols[2].metric("Σ Unrealized P&L", f"${total_unrl:+,.0f}")
    cols[3].metric("Σ Δ shares-equiv", f"{total_delta_shares:+,.0f}",
                   help="Σ option Δ × 100 × |qty|. Approximates equivalent share exposure from options. Stocks not included.")
    cols[4].metric("Σ Θ/day ($)", f"${total_theta_dollars:+,.0f}",
                   help="Σ option Θ × 100 × |qty|. Daily $ P&L from time decay (positive = collecting decay).")
    st.caption(f"**Type breakdown:** {type_summary}")


@st.fragment
def render_transactions(df_orders: pd.DataFrame, days: int = 14):
    """📜 Transactions — full transaction history with filters (date ranges + type/side/ticker).

    Wrapped in @st.fragment so filter changes don't trigger a full-app rerun
    (which used to bounce the user back to the Cockpit tab).
    """
    st.markdown("### 📜 Transaction History")
    st.caption("Filter by transaction date, expiry date, type, side, and ticker.")

    if df_orders.empty:
        st.info("No fills in the lookback window.")
        return

    df = df_orders.copy()
    df["TradeDate_dt"] = pd.to_datetime(df.get("TradeDate", df.get("TradeDateTime")), errors="coerce")
    df["Expiry_dt"] = pd.to_datetime(df.get("Expiry_Date"), errors="coerce")

    # Sensible defaults: last 14 days
    today = date.today()
    default_start_txn = today - timedelta(days=14)

    # ── Filter row 1: Transaction Date range ────────────────────
    f1 = st.columns([1, 1, 1, 1, 1])
    with f1[0]:
        txn_start = st.date_input(
            "Txn date from",
            value=default_start_txn,
            key="txn_start_filter",
            help="Filter by transaction (fill) date — start of range.",
        )
    with f1[1]:
        txn_end = st.date_input(
            "Txn date to",
            value=today,
            key="txn_end_filter",
            help="Filter by transaction (fill) date — end of range.",
        )

    # ── Filter row 2: Expiry Date range ─────────────────────────
    expiry_dates_present = df["Expiry_dt"].dropna()
    if not expiry_dates_present.empty:
        exp_min = expiry_dates_present.min().date()
        exp_max = expiry_dates_present.max().date()
    else:
        exp_min = today
        exp_max = today + timedelta(days=365)
    with f1[2]:
        exp_start = st.date_input(
            "Expiry from",
            value=exp_min,
            key="txn_exp_start_filter",
            help="Filter by option expiry — leave wide to ignore.",
        )
    with f1[3]:
        exp_end = st.date_input(
            "Expiry to",
            value=exp_max,
            key="txn_exp_end_filter",
        )
    with f1[4]:
        if st.button("Reset filters", use_container_width=True, key="txn_reset"):
            for k in ("txn_start_filter", "txn_end_filter", "txn_exp_start_filter",
                      "txn_exp_end_filter", "txn_type_filter", "txn_side_filter",
                      "txn_ticker_filter"):
                st.session_state.pop(k, None)
            # Rerun ONLY the fragment (preserves active tab)
            try:
                st.rerun(scope="fragment")
            except TypeError:
                st.rerun()  # older Streamlit fallback

    # ── Filter row 3: Type / Event / Ticker ─────────────────────
    f2 = st.columns([1, 1, 1, 2])
    type_options = ["All"] + sorted(df["TradeType"].dropna().unique().tolist())
    type_filter = f2[0].selectbox("Type", type_options, key="txn_type_filter")
    event_options = ["All"] + sorted(df.get("Event", pd.Series(dtype=str)).dropna().unique().tolist()) \
        if "Event" in df.columns else ["All"]
    event_filter = f2[1].selectbox("Event", event_options, key="txn_event_filter",
                                   help="STO=Sell-to-Open · BTC=Buy-to-Close · BTO=Buy-to-Open · STC=Sell-to-Close · EXPIRED=expired worthless")
    ticker_options = ["All"] + sorted(df["Ticker"].dropna().unique().tolist())
    ticker_filter = f2[2].selectbox("Ticker", ticker_options, key="txn_ticker_filter")

    # ── Apply all filters ───────────────────────────────────────
    filt = df.copy()
    # Transaction date range
    filt = filt[
        (filt["TradeDate_dt"].dt.date >= txn_start) &
        (filt["TradeDate_dt"].dt.date <= txn_end)
    ]
    # Expiry date range — only restrict OPTION rows (skip stocks where expiry is empty)
    is_option = filt["Expiry_dt"].notna()
    expiry_pass = (
        ~is_option |
        ((filt["Expiry_dt"].dt.date >= exp_start) & (filt["Expiry_dt"].dt.date <= exp_end))
    )
    filt = filt[expiry_pass]
    if type_filter != "All":
        filt = filt[filt["TradeType"] == type_filter]
    if event_filter != "All" and "Event" in filt.columns:
        filt = filt[filt["Event"] == event_filter]
    if ticker_filter != "All":
        filt = filt[filt["Ticker"] == ticker_filter]

    if filt.empty:
        st.info("No transactions match the current filters.")
        return

    days_span = max(1, (txn_end - txn_start).days)

    # Event icons — visual quick-scan
    EVENT_ICONS = {
        "STO": "🟢",      # opening short (CSP/CC) — premium received
        "BTO": "🔵",      # opening long (LEAP)
        "BTC": "🔴",      # closing short via active buy
        "STC": "🔵",      # closing long
        "EXPIRED": "⏰",  # expired worthless — full premium kept
    }

    disp_rows = []
    for _, r in filt.iterrows():
        ev = r.get("Event", r["Action"])
        ev_icon = EVENT_ICONS.get(ev, "•")
        contract = r["Ticker"]
        if r["TradeType"] != "STOCK":
            contract += f" {r['Expiry_Date']} {r.get('Right', '')[:1]}{r['Option_Strike_Price_(USD)']}"
        pl = float(r.get("Actual_Profit_(USD)") or 0)
        disp_rows.append({
            "Date Time": r["TradeDateTime"].strftime("%Y-%m-%d %H:%M") if pd.notna(r["TradeDateTime"]) else "",
            "": ev_icon,
            "Event": ev,
            "Type": r["TradeType"],
            "Contract": contract,
            "Qty": int(r["Quantity"]) if r["Quantity"] else 0,
            "Fill $": f"${r['FillPrice']:.2f}" if r["FillPrice"] else "—",
            "Total $": f"${r['FilledCashAmount']:,.0f}",
            "Comm $": f"${r['Commission']:.2f}" if r["Commission"] else "—",
            "GST $": f"${r['GST']:.2f}" if r["GST"] else "—",
            "P&L $": pl,  # numeric — colored + formatted via Styler
        })
    df_txn_disp = pd.DataFrame(disp_rows)
    if not df_txn_disp.empty:
        styled_txn = df_txn_disp.style.map(_color_pnl, subset=["P&L $"]).format({"P&L $": _fmt_pnl})
        st.dataframe(styled_txn, use_container_width=True, hide_index=True)
    else:
        st.dataframe(df_txn_disp, use_container_width=True, hide_index=True)

    # Summary strip
    st.divider()
    total_premium = float(filt[
        (filt["is_opening"] == True) &
        (filt["Action"] == "SELL") &
        (filt["TradeType"].isin(["CSP", "CC"]))
    ]["FilledCashAmount"].sum())
    total_realized = float(filt[filt["is_opening"] == False]["Actual_Profit_(USD)"].sum())
    total_comm = float(filt["Commission"].sum())
    total_gst = float(filt["GST"].sum())
    fills_count = len(filt)

    s = st.columns(5)
    s[0].metric(f"Premium ({days_span}d)", f"${total_premium:,.0f}",
                help="Sum of opening short option fills (CSPs + CCs) within the filtered range.")
    s[1].metric(f"Realized P&L ({days_span}d)", f"${total_realized:+,.0f}",
                help="Sum of broker-side P&L on closing fills within the filtered range.")
    s[2].metric(f"Commission ({days_span}d)", f"${total_comm:,.0f}")
    s[3].metric(f"GST ({days_span}d)", f"${total_gst:,.0f}")
    s[4].metric("Fills", fills_count)


# ────────────────────────────────────────────────────────────────
# 📅 LADDER — P&L Expiry Ladder (premium realization schedule)
# ────────────────────────────────────────────────────────────────
@st.fragment
def render_ladder(df_open, spot_prices: dict = None, settings: dict = None):
    """📅 P&L Expiry Ladder — calendar of premium realization & capital release.

    For each week (Friday ending) ahead:
      • Per-position rows: ticker, strike, qty, premium received, current Last,
                          captured %, ITM/OTM vs spot, $ if expires worthless
      • Weekly aggregate: total premium captured if all expire worthless,
                         CSP collateral released, # contracts expiring

    Filters: Type · Ticker · Pot · Weeks-ahead · Moneyness (ITM/OTM)
    Uses Alpaca option quotes (cached, batched) for current Last price + mid.
    Uses spot prices (Yahoo/Alpaca) to determine ITM/OTM moneyness.
    """
    settings = settings or {}
    st.markdown("### 📅 P&L Expiry Ladder")
    st.caption(
        "Forward-looking premium realization schedule. Each row is one open option "
        "position. **If-Expire $** = the credit you keep if the option expires worthless. "
        "**Captured %** uses Alpaca's last trade price as buy-back cost. "
        "Rows are color-marked for ITM/OTM risk vs current spot."
    )

    if df_open is None or df_open.empty:
        st.info("No open positions.")
        return

    # Accept either the parent's spot_prices dict or fetch fresh
    spot_prices = spot_prices or {}

    options = df_open[df_open["TradeType"].isin(["CSP", "CC", "LEAP"])].copy()
    if options.empty:
        st.info("No open option positions.")
        return

    options["Expiry_Date"] = pd.to_datetime(options["Expiry_Date"], errors="coerce")
    options = options[options["Expiry_Date"].notna()].copy()
    today = pd.Timestamp.now().normalize()
    options["DTE"] = (options["Expiry_Date"] - today).dt.days

    # Friday-end-of-week bucket for each position
    def _friday_for(d: pd.Timestamp) -> pd.Timestamp:
        if pd.isna(d):
            return d
        # Add days needed to reach next Friday (4 = Fri); if already Fri or later in week,
        # use that Friday
        wd = d.weekday()
        if wd <= 4:  # Mon–Fri
            return (d + pd.Timedelta(days=(4 - wd))).normalize()
        else:  # Sat/Sun → next Friday
            return (d + pd.Timedelta(days=(11 - wd))).normalize()

    options["WeekEnd"] = options["Expiry_Date"].apply(_friday_for)

    # ── Pre-fetch option quotes (Alpaca) for ALL options in one batch ──
    from tiger_api import tiger_data as _td_mod
    quote_key = []
    for _, r in options.iterrows():
        try:
            tkr = str(r["Ticker"]).upper()
            exp = r["Expiry_Date"].strftime("%Y-%m-%d")
            strike = float(r["Option_Strike_Price_(USD)"])
            ttype = r["TradeType"]
            pc = "C" if ttype in ("CC", "LEAP") else "P"
            quote_key.append((tkr, exp, strike, pc))
        except (TypeError, ValueError, KeyError):
            continue
    quotes = {}
    if quote_key:
        try:
            quotes = _td_mod.load_option_quotes(tuple(quote_key))
        except Exception as e:
            logger.debug("Ladder option quote fetch failed: %s", e)

    # ── Filters ────────────────────────────────────────────────
    ticker_pots = settings.get("ticker_pots", {}) or {}
    available_pots = sorted({str(p) for p in ticker_pots.values() if p}) or ["Core", "Active"]
    f1 = st.columns([1, 1, 1, 1, 1, 1])
    type_options = ["All"] + sorted(options["TradeType"].dropna().unique().tolist())
    type_filter = f1[0].selectbox("Type", type_options, key="ladder_type_filter")
    ticker_options = ["All"] + sorted(options["Ticker"].dropna().unique().tolist())
    ticker_filter = f1[1].selectbox("Ticker", ticker_options, key="ladder_ticker_filter")
    pot_filter = f1[2].multiselect(
        "Pot(s)", available_pots, key="ladder_pot_filter",
        placeholder="All pots",
        help="Restrict to tickers in selected pot(s). Empty = all.",
    )
    weeks_ahead = f1[3].number_input(
        "Show next N weeks", min_value=1, max_value=104, value=12, step=1,
        key="ladder_weeks_ahead",
        help="Limit to nearest N weeks. Set high (52+) to include LEAPs.",
    )
    moneyness_filter = f1[4].selectbox(
        "Moneyness", ["All", "OTM only", "ITM only"], key="ladder_money_filter",
        help="Filter by current ITM/OTM status vs spot.",
    )
    if f1[5].button("Reset", key="ladder_reset", use_container_width=True):
        for k in ("ladder_type_filter", "ladder_ticker_filter", "ladder_pot_filter",
                  "ladder_weeks_ahead", "ladder_money_filter"):
            st.session_state.pop(k, None)
        try:
            st.rerun(scope="fragment")
        except TypeError:
            st.rerun()

    # Resolve pot filter → set of tickers
    pot_tickers_set = None
    if pot_filter:
        pot_tickers_set = {t for t, p in ticker_pots.items() if p in pot_filter}

    # ── Build per-position row data ────────────────────────────
    cutoff = today + pd.Timedelta(weeks=int(weeks_ahead))
    rows = []
    for _, r in options.iterrows():
        try:
            tkr = str(r["Ticker"]).upper()
            ttype = r["TradeType"]
            strike = float(r["Option_Strike_Price_(USD)"])
            exp_ts = r["Expiry_Date"]
            exp_str = exp_ts.strftime("%Y-%m-%d")
            week_end = r["WeekEnd"].strftime("%Y-%m-%d") if pd.notna(r["WeekEnd"]) else "—"
            if exp_ts > cutoff:
                continue
            if type_filter != "All" and ttype != type_filter:
                continue
            if ticker_filter != "All" and tkr != ticker_filter:
                continue
            if pot_tickers_set is not None and tkr not in pot_tickers_set:
                continue
            qty = abs(int(r["Quantity"])) if r.get("Quantity") else 0
            avg = float(r.get("_avg_cost") or 0)
            pc = "C" if ttype in ("CC", "LEAP") else "P"
            q = quotes.get((tkr, exp_str, strike, pc), {})
            last = q.get("last")
            mid = q.get("mid")
            if last is None and r.get("_market_price"):
                last = float(r["_market_price"])
            if last is None:
                last = 0
            if mid is None:
                mid = last  # fallback
            spot = spot_prices.get(tkr) or float(r.get("_market_price") or 0)
            # Moneyness logic — for shorts, ITM = bad (assignment risk)
            # CSP: ITM = spot < strike. CC: ITM = spot > strike. LEAP: ITM = spot > strike.
            if ttype == "CSP":
                itm = spot > 0 and spot < strike
            else:  # CC or LEAP (call)
                itm = spot > 0 and spot > strike
            if moneyness_filter == "OTM only" and itm:
                continue
            if moneyness_filter == "ITM only" and not itm:
                continue

            # Captured% & if-expire-worthless $
            captured_pct = (avg - last) / avg * 100 if avg > 0 else None
            # Maximum profit at expiry (assumes OTM at expiry):
            #  Short premium (CSP/CC): max_profit = avg × 100 × qty (full premium kept)
            #  Long LEAP: max profit at expiry depends on intrinsic vs cost — show -avg (loss if OTM)
            if ttype in ("CSP", "CC"):
                if_expire_dollars = avg * 100 * qty  # premium kept if expires worthless
            else:  # LEAP — long, so "expires worthless" means TOTAL LOSS of premium paid
                intrinsic = max(0, spot - strike) if spot > 0 else 0
                if_expire_dollars = (intrinsic - avg) * 100 * qty

            csp_collateral = strike * 100 * qty if ttype == "CSP" else 0

            # Annualized yield % — capital efficiency metric
            yield_pct = None
            if ttype in ("CSP", "CC") and avg > 0 and strike > 0:
                _dte = int(r["DTE"]) if pd.notna(r["DTE"]) else 0
                if _dte > 0:
                    yield_pct = (avg / strike) * (365.0 / _dte) * 100.0

            rows.append({
                "Week": week_end,
                "Expiry": exp_str,
                "DTE": int(r["DTE"]) if pd.notna(r["DTE"]) else 0,
                "Ticker": tkr,
                "Type": ttype,
                "Strike": strike,
                "Spot": spot if spot > 0 else None,
                "Moneyness": "ITM" if itm else "OTM",
                "Qty": qty,
                "Premium": avg,
                "Last": last if last > 0 else None,
                "Mid": mid if mid > 0 else None,
                "Yield %/yr": yield_pct,
                "Captured %": captured_pct,
                "If-Expire $": if_expire_dollars,
                "CSP Coll $": csp_collateral if csp_collateral > 0 else None,
            })
        except (TypeError, ValueError, KeyError) as e:
            logger.debug("Ladder row skip: %s", e)
            continue

    if not rows:
        st.info("No positions match the current filters.")
        return

    ldf = pd.DataFrame(rows).sort_values(["Expiry", "Ticker"]).reset_index(drop=True)

    # ── Headline metrics ───────────────────────────────────────
    h1, h2, h3, h4, h5 = st.columns(5)
    n_pos = len(ldf)
    sum_premium = float(ldf[ldf["Type"].isin(["CSP", "CC"])].apply(
        lambda r: float(r["Premium"]) * 100 * float(r["Qty"]), axis=1
    ).sum()) if not ldf.empty else 0
    sum_buyback = float(ldf[ldf["Type"].isin(["CSP", "CC"])].apply(
        lambda r: float(r["Last"] or 0) * 100 * float(r["Qty"]), axis=1
    ).sum()) if not ldf.empty else 0
    if_expire_total = sum_premium - sum_buyback
    csp_coll_total = float(ldf[ldf["Type"] == "CSP"].apply(
        lambda r: float(r["Strike"]) * 100 * float(r["Qty"]), axis=1
    ).sum()) if not ldf.empty else 0
    h1.metric("Σ Open positions", n_pos)
    h2.metric("Σ Premium received",
              f"${sum_premium:,.0f}",
              help="Sum of (premium per share × 100 × qty) across all SHORT options shown.")
    h3.metric("Σ Buy-back cost (now)",
              f"${sum_buyback:,.0f}",
              help="What you'd pay to close all SHORT options today using Alpaca Last.")
    h4.metric("Σ If-expire P&L",
              f"${if_expire_total:+,.0f}",
              help="Premium received − buy-back cost. The profit you'd realize if all "
                   "shorts expire worthless from current state.")
    h5.metric("Σ CSP collateral",
              f"${csp_coll_total:,.0f}",
              help="Cash freed up if all CSP positions expire worthless.")

    # ── 📊 Premium Realization Chart (the hero visual) ─────────
    # Stacked bar chart: x=Friday week, y=premium $, color=ticker.
    # Two view modes: Aggregated (one bar per week) vs By Ticker (stacked).
    st.markdown("##### 📊 Premium Realization by Week")
    shorts_only = ldf[ldf["Type"].isin(["CSP", "CC"])].copy()
    if not shorts_only.empty:
        shorts_only["Premium $"] = shorts_only["Premium"].astype(float) * 100 * shorts_only["Qty"].astype(float)
        shorts_only["If-Expire"] = shorts_only["Premium $"] - (
            shorts_only["Last"].fillna(0).astype(float) * 100 * shorts_only["Qty"].astype(float)
        )

        chart_view = st.radio(
            "View",
            ["By Ticker (stacked)", "By Type (CSP vs CC)", "Aggregated"],
            horizontal=True,
            key="ladder_chart_view",
            label_visibility="collapsed",
        )
        chart_metric = st.radio(
            "Metric",
            ["Premium received (gross)", "If-Expire P&L (net)"],
            horizontal=True,
            key="ladder_chart_metric",
            label_visibility="collapsed",
            help="Gross = total premium received when sold. Net = locked-in profit if expires worthless from now (after buy-back cost).",
        )
        ycol = "Premium $" if chart_metric.startswith("Premium received") else "If-Expire"

        try:
            import plotly.express as px
            if chart_view == "Aggregated":
                agg = shorts_only.groupby("Week", as_index=False)[ycol].sum()
                fig = px.bar(
                    agg, x="Week", y=ycol,
                    labels={"Week": "Expiry Week (Friday)", ycol: f"{ycol} ($)"},
                    title=f"{ycol} by Week — Aggregated",
                    text=ycol,
                )
                fig.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
            elif chart_view == "By Type (CSP vs CC)":
                agg = shorts_only.groupby(["Week", "Type"], as_index=False)[ycol].sum()
                fig = px.bar(
                    agg, x="Week", y=ycol, color="Type",
                    labels={"Week": "Expiry Week (Friday)", ycol: f"{ycol} ($)"},
                    title=f"{ycol} by Week — Stacked by CSP/CC",
                    color_discrete_map={"CSP": "#2e7d32", "CC": "#1976d2"},
                    barmode="stack",
                )
            else:  # By Ticker
                agg = shorts_only.groupby(["Week", "Ticker"], as_index=False)[ycol].sum()
                fig = px.bar(
                    agg, x="Week", y=ycol, color="Ticker",
                    labels={"Week": "Expiry Week (Friday)", ycol: f"{ycol} ($)"},
                    title=f"{ycol} by Week — Stacked by Ticker",
                    barmode="stack",
                )
            fig.update_layout(
                height=420,
                xaxis_tickangle=-45,
                hovermode="x unified",
                yaxis_tickformat="$,.0f",
                margin=dict(l=10, r=10, t=50, b=10),
            )
            st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            st.warning("plotly not installed — `pip install plotly` to enable the chart.")

        # Quick summary stats below the chart
        n_weeks = shorts_only["Week"].nunique()
        avg_per_week = sum_premium / n_weeks if n_weeks > 0 else 0
        peak_row = shorts_only.groupby("Week")["Premium $"].sum().reset_index() if not shorts_only.empty else pd.DataFrame()
        if not peak_row.empty:
            peak_idx = peak_row["Premium $"].idxmax()
            peak_week = peak_row.loc[peak_idx, "Week"]
            peak_val = peak_row.loc[peak_idx, "Premium $"]
        else:
            peak_week, peak_val = "—", 0
        cs1, cs2, cs3 = st.columns(3)
        cs1.metric("Σ Premium (filter range)", f"${sum_premium:,.0f}")
        cs2.metric("Avg per week", f"${avg_per_week:,.0f}",
                   help=f"{n_weeks} unique expiry week(s) in range.")
        cs3.metric("Peak week", f"${peak_val:,.0f}",
                   delta=peak_week, delta_color="off")
    else:
        st.caption("_(no short options in filter range to chart)_")

    # ── Weekly summary aggregates (P&L by Friday) ──────────────
    st.markdown("##### 📊 Weekly Premium Realization Schedule")
    weekly = []
    for week_end, grp in ldf.groupby("Week"):
        shorts = grp[grp["Type"].isin(["CSP", "CC"])]
        prem_sum = float((shorts["Premium"] * 100 * shorts["Qty"]).sum()) if not shorts.empty else 0
        last_sum = float((shorts["Last"].fillna(0) * 100 * shorts["Qty"]).sum()) if not shorts.empty else 0
        if_exp = prem_sum - last_sum
        csp_in_wk = grp[grp["Type"] == "CSP"]
        csp_coll = float((csp_in_wk["Strike"] * 100 * csp_in_wk["Qty"]).sum()) if not csp_in_wk.empty else 0
        cc_in_wk = grp[grp["Type"] == "CC"]
        weekly.append({
            "Week Ending": week_end,
            "Positions": len(grp),
            "CSPs": int((grp["Type"] == "CSP").sum()),
            "CCs": int((grp["Type"] == "CC").sum()),
            "LEAPs": int((grp["Type"] == "LEAP").sum()),
            "Premium $": prem_sum,
            "Buyback $": last_sum,
            "If-Expire $": if_exp,
            "CSP Coll Released": csp_coll,
        })
    wdf = pd.DataFrame(weekly).sort_values("Week Ending").reset_index(drop=True)
    st.dataframe(
        wdf, use_container_width=True, hide_index=True,
        column_config={
            "Premium $":          st.column_config.NumberColumn(format="$%,.0f"),
            "Buyback $":          st.column_config.NumberColumn(format="$%,.0f"),
            "If-Expire $":        st.column_config.NumberColumn(format="$%+,.0f"),
            "CSP Coll Released":  st.column_config.NumberColumn(format="$%,.0f"),
        },
    )
    st.caption(
        "**Premium $** = received on opening · **Buyback $** = cost to close TODAY · "
        "**If-Expire $** = profit realized if all that week expires worthless · "
        "**CSP Coll Released** = cash freed up that week."
    )

    # ── Per-position detail dataframe (sortable, color-coded) ──
    st.markdown("##### 📋 Per-Position Detail")
    # Style: red text for ITM rows (assignment risk for shorts; loss territory for LEAPs)
    def _highlight_itm(row):
        if row.get("Moneyness") == "ITM":
            return ["color: #d32f2f; font-weight: 600;"] * len(row)
        return [""] * len(row)

    styler = ldf.style.apply(_highlight_itm, axis=1)
    st.dataframe(
        styler, use_container_width=True, hide_index=True,
        column_config={
            "Strike":       st.column_config.NumberColumn(format="$%.2f"),
            "Spot":         st.column_config.NumberColumn(format="$%.2f"),
            "Premium":      st.column_config.NumberColumn(format="$%.2f"),
            "Last":         st.column_config.NumberColumn(format="$%.2f"),
            "Mid":          st.column_config.NumberColumn(format="$%.2f"),
            "Yield %/yr":   st.column_config.NumberColumn(format="%.0f%%"),
            "Captured %":   st.column_config.NumberColumn(format="%.0f%%"),
            "If-Expire $":  st.column_config.NumberColumn(format="$%+,.0f"),
            "CSP Coll $":   st.column_config.NumberColumn(format="$%,.0f"),
            "DTE":          st.column_config.NumberColumn(format="%d"),
            "Qty":          st.column_config.NumberColumn(format="%d"),
        },
    )
    st.caption(
        "Rows in **red** = ITM (assignment risk for shorts; loss territory for LEAPs). "
        "Click any column header to sort. **Captured %** = how much of the original premium "
        "you've already locked in via theta decay. **If-Expire $** = premium kept if it "
        "expires worthless from now."
    )


# ────────────────────────────────────────────────────────────────
# 📊 P&L — placeholder (3D slicer comes next)
# ────────────────────────────────────────────────────────────────
# Asset class buckets — used to split realized P&L into Stock vs Options
_STOCK_TYPES = {"STOCK"}
_OPTION_TYPES = {"CSP", "CC", "LEAP"}


@st.fragment
def render_pl(df_orders, df_open, settings: dict = None):
    """📊 P&L Analytics — Realized (period) + Unrealized (snapshot), split by
    Stock vs Options, with cumulative curve chart and per-ticker breakdown.
    Supports filters by Period · Month · Ticker(s) · Pot(s).
    """
    settings = settings or {}
    st.markdown("### 📊 P&L Analytics")
    st.caption(
        "**Realized** = closed trades within the selected period (Tiger's "
        "`Actual_Profit_(USD)` on closing fills). **Unrealized** = current "
        "mark-to-market on OPEN positions (snapshot — independent of period). "
        "Stock vs Options are separated so you can see which book is driving returns."
    )

    if df_orders.empty:
        st.info("No closed trades in lookback window.")
        return

    # ── Period selector ────────────────────────────────────────
    today = date.today()
    p1, p2, p3 = st.columns([3, 1, 1])
    with p1:
        period = st.radio(
            "Period",
            ["YTD", "MTD", "WTD", "Last 30d", "Last 90d", "Last 12M", "Lifetime", "Month", "Custom"],
            horizontal=True,
            key="pl_period",
        )
    if period == "YTD":
        start = date(today.year, 1, 1); end = today
    elif period == "MTD":
        start = today.replace(day=1); end = today
    elif period == "WTD":
        start = today - timedelta(days=today.weekday()); end = today
    elif period == "Last 30d":
        start = today - timedelta(days=30); end = today
    elif period == "Last 90d":
        start = today - timedelta(days=90); end = today
    elif period == "Last 12M":
        start = today - timedelta(days=365); end = today
    elif period == "Lifetime":
        start = date(2024, 1, 1); end = today
    elif period == "Month":
        # Month picker — populated from months actually present in df_orders
        df_dates_tmp = pd.to_datetime(df_orders.get("TradeDate"), errors="coerce")
        available_months = sorted(
            df_dates_tmp.dt.strftime("%Y-%m").dropna().unique().tolist(),
            reverse=True,  # most recent first
        )
        if not available_months:
            available_months = [today.strftime("%Y-%m")]
        with p2:
            month_pick = st.selectbox(
                "Month (YYYY-MM)", available_months, key="pl_month_pick",
                help="Show only trades within this calendar month.",
            )
        try:
            yr, mo = map(int, month_pick.split("-"))
            start = date(yr, mo, 1)
            # Last day of month
            if mo == 12:
                end = date(yr, 12, 31)
            else:
                end = date(yr, mo + 1, 1) - timedelta(days=1)
            if end > today:
                end = today
        except (ValueError, TypeError):
            start = today.replace(day=1); end = today
    else:  # Custom
        with p2:
            start = st.date_input("Start", value=today - timedelta(days=30), key="pl_start_custom")
        with p3:
            end = st.date_input("End", value=today, key="pl_end_custom")

    days_in_period = max(1, (end - start).days)

    # ── Ticker + Pot filters (multiselect, apply on top of period) ──
    available_tickers = sorted(df_orders.get("Ticker", pd.Series(dtype=str)).dropna().unique().tolist())
    ticker_pots = settings.get("ticker_pots", {}) or {}
    available_pots = sorted({str(p) for p in ticker_pots.values() if p}) or ["Core", "Active"]

    f1, f2, f3 = st.columns([2, 2, 1])
    with f1:
        ticker_filter = st.multiselect(
            "Filter by Ticker(s)",
            available_tickers,
            key="pl_ticker_filter",
            placeholder="All tickers (leave empty for all)",
            help="Restrict P&L to specific tickers. Multi-select supported. "
                 "Leave empty to include every ticker.",
        )
    with f2:
        pot_filter = st.multiselect(
            "Filter by Pot(s)",
            available_pots,
            key="pl_pot_filter",
            placeholder="All pots (leave empty for all)",
            help="Restrict P&L to tickers assigned to specific pot(s) "
                 "(Core / Active). Set per-ticker in Config tab. "
                 "Composes with Ticker filter (intersection).",
        )
    with f3:
        st.write("")
        if st.button("Reset filters", key="pl_reset", use_container_width=True):
            for k in ("pl_period", "pl_month_pick", "pl_ticker_filter",
                      "pl_pot_filter", "pl_start_custom", "pl_end_custom"):
                st.session_state.pop(k, None)
            try:
                st.rerun(scope="fragment")
            except TypeError:
                st.rerun()

    # Resolve pot filter → set of tickers in those pots
    pot_tickers = None  # None = no pot filter applied
    if pot_filter:
        pot_tickers = {t for t, p in ticker_pots.items() if p in pot_filter}

    # ── Apply period + ticker + pot filters ───────────────────
    df = df_orders.copy()
    df["TradeDate"] = pd.to_datetime(df["TradeDate"], errors="coerce")
    df = df[(df["TradeDate"] >= pd.Timestamp(start)) & (df["TradeDate"] <= pd.Timestamp(end))]
    if ticker_filter:
        df = df[df["Ticker"].isin(ticker_filter)]
    if pot_tickers is not None:
        df = df[df["Ticker"].isin(pot_tickers)]
    closed = df[df["is_opening"] == False].copy()

    # ── Compute realized split (stock vs options) ──────────────
    realized_stock = 0.0
    realized_options = 0.0
    realized_options_breakdown = {"CSP": 0.0, "CC": 0.0, "LEAP": 0.0}
    n_trades_stock = 0
    n_trades_options = 0
    if not closed.empty:
        stk = closed[closed["TradeType"].isin(list(_STOCK_TYPES))]
        opt = closed[closed["TradeType"].isin(list(_OPTION_TYPES))]
        realized_stock = float(stk["Actual_Profit_(USD)"].sum())
        realized_options = float(opt["Actual_Profit_(USD)"].sum())
        n_trades_stock = len(stk)
        n_trades_options = len(opt)
        for ttype in ("CSP", "CC", "LEAP"):
            sub = closed[closed["TradeType"] == ttype]
            realized_options_breakdown[ttype] = float(sub["Actual_Profit_(USD)"].sum())

    # ── Compute unrealized split (snapshot from df_open) ───────
    unrealized_stock = 0.0
    unrealized_options = 0.0
    unrealized_options_breakdown = {"CSP": 0.0, "CC": 0.0, "LEAP": 0.0}
    n_open_stock = 0
    n_open_options = 0
    if df_open is not None and not df_open.empty:
        d = df_open.copy()
        d["_unrealized_pnl"] = pd.to_numeric(d.get("_unrealized_pnl", 0), errors="coerce").fillna(0)
        # Apply ticker + pot filters to unrealized snapshot too (period filter
        # doesn't apply to unrealized — those are current open positions
        # regardless of when they were opened).
        if ticker_filter:
            d = d[d["Ticker"].isin(ticker_filter)]
        if pot_tickers is not None:
            d = d[d["Ticker"].isin(pot_tickers)]
        stk_o = d[d["TradeType"].isin(list(_STOCK_TYPES))]
        opt_o = d[d["TradeType"].isin(list(_OPTION_TYPES))]
        unrealized_stock = float(stk_o["_unrealized_pnl"].sum())
        unrealized_options = float(opt_o["_unrealized_pnl"].sum())
        n_open_stock = len(stk_o)
        n_open_options = len(opt_o)
        for ttype in ("CSP", "CC", "LEAP"):
            sub = d[d["TradeType"] == ttype]
            unrealized_options_breakdown[ttype] = float(sub["_unrealized_pnl"].sum())

    # ── Fees (period) ──────────────────────────────────────────
    commission = float(df["Commission"].sum()) if not df.empty else 0.0
    gst = float(df["GST"].sum()) if not df.empty else 0.0
    fees_total = commission + gst

    realized_total = realized_stock + realized_options
    unrealized_total = unrealized_stock + unrealized_options
    net_period = realized_total - fees_total

    # ── 5-metric headline strip ────────────────────────────────
    h = st.columns(5)
    h[0].metric(f"Realized ({period if period != 'Custom' else 'custom'})",
                f"${realized_total:+,.0f}",
                delta=f"{n_trades_stock + n_trades_options} closed trades",
                delta_color="off",
                help="Tiger's broker-side P&L on closing fills within the period.")
    h[1].metric("Unrealized (snapshot)",
                f"${unrealized_total:+,.0f}",
                delta=f"{n_open_stock + n_open_options} open positions",
                delta_color="off",
                help="Mark-to-market on currently-open positions. Not period-dependent.")
    h[2].metric("Commission + GST",
                f"−${fees_total:,.0f}",
                delta=f"−${commission:,.0f} comm · −${gst:,.0f} GST",
                delta_color="off")
    h[3].metric("Net realized (after fees)",
                f"${net_period:+,.0f}",
                help="Realized P&L minus commission and GST (within period).")
    # Period yield: net realized / period in days, annualized
    if days_in_period > 0:
        annualized = net_period * (365 / days_in_period)
        h[4].metric("Annualized return",
                    f"${annualized:+,.0f}/yr",
                    delta=f"{period} → 365d",
                    delta_color="off",
                    help=f"Net realized × (365 / {days_in_period} days). "
                         f"Pro-rated annual rate based on this period.")
    else:
        h[4].metric("Annualized return", "—")

    st.divider()

    # ── 🥧 Stock vs Options split (Realized + Unrealized) ──────
    st.markdown("##### 📊 Stock vs Options Breakdown")
    sb1, sb2 = st.columns(2)
    with sb1:
        st.markdown("**Realized P&L (period)**")
        rdf = pd.DataFrame([
            {"Asset": "📊 Stock", "Closed Trades": n_trades_stock, "P&L $": realized_stock},
            {"Asset": "📉 Options — CSP", "Closed Trades": int((closed["TradeType"] == "CSP").sum()) if not closed.empty else 0,
             "P&L $": realized_options_breakdown["CSP"]},
            {"Asset": "📈 Options — CC", "Closed Trades": int((closed["TradeType"] == "CC").sum()) if not closed.empty else 0,
             "P&L $": realized_options_breakdown["CC"]},
            {"Asset": "🔒 Options — LEAP", "Closed Trades": int((closed["TradeType"] == "LEAP").sum()) if not closed.empty else 0,
             "P&L $": realized_options_breakdown["LEAP"]},
            {"Asset": "Σ Total Realized", "Closed Trades": n_trades_stock + n_trades_options,
             "P&L $": realized_total},
        ])
        st.dataframe(
            rdf, use_container_width=True, hide_index=True,
            column_config={"P&L $": st.column_config.NumberColumn(format="$%+,.0f")},
        )
    with sb2:
        st.markdown("**Unrealized P&L (snapshot — current open positions)**")
        udf = pd.DataFrame([
            {"Asset": "📊 Stock", "Open Positions": n_open_stock, "P&L $": unrealized_stock},
            {"Asset": "📉 Options — CSP",
             "Open Positions": int((df_open["TradeType"] == "CSP").sum()) if df_open is not None and not df_open.empty else 0,
             "P&L $": unrealized_options_breakdown["CSP"]},
            {"Asset": "📈 Options — CC",
             "Open Positions": int((df_open["TradeType"] == "CC").sum()) if df_open is not None and not df_open.empty else 0,
             "P&L $": unrealized_options_breakdown["CC"]},
            {"Asset": "🔒 Options — LEAP",
             "Open Positions": int((df_open["TradeType"] == "LEAP").sum()) if df_open is not None and not df_open.empty else 0,
             "P&L $": unrealized_options_breakdown["LEAP"]},
            {"Asset": "Σ Total Unrealized", "Open Positions": n_open_stock + n_open_options,
             "P&L $": unrealized_total},
        ])
        st.dataframe(
            udf, use_container_width=True, hide_index=True,
            column_config={"P&L $": st.column_config.NumberColumn(format="$%+,.0f")},
        )

    if closed.empty:
        st.info("No closed trades in this period — only unrealized data shown above.")
        return

    st.divider()

    # ── 📈 Cumulative Realized P&L chart ───────────────────────
    st.markdown("##### 📈 Cumulative Realized P&L (period)")
    try:
        import plotly.graph_objects as go
        # Aggregate by date — split stock vs options for stacked area
        daily = closed.copy()
        daily["TradeDate"] = pd.to_datetime(daily["TradeDate"], errors="coerce").dt.date
        daily["Asset"] = daily["TradeType"].apply(
            lambda t: "Stock" if t in _STOCK_TYPES else ("Options" if t in _OPTION_TYPES else "Other")
        )
        # Sum P&L per (date, Asset)
        agg = daily.groupby(["TradeDate", "Asset"], as_index=False)["Actual_Profit_(USD)"].sum()
        # Pivot for cumsum per asset class
        pivot = agg.pivot(index="TradeDate", columns="Asset", values="Actual_Profit_(USD)").fillna(0)
        pivot = pivot.sort_index().cumsum()
        # Reset index for plotting
        pivot_long = pivot.reset_index().melt(id_vars="TradeDate", var_name="Asset", value_name="Cumulative P&L")

        fig_cum = go.Figure()
        for asset_name, color in [("Stock", "#1976d2"), ("Options", "#2e7d32"), ("Other", "#9c27b0")]:
            sub = pivot_long[pivot_long["Asset"] == asset_name]
            if not sub.empty:
                fig_cum.add_trace(go.Scatter(
                    x=sub["TradeDate"], y=sub["Cumulative P&L"],
                    mode="lines+markers", name=asset_name, line=dict(width=2, color=color),
                    hovertemplate="<b>%{x}</b><br>" + asset_name + ": $%{y:,.0f}<extra></extra>",
                ))
        # Total line (sum across assets) — bold
        if not pivot_long.empty:
            total_pivot = pivot.sum(axis=1).reset_index()
            total_pivot.columns = ["TradeDate", "Total"]
            fig_cum.add_trace(go.Scatter(
                x=total_pivot["TradeDate"], y=total_pivot["Total"],
                mode="lines", name="Total", line=dict(width=3, color="#000", dash="solid"),
                hovertemplate="<b>%{x}</b><br>Total: $%{y:,.0f}<extra></extra>",
            ))
        fig_cum.update_layout(
            height=380,
            xaxis_title="Trade Date", yaxis_title="Cumulative Realized P&L ($)",
            yaxis_tickformat="$,.0f",
            hovermode="x unified",
            margin=dict(l=10, r=10, t=30, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_cum, use_container_width=True)
    except ImportError:
        st.warning("plotly not installed — install for the cumulative P&L chart.")

    st.divider()

    # ── 📊 Win-rate & ticker tables ────────────────────────────
    tab_type, tab_ticker, tab_pivot = st.tabs([
        "By Type (win-rate)", "By Ticker", "Pivot: Type × Ticker"
    ])

    with tab_type:
        by_type = closed.groupby("TradeType").agg(
            Realized=("Actual_Profit_(USD)", "sum"),
            Trades=("TradeID", "count"),
            Wins=("Actual_Profit_(USD)", lambda s: (s > 0).sum()),
        ).reset_index()
        by_type["Win %"] = (by_type["Wins"] / by_type["Trades"] * 100).round(0).astype(int)
        by_type["Avg per trade"] = (by_type["Realized"] / by_type["Trades"]).round(2)
        by_type = by_type.sort_values("Realized", ascending=False)
        st.dataframe(
            by_type[["TradeType", "Realized", "Trades", "Wins", "Win %", "Avg per trade"]],
            use_container_width=True, hide_index=True,
            column_config={
                "Realized":      st.column_config.NumberColumn(format="$%+,.0f"),
                "Avg per trade": st.column_config.NumberColumn(format="$%+,.2f"),
                "Win %":         st.column_config.NumberColumn(format="%d%%"),
                "Trades":        st.column_config.NumberColumn(format="%d"),
                "Wins":          st.column_config.NumberColumn(format="%d"),
            },
        )

    with tab_ticker:
        # Compute realized AND unrealized per ticker (both already ticker-filtered above)
        by_tk_realized = closed.groupby("Ticker").agg(
            Realized=("Actual_Profit_(USD)", "sum"),
            Trades=("TradeID", "count"),
        ).reset_index()
        # df_open ticker+pot-filtered version for unrealized — d was built earlier with filter applied
        df_open_filtered = df_open.copy() if df_open is not None else None
        if df_open_filtered is not None and ticker_filter:
            df_open_filtered = df_open_filtered[df_open_filtered["Ticker"].isin(ticker_filter)]
        if df_open_filtered is not None and pot_tickers is not None:
            df_open_filtered = df_open_filtered[df_open_filtered["Ticker"].isin(pot_tickers)]
        if df_open_filtered is not None and not df_open_filtered.empty:
            by_tk_unrl = df_open_filtered.groupby("Ticker").agg(
                Unrealized=("_unrealized_pnl", "sum"),
                OpenPositions=("Ticker", "count"),
            ).reset_index()
            by_tk = by_tk_realized.merge(by_tk_unrl, on="Ticker", how="outer").fillna(0)
        else:
            by_tk = by_tk_realized.copy()
            by_tk["Unrealized"] = 0
            by_tk["OpenPositions"] = 0
        by_tk["Total P&L"] = by_tk["Realized"] + by_tk["Unrealized"]
        by_tk = by_tk.sort_values("Total P&L", ascending=False)
        st.dataframe(
            by_tk, use_container_width=True, hide_index=True,
            column_config={
                "Realized":      st.column_config.NumberColumn(format="$%+,.0f"),
                "Unrealized":    st.column_config.NumberColumn(format="$%+,.0f"),
                "Total P&L":     st.column_config.NumberColumn(format="$%+,.0f"),
                "Trades":        st.column_config.NumberColumn(format="%d"),
                "OpenPositions": st.column_config.NumberColumn(format="%d"),
            },
        )

    with tab_pivot:
        # Pivot: TradeType (rows) × Ticker (cols), values = Realized P&L
        if not closed.empty:
            pivot_pnl = closed.pivot_table(
                index="TradeType", columns="Ticker",
                values="Actual_Profit_(USD)", aggfunc="sum", fill_value=0,
            )
            # Add row+column totals
            pivot_pnl["Σ Total"] = pivot_pnl.sum(axis=1)
            pivot_pnl.loc["Σ Total"] = pivot_pnl.sum()
            # Format numbers as strings for display (since pivot has mixed types)
            disp = pivot_pnl.copy().reset_index()
            for col in disp.columns:
                if col == "TradeType":
                    continue
                disp[col] = disp[col].apply(lambda v: f"${v:+,.0f}" if v != 0 else "—")
            st.dataframe(disp, use_container_width=True, hide_index=True)
        else:
            st.info("No closed trades to pivot.")


# ────────────────────────────────────────────────────────────────
# ⚠️ RISK — placeholder
# ────────────────────────────────────────────────────────────────
@st.fragment
def render_risk(df_open, summary, settings, spot_prices: dict = None):
    """⚠️ Risk & Rolls — expiring soon, concentration, AND Roll/Close candidates."""
    spot_prices = spot_prices or {}
    portfolio_deposit = float(settings.get("portfolio_deposit_usd", 0))

    st.markdown("### ⚠️ Risk & Rolls")
    st.caption(
        "Defensive view of the book: what's expiring soon, where you're concentrated, "
        "and which short positions are ripe to close/roll for profit redeployment."
    )

    # ── 🎯 Roll / Close Candidates (moved from Positions tab) ────
    st.markdown("#### 🎯 Roll / Close Candidates")
    st.caption(
        "Short premium positions where you've already captured ≥ threshold% of the "
        "max profit. Closing here frees capital and CSP collateral for redeployment "
        "into fresh premium. Standard wheel discipline: close at 50%+ profit."
    )

    # Pre-fetch Alpaca quotes for ALL short options (one batched call)
    from tiger_api import tiger_data as _td_mod
    shorts_df = df_open[df_open["TradeType"].isin(["CSP", "CC"])].copy() if df_open is not None and not df_open.empty else pd.DataFrame()
    quote_key = []
    for _, r in shorts_df.iterrows():
        try:
            tkr = str(r["Ticker"]).upper()
            exp = str(r["Expiry_Date"])
            strike = float(r["Option_Strike_Price_(USD)"])
            ttype = r["TradeType"]
            pc = "C" if ttype == "CC" else "P"
            quote_key.append((tkr, exp, strike, pc))
        except (TypeError, ValueError, KeyError):
            continue
    quotes = {}
    if quote_key:
        try:
            quotes = _td_mod.load_option_quotes(tuple(quote_key))
        except Exception as e:
            logger.debug("Risk option quote fetch failed: %s", e)

    # DTE on shorts
    if not shorts_df.empty:
        shorts_df["Expiry_dt"] = pd.to_datetime(shorts_df["Expiry_Date"], errors="coerce")
        today = pd.Timestamp.now().normalize()
        shorts_df["DTE"] = (shorts_df["Expiry_dt"] - today).dt.days

    # Filter row above the candidates table
    ticker_pots = settings.get("ticker_pots", {}) or {}
    available_pots = sorted({str(p) for p in ticker_pots.values() if p}) or ["Core", "Active"]
    rc1, rc2, rc3, rc4, rc5 = st.columns([2, 1, 1, 1, 1])
    with rc1:
        threshold_pct = st.slider(
            "Profit captured threshold (%)",
            min_value=25, max_value=95, value=50, step=5,
            key="rr_profit_threshold",
            help="Default 50% — classic 'manage early' rule for short premium. "
                 "Lower = more aggressive. Higher = hold for max profit (gamma risk).",
        )
    with rc2:
        cand_type = st.selectbox(
            "Type", ["All", "CSP", "CC"], key="rr_type_filter",
            help="Filter to just CSPs or CCs.",
        )
    with rc3:
        cand_pot_filter = st.multiselect(
            "Pot(s)", available_pots, key="rr_pot_filter",
            placeholder="All pots",
            help="Restrict to tickers in selected pot(s). Empty = all.",
        )
    with rc4:
        cand_dte_max = st.number_input(
            "Max DTE", min_value=0, max_value=400, value=400, step=5,
            key="rr_dte_max",
            help="Hide candidates with DTE above this. Useful to focus on near-expiry.",
        )
    with rc5:
        sort_by = st.selectbox(
            "Sort by",
            ["Captured % ↓", "Captured $ ↓", "DTE ↑", "DTE ↓",
             "Ticker ↑", "Premium ↓"],
            key="rr_sort_by",
            help="Default: highest captured % first.",
        )

    # Resolve pot filter → set of tickers
    cand_pot_tickers = None
    if cand_pot_filter:
        cand_pot_tickers = {t for t, p in ticker_pots.items() if p in cand_pot_filter}

    candidates = []
    for _, r in shorts_df.iterrows():
        try:
            avg = float(r.get("_avg_cost") or 0)
            if avg <= 0:
                continue
            tkr = str(r["Ticker"]).upper()
            exp = str(r["Expiry_Date"])
            strike = float(r["Option_Strike_Price_(USD)"])
            ttype = r["TradeType"]
            pc = "C" if ttype == "CC" else "P"
            q = quotes.get((tkr, exp, strike, pc), {})
            last = q.get("last")
            if last is None and r.get("_market_price"):
                last = float(r["_market_price"])
            if last is None or last <= 0:
                continue
            captured_pct = (avg - last) / avg * 100
            if captured_pct < threshold_pct:
                continue
            qty = abs(int(r["Quantity"])) if r.get("Quantity") else 0
            captured_dollars = (avg - last) * 100 * qty
            remaining_dollars = last * 100 * qty
            dte = int(r["DTE"]) if pd.notna(r["DTE"]) else 0
            if cand_type != "All" and ttype != cand_type:
                continue
            if cand_pot_tickers is not None and tkr not in cand_pot_tickers:
                continue
            if dte > cand_dte_max:
                continue
            candidates.append({
                "Ticker": tkr, "Type": ttype, "Strike": strike,
                "Expiry": exp, "DTE": dte, "Qty": qty,
                "Premium": avg, "Last": last,
                "Captured $": captured_dollars,
                "To Close $": remaining_dollars,
                "Captured %": captured_pct,
            })
        except (TypeError, ValueError, KeyError):
            continue

    if not candidates:
        st.info(
            f"No short positions match: ≥{threshold_pct}% captured · type={cand_type} · DTE≤{cand_dte_max}. "
            "Loosen filters to see partial candidates."
        )
    else:
        cdf = pd.DataFrame(candidates)
        sort_map = {
            "Captured % ↓": ("Captured %", False),
            "Captured $ ↓": ("Captured $", False),
            "DTE ↑": ("DTE", True),
            "DTE ↓": ("DTE", False),
            "Ticker ↑": ("Ticker", True),
            "Premium ↓": ("Premium", False),
        }
        col, asc = sort_map.get(sort_by, ("Captured %", False))
        cdf = cdf.sort_values(col, ascending=asc).reset_index(drop=True)
        st.dataframe(
            cdf, use_container_width=True, hide_index=True,
            column_config={
                "Strike":      st.column_config.NumberColumn(format="$%.2f"),
                "Premium":     st.column_config.NumberColumn(format="$%.2f"),
                "Last":        st.column_config.NumberColumn(format="$%.2f"),
                "Captured $":  st.column_config.NumberColumn(format="$%,.0f"),
                "To Close $":  st.column_config.NumberColumn(format="$%,.0f"),
                "Captured %":  st.column_config.NumberColumn(format="%.0f%%"),
                "DTE":         st.column_config.NumberColumn(format="%d"),
                "Qty":         st.column_config.NumberColumn(format="%d"),
            },
        )
        total_captured = float(cdf["Captured $"].sum())
        total_to_close = float(cdf["To Close $"].sum())
        st.caption(
            f"💡 **{len(cdf)} candidate{'s' if len(cdf) != 1 else ''}** match. "
            f"Closing all = **+${total_captured:,.0f} locked profit**, "
            f"costs **${total_to_close:,.0f}** to buy back, frees CSP collateral. "
            f"Click any column header to re-sort."
        )

    st.divider()

    # ── Expiring Within 14 Days (per-ticker grouping) ───────────
    _panel_expiring_soon(df_open, spot_prices, days=14)

    st.divider()

    # ── Concentration by Ticker ─────────────────────────────────
    st.markdown("#### 📊 Concentration by Ticker")
    if df_open is None or df_open.empty:
        st.info("No open positions.")
    else:
        df = df_open.copy()
        df["_market_value"] = pd.to_numeric(df.get("_market_value", 0), errors="coerce").fillna(0).abs()
        by_tk = df.groupby("Ticker")["_market_value"].sum().sort_values(ascending=False)
        rows = []
        for t, mv in by_tk.items():
            pct = (mv / portfolio_deposit * 100) if portfolio_deposit > 0 else 0
            flag = "🔴" if pct > 30 else ("🟡" if pct > 20 else "🟢")
            rows.append({
                "Ticker": t,
                "Market Value": f"${mv:,.0f}",
                "% of Portfolio": f"{pct:.1f}%",
                "Status": flag,
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    st.caption(
        "⏳ Coming next: stress test (spot −10/−20/−30%), wheel-cycle status, theta accrual."
    )


def _panel_expiring_soon(df_open: pd.DataFrame, spot_prices: dict, days: int = 14):
    """⏰ Expiring Within {days} Days — per-ticker grouping (CSP + CC).

    Mirrors legacy ARGUS Daily Helper's "Expiring Soon" view. Tickers with
    nothing expiring show a green ✅ inline, tickers with positions show a
    table sorted by DTE (closest first) then distance-to-spot (most ITM first).
    """
    st.markdown(f"#### ⏰ Expiring Within {days} Days — All Tickers")

    if df_open.empty:
        st.info("No open positions.")
        return

    options = df_open[df_open["TradeType"].isin(["CC", "CSP"])].copy()
    if options.empty:
        st.success("✅ No CC or CSP positions to track.")
        return

    options["q"] = pd.to_numeric(options["Quantity"], errors="coerce").fillna(0).abs().astype(int)
    options["k"] = pd.to_numeric(options["Option_Strike_Price_(USD)"], errors="coerce").fillna(0)
    options["exp"] = pd.to_datetime(options["Expiry_Date"], errors="coerce")
    options = options[options["exp"].notna()].copy()
    today = pd.Timestamp(datetime.now().date())
    options["DTE"] = (options["exp"] - today).dt.days

    soon = options[options["DTE"] <= days].copy()
    all_tickers = sorted(options["Ticker"].dropna().unique().tolist())

    for ticker in all_tickers:
        ticker_soon = soon[soon["Ticker"] == ticker].copy()
        spot = float(spot_prices.get(ticker, 0) or 0)

        if ticker_soon.empty:
            c1, c2 = st.columns([3, 2])
            with c1:
                st.markdown(f"**{ticker}**" + (f" · spot ${spot:.2f}" if spot else ""))
            with c2:
                st.success(f"✅ Nothing expiring within {days}d")
            continue

        # Compute distance to spot (CC: spot-strike OTM; CSP: strike-spot OTM)
        if spot:
            ticker_soon["Distance"] = ticker_soon.apply(
                lambda r: (spot - r["k"]) if r["TradeType"] == "CC" else (r["k"] - spot),
                axis=1,
            )
        else:
            ticker_soon["Distance"] = 0
        ticker_soon = ticker_soon.sort_values(["DTE", "Distance"], ascending=[True, True])

        st.markdown(f"**{ticker}**" + (f" · spot ${spot:.2f}" if spot else ""))
        rows = []
        for _, r in ticker_soon.iterrows():
            distance_pct = ((r["Distance"] / spot) * 100) if spot > 0 else 0
            # ITM warning logic
            is_itm = (r["TradeType"] == "CC" and r["k"] < spot and spot > 0) or \
                     (r["TradeType"] == "CSP" and r["k"] > spot and spot > 0)
            risk_flag = "🔴 ITM" if is_itm else (
                "🟡" if (spot > 0 and abs(distance_pct) < 3) else "🟢"
            )
            rows.append({
                "DTE": int(r["DTE"]),
                "Type": r["TradeType"],
                "Strike": f"${r['k']:.2f}",
                "Qty": int(r["q"]),
                "Expiry": r["exp"].strftime("%Y-%m-%d"),
                "Distance to Spot": f"${r['Distance']:+.2f} ({distance_pct:+.1f}%)" if spot else "—",
                "Risk": risk_flag,
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ────────────────────────────────────────────────────────────────
# ⚙️ CONFIG — Settings tab editor
# ────────────────────────────────────────────────────────────────
def render_config(settings: dict, save_settings_fn):
    st.markdown("### ⚙️ Configuration")
    st.caption("Settings persist in gSheet's Settings tab — survives Streamlit Cloud restarts.")

    with st.form("config_form"):
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**💰 Pot Deposits (SGD primary — USD auto-derived)**")
            fx = st.number_input("SGD/USD FX rate", min_value=0.5, max_value=3.0,
                                 value=float(settings.get("sgd_usd_fx_rate", 1.35)),
                                 step=0.01, format="%.4f",
                                 help="1 USD = X SGD. Used to convert SGD inputs to USD for the rest of the app.")
            margin_rate_pct = st.number_input(
                "Tiger USD margin rate (% APR)",
                min_value=0.0, max_value=15.0,
                value=float(settings.get("tiger_margin_rate_pct", 7.0)),
                step=0.1, format="%.2f",
                help="Tiger TBSG's USD margin loan rate. Default 7.0% based on actual "
                     "interest paid in your statement. Used for carry analysis and daily "
                     "interest cost. Check Tiger's published schedule for your tier.",
            )
            mmf_yield_pct = st.number_input(
                "SGD MMF actual yield (% APR)",
                min_value=0.0, max_value=10.0,
                value=float(settings.get("mmf_yield_pct", 1.0031)),
                step=0.01, format="%.4f",
                help="Actual yield on your subscribed SGD MMF in Tiger Vault. "
                     "Current Tiger Vault offerings (May 2026): Fullerton SGD Liquidity "
                     "Fund Class A (~1.0031%), Phillip Money Market Fund A ACC. "
                     "Check Tiger Vault page for current factsheet yield.",
            )
            base_dep_sgd = st.number_input("Base Pot deposit (SGD)", min_value=0.0,
                                            value=float(settings.get("base_pot_deposit_sgd", 0)),
                                            step=1000.0)
            base_dep = base_dep_sgd / fx if fx > 0 else 0.0
            st.caption(f"≈ **${base_dep:,.0f} USD** (at FX {fx:.4f})")

            active_dep_sgd = st.number_input("Active Pot deposit (SGD)", min_value=0.0,
                                              value=float(settings.get("active_pot_deposit_sgd", 0)),
                                              step=1000.0)
            active_dep = active_dep_sgd / fx if fx > 0 else 0.0
            st.caption(f"≈ **${active_dep:,.0f} USD** (at FX {fx:.4f})")

            total_sgd = base_dep_sgd + active_dep_sgd
            total_usd = base_dep + active_dep
            st.caption(f"**Σ Total: S\\${total_sgd:,.0f} ≈ \\${total_usd:,.0f} USD**")

            st.markdown("**🚫 Ignored Tickers**")
            ignored_str = st.text_area(
                "Hide from all displays (comma-separated)",
                value=", ".join(settings.get("ignored_tickers", ["KO", "NVDA"])),
                height=70,
                help="Reward shares (KO, NVDA) and other dust positions you don't actively trade.",
            )

        with c2:
            st.markdown("**🏦 Tiger Deposit History (reference)**")
            st.caption(
                "Pulled live from Tiger — your funding records. Configure pots above based on this."
            )
            try:
                from tiger_api import tiger_data
                fh = tiger_data.load_funding_history()
            except Exception as e:
                fh = None
                st.caption(f"_Could not load funding history: {e}_")

            if fh is not None and not fh.empty and "currency" in fh.columns and "amount" in fh.columns:
                # Filter to deposits only (type_desc='Deposit')
                deposits = fh[fh.get("type_desc", "").astype(str).str.lower().str.contains("deposit", na=False)] \
                    if "type_desc" in fh.columns else fh
                # Per-currency totals
                tot_by_ccy = deposits.groupby("currency")["amount"].sum().to_dict()
                tot_sgd = float(tot_by_ccy.get("SGD", 0))
                tot_usd = float(tot_by_ccy.get("USD", 0))
                cnt = len(deposits)

                tcols = st.columns(3)
                tcols[0].metric("Total SGD", f"S${tot_sgd:,.0f}")
                tcols[1].metric("Total USD", f"${tot_usd:,.0f}")
                tcols[2].metric("Events", cnt)

                with st.expander(f"📋 All {cnt} deposit events", expanded=False):
                    cols_show = [c for c in ("business_date", "type_desc", "currency", "amount", "ref_id")
                                 if c in deposits.columns]
                    disp = deposits[cols_show].copy().sort_values("business_date", ascending=False)
                    if "amount" in disp.columns:
                        disp["amount"] = disp["amount"].apply(lambda v: f"{v:,.2f}")
                    st.dataframe(disp, use_container_width=True, hide_index=True)
            else:
                st.caption("_No deposit history available._")

            st.markdown("**🔄 PMCC Tickers**")
            pmcc_str = st.text_area("Tickers using PMCC (comma-separated)",
                                     value=", ".join(settings.get("pmcc_tickers", ["SPY"])),
                                     height=70,
                                     help="Tickers where short calls are covered by LEAPs (not stock).")

        st.markdown("**💵 Capital Allocation — Per Pot**")
        st.caption(
            "Each pot has its own table. Add tickers and the % of that pot's capital "
            "you want to allocate to each. **Capital \\$** and **Weekly \\$/4** are derived "
            "live as you type."
        )
        alloc_pct_existing = settings.get("allocation_pct", {}) or {}
        ticker_pots_existing = settings.get("ticker_pots", {}) or {}

        def _pot_rows_existing(pot_name: str):
            return sorted(
                [t for t in alloc_pct_existing if ticker_pots_existing.get(t, "Core") == pot_name]
            )

        # ── Core Pot table ───────────────────────────────────────
        st.markdown(f"##### 🏛️ Core Pot — Deposit: **${base_dep:,.0f}**")
        core_tickers = _pot_rows_existing("Core")
        core_rows = [
            {
                "Ticker": t,
                "Alloc %": float(alloc_pct_existing.get(t, 0)),
                "Capital $": base_dep * float(alloc_pct_existing.get(t, 0)) / 100,
                "Weekly $/4": base_dep * float(alloc_pct_existing.get(t, 0)) / 400,
            }
            for t in core_tickers
        ]
        if not core_rows:
            core_rows = [{"Ticker": "", "Alloc %": 0.0, "Capital $": 0.0, "Weekly $/4": 0.0}]
        core_df = pd.DataFrame(core_rows)
        edited_core = st.data_editor(
            core_df,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                "Alloc %": st.column_config.NumberColumn(
                    "Alloc % (of Core pot)", format="%.1f%%",
                    min_value=0.0, max_value=200.0, step=1.0),
                "Capital $": st.column_config.NumberColumn(
                    "Capital $", format="$%d", disabled=True,
                    help="= Core pot deposit × Alloc % (auto-derived)"),
                "Weekly $/4": st.column_config.NumberColumn(
                    "Weekly $/4", format="$%d", disabled=True,
                    help="= Capital $ ÷ 4 (your weekly target — recompute Capital after Save to refresh)"),
            },
            key="alloc_core_editor",
        )
        core_total_pct = float(edited_core["Alloc %"].sum()) if not edited_core.empty else 0
        st.caption(
            f"Core Σ allocated: **{core_total_pct:.1f}%** "
            f"(\\${base_dep * core_total_pct / 100:,.0f} of \\${base_dep:,.0f}) · "
            f"Unallocated: \\${base_dep * (100 - core_total_pct) / 100:,.0f}"
        )

        # ── Active Pot table ─────────────────────────────────────
        st.markdown(f"##### ⚡ Active Pot — Deposit: **${active_dep:,.0f}**")
        active_tickers = _pot_rows_existing("Active")
        active_rows = [
            {
                "Ticker": t,
                "Alloc %": float(alloc_pct_existing.get(t, 0)),
                "Capital $": active_dep * float(alloc_pct_existing.get(t, 0)) / 100,
                "Weekly $/4": active_dep * float(alloc_pct_existing.get(t, 0)) / 400,
            }
            for t in active_tickers
        ]
        if not active_rows:
            active_rows = [{"Ticker": "", "Alloc %": 0.0, "Capital $": 0.0, "Weekly $/4": 0.0}]
        active_df = pd.DataFrame(active_rows)
        edited_active = st.data_editor(
            active_df,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                "Alloc %": st.column_config.NumberColumn(
                    "Alloc % (of Active pot)", format="%.1f%%",
                    min_value=0.0, max_value=200.0, step=1.0),
                "Capital $": st.column_config.NumberColumn(
                    "Capital $", format="$%d", disabled=True,
                    help="= Active pot deposit × Alloc % (auto-derived)"),
                "Weekly $/4": st.column_config.NumberColumn(
                    "Weekly $/4", format="$%d", disabled=True,
                    help="= Capital $ ÷ 4 (recompute on Save)"),
            },
            key="alloc_active_editor",
        )
        active_total_pct = float(edited_active["Alloc %"].sum()) if not edited_active.empty else 0
        st.caption(
            f"Active Σ allocated: **{active_total_pct:.1f}%** "
            f"(\\${active_dep * active_total_pct / 100:,.0f} of \\${active_dep:,.0f}) · "
            f"Unallocated: \\${active_dep * (100 - active_total_pct) / 100:,.0f}"
        )

        if st.form_submit_button("💾 Save", type="primary", use_container_width=True):
            new_alloc_pct = {}
            new_ticker_pots = {}
            for _, r in edited_core.iterrows():
                t = str(r.get("Ticker", "")).strip().upper()
                p = float(r.get("Alloc %", 0) or 0)
                if t and p > 0:
                    new_alloc_pct[t] = p
                    new_ticker_pots[t] = "Core"
            for _, r in edited_active.iterrows():
                t = str(r.get("Ticker", "")).strip().upper()
                p = float(r.get("Alloc %", 0) or 0)
                if t and p > 0:
                    new_alloc_pct[t] = p
                    new_ticker_pots[t] = "Active"

            new_settings = dict(settings)
            new_settings.update({
                "base_pot_deposit_sgd": base_dep_sgd,
                "active_pot_deposit_sgd": active_dep_sgd,
                "base_pot_deposit_usd": base_dep,
                "active_pot_deposit_usd": active_dep,
                "portfolio_deposit_usd": base_dep + active_dep,
                "sgd_usd_fx_rate": fx,
                "tiger_margin_rate_pct": margin_rate_pct,
                "mmf_yield_pct": mmf_yield_pct,
                "pmcc_tickers": [t.strip().upper() for t in pmcc_str.split(",") if t.strip()],
                "ignored_tickers": [t.strip().upper() for t in ignored_str.split(",") if t.strip()],
                "allocation_pct": new_alloc_pct,
                "ticker_pots": new_ticker_pots,
            })
            save_settings_fn(new_settings)
            st.success("✅ Saved to gSheet Settings tab")
            st.rerun()

    st.divider()
    st.markdown("**Connection Status**")
    try:
        from tiger_api import tiger_data
        c1, c2, c3 = st.columns(3)
        c1.metric("Account", tiger_data.get_account())
        c2.metric("License", tiger_data.get_license())
        c3.metric("Mode", "SANDBOX" if tiger_data.is_sandbox() else "PRODUCTION")
    except Exception as e:
        st.error(f"Tiger API not connected: {e}")

    # ── 📦 Transaction Archive (smart-detect + two-tier persistence) ──
    st.divider()
    st.markdown("**📦 Transaction Archive**")
    st.caption(
        "**Architecture**: Tiger gives us the **last 90 days** of fills every session. "
        "Anything older lives in gSheet `Orders_Archive` (Tier 1, canonical) mirrored "
        "to a local parquet cache (Tier 2, fast). On every session start we check if "
        "the archive's latest fill is more than 80 days old — if so, we **auto-append** "
        "the gap. The header strip always shows the current archive range vs Tiger's "
        "live window so you can verify coverage at a glance."
    )

    try:
        from tiger_api.archive import (
            archive_summary, delete_archive, read_archive_from_gsheet,
            _write_parquet_cache,
        )
        from tiger_api import tiger_data as _td

        # ── Coverage health (same data as header) ──────────────
        cov = _td.get_data_coverage()
        arc = cov["archive"]
        live = cov["live"]
        c1, c2, c3 = st.columns(3)
        if arc["exists"]:
            c1.metric("Archive (gSheet)",
                      f"{arc['rows']:,} rows",
                      help=f"{arc['earliest']} → {arc['latest']}")
        else:
            c1.metric("Archive (gSheet)", "Empty",
                      help="Click 'Backfill now' below to create the first archive.")
        c2.metric("Tiger live window",
                  f"{live['days']}d",
                  help=f"{live['earliest']} → {live['latest']}")
        if cov["health"] == "OK_OVERLAP":
            c3.metric("Coverage", "✅ Continuous",
                      delta=f"{-cov['gap_days']}d overlap", delta_color="off")
        elif cov["health"] == "GAP":
            c3.metric("Coverage", "⚠️ GAP",
                      delta=f"{cov['gap_days']}d missing", delta_color="inverse")
        elif cov["health"] == "NO_ARCHIVE":
            c3.metric("Coverage", "⚪ No archive yet")
        else:
            c3.metric("Coverage", "🔴 Error")

        # ── Auto-archive last-run status ───────────────────────
        try:
            auto = _td.auto_archive_if_stale(tuple(settings.get("pmcc_tickers", ["SPY"])))
            action = auto.get("action", "?")
            label_map = {
                "ok": "✅ Healthy — auto-archive not needed",
                "first_run": "⚪ First run — manual backfill required",
                "archived": f"📥 Just auto-archived ({auto.get('days_old', '?')}d gap closed)",
                "no_date": "⚠️ Archive has no parseable dates",
                "error": "🔴 Auto-archive check failed",
            }
            st.caption(f"**Auto-archive status:** {label_map.get(action, action)} · "
                       f"_{auto.get('msg', '')}_")
        except Exception as e:
            st.caption(f"Auto-archive check unavailable: {e}")

        # ── Parquet cache (Tier 2) tucked into expander ────────
        with st.expander("🗄️ Tier 2 — local parquet cache details", expanded=False):
            summary = archive_summary()
            pq = summary["parquet"]
            pq1, pq2, pq3 = st.columns(3)
            if pq.get("exists"):
                pq1.metric("Parquet rows", f"{pq['rows']:,}")
                pq2.metric("Saved at", pq.get("saved_at", "—") or "—")
                pq3.metric("Status", "✅ Cached locally")
            else:
                pq1.metric("Parquet", "Not cached")
                pq2.metric("Saved at", "—")
                pq3.metric("Status", "ℹ️ Will be rebuilt from gSheet on next read")

        # ── Manual override actions ────────────────────────────
        st.markdown("##### Manual override")
        st.caption(
            "Smart-detect handles 99% of cases. Use these only if you need to force "
            "a deeper backfill or wipe and start over."
        )
        ab1, ab2, ab3, ab4 = st.columns([2, 1.3, 1.3, 1.3])
        with ab1:
            archive_days = st.number_input(
                "Force days back",
                min_value=90, max_value=600, step=30,
                value=365, key="archive_days_input",
                help="Tiger keeps fills back to ~Jan 2025 (~16 months max).",
            )
        with ab2:
            st.write(""); st.write("")
            label = "📥 Backfill now" if not arc["exists"] else "🔁 Force re-pull"
            if st.button(label,
                         use_container_width=True, type="primary",
                         help="Pull from Tiger → append to gSheet AND parquet."):
                pmcc_tuple_now = tuple(settings.get("pmcc_tickers", ["SPY"]))
                result = _td.rebuild_orders_archive(
                    days_back=int(archive_days), pmcc_tickers_tuple=pmcc_tuple_now,
                )
                _td.load_orders_full.clear()
                _td.get_data_coverage.clear()
                _td.auto_archive_if_stale.clear()
                if result.get("ok"):
                    msg = f"✅ Archived {result['rows']:,} rows"
                    if result.get("gsheet_ok"):
                        msg += " · gSheet ✓"
                    if result.get("parquet_ok"):
                        msg += " · parquet ✓"
                    st.success(msg)
                else:
                    st.error(f"Failed: {result.get('msg', 'unknown')}")
                st.rerun()
        with ab3:
            st.write(""); st.write("")
            if st.button("⬇ Reload parquet from gSheet",
                         use_container_width=True,
                         help="Read gSheet → rebuild parquet cache (no Tiger API call)."):
                df_g = read_archive_from_gsheet()
                if df_g.empty:
                    st.warning("gSheet archive is empty — backfill from Tiger first.")
                else:
                    _write_parquet_cache(df_g)
                    _td.load_orders_full.clear()
                    _td.get_data_coverage.clear()
                    st.success(f"✅ Reloaded {len(df_g):,} rows from gSheet to local cache.")
                    st.rerun()
        with ab4:
            st.write(""); st.write("")
            if st.button("🗑️ Wipe both layers",
                         use_container_width=True,
                         help="Delete parquet cache AND clear gSheet tab. Cannot be undone."):
                delete_archive()
                # Clear gSheet too
                try:
                    from tiger_api.archive import _get_gsheet_handler, ARCHIVE_SHEET_TITLE
                    h = _get_gsheet_handler()
                    if h:
                        try:
                            ws = h.spreadsheet.worksheet(ARCHIVE_SHEET_TITLE)
                            ws.clear()
                        except Exception:
                            pass
                except Exception:
                    pass
                _td.load_orders_full.clear()
                _td.get_data_coverage.clear()
                _td.auto_archive_if_stale.clear()
                st.success("Both archive layers wiped.")
                st.rerun()
    except Exception as e:
        st.warning(f"Archive panel error: {e}")


# ────────────────────────────────────────────────────────────────
# Settings I/O — thin wrapper around existing persistence layer
# ────────────────────────────────────────────────────────────────
def load_settings_dict() -> dict:
    """Load Settings from gSheet (via existing persistence module).
    SGD is canonical for pot deposits — USD is derived via FX rate.
    Auto-migrates any legacy $ capital_allocation to allocation_pct using NAV.
    """
    try:
        from persistence import (
            get_pot_deposit, get_pot_deposit_sgd, get_fx_rate,
            get_pmcc_tickers, load_settings,
            get_pot_capital_allocation,
        )
        portfolio = "Income Wheel"
        raw = load_settings() or {}
        fx_rate = get_fx_rate(portfolio) or 1.35

        # Pot deposits: prefer stored SGD, derive USD from FX
        base_sgd = get_pot_deposit_sgd("Base", portfolio)
        active_sgd = get_pot_deposit_sgd("Active", portfolio)
        # Fallback: if SGD not stored but USD is, derive SGD
        if base_sgd <= 0:
            base_usd_legacy = get_pot_deposit("Base", portfolio)
            if base_usd_legacy > 0:
                base_sgd = base_usd_legacy * fx_rate
        if active_sgd <= 0:
            active_usd_legacy = get_pot_deposit("Active", portfolio)
            if active_usd_legacy > 0:
                active_sgd = active_usd_legacy * fx_rate

        base_usd = base_sgd / fx_rate if fx_rate > 0 else 0
        active_usd = active_sgd / fx_rate if fx_rate > 0 else 0

        alloc_pct = dict(raw.get("income_wheel_allocation_pct", {}) or {})
        if not alloc_pct:
            base_alloc = get_pot_capital_allocation("Base", portfolio) or {}
            active_alloc = get_pot_capital_allocation("Active", portfolio) or {}
            combined = {}
            for src in (base_alloc, active_alloc):
                for k, v in src.items():
                    combined[k] = combined.get(k, 0) + float(v or 0)
            if not combined:
                combined = raw.get("income_wheel_capital_allocation", {}) or {}
            try:
                from tiger_api import tiger_data
                _baseline = float(tiger_data.load_account_summary().nav)
            except Exception:
                _baseline = base_usd + active_usd
            if _baseline > 0 and combined:
                alloc_pct = {t: round(v / _baseline * 100, 2) for t, v in combined.items()}

        return {
            "base_pot_deposit_sgd": base_sgd,
            "active_pot_deposit_sgd": active_sgd,
            "base_pot_deposit_usd": base_usd,
            "active_pot_deposit_usd": active_usd,
            "portfolio_deposit_usd": base_usd + active_usd,
            "sgd_usd_fx_rate": fx_rate,
            "tiger_margin_rate_pct": float(raw.get("income_wheel_tiger_margin_rate_pct", 7.0)),
            "mmf_yield_pct": float(raw.get("income_wheel_mmf_yield_pct", 1.0031)),
            "history_days": int(raw.get("income_wheel_history_days", 365)),
            "pmcc_tickers": list(get_pmcc_tickers(portfolio) or ["SPY"]),
            "ignored_tickers": list(raw.get("income_wheel_ignored_tickers", ["KO", "NVDA"])),
            "allocation_pct": alloc_pct,
            "ticker_pots": dict(raw.get("income_wheel_ticker_pots", {}) or {}),
        }
    except Exception as e:
        st.warning(f"Settings load failed: {e}")
        return {}


def save_settings_dict(settings: dict):
    """SGD is canonical — USD is derived. Save both to keep legacy code happy."""
    from persistence import (
        save_pot_deposit, save_pot_deposit_sgd, save_fx_rate,
        save_pmcc_tickers, save_settings, load_settings,
    )
    portfolio = "Income Wheel"
    fx = float(settings["sgd_usd_fx_rate"])
    save_fx_rate(fx, portfolio)

    # Save SGD as canonical, USD as derived
    base_sgd = float(settings.get("base_pot_deposit_sgd", 0))
    active_sgd = float(settings.get("active_pot_deposit_sgd", 0))
    save_pot_deposit_sgd("Base", base_sgd, portfolio)
    save_pot_deposit_sgd("Active", active_sgd, portfolio)
    save_pot_deposit("Base", base_sgd / fx if fx > 0 else 0, portfolio)
    save_pot_deposit("Active", active_sgd / fx if fx > 0 else 0, portfolio)

    save_pmcc_tickers(set(settings["pmcc_tickers"]), portfolio)
    raw = load_settings() or {}
    raw.update({
        "income_wheel_history_days": int(settings.get("history_days", 365)),
        "income_wheel_portfolio_deposit_usd": float(settings["portfolio_deposit_usd"]),
        "income_wheel_ignored_tickers": list(settings.get("ignored_tickers", [])),
        "income_wheel_allocation_pct": dict(settings.get("allocation_pct", {})),
        "income_wheel_ticker_pots": dict(settings.get("ticker_pots", {})),
        "income_wheel_tiger_margin_rate_pct": float(settings.get("tiger_margin_rate_pct", 7.0)),
        "income_wheel_mmf_yield_pct": float(settings.get("mmf_yield_pct", 1.0031)),
    })
    save_settings(raw)


# ────────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────────
def main():
    # Load all data via cached loaders
    try:
        from tiger_api import tiger_data
    except ImportError as e:
        st.error(f"Tiger SDK not available: {e}\n\nRun `pip install tigeropen`.")
        return

    settings = load_settings_dict()
    pmcc_tuple = tuple(settings.get("pmcc_tickers", ["SPY"]))

    try:
        with st.spinner("Connecting to Tiger..."):
            summary = tiger_data.load_account_summary()
    except Exception as e:
        st.error(f"❌ Tiger API unreachable: {e}")
        st.info("Check `.streamlit/tiger_openapi_config.properties` and network.")
        return

    df_open = tiger_data.load_open_positions(pmcc_tuple)
    # Live orders: 90 days only (fast cold start). Cockpit / Positions /
    # Transactions all use this. P&L Slicer uses load_orders_full() which
    # merges live + on-disk archive for YTD / lifetime analytics.
    df_orders = tiger_data.load_orders(days=90, pmcc_tickers_tuple=pmcc_tuple)

    # ── Smart-detect auto-archive (Option C) ────────────────────
    # Cached for 10 min — runs at most once per ten minutes per session.
    # Silent unless an actual archive ran (then we toast).
    try:
        auto = tiger_data.auto_archive_if_stale(pmcc_tuple)
        if auto.get("action") == "archived":
            r = auto.get("result") or {}
            st.toast(
                f"📥 Auto-archived: {r.get('added', 0)} new rows "
                f"(archive was {auto.get('days_old', '?')}d stale)",
                icon="✅",
            )
            # Bust caches that depend on archive
            tiger_data.load_orders_full.clear()
            tiger_data.get_data_coverage.clear()
    except Exception as e:
        logger.debug("Auto-archive check skipped: %s", e)

    # ── Apply ignore filter (KO, NVDA reward shares, etc.) ──────
    ignored = set(t.strip().upper() for t in settings.get("ignored_tickers", []) if t.strip())
    if ignored:
        if not df_open.empty and "Ticker" in df_open.columns:
            df_open = df_open[~df_open["Ticker"].str.upper().isin(ignored)].reset_index(drop=True)
        if not df_orders.empty and "Ticker" in df_orders.columns:
            df_orders = df_orders[~df_orders["Ticker"].str.upper().isin(ignored)].reset_index(drop=True)

    # ── Spot prices (once per refresh; used by header + pacing) ─
    open_tickers = sorted(df_open["Ticker"].dropna().unique().tolist()) if not df_open.empty else []
    spot_prices = {}
    if open_tickers:
        try:
            spot_prices = tiger_data.load_spot_prices(tuple(open_tickers))
        except Exception:
            spot_prices = {}
        # Fallback: stock position market price for tickers without spot
        stk = df_open[df_open["TradeType"] == "STOCK"]
        for _, r in stk.iterrows():
            t = r.get("Ticker")
            mp = r.get("_market_price")
            if t and mp and t not in spot_prices:
                try:
                    spot_prices[t] = float(mp)
                except (TypeError, ValueError):
                    pass

    # ── Header (persistent) ─────────────────────────────────────
    render_header(summary, settings, df_open, spot_prices)

    # ── 🚨 Vault Pull Alert (above tabs, dismissable per-session) ────
    # Detects FUND→SEC transfers ≥ S$5,000 in the last 14 days. Tiger
    # auto-pulls MMF when margin pressure hits and provides ZERO notification,
    # so the only way to learn about it is via this dashboard.
    try:
        if not st.session_state.get("vault_alert_dismissed"):
            alert = tiger_data.detect_vault_pull_alert(window_days=14, min_amount_sgd=5000)
            if alert.get("alert"):
                ac1, ac2 = st.columns([10, 1])
                with ac1:
                    events_str = " · ".join(
                        f"{ev['date']}: S${ev['amount_sgd']:,.0f}" for ev in alert["events"]
                    )
                    st.warning(
                        f"⚠️ **Vault redemption detected** — "
                        f"S${alert['total_sgd']:,.0f} moved FUND→SEC in last 14d "
                        f"({events_str}). Could be: (a) margin auto-pull, OR "
                        f"(b) Tiger fund migration (e.g. discontinued fund). "
                        f"Open Cockpit → Cash Maximization panel and check the "
                        f"Vault history `desc` field to identify which."
                    )
                with ac2:
                    if st.button("Dismiss", key="vault_alert_dismiss", use_container_width=True):
                        st.session_state["vault_alert_dismissed"] = True
                        st.rerun()
    except Exception as e:
        logger.debug("Vault pull alert skipped: %s", e)

    # ── Top tabs (stateful: survives reruns) ────────────────────
    # st.tabs is stateless — every full rerun bounces user back to first tab.
    # We use a session-state-backed radio styled as tabs so the active tab
    # persists across header refreshes, archive runs, form saves, etc.
    TAB_OPTIONS = [
        ("🎯 Cockpit",       "cockpit"),
        ("⚠️ Risk & Rolls",  "risk"),
        ("📦 Positions",     "positions"),
        ("📜 Transactions",  "transactions"),
        ("📅 Ladder",        "ladder"),
        ("📊 P&L",           "pl"),
        ("🔎 Lookup",        "lookup"),
        ("⚙️ Config",        "config"),
        ("💰 Cash",          "cash"),
    ]
    labels = [t[0] for t in TAB_OPTIONS]
    keys = [t[1] for t in TAB_OPTIONS]

    if "active_tab" not in st.session_state:
        st.session_state.active_tab = keys[0]

    # Map session-state key → label index (default to 0 if unknown)
    try:
        cur_idx = keys.index(st.session_state.active_tab)
    except ValueError:
        cur_idx = 0

    selected_label = st.radio(
        "Tab", labels,
        index=cur_idx,
        horizontal=True,
        label_visibility="collapsed",
        key="_argus_tab_radio",
    )
    st.session_state.active_tab = keys[labels.index(selected_label)]
    active = st.session_state.active_tab
    st.divider()

    if active == "cockpit":
        render_cockpit(summary, df_open, df_orders, settings, spot_prices)
    elif active == "positions":
        render_positions(df_open, spot_prices, settings)
    elif active == "cash":
        render_cash(summary, df_open, settings)
    elif active == "transactions":
        render_transactions(df_orders, days=14)
    elif active == "ladder":
        render_ladder(df_open, spot_prices, settings)
    elif active == "pl":
        # P&L analytics gets full-history (live 90d + on-disk archive merged)
        df_orders_full = tiger_data.load_orders_full(pmcc_tuple)
        if ignored and not df_orders_full.empty and "Ticker" in df_orders_full.columns:
            df_orders_full = df_orders_full[
                ~df_orders_full["Ticker"].str.upper().isin(ignored)
            ].reset_index(drop=True)
        render_pl(df_orders_full, df_open, settings)
    elif active == "risk":
        render_risk(df_open, summary, settings, spot_prices)
    elif active == "lookup":
        # Standalone contract price lookup — paste OCC option codes, get last
        # price via Alpaca. Independent of the Income Wheel — useful for
        # researching positions outside the portfolio.
        try:
            from contract_price_lookup import render_contract_price_lookup
            render_contract_price_lookup()
        except Exception as e:
            st.error(f"Contract lookup unavailable: {e}")
    elif active == "config":
        render_config(settings, save_settings_dict)


if __name__ == "__main__":
    main()
