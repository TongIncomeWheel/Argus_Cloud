"""Streamlit UI for the Scanner module — rendered as a sub-tab under Lookup.

Entry point: render_theta_scanner(df_open, settings).

Layout: a collapsible filter panel (Options / Fundamentals / Technicals /
Dividends / Global) over a stats bar, a column/layout toolbar, and a
sortable, paginated, configurable results table.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Optional

import pandas as pd
import streamlit as st

from . import columns as cols
from . import data as data_mod
from . import filters as filt
from . import fundamentals as fund_mod
from . import presets as presets_mod
from . import scan as scan_mod
from . import universe as universe_mod

logger = logging.getLogger(__name__)

# ─── Session-state keys ────────────────────────────────────────────
SS_RESULT = "ts_scan_result"
SS_FSTATE = "ts_filter_state"       # seed values for filter widgets
SS_FEPOCH = "ts_filter_epoch"       # bumped to force-refresh filter widgets
SS_LAYOUT = "ts_layout"             # current visible column keys
SS_LEPOCH = "ts_layout_epoch"
SS_LAST_ARGS = "ts_last_scan_args"
SS_OPT_TYPE = "ts_option_type"

_DEFAULT_DTE = (7, 60)              # scan window when the DTE filter is blank


# ─── State bootstrap ───────────────────────────────────────────────


def _ensure_state() -> None:
    ss = st.session_state
    if SS_FSTATE not in ss:
        ss[SS_FSTATE] = filt.default_filter_state()
    ss.setdefault(SS_FEPOCH, 0)
    ss.setdefault(SS_LEPOCH, 0)
    if SS_LAYOUT not in ss:
        ss[SS_LAYOUT] = list(cols.DEFAULT_LAYOUT)


def _fkey(filter_key: str, suffix: str) -> str:
    return f"tsf_{filter_key}_{suffix}_{st.session_state[SS_FEPOCH]}"


# ─── Filter widgets ────────────────────────────────────────────────


def _num_input(container, label: str, filter_key: str, suffix: str,
               step: float, placeholder: str):
    wkey = _fkey(filter_key, suffix)
    kwargs = dict(key=wkey, step=step, label_visibility="collapsed",
                  placeholder=placeholder)
    if wkey not in st.session_state:
        kwargs["value"] = st.session_state[SS_FSTATE].get(f"{filter_key}_{suffix}")
    return container.number_input(label, **kwargs)


def _render_range(fdef: filt.FilterDef, has_min: bool = True) -> None:
    row = st.columns([2.3, 1.5, 1.5])
    tip = f" — {fdef.help}" if fdef.help else ""
    row[0].markdown(f"**{fdef.label}**{tip}")
    if has_min:
        _num_input(row[1], f"{fdef.label} min", fdef.key, "min", fdef.step, "Min")
    else:
        row[1].markdown("&nbsp;", unsafe_allow_html=True)
    _num_input(row[2], f"{fdef.label} max", fdef.key, "max", fdef.step, "Max")


def _render_choice(fdef: filt.FilterDef, options: list) -> None:
    wkey = _fkey(fdef.key, "c")
    seed = st.session_state[SS_FSTATE].get(fdef.key, fdef.default_choice)
    kwargs = dict(key=wkey, help=fdef.help or None)
    if wkey not in st.session_state:
        kwargs["index"] = options.index(seed) if seed in options else 0
    st.selectbox(fdef.label, options, **kwargs)


def _render_toggle(container, fdef: filt.FilterDef) -> None:
    wkey = _fkey(fdef.key, "t")
    kwargs = dict(key=wkey, help=fdef.help or None)
    if wkey not in st.session_state:
        kwargs["value"] = bool(st.session_state[SS_FSTATE].get(fdef.key, fdef.default_on))
    container.toggle(fdef.label, **kwargs)


def _render_section(section: str, sectors: list) -> None:
    defs = filt.defs_for_section(section)
    toggles = [f for f in defs if f.kind == "toggle"]
    ranges = [f for f in defs if f.kind in ("range", "max")]
    choices = [f for f in defs if f.kind == "choice"]
    directions = [f for f in defs if f.kind == "direction"]

    for fdef in ranges:
        if fdef.key == "iv":
            # IV basis toggle sits beside the IV range.
            head = st.columns([2.3, 3.0])
            head[0].markdown(f"**{fdef.label}**")
            bkey = _fkey("iv_basis", "c")
            bkwargs = dict(key=bkey, options=["Option IV", "Stock IV"],
                           horizontal=True, label_visibility="collapsed")
            if bkey not in st.session_state:
                seed = st.session_state[SS_FSTATE].get("iv_basis", "Option IV")
                bkwargs["index"] = 0 if seed == "Option IV" else 1
            head[1].radio("IV basis", **bkwargs)
            rng = st.columns([2.3, 1.5, 1.5])
            rng[0].caption("Applies to the IV basis selected above")
            _num_input(rng[1], "IV min", "iv", "min", fdef.step, "Min")
            _num_input(rng[2], "IV max", "iv", "max", fdef.step, "Max")
        else:
            _render_range(fdef, has_min=(fdef.kind == "range"))

    for fdef in directions:
        wkey = _fkey(fdef.key, "c")
        kwargs = dict(key=wkey, options=list(fdef.choices), horizontal=True,
                      help="Underlying price relative to the moving average")
        if wkey not in st.session_state:
            seed = st.session_state[SS_FSTATE].get(fdef.key, "Any")
            kwargs["index"] = list(fdef.choices).index(seed) if seed in fdef.choices else 0
        st.radio(fdef.label, **kwargs)

    for fdef in choices:
        opts = [filt.ANY_SECTOR] + sectors if fdef.key == "sector" else list(fdef.choices)
        _render_choice(fdef, opts)

    if toggles:
        st.markdown("---")
        cgrid = st.columns(min(len(toggles), 3))
        for i, fdef in enumerate(toggles):
            _render_toggle(cgrid[i % len(cgrid)], fdef)


def _render_filters(sectors: list) -> None:
    with st.expander("⚙️ Filters", expanded=True):
        tabs = st.tabs([f"{s}" for s in filt.SECTIONS])
        for tab, section in zip(tabs, filt.SECTIONS):
            with tab:
                _render_section(section, sectors)


def _collect_filter_state() -> dict:
    ss = st.session_state
    state = {"iv_basis": ss.get(_fkey("iv_basis", "c"), "Option IV")}
    for f in filt.FILTER_DEFS:
        if f.kind == "range":
            state[f"{f.key}_min"] = ss.get(_fkey(f.key, "min"))
            state[f"{f.key}_max"] = ss.get(_fkey(f.key, "max"))
        elif f.kind == "max":
            state[f"{f.key}_max"] = ss.get(_fkey(f.key, "max"))
        elif f.kind == "toggle":
            state[f.key] = bool(ss.get(_fkey(f.key, "t"), f.default_on))
        elif f.kind in ("choice", "direction"):
            state[f.key] = ss.get(_fkey(f.key, "c"), f.default_choice)
    return state


# ─── Preset bar ────────────────────────────────────────────────────


def _render_preset_bar() -> None:
    presets = presets_mod.load_filter_presets()
    bar = st.columns([3, 3, 1.2, 1.2])

    names = ["—"] + sorted(presets.keys())
    chosen = bar[0].selectbox("Load saved filter", names, key="ts_preset_pick")
    if chosen != "—" and bar[0].button("Load preset", key="ts_preset_load"):
        merged = filt.default_filter_state()
        merged.update(presets.get(chosen, {}))
        st.session_state[SS_FSTATE] = merged
        st.session_state[SS_FEPOCH] += 1
        st.rerun()

    new_name = bar[1].text_input("Save current filters as", key="ts_preset_name",
                                 placeholder="Preset name")
    if bar[1].button("💾 Save filter", key="ts_preset_save"):
        if presets_mod.save_filter_preset(new_name, _collect_filter_state()):
            st.success(f"Saved filter preset '{new_name}'.")
            st.rerun()
        else:
            st.warning("Enter a name to save the preset.")

    bar[2].markdown("&nbsp;")
    if bar[2].button("🗑 Delete", key="ts_preset_del", disabled=(chosen == "—")):
        presets_mod.delete_filter_preset(chosen)
        st.rerun()

    bar[3].markdown("&nbsp;")
    if bar[3].button("↺ Reset", key="ts_preset_reset"):
        st.session_state[SS_FSTATE] = filt.default_filter_state()
        st.session_state[SS_FEPOCH] += 1
        st.rerun()


# ─── Stats bar ─────────────────────────────────────────────────────


def _avg(df: pd.DataFrame, col: str):
    if col in df.columns and df[col].notna().any():
        return float(df[col].mean())
    return None


def _render_stats(df: pd.DataFrame, result: scan_mod.ScanResult) -> None:
    m = st.columns(5)
    m[0].metric("Total Contracts", len(df))
    m[1].metric("Unique Tickers", df["symbol"].nunique() if "symbol" in df else 0)
    roc = _avg(df, "roc")
    m[2].metric("Avg ROC", f"{roc:.2f}%" if roc is not None else "—")
    dlt = _avg(df, "delta")
    m[3].metric("Avg Delta", f"{dlt:.3f}" if dlt is not None else "—")
    iv = _avg(df, "iv_pct")
    m[4].metric("Avg IV", f"{iv:.0f}%" if iv is not None else "—")

    if result.scanned_at:
        age = (datetime.now() - result.scanned_at).total_seconds() / 60.0
        when = "just now" if age < 1 else f"{age:.0f} min ago"
        fresh = st.columns([4, 1])
        fresh[0].caption(
            f"Last updated {when} · fundamentals via {result.fundamentals_source} · "
            f"{result.n_tickers_ok} tickers scanned"
            + (f" · {len(result.errors)} with no chain" if result.errors else "")
        )
        if fresh[1].button("🔄 Load new data", key="ts_reload"):
            _reload_data()


def _reload_data() -> None:
    """Clear the scanner's own caches and re-run the last scan."""
    from . import technicals as tech_mod
    for fn in (tech_mod.compute_technicals, fund_mod._fmp_fundamentals,
               fund_mod._yf_fundamentals):
        try:
            fn.clear()
        except Exception:
            pass
    args = st.session_state.get(SS_LAST_ARGS)
    if args:
        _do_scan(*args)
    st.rerun()


# ─── Table toolbar + table ─────────────────────────────────────────


def _render_toolbar(df: pd.DataFrame) -> list:
    layouts = presets_mod.load_column_layouts()
    bar = st.columns([2.4, 2.4, 1.1, 1.6, 2.0])

    names = ["—"] + sorted(layouts.keys())
    pick = bar[0].selectbox("Load layout", names, key="ts_layout_pick")
    if pick != "—" and bar[0].button("Apply layout", key="ts_layout_apply"):
        chosen = [k for k in layouts.get(pick, []) if cols.get(k)]
        if chosen:
            st.session_state[SS_LAYOUT] = chosen
            st.session_state[SS_LEPOCH] += 1
            st.rerun()

    lname = bar[1].text_input("Save layout as", key="ts_layout_name",
                              placeholder="Layout name")
    if bar[1].button("💾 Save layout", key="ts_layout_save"):
        if presets_mod.save_column_layout(lname, st.session_state[SS_LAYOUT]):
            st.success(f"Saved layout '{lname}'.")
            st.rerun()
        else:
            st.warning("Enter a name to save the layout.")

    bar[2].markdown("&nbsp;")
    if bar[2].button("↺ Reset", key="ts_layout_reset"):
        st.session_state[SS_LAYOUT] = list(cols.DEFAULT_LAYOUT)
        st.session_state[SS_LEPOCH] += 1
        st.rerun()

    bar[3].markdown("&nbsp;")
    visible = st.session_state[SS_LAYOUT]
    export_df = df[[c for c in visible if c in df.columns]]
    bar[3].download_button(
        "⬇ Export CSV", export_df.to_csv(index=False).encode("utf-8"),
        file_name="theta_scan.csv", mime="text/csv", key="ts_export",
    )

    with bar[4].popover("🧱 Columns", use_container_width=True):
        _render_column_picker()

    return st.session_state[SS_LAYOUT]


def _render_column_picker() -> None:
    epoch = st.session_state[SS_LEPOCH]
    grouped = cols.keys_by_category()
    chosen: list = []
    st.caption("Tick columns to show. Order follows the category order below.")
    for cat, keys in grouped.items():
        wkey = f"tsc_{cat}_{epoch}"
        default = [k for k in keys if k in st.session_state[SS_LAYOUT]]
        kwargs = dict(key=wkey,
                      options=keys,
                      format_func=cols.label)
        if wkey not in st.session_state:
            kwargs["default"] = default
        picked = st.multiselect(cat, **kwargs)
        chosen.extend(picked)
    if chosen:
        st.session_state[SS_LAYOUT] = chosen


def _render_table(df: pd.DataFrame, visible: list) -> None:
    visible = [c for c in visible if c in df.columns] or list(cols.DEFAULT_LAYOUT)

    ctrl = st.columns([2, 1, 1.4])
    sort_col = ctrl[0].selectbox("Sort by", visible, key="ts_sort_col",
                                 format_func=cols.label)
    descending = ctrl[1].radio("Order", ["Desc", "Asc"], horizontal=True,
                               key="ts_sort_dir") == "Desc"
    per_page = ctrl[2].selectbox("Rows per page", [25, 50, 100, 250, "All"],
                                 index=1, key="ts_per_page")

    view = df.sort_values(sort_col, ascending=not descending,
                          na_position="last", kind="stable")

    n = len(view)
    if per_page == "All":
        page_df, page, pages = view, 1, 1
    else:
        pages = max(1, math.ceil(n / per_page))
        page = min(int(st.session_state.get("ts_page", 1)), pages)
        nav = st.columns([1, 1, 4])
        if nav[0].button("◀ Prev", key="ts_prev", disabled=(page <= 1)):
            st.session_state["ts_page"] = page - 1
            st.rerun()
        if nav[1].button("Next ▶", key="ts_next", disabled=(page >= pages)):
            st.session_state["ts_page"] = page + 1
            st.rerun()
        nav[2].caption(f"Page {page} of {pages} · {n} contracts")
        start = (page - 1) * per_page
        page_df = view.iloc[start:start + per_page]

    st.dataframe(
        page_df[visible], use_container_width=True, hide_index=True,
        column_config=cols.column_config(visible),
    )
    st.caption(
        "**Option Score** 0-100 blend (yield 40% · distance OTM 30% · delta 30%). "
        "**Stock Rating** = technical health of the underlying. **Stock IV** = median "
        "IV across the ticker's contracts. Scores are ARGUS-computed. "
        "Click a header to sort the current page; use *Sort by* to sort all rows."
    )


# ─── Scan ──────────────────────────────────────────────────────────


def _do_scan(tickers: list, option_type: str, dte_min: int, dte_max: int) -> None:
    st.session_state[SS_LAST_ARGS] = (tickers, option_type, dte_min, dte_max)
    pbar = st.progress(0.0, text="Starting scan…")
    try:
        result = scan_mod.run_scan(
            tickers, option_type, dte_min, dte_max,
            progress=lambda f, t: pbar.progress(min(f, 1.0), text=t),
        )
    finally:
        pbar.empty()
    st.session_state[SS_RESULT] = result
    st.session_state["ts_page"] = 1


# ─── Main entry point ──────────────────────────────────────────────


def render_theta_scanner(df_open: Optional[pd.DataFrame] = None,
                         settings: Optional[dict] = None) -> None:
    """Render the Scanner module."""
    _ensure_state()

    st.markdown("### θ Theta Scanner — Options Screener")
    st.caption(
        "Scan an options universe for cash-secured puts and covered calls. "
        "Filter on ~35 criteria across options, fundamentals, technicals and "
        "dividends; sort, customize columns, and save presets."
    )

    if not data_mod.alpaca_configured():
        st.error("Alpaca credentials missing — the scanner needs Alpaca for option chains.")
        return

    uni = universe_mod.get_universe()
    fund_src = "FMP live" if universe_mod.fmp_configured() else "yfinance"
    st.caption(
        f"**Universe:** {uni['count']} tickers ({uni['source']}) · "
        f"**Fundamentals/technicals:** {fund_src}"
        + ("" if universe_mod.fmp_configured()
           else " — add an `FMP_API_KEY` to Streamlit secrets for the fast batched feed.")
    )

    # Sector options come from the last scan, if any.
    result: Optional[scan_mod.ScanResult] = st.session_state.get(SS_RESULT)
    sectors: list = []
    if result is not None and not result.df.empty and "sector" in result.df:
        sectors = sorted(s for s in result.df["sector"].dropna().unique())

    _render_preset_bar()

    head = st.columns([1.4, 3])
    option_type = head[0].radio("Option type", ["Puts", "Calls"], horizontal=True,
                                key=SS_OPT_TYPE)
    search = head[1].text_input(
        "Ticker search (comma-separated — blank scans the whole universe)",
        key="ts_search", placeholder="e.g. AAPL, MSFT, NVDA",
    )

    _render_filters(sectors)

    # Watchlist management.
    with st.expander("⭐ Manage Watchlist"):
        wl = presets_mod.load_watchlist()
        wl_text = st.text_area("Watchlist tickers (comma or newline separated)",
                               value=", ".join(wl), key="ts_wl_text")
        if st.button("Save watchlist", key="ts_wl_save"):
            parsed = [t.strip() for t in wl_text.replace("\n", ",").split(",")]
            presets_mod.save_watchlist(parsed)
            st.success("Watchlist saved.")
            st.rerun()

    # Resolve the scan ticker list.
    if search.strip():
        scan_tickers = sorted({t.strip().upper()
                               for t in search.split(",") if t.strip()})
    else:
        scan_tickers = list(uni["tickers"])

    # DTE filter doubles as the chain-pull window.
    fstate = _collect_filter_state()
    dte_min = int(fstate.get("dte_min") or _DEFAULT_DTE[0])
    dte_max = int(fstate.get("dte_max") or _DEFAULT_DTE[1])
    if dte_max < dte_min:
        dte_min, dte_max = _DEFAULT_DTE

    active = filt.count_active(fstate)
    run = st.button(
        f"🔍 Run scan — {len(scan_tickers)} ticker(s), DTE {dte_min}-{dte_max}, "
        f"{active} filter(s) active",
        type="primary", key="ts_run",
    )
    st.caption(
        "First run pulls option chains + price history per ticker — a full "
        f"{uni['count']}-ticker scan takes 1-3 min; results cache after. Narrow "
        "with Ticker search for a fast scan."
    )
    if run:
        _do_scan(scan_tickers, option_type, dte_min, dte_max)
        result = st.session_state.get(SS_RESULT)

    if result is None:
        st.info("Set your filters and click **Run scan**.")
        return
    if result.df.empty:
        st.warning(
            "No contracts returned. Check Alpaca chain availability and the DTE window."
            + (f" No chain for: {', '.join(result.errors[:15])}" if result.errors else "")
        )
        return

    if result.df["type"].iloc[0].lower() != option_type.lower().rstrip("s"):
        st.info(f"Results below are **{result.df['type'].iloc[0]}s** — "
                f"click **Run scan** to refresh for {option_type}.")

    # Apply filters.
    watchlist = presets_mod.load_watchlist()
    filtered = filt.apply_filters(result.df, fstate, watchlist)

    st.markdown("#### Results")
    _render_stats(filtered, result)

    if filtered.empty:
        st.warning(
            f"Scanned {len(result.df)} contracts — none passed the {active} active "
            "filter(s). Loosen the thresholds above."
        )
        return

    visible = _render_toolbar(filtered)
    _render_table(filtered, visible)
