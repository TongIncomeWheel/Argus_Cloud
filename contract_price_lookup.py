"""
Contract Price Lookup
=====================
Enter up to 20 contract codes, click Refresh, get last price.
"""

import logging

import pandas as pd
import streamlit as st

from market_data.config import ALPACA_API_KEY, ALPACA_SECRET_KEY
from persistence import load_settings, save_settings

logger = logging.getLogger(__name__)

_SLOT_COUNT = 40
_PERSIST_KEY = "contract_lookup_codes"


def _load_saved_codes() -> list:
    codes = load_settings().get(_PERSIST_KEY, [])
    codes = list(codes) + [""] * _SLOT_COUNT
    return codes[:_SLOT_COUNT]


def _save_codes(codes: list) -> None:
    settings = load_settings()
    settings[_PERSIST_KEY] = codes
    save_settings(settings)


def _init_state() -> None:
    if "cpl_initialized" not in st.session_state:
        for i, code in enumerate(_load_saved_codes()):
            st.session_state[f"cpl_slot_{i}"] = code
        st.session_state["cpl_initialized"] = True


def _fetch_last_prices(codes: list) -> pd.DataFrame:
    clean = [c.strip().upper() for c in codes if c and c.strip()]
    if not clean:
        return pd.DataFrame(columns=["Contract", "Last Price"])

    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        st.error("Alpaca API keys not configured — add ALPACA_API_KEY and ALPACA_SECRET_KEY to .env")
        return pd.DataFrame(columns=["Contract", "Last Price"])

    try:
        from alpaca.data.historical.option import OptionHistoricalDataClient
        from alpaca.data.requests import OptionSnapshotRequest

        client = OptionHistoricalDataClient(api_key=ALPACA_API_KEY, secret_key=ALPACA_SECRET_KEY)
        snapshots = client.get_option_snapshot(OptionSnapshotRequest(symbol_or_symbols=clean))

        rows = []
        for code in clean:
            snap = snapshots.get(code)
            last = getattr(snap.latest_trade, "price", None) if (snap and snap.latest_trade) else None
            rows.append({"Contract": code, "Last Price": last})

        return pd.DataFrame(rows)

    except Exception as exc:
        logger.error(f"Contract price lookup error: {exc}")
        st.error(f"Alpaca fetch failed: {exc}")
        return pd.DataFrame(columns=["Contract", "Last Price"])


def render_contract_price_lookup():
    st.header("🔎 Contract Price Lookup")

    _init_state()

    # ── Paste from Spreadsheet ───────────────────────────────────────────
    with st.expander("📋 Paste from Spreadsheet"):
        st.caption("Copy a column of contract codes from your spreadsheet and paste below, one per line.")
        pasted = st.text_area("Paste here", height=200, placeholder="MARA250321C00020000\nSPY250321P00450000\n...", label_visibility="collapsed", key="cpl_paste")
        if st.button("Load into slots", key="cpl_load_paste"):
            # Split by newlines, take first tab-separated token per row (handles multi-column Excel pastes)
            lines = [row.split("\t")[0].strip().upper() for row in pasted.splitlines() if row.strip()]
            lines = lines[:_SLOT_COUNT]
            for i in range(_SLOT_COUNT):
                st.session_state[f"cpl_slot_{i}"] = lines[i] if i < len(lines) else ""
            st.rerun()

    # ── 20 input rows ─────────────────────────────────────────────────────
    for i in range(_SLOT_COUNT):
        st.text_input(
            f"Slot {i + 1}",
            key=f"cpl_slot_{i}",
            placeholder=f"Contract code {i + 1}",
            label_visibility="collapsed",
        )

    st.divider()

    col_refresh, col_clear = st.columns([1, 1])
    with col_refresh:
        refresh = st.button("🔄 Refresh Prices", type="primary", key="cpl_refresh", use_container_width=True)
    with col_clear:
        if st.button("🗑️ Clear All", key="cpl_clear", use_container_width=True):
            for i in range(_SLOT_COUNT):
                st.session_state[f"cpl_slot_{i}"] = ""
            _save_codes([""] * _SLOT_COUNT)
            st.session_state.pop("cpl_results", None)
            st.rerun()

    if refresh:
        codes = [st.session_state.get(f"cpl_slot_{i}", "") for i in range(_SLOT_COUNT)]
        filled = [c for c in codes if c.strip()]
        if not filled:
            st.warning("Enter at least one contract code.")
        else:
            _save_codes(codes)
            with st.spinner(f"Fetching {len(filled)} contract(s)…"):
                df = _fetch_last_prices(codes)
            st.session_state["cpl_results"] = df

    # ── Results ───────────────────────────────────────────────────────────
    df: pd.DataFrame = st.session_state.get("cpl_results")
    if df is not None and not df.empty:
        st.dataframe(df, use_container_width=False, hide_index=True)

        st.download_button(
            "⬇️ Download CSV",
            data=df.to_csv(index=False),
            file_name="contract_prices.csv",
            mime="text/csv",
            key="cpl_download",
        )
