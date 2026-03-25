"""
Income Scanner Module — ARGUS v6 Framework
==========================================
Scans ThetaScanner CSV exports (or manual ticker lists) through the full
ARGUS v6 5-gate filter, assigns T1/T2/T3 tiers, computes position sizing,
and surfaces execution-ready CSP recommendations.

Data source: ThetaScanner Pro CSV (all fields — no IBKR or external API needed).

Enhancements v2:
  - Full configurable threshold panel (⚙️ Scan Parameters expander)
  - Gate-by-gate rejection breakdown showing every dropped ticker and why
"""

import io
import streamlit as st
import pandas as pd
from typing import Optional

# ─────────────────────────────────────────────
# ARGUS v6 DEFAULT THRESHOLDS
# These are DEFAULTS only — runtime values live in st.session_state['scanner_cfg']
# Edit here to change what "Reset to Defaults" restores.
# ─────────────────────────────────────────────
ACTIVE_DTE_MIN      = 5
ACTIVE_DTE_MAX      = 21
CORE_DTE_MIN        = 22
CORE_DTE_MAX        = 45
ACTIVE_ROC_MIN      = 0.30    # % (0.30 = 0.30%, not 30%)
CORE_ROC_MIN        = 1.00
ACTIVE_DELTA_MAX    = 0.30    # absolute value — typical CSP delta range
CORE_DELTA_MAX      = 0.35
ACTIVE_SCORE_MIN    = 73
CORE_SCORE_MIN      = 75
MARKET_CAP_MIN      = 2.0     # $B
OPEN_INT_MIN        = 100
VOLUME_MIN          = 50
SPREAD_MAX_PCT      = 5.0     # % of midpoint
IV_SPIKE_THRESH     = 30      # % premium of option IV over stock HV
IV_ELEV_THRESH      = 5       # % premium of option IV over stock HV
SECTOR_CAP          = 0.40    # 40% max per GICS sector
MAX_POSITIONS       = 5
CASH_BUFFER         = 0.05    # 5% reserve — never deploy
TIER_SIZE_PCT       = {'T1': 0.22, 'T2': 0.15, 'T3': 0.10}
BTC_TARGET          = {'T1': 0.60, 'T2': 0.50, 'T3': 0.50}
GURU_LIST           = []   # Default empty — populated at runtime via CSV upload

# ThetaScanner column → internal name
CSV_COLUMN_MAP = {
    'Symbol':       'ticker',
    'Strike':       'strike',
    'Expiration':   'expiry',
    'DTE':          'dte',
    'Days to ER':   'days_to_er',
    'Last Price':   'stock_price',
    '% Change':     'pct_change',
    'Mark':         'premium_mid',
    'ROC':          'roc_pct',
    'Annual Yield': 'ann_yield_pct',
    'Delta':        'delta',
    'Theta':        'theta',
    'Open Int':     'open_int',
    '% OTM':        'pct_otm',
    'Sector':       'sector',
    'Market Cap':   'market_cap_b',
    'Stock IV':     'stock_hv',
    'Option Score': 'score',
    'IV':           'option_iv',
    'Avg Vol':      'avg_vol',
    'Volume':       'volume',
    'Type':         'option_type',
}

# ─────────────────────────────────────────────
# CONFIG — init & panel
# ─────────────────────────────────────────────

def _init_cfg(force: bool = False):
    """Seed scanner_cfg session state from module-level defaults."""
    if force or 'scanner_cfg' not in st.session_state:
        st.session_state['scanner_cfg'] = {
            # Active bucket
            'active_dte_min':   ACTIVE_DTE_MIN,
            'active_dte_max':   ACTIVE_DTE_MAX,
            'active_roc_min':   ACTIVE_ROC_MIN,
            'active_delta_max': ACTIVE_DELTA_MAX,
            'active_score_min': ACTIVE_SCORE_MIN,
            # Core bucket
            'core_dte_min':     CORE_DTE_MIN,
            'core_dte_max':     CORE_DTE_MAX,
            'core_roc_min':     CORE_ROC_MIN,
            'core_delta_max':   CORE_DELTA_MAX,
            'core_score_min':   CORE_SCORE_MIN,
            # Common filters
            'market_cap_min':   MARKET_CAP_MIN,
            'open_int_min':     float(OPEN_INT_MIN),
            'volume_min':       float(VOLUME_MIN),
            'spread_max_pct':   SPREAD_MAX_PCT,
            # IV detection
            'iv_spike_thresh':  IV_SPIKE_THRESH,
            'iv_elev_thresh':   IV_ELEV_THRESH,
            # Portfolio
            'sector_cap_pct':   int(SECTOR_CAP * 100),   # stored as 0-100 int
            'max_positions':    MAX_POSITIONS,
            'cash_buffer_pct':  int(CASH_BUFFER * 100),  # stored as 0-100 int
            # Sizing (stored as 0-100 int percentages)
            't1_size_pct':      int(TIER_SIZE_PCT['T1'] * 100),
            't2_size_pct':      int(TIER_SIZE_PCT['T2'] * 100),
            't3_size_pct':      int(TIER_SIZE_PCT['T3'] * 100),
            'btc_t1_pct':       int(BTC_TARGET['T1'] * 100),
            'btc_t2_pct':       int(BTC_TARGET['T2'] * 100),
        }
    # Also reset all cfg_ widget keys so widgets pick up new values
    if force:
        for k in list(st.session_state.keys()):
            if k.startswith('cfg_'):
                del st.session_state[k]


def _sync_cfg_from_widgets():
    """Pull widget values (cfg_* keys) back into scanner_cfg dict."""
    cfg = st.session_state.get('scanner_cfg', {})
    mapping = {
        'cfg_active_dte':        ('active_dte_min', 'active_dte_max'),  # tuple range
        'cfg_active_roc_min':    'active_roc_min',
        'cfg_active_delta_max':  'active_delta_max',
        'cfg_active_score_min':  'active_score_min',
        'cfg_core_dte':          ('core_dte_min', 'core_dte_max'),
        'cfg_core_roc_min':      'core_roc_min',
        'cfg_core_delta_max':    'core_delta_max',
        'cfg_core_score_min':    'core_score_min',
        'cfg_market_cap_min':    'market_cap_min',
        'cfg_open_int_min':      'open_int_min',
        'cfg_volume_min':        'volume_min',
        'cfg_spread_max_pct':    'spread_max_pct',
        'cfg_iv_spike_thresh':   'iv_spike_thresh',
        'cfg_iv_elev_thresh':    'iv_elev_thresh',
        'cfg_sector_cap_pct':    'sector_cap_pct',
        'cfg_max_positions':     'max_positions',
        'cfg_cash_buffer_pct':   'cash_buffer_pct',
        'cfg_t1_size_pct':       't1_size_pct',
        'cfg_t2_size_pct':       't2_size_pct',
        'cfg_t3_size_pct':       't3_size_pct',
        'cfg_btc_t1_pct':        'btc_t1_pct',
        'cfg_btc_t2_pct':        'btc_t2_pct',
    }
    for widget_key, cfg_key in mapping.items():
        if widget_key in st.session_state:
            val = st.session_state[widget_key]
            if isinstance(cfg_key, tuple):
                cfg[cfg_key[0]], cfg[cfg_key[1]] = val[0], val[1]
            else:
                cfg[cfg_key] = val
    st.session_state['scanner_cfg'] = cfg


def _get_cfg() -> dict:
    """Return a resolved cfg dict with float conversions for percentages."""
    raw = st.session_state.get('scanner_cfg', {})
    if not raw:
        _init_cfg()
        raw = st.session_state['scanner_cfg']
    # Convert stored ints back to floats where needed
    return {
        **raw,
        'sector_cap':    raw.get('sector_cap_pct', 40) / 100.0,
        'cash_buffer':   raw.get('cash_buffer_pct', 5)  / 100.0,
        't1_size':       raw.get('t1_size_pct', 22)     / 100.0,
        't2_size':       raw.get('t2_size_pct', 15)     / 100.0,
        't3_size':       raw.get('t3_size_pct', 10)     / 100.0,
        'btc_t1':        raw.get('btc_t1_pct', 60)      / 100.0,
        'btc_t2':        raw.get('btc_t2_pct', 50)      / 100.0,
        'guru_list':     st.session_state.get('guru_list', []),
    }


def _render_config_panel():
    """Collapsible panel of all configurable ARGUS v6 thresholds."""
    _init_cfg()
    cfg = st.session_state['scanner_cfg']

    with st.expander("⚙️ Scan Parameters — click to adjust thresholds", expanded=False):
        tab_bucket, tab_common, tab_portfolio = st.tabs([
            "📊 Bucket Filters",
            "🔬 Common + IV",
            "💼 Portfolio + Sizing",
        ])

        # ── Tab 1: Bucket Filters ──────────────────────────────
        with tab_bucket:
            col_a, col_c = st.columns(2)

            with col_a:
                st.markdown("**🔵 Active Bucket (short-dated)**")
                st.slider(
                    "DTE range",
                    min_value=1, max_value=21,
                    value=(cfg['active_dte_min'], cfg['active_dte_max']),
                    key='cfg_active_dte',
                    help="Days-to-expiry window for Active bucket",
                )
                st.number_input(
                    "Min ROC %",
                    min_value=0.0, max_value=5.0,
                    value=float(cfg['active_roc_min']),
                    step=0.05, format="%.2f",
                    key='cfg_active_roc_min',
                    help="Minimum return on collateral (0.30 = 0.30%)",
                )
                st.slider(
                    "Max |Delta|",
                    min_value=0.05, max_value=0.50,
                    value=float(cfg['active_delta_max']),
                    step=0.01, format="%.2f",
                    key='cfg_active_delta_max',
                    help="Max absolute delta (proxy for PoP ≥ 80%)",
                )
                st.slider(
                    "Min Option Score",
                    min_value=50, max_value=100,
                    value=int(cfg['active_score_min']),
                    key='cfg_active_score_min',
                    help="ThetaScanner quality score minimum",
                )

            with col_c:
                st.markdown("**🟣 Core Bucket (longer-dated)**")
                st.slider(
                    "DTE range ",
                    min_value=15, max_value=60,
                    value=(cfg['core_dte_min'], cfg['core_dte_max']),
                    key='cfg_core_dte',
                    help="Days-to-expiry window for Core bucket",
                )
                st.number_input(
                    "Min ROC % ",
                    min_value=0.0, max_value=10.0,
                    value=float(cfg['core_roc_min']),
                    step=0.05, format="%.2f",
                    key='cfg_core_roc_min',
                    help="Minimum return on collateral (1.25 = 1.25%)",
                )
                st.slider(
                    "Max |Delta| ",
                    min_value=0.05, max_value=0.50,
                    value=float(cfg['core_delta_max']),
                    step=0.01, format="%.2f",
                    key='cfg_core_delta_max',
                    help="Max absolute delta for Core positions",
                )
                st.slider(
                    "Min Option Score ",
                    min_value=50, max_value=100,
                    value=int(cfg['core_score_min']),
                    key='cfg_core_score_min',
                    help="ThetaScanner quality score minimum (Core bar is higher)",
                )

        # ── Tab 2: Common + IV ─────────────────────────────────
        with tab_common:
            col_com, col_iv = st.columns(2)

            with col_com:
                st.markdown("**Common Filters (both buckets)**")
                st.number_input(
                    "Min Market Cap ($B)",
                    min_value=0.0, max_value=50.0,
                    value=float(cfg['market_cap_min']),
                    step=0.5,
                    key='cfg_market_cap_min',
                )
                st.number_input(
                    "Min Open Interest",
                    min_value=0.0, max_value=10000.0,
                    value=float(cfg['open_int_min']),
                    step=10.0,
                    key='cfg_open_int_min',
                )
                st.number_input(
                    "Min Volume",
                    min_value=0.0, max_value=5000.0,
                    value=float(cfg['volume_min']),
                    step=10.0,
                    key='cfg_volume_min',
                )
                st.number_input(
                    "Max Bid-Ask Spread %",
                    min_value=0.0, max_value=25.0,
                    value=float(cfg['spread_max_pct']),
                    step=0.5,
                    key='cfg_spread_max_pct',
                    help="As % of midpoint price. Gate skipped if bid/ask columns absent.",
                )

            with col_iv:
                st.markdown("**IV Spike Detection**")
                st.slider(
                    "IV Spike threshold %",
                    min_value=10, max_value=100,
                    value=int(cfg['iv_spike_thresh']),
                    key='cfg_iv_spike_thresh',
                    help="Option IV vs stock HV premium % above this → SPIKE (always T2)",
                )
                st.slider(
                    "IV Elevated threshold %",
                    min_value=1, max_value=40,
                    value=int(cfg['iv_elev_thresh']),
                    key='cfg_iv_elev_thresh',
                    help="Option IV vs stock HV premium % above this → ELEVATED (T1 if guru, else T2)",
                )
                st.markdown("""
                **IV classification:**
                - `> spike thresh` → 🔴 SPIKE (always T2)
                - `> elev thresh` → 🟡 ELEVATED
                - `within ±elev` → 🟢 NORMAL
                - `< -elev thresh` → ⚪ COMPRESSED (T3 only)
                """)

        # ── Tab 3: Portfolio + Sizing ──────────────────────────
        with tab_portfolio:
            col_port, col_size = st.columns(2)

            with col_port:
                st.markdown("**Portfolio Constraints**")
                st.slider(
                    "Max open positions",
                    min_value=1, max_value=10,
                    value=int(cfg['max_positions']),
                    key='cfg_max_positions',
                )
                st.slider(
                    "Sector cap %",
                    min_value=10, max_value=60,
                    value=int(cfg['sector_cap_pct']),
                    key='cfg_sector_cap_pct',
                    help="Maximum % of total capital in any single GICS sector",
                )
                st.slider(
                    "Cash reserve %",
                    min_value=0, max_value=20,
                    value=int(cfg['cash_buffer_pct']),
                    key='cfg_cash_buffer_pct',
                    help="Minimum liquidity buffer — never deployed",
                )
                _guru_loaded = st.session_state.get('guru_list', [])
                if _guru_loaded:
                    st.caption(
                        f"Guru list active: **{len(_guru_loaded)} tickers** loaded — "
                        "upload a new file in Data Input to change"
                    )
                else:
                    st.caption(
                        "No guru list loaded — upload the community CSV in the "
                        "Data Input section below to enable T1 guru-path candidates"
                    )

            with col_size:
                st.markdown("**Position Sizing (% of available capital)**")
                st.slider("T1 max size %", 5, 40, int(cfg['t1_size_pct']),
                          key='cfg_t1_size_pct', help="Highest conviction — auto-execute")
                st.slider("T2 max size %", 5, 30, int(cfg['t2_size_pct']),
                          key='cfg_t2_size_pct', help="Review required — conservative sizing")
                st.slider("T3 max size %", 5, 20, int(cfg['t3_size_pct']),
                          key='cfg_t3_size_pct', help="Backup — minimal bet")

                st.markdown("**BTC Profit Targets**")
                st.slider("T1 BTC target %", 40, 80, int(cfg['btc_t1_pct']),
                          key='cfg_btc_t1_pct', help="Close position when this % of premium collected")
                st.slider("T2 BTC target %", 40, 70, int(cfg['btc_t2_pct']),
                          key='cfg_btc_t2_pct')

        st.markdown("---")
        col_rst, col_note = st.columns([1, 3])
        with col_rst:
            if st.button("↺ Reset to Defaults", key='cfg_reset_btn'):
                _init_cfg(force=True)
                st.rerun()
        with col_note:
            st.caption("Changes apply on next scan. Reset restores ARGUS v6 spec defaults.")

    # Sync widget values → cfg dict after expander renders
    _sync_cfg_from_widgets()


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _fmt_pct(v, decimals=2):
    try:
        return f"{float(v):.{decimals}f}%"
    except Exception:
        return "—"

def _fmt_cur(v):
    try:
        return f"${float(v):,.2f}"
    except Exception:
        return "—"

def _fmt_num(v, decimals=0):
    try:
        return f"{float(v):,.{decimals}f}"
    except Exception:
        return "—"

def _pop_from_delta(delta_abs: float) -> float:
    """Approx PoP from absolute delta: PoP ≈ (1 - delta) * 100"""
    return round((1.0 - min(delta_abs, 1.0)) * 100, 1)

def _occ_contract(ticker: str, expiry, strike: float, opt_type: str = 'P') -> str:
    """Generate OCC-standard contract name: VALE260131P10"""
    try:
        dt = pd.to_datetime(expiry)
        date_str = dt.strftime('%y%m%d')
    except Exception:
        date_str = str(expiry)[:6]
    try:
        strike_val = int(strike) if float(strike) == int(float(strike)) else float(strike)
    except Exception:
        strike_val = strike
    return f"{ticker.upper()}{date_str}{opt_type.upper()}{strike_val}"

def _iv_status(option_iv: float, stock_hv: float, cfg: dict) -> tuple:
    """
    Classify IV vs stock HV using cfg thresholds.
    Returns (status_label, iv_premium_pct)
    """
    if stock_hv is None or stock_hv == 0 or pd.isna(stock_hv):
        return 'UNKNOWN', 0.0
    premium = ((option_iv - stock_hv) / stock_hv) * 100
    spike_t = cfg.get('iv_spike_thresh', IV_SPIKE_THRESH)
    elev_t  = cfg.get('iv_elev_thresh',  IV_ELEV_THRESH)
    if premium > spike_t:
        return 'SPIKE', premium
    elif premium > elev_t:
        return 'ELEVATED', premium
    elif premium > -elev_t:
        return 'NORMAL', premium
    else:
        return 'COMPRESSED', premium

def _iv_badge(status: str) -> str:
    return {
        'SPIKE':      '🔴 SPIKE',
        'ELEVATED':   '🟡 ELEVATED',
        'NORMAL':     '🟢 NORMAL',
        'COMPRESSED': '⚪ COMPRESSED',
        'UNKNOWN':    '❓ N/A',
    }.get(status, status)

def _make_rejection(gate: int, gate_name: str, row: pd.Series,
                    reason: str, actual: str, threshold: str) -> dict:
    """Build a rejection record dict."""
    return {
        'Gate':      gate,
        'Gate Name': gate_name,
        'Ticker':    str(row.get('ticker', '')).upper(),
        'Strike':    row.get('strike', '—'),
        'Expiry':    str(row.get('expiry', '—')),
        'Reason':    reason,
        'Actual':    actual,
        'Threshold': threshold,
    }

# ─────────────────────────────────────────────
# GURU LIST PARSING
# ─────────────────────────────────────────────

def _parse_guru_file(uploaded_file):
    """
    Parse community guru list CSV.
    Expected format: Ticker, Industry, Company, Sector, Ideal Sell Put Strike
    Handles duplicate rows (keeps first occurrence which has the strike data).
    Returns:
        tickers   : list[str]        — deduplicated uppercase ticker symbols
        strike_map: dict[str, float] — ticker -> community suggested strike (0.0 if not provided)
    """
    try:
        content = uploaded_file.read()
        df = pd.read_csv(io.BytesIO(content))
        df.columns = [c.strip() for c in df.columns]

        ticker_col = df.columns[0]  # Always first column
        strike_col = next((c for c in df.columns if 'strike' in c.lower()), None)

        tickers    = []
        strike_map = {}
        seen       = set()

        for _, row in df.iterrows():
            tkr = str(row[ticker_col]).strip().upper()
            if not tkr or tkr in ('TICKER', 'NAN', ''):
                continue
            if tkr in seen:
                continue  # deduplicate — first occurrence has the strike data
            seen.add(tkr)
            tickers.append(tkr)

            if strike_col:
                raw = str(row.get(strike_col, '')).replace('$', '').strip()
                try:
                    strike_map[tkr] = float(raw) if raw and raw.lower() != 'nan' else 0.0
                except ValueError:
                    strike_map[tkr] = 0.0

        return tickers, strike_map

    except Exception as e:
        st.error(f"Guru list parse error: {e}")
        return [], {}


# ─────────────────────────────────────────────
# CSV PARSING
# ─────────────────────────────────────────────

def _parse_csv(uploaded_file) -> Optional[pd.DataFrame]:
    """Parse ThetaScanner CSV, normalise columns, return DataFrame or None."""
    try:
        content = uploaded_file.read()
        df = pd.read_csv(io.BytesIO(content))
        df.columns = [c.strip() for c in df.columns]
        rename = {k: v for k, v in CSV_COLUMN_MAP.items() if k in df.columns}
        df = df.rename(columns=rename)
        for col in ['strike', 'dte', 'days_to_er', 'stock_price', 'premium_mid',
                    'roc_pct', 'ann_yield_pct', 'delta', 'theta', 'open_int',
                    'market_cap_b', 'stock_hv', 'option_iv', 'score', 'volume', 'avg_vol']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        if 'option_type' in df.columns:
            df['option_type'] = df['option_type'].str.lower().str.strip()
        return df
    except Exception as e:
        st.error(f"❌ CSV parse error: {e}")
        return None

def _filter_to_tickers(df: pd.DataFrame, ticker_text: str) -> pd.DataFrame:
    """Filter DataFrame to only rows matching tickers in the text input."""
    tickers = [t.strip().upper() for t in ticker_text.replace(',', ' ').split() if t.strip()]
    if not tickers:
        return df
    return df[df['ticker'].str.upper().isin(tickers)].copy()

# ─────────────────────────────────────────────
# 5-GATE FILTER ENGINE  (with rejection tracking)
# ─────────────────────────────────────────────

def _apply_filters(df: pd.DataFrame, bucket: str, cfg: dict) -> tuple:
    """
    Apply all 5 gates sequentially.
    Returns (passing_df, funnel_stats, rejections_list).
    bucket: 'Active' | 'Core'
    """
    stats      = {'input': len(df)}
    rejections = []
    remaining  = df.copy()

    # Resolve bucket thresholds from cfg
    if bucket == 'Active':
        dte_min   = cfg.get('active_dte_min',   ACTIVE_DTE_MIN)
        dte_max   = cfg.get('active_dte_max',   ACTIVE_DTE_MAX)
        roc_min   = cfg.get('active_roc_min',   ACTIVE_ROC_MIN)
        delta_max = cfg.get('active_delta_max', ACTIVE_DELTA_MAX)
        score_min = cfg.get('active_score_min', ACTIVE_SCORE_MIN)
    else:
        dte_min   = cfg.get('core_dte_min',   CORE_DTE_MIN)
        dte_max   = cfg.get('core_dte_max',   CORE_DTE_MAX)
        roc_min   = cfg.get('core_roc_min',   CORE_ROC_MIN)
        delta_max = cfg.get('core_delta_max', CORE_DELTA_MAX)
        score_min = cfg.get('core_score_min', CORE_SCORE_MIN)

    mkt_cap_min   = cfg.get('market_cap_min',  MARKET_CAP_MIN)
    open_int_min  = cfg.get('open_int_min',    OPEN_INT_MIN)
    volume_min    = cfg.get('volume_min',      VOLUME_MIN)
    spread_max    = cfg.get('spread_max_pct',  SPREAD_MAX_PCT)

    # ── Gate 1: Instrument Quality ─────────────────────────────
    GATE1 = 'Instrument Quality'

    if 'option_type' in remaining.columns:
        dropped = remaining[remaining['option_type'] != 'put']
        for _, r in dropped.iterrows():
            rejections.append(_make_rejection(
                1, GATE1, r, 'Not a put option',
                str(r.get('option_type', '?')), 'put'))
        remaining = remaining[remaining['option_type'] == 'put']

    if 'market_cap_b' in remaining.columns:
        dropped = remaining[remaining['market_cap_b'] < mkt_cap_min]
        for _, r in dropped.iterrows():
            val = r.get('market_cap_b', 0)
            rejections.append(_make_rejection(
                1, GATE1, r,
                f'Market cap below minimum',
                f'${val:.2f}B', f'≥${mkt_cap_min:.1f}B'))
        remaining = remaining[remaining['market_cap_b'] >= mkt_cap_min]

    if 'open_int' in remaining.columns:
        dropped = remaining[remaining['open_int'] < open_int_min]
        for _, r in dropped.iterrows():
            rejections.append(_make_rejection(
                1, GATE1, r, 'Open interest below minimum',
                str(int(r.get('open_int', 0))), f'≥{int(open_int_min)}'))
        remaining = remaining[remaining['open_int'] >= open_int_min]

    if 'volume' in remaining.columns:
        dropped = remaining[remaining['volume'] < volume_min]
        for _, r in dropped.iterrows():
            rejections.append(_make_rejection(
                1, GATE1, r, 'Volume below minimum',
                str(int(r.get('volume', 0))), f'≥{int(volume_min)}'))
        remaining = remaining[remaining['volume'] >= volume_min]

    stats['after_gate1'] = len(remaining)

    # ── Gate 2: Bucket (DTE + Yield + Delta + Score) ───────────
    GATE2 = f'Bucket ({bucket})'

    if 'dte' in remaining.columns:
        dropped = remaining[(remaining['dte'] < dte_min) | (remaining['dte'] > dte_max)]
        for _, r in dropped.iterrows():
            rejections.append(_make_rejection(
                2, GATE2, r, 'DTE outside bucket range',
                str(int(r.get('dte', 0))), f'{dte_min}–{dte_max} days'))
        remaining = remaining[(remaining['dte'] >= dte_min) & (remaining['dte'] <= dte_max)]

    if 'roc_pct' in remaining.columns:
        dropped = remaining[remaining['roc_pct'] < roc_min]
        for _, r in dropped.iterrows():
            rejections.append(_make_rejection(
                2, GATE2, r, 'ROC below minimum yield',
                f"{r.get('roc_pct', 0):.3f}%", f'≥{roc_min:.2f}%'))
        remaining = remaining[remaining['roc_pct'] >= roc_min]

    if 'delta' in remaining.columns:
        dropped = remaining[remaining['delta'].abs() > delta_max]
        for _, r in dropped.iterrows():
            rejections.append(_make_rejection(
                2, GATE2, r, '|Delta| exceeds maximum',
                f"{abs(r.get('delta', 0)):.3f}", f'≤{delta_max:.2f}'))
        remaining = remaining[remaining['delta'].abs() <= delta_max]

    if 'score' in remaining.columns:
        dropped = remaining[remaining['score'] < score_min]
        for _, r in dropped.iterrows():
            rejections.append(_make_rejection(
                2, GATE2, r, 'Option Score below minimum',
                str(int(r.get('score', 0))), f'≥{score_min}'))
        remaining = remaining[remaining['score'] >= score_min]

    stats['after_gate2'] = len(remaining)

    # ── Gate 3: Earnings Safety ─────────────────────────────────
    GATE3 = 'Earnings Safety'

    if 'days_to_er' in remaining.columns and 'dte' in remaining.columns:
        # Rows with ER scheduled AND inside the holding window
        has_er       = remaining['days_to_er'].notna()
        er_too_close = has_er & (remaining['days_to_er'] <= remaining['dte'])
        dropped = remaining[er_too_close]
        for _, r in dropped.iterrows():
            rejections.append(_make_rejection(
                3, GATE3, r,
                'Earnings inside holding period',
                f"{int(r.get('days_to_er', 0))} days to ER",
                f'>{int(r.get("dte", 0))} days required'))
        remaining = remaining[~er_too_close]

    stats['after_gate3'] = len(remaining)

    # ── Gate 4: Portfolio Fit ───────────────────────────────────
    # Pass-through here — actual per-row check done in _assign_tiers()
    # which also appends to the same rejections list (gate=6)
    stats['after_gate4'] = len(remaining)

    # ── Gate 5: Bid-Ask Spread ──────────────────────────────────
    GATE5 = 'Bid-Ask Spread'

    if 'bid' in remaining.columns and 'ask' in remaining.columns and 'premium_mid' in remaining.columns:
        mid_safe   = remaining['premium_mid'].replace(0, float('nan'))
        spread_pct = ((remaining['ask'] - remaining['bid']) / mid_safe) * 100
        too_wide   = spread_pct > spread_max
        dropped    = remaining[too_wide]
        for _, r in dropped.iterrows():
            sp = ((r.get('ask', 0) - r.get('bid', 0)) /
                  (r.get('premium_mid', 1) or 1)) * 100
            rejections.append(_make_rejection(
                5, GATE5, r, 'Bid-ask spread too wide',
                f'{sp:.1f}%', f'≤{spread_max:.1f}%'))
        remaining = remaining[~too_wide]
    # else: gate skipped (columns absent) — no rejection records added

    stats['after_gate5'] = len(remaining)

    return remaining, stats, rejections

# ─────────────────────────────────────────────
# TIER ASSIGNMENT  (with rejection tracking)
# ─────────────────────────────────────────────

def _assign_tiers(df: pd.DataFrame, cfg: dict, rejections: list, bucket: str = 'Core') -> pd.DataFrame:
    """
    Add tier, iv_status, iv_premium, guru, flags columns.
    Also appends portfolio-fit rejections (gate 6) to the rejections list.
    bucket='Active' or 'Core' — controls score thresholds and tier logic.
    """
    result = df.copy()

    tiers, iv_statuses, iv_premiums, gurus, flags_list = [], [], [], [], []

    open_tickers     = set()
    open_sectors     = {}
    existing_count   = 0
    portfolio_deposit = st.session_state.get('portfolio_deposit', 0)

    df_open = st.session_state.get('df_open')
    if df_open is not None and len(df_open) > 0:
        existing_count = len(df_open)
        if 'Ticker' in df_open.columns:
            open_tickers = set(df_open['Ticker'].str.upper().tolist())
        if 'Sector' in df_open.columns and 'Strike' in df_open.columns:
            for _, row in df_open.iterrows():
                sec = str(row.get('Sector', 'Unknown'))
                try:
                    qty = abs(float(row.get('Quantity', row.get('Contracts', 1))))
                    collateral = float(row.get('Strike', 0)) * 100 * qty
                except Exception:
                    collateral = 0
                open_sectors[sec] = open_sectors.get(sec, 0) + collateral

    # Resolved thresholds from cfg — bucket-specific
    guru_list  = cfg.get('guru_list', GURU_LIST)
    sector_cap = cfg.get('sector_cap', SECTOR_CAP)   # float 0-1

    if bucket == 'Active':
        # Active (6–10 DTE): tactical, momentum-driven — lower score bar, guru optional for T1
        score_t1_min = cfg.get('active_score_min', ACTIVE_SCORE_MIN)        # 73
        score_t2_min = max(cfg.get('active_score_min', ACTIVE_SCORE_MIN) - 3, 60)  # 70
    else:
        # Core (30–45 DTE): income wheel — guru validation required for T1, higher score bar
        score_t1_min = cfg.get('core_score_min', CORE_SCORE_MIN)            # 75
        score_t2_min = cfg.get('active_score_min', ACTIVE_SCORE_MIN)        # 73

    GATE_PORT = 'Portfolio Fit'

    for _, row in result.iterrows():
        ticker  = str(row.get('ticker', '')).upper()
        opt_iv  = row.get('option_iv', float('nan'))
        stock_hv= row.get('stock_hv',  float('nan'))
        score   = row.get('score', 0)
        sector  = str(row.get('sector', 'Unknown'))
        strike  = row.get('strike', 0)

        flags = []

        # IV status
        if pd.notna(opt_iv) and pd.notna(stock_hv) and stock_hv > 0:
            iv_s, iv_prem = _iv_status(float(opt_iv), float(stock_hv), cfg)
        else:
            iv_s, iv_prem = 'UNKNOWN', 0.0
        iv_statuses.append(iv_s)
        iv_premiums.append(round(iv_prem, 1))

        # Guru check
        is_guru = ticker in guru_list
        gurus.append(is_guru)
        if not is_guru:
            flags.append('No guru validation')

        # IV flags
        if iv_s == 'SPIKE':
            flags.append(f'⚠️ IV Spike +{iv_prem:.1f}% vs HV')
        elif iv_s == 'ELEVATED':
            flags.append(f'📊 IV Elevated +{iv_prem:.1f}% vs HV')
        elif iv_s == 'COMPRESSED':
            flags.append('IV Compressed — thin premium')

        # Portfolio fit checks
        # Note: portfolio_full intentionally excluded — scan is advisory only;
        # the user decides whether to execute regardless of current position count.
        dupe_ticker     = ticker in open_tickers
        sector_breached = False

        if portfolio_deposit > 0 and strike > 0:
            new_sector_total = open_sectors.get(sector, 0) + (strike * 100)
            sector_pct       = new_sector_total / portfolio_deposit
            if sector_pct > sector_cap:
                sector_breached = True
                flags.append(f'🔴 Sector cap: {sector} → {sector_pct*100:.0f}% (limit {sector_cap*100:.0f}%)')

        if dupe_ticker:
            flags.append(f'⚠️ Already holding {ticker}')

        # ── Tier logic — bucket-specific ──────────────────────────
        blocked = dupe_ticker or sector_breached

        if blocked:
            tier = 'BLOCKED'
            # Record as gate-6 rejection
            if dupe_ticker:
                rejections.append(_make_rejection(6, GATE_PORT, row,
                                                  f'Already holding {ticker}',
                                                  ticker, 'No duplicates'))
            elif sector_breached:
                rejections.append(_make_rejection(6, GATE_PORT, row,
                                                  f'{sector} sector cap breached',
                                                  f'{sector_pct*100:.0f}%',
                                                  f'≤{sector_cap*100:.0f}%'))

        elif bucket == 'Active':
            # Active T1: high score + acceptable IV (guru is a bonus, not a gate)
            if score >= score_t1_min and iv_s in ('NORMAL', 'ELEVATED', 'SPIKE', 'UNKNOWN'):
                tier = 'T1'
                if is_guru:
                    flags.append('✅ Guru validated')
            elif score >= score_t2_min and iv_s not in ('COMPRESSED',):
                tier = 'T2'
            else:
                tier = 'T3'

        else:
            # Core T1: guru validation required + good IV + high score
            if is_guru and iv_s in ('NORMAL', 'ELEVATED') and score >= score_t1_min:
                tier = 'T1'
            elif score >= score_t2_min and iv_s not in ('SPIKE', 'COMPRESSED'):
                tier = 'T2'
            else:
                tier = 'T3'

        tiers.append(tier)
        flags_list.append(' | '.join(flags))

    result['tier']       = tiers
    result['iv_status']  = iv_statuses
    result['iv_premium'] = iv_premiums
    result['guru']       = gurus
    result['flags']      = flags_list
    return result

# ─────────────────────────────────────────────
# POSITION SIZING
# ─────────────────────────────────────────────

def _compute_sizing(df: pd.DataFrame, portfolio_deposit: float, cfg: dict) -> pd.DataFrame:
    """Add contracts, collateral, premium_total, util_delta columns."""
    result   = df.copy()
    cash_buf = cfg.get('cash_buffer', CASH_BUFFER)
    available = portfolio_deposit * (1 - cash_buf)
    tier_sizes = {
        'T1': cfg.get('t1_size', TIER_SIZE_PCT['T1']),
        'T2': cfg.get('t2_size', TIER_SIZE_PCT['T2']),
        'T3': cfg.get('t3_size', TIER_SIZE_PCT['T3']),
    }
    rows = []
    for _, row in result.iterrows():
        tier     = row.get('tier', 'T3')
        strike   = float(row.get('strike', 0))
        prem     = float(row.get('premium_mid', 0))
        size_pct = tier_sizes.get(tier, tier_sizes['T3'])
        max_coll = available * size_pct
        if strike > 0:
            contracts  = max(1, int(max_coll / (strike * 100)))
            collateral = contracts * strike * 100
            prem_total = contracts * prem * 100
            util_delta = (collateral / portfolio_deposit * 100) if portfolio_deposit > 0 else 0
        else:
            contracts, collateral, prem_total, util_delta = 0, 0, 0, 0
        rows.append({
            'contracts':  contracts,
            'collateral': collateral,
            'prem_total': prem_total,
            'util_delta': round(util_delta, 1),
        })
    sizing_df = pd.DataFrame(rows, index=result.index)
    return pd.concat([result, sizing_df], axis=1)

# ─────────────────────────────────────────────
# SCAN ORCHESTRATOR
# ─────────────────────────────────────────────

def _run_scan(df_raw: pd.DataFrame, ticker_text: str, bucket: str) -> tuple:
    """
    Full scan pipeline — Active and Core are kept entirely separate.
    Returns (results_active, results_core, funnel_stats, rejections).
      results_active / results_core: dict with keys t1/t2/t3/blocked/funnel, or None
    """
    cfg               = _get_cfg()
    portfolio_deposit = st.session_state.get('portfolio_deposit', 0)
    all_rejections    = []

    if ticker_text.strip():
        df_raw = _filter_to_tickers(df_raw, ticker_text)

    def _run_single_bucket(df, bkt):
        """Run full pipeline for one bucket. Returns (bucket_result_dict, stats)."""
        df_f, stats, rej = _apply_filters(df, bkt, cfg)
        all_rejections.extend(rej)
        if df_f.empty:
            _empty = pd.DataFrame()
            return {'t1': _empty, 't2': _empty, 't3': _empty, 'blocked': _empty, 'funnel': stats}, stats
        df_f = df_f.copy()
        df_f['_bucket'] = bkt
        df_t = _assign_tiers(df_f, cfg, all_rejections, bucket=bkt)
        df_s = _compute_sizing(df_t, portfolio_deposit, cfg)
        res = {
            't1':      df_s[df_s['tier'] == 'T1'].sort_values('ann_yield_pct', ascending=False),
            't2':      df_s[df_s['tier'] == 'T2'].sort_values('ann_yield_pct', ascending=False),
            't3':      df_s[df_s['tier'] == 'T3'].sort_values('ann_yield_pct', ascending=False),
            'blocked': df_s[df_s['tier'] == 'BLOCKED'],
            'funnel':  stats,
        }
        stats['after_tier'] = len(res['t1']) + len(res['t2']) + len(res['t3'])
        return res, stats

    if bucket == 'Both':
        res_a, stats_a = _run_single_bucket(df_raw, 'Active')
        res_c, stats_c = _run_single_bucket(df_raw, 'Core')
        _sa = stats_a or {}
        _sc = stats_c or {}
        combined_funnel = {
            'input':        _sa.get('input', 0),
            'after_gate1':  _sa.get('after_gate1', 0) + _sc.get('after_gate1', 0),
            'after_gate2':  _sa.get('after_gate2', 0) + _sc.get('after_gate2', 0),
            'after_gate3':  _sa.get('after_gate3', 0) + _sc.get('after_gate3', 0),
            'after_gate4':  _sa.get('after_gate4', 0) + _sc.get('after_gate4', 0),
            'after_gate5':  _sa.get('after_gate5', 0) + _sc.get('after_gate5', 0),
            'after_tier':   (_sa.get('after_tier', 0) + _sc.get('after_tier', 0)),
        }
        return res_a, res_c, combined_funnel, all_rejections

    elif 'Active' in bucket:
        res, stats = _run_single_bucket(df_raw, 'Active')
        return res, None, stats or {}, all_rejections

    else:
        res, stats = _run_single_bucket(df_raw, 'Core')
        return None, res, stats or {}, all_rejections

# ─────────────────────────────────────────────
# UI — FUNNEL SUMMARY
# ─────────────────────────────────────────────

def _render_funnel_summary(stats: dict):
    st.markdown("#### 🔽 Scan Funnel")
    labels = [
        ('Input',              'input'),
        ('Gate 1\nQuality',    'after_gate1'),
        ('Gate 2\nBucket',     'after_gate2'),
        ('Gate 3\nEarnings',   'after_gate3'),
        ('Gate 4\nPortfolio',  'after_gate4'),
        ('Gate 5\nSpread',     'after_gate5'),
        ('Final\nPassed',      'after_tier'),
    ]
    cols = st.columns(len(labels))
    prev = None
    for col, (label, key) in zip(cols, labels):
        val = stats.get(key, '—')
        if prev is not None and isinstance(val, int) and isinstance(prev, int):
            dropped = prev - val
            col.metric(label, val, delta=f"−{dropped}" if dropped else "✓",
                       delta_color="inverse" if dropped else "off")
        else:
            col.metric(label, val)
        prev = val if isinstance(val, int) else prev

# ─────────────────────────────────────────────
# UI — REJECTION BREAKDOWN
# ─────────────────────────────────────────────

def _render_rejection_breakdown(rejections: list):
    """Gate-by-gate rejection detail — collapsible expander."""
    if not rejections:
        return

    total = len(rejections)
    with st.expander(f"🔎 Gate-by-Gate Rejection Detail — {total} rows excluded", expanded=False):
        st.caption(
            "Every row dropped by each gate is listed below with the exact metric "
            "that caused rejection vs the active threshold."
        )

        # Organise by gate
        gate_defs = [
            (1, 'Gate 1 — Instrument Quality'),
            (2, 'Gate 2 — Bucket Thresholds'),
            (3, 'Gate 3 — Earnings Safety'),
            (5, 'Gate 5 — Bid-Ask Spread'),
            (6, 'Gate 6 — Portfolio Fit'),
        ]

        gate_tabs = st.tabs([f"{label} ({sum(1 for r in rejections if r['Gate']==gnum)})"
                              for gnum, label in gate_defs])

        for tab, (gnum, gate_label) in zip(gate_tabs, gate_defs):
            with tab:
                gate_rows = [r for r in rejections if r['Gate'] == gnum]
                if not gate_rows:
                    st.caption("✅ No rows dropped at this gate.")
                else:
                    st.caption(f"**{len(gate_rows)} row(s) dropped**")

                    # Gate-specific hint
                    hints = {
                        1: "Rows excluded because they failed basic liquidity/instrument checks.",
                        2: "Rows excluded because DTE, ROC%, Delta, or Score didn't meet bucket minimums.",
                        3: "Rows excluded because earnings fall inside the planned holding period.",
                        5: "Rows excluded because the bid-ask spread is too wide (liquidity risk).",
                        6: "Rows that passed all 5 gates but were blocked by portfolio constraints.",
                    }
                    st.info(hints.get(gnum, ""))

                    display = pd.DataFrame([{
                        'Ticker':    r['Ticker'],
                        'Strike':    r['Strike'],
                        'Expiry':    r['Expiry'],
                        'Reason':    r['Reason'],
                        'Actual':    r['Actual'],
                        'Required':  r['Threshold'],
                    } for r in gate_rows])
                    st.dataframe(display, use_container_width=True, hide_index=True)

        # Combined table
        st.markdown("---")
        st.markdown("**📋 All Rejections Combined**")
        all_df = pd.DataFrame([{
            'Gate':      r['Gate'],
            'Gate Name': r['Gate Name'],
            'Ticker':    r['Ticker'],
            'Strike':    r['Strike'],
            'Expiry':    r['Expiry'],
            'Reason':    r['Reason'],
            'Actual':    r['Actual'],
            'Required':  r['Threshold'],
        } for r in rejections]).sort_values('Gate')
        st.dataframe(all_df, use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# UI — DEEP DIVE EXPANDER
# ─────────────────────────────────────────────

def _render_deep_dive(row: pd.Series, portfolio_deposit: float, cfg: dict):
    """Render the CIO / CFO / ARGUS Verdict deep dive for a single candidate."""
    ticker    = str(row.get('ticker', '')).upper()
    strike    = row.get('strike', 0)
    expiry    = row.get('expiry', '')
    dte       = row.get('dte', 0)
    roc       = row.get('roc_pct', 0)
    ann_yield = row.get('ann_yield_pct', 0)
    delta_raw = row.get('delta', 0)
    premium   = row.get('premium_mid', 0)
    score     = row.get('score', 0)
    sector    = row.get('sector', '—')
    tier      = row.get('tier', 'T3')
    iv_s      = row.get('iv_status', 'UNKNOWN')
    iv_prem   = row.get('iv_premium', 0)
    opt_iv    = row.get('option_iv', float('nan'))
    stock_hv  = row.get('stock_hv', float('nan'))
    contracts = int(row.get('contracts', 0))
    collateral= row.get('collateral', 0)
    prem_total= row.get('prem_total', 0)
    util_delta= row.get('util_delta', 0)
    is_guru   = row.get('guru', False)
    flags     = row.get('flags', '')
    bucket    = row.get('_bucket', '—')

    pop       = _pop_from_delta(abs(float(delta_raw)) if delta_raw else 0)
    occ       = _occ_contract(ticker, expiry, strike)
    limit_px  = round(float(premium) - 0.05, 2) if float(premium) >= 0.50 else float(premium)

    # Resolved from cfg
    cash_buf   = cfg.get('cash_buffer',    CASH_BUFFER)
    sector_cap = cfg.get('sector_cap',     SECTOR_CAP)
    score_t1   = cfg.get('core_score_min', CORE_SCORE_MIN)
    btc_t1     = cfg.get('btc_t1',        BTC_TARGET['T1'])
    btc_t2     = cfg.get('btc_t2',        BTC_TARGET['T2'])
    btc_rates  = {'T1': btc_t1, 'T2': btc_t2, 'T3': 0.50}
    t1_sz      = cfg.get('t1_size',       TIER_SIZE_PCT['T1'])
    t2_sz      = cfg.get('t2_size',       TIER_SIZE_PCT['T2'])
    t3_sz      = cfg.get('t3_size',       TIER_SIZE_PCT['T3'])
    sizes      = {'T1': t1_sz, 'T2': t2_sz, 'T3': t3_sz}

    btc_rate   = btc_rates.get(tier, 0.50)
    btc_px     = round(float(premium) * btc_rate, 2)
    available  = portfolio_deposit * (1 - cash_buf)
    size_pct   = sizes.get(tier, t3_sz)

    tier_label = {'T1': '🟢 T1 AUTO-EXECUTE', 'T2': '🟡 T2 REVIEW', 'T3': '⚪ T3 BACKUP'}.get(tier, tier)

    tab_cio, tab_cfo, tab_verdict = st.tabs(["📋 CIO View", "💼 CFO View", "⚖️ ARGUS Verdict"])

    with tab_cio:
        st.markdown(f"**Trade Setup — {ticker} {bucket} Bucket**")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Strike",    _fmt_cur(strike))
        c2.metric("Expiry",    str(expiry))
        c3.metric("DTE",       f"{dte} days")
        c4.metric("Bucket",    bucket)

        st.markdown("---")
        st.markdown("**Yield Analysis**")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("ROC",          _fmt_pct(roc))
        c2.metric("Annualised",   _fmt_pct(ann_yield))
        c3.metric("PoP",          f"{pop}%")
        c4.metric("Option Score", f"{score}/100")

        st.markdown("---")
        st.markdown("**IV Analysis**")
        c1, c2, c3 = st.columns(3)
        c1.metric("IV Status",      _iv_badge(iv_s))
        c2.metric("Option IV",      _fmt_pct(opt_iv) if pd.notna(opt_iv) else "—")
        c3.metric("Stock HV (30D)", _fmt_pct(stock_hv) if pd.notna(stock_hv) else "—")
        if iv_prem != 0:
            sign = "+" if iv_prem > 0 else ""
            st.caption(f"IV Premium vs HV: {sign}{iv_prem:.1f}%")

        st.markdown("---")
        st.markdown("**Earnings Check**")
        days_er = row.get('days_to_er', float('nan'))
        if pd.isna(days_er):
            st.success("✅ No earnings scheduled")
        else:
            st.success(f"✅ Earnings in {int(days_er)} days (outside {dte}-day hold)")

        st.markdown("---")
        st.markdown("**Guru Status**")
        guru_list   = cfg.get('guru_list', GURU_LIST)
        strike_map  = st.session_state.get('guru_strike_map', {})
        guru_strike = strike_map.get(ticker, 0.0)
        if is_guru:
            strike_hint = f" — community target: **${guru_strike:.0f}**" if guru_strike > 0 else ""
            st.success(f"✅ {ticker} is on the GURU list{strike_hint} — higher conviction")
        else:
            guru_label = f"{len(guru_list)} tickers loaded" if guru_list else "no list loaded"
            st.warning(f"⚠️ {ticker} not on GURU list ({guru_label}) — moderate conviction only")

    with tab_cfo:
        st.markdown(f"**Position Sizing — {tier_label}**")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Contracts",      str(contracts))
        c2.metric("Collateral",     _fmt_cur(collateral))
        c3.metric("% of Capital",   _fmt_pct(collateral / portfolio_deposit * 100 if portfolio_deposit else 0))
        c4.metric("Premium Coll.",  _fmt_cur(prem_total))

        st.caption(f"Max allocation for {tier}: {size_pct*100:.0f}% of available capital ({_fmt_cur(available * size_pct)})")

        st.markdown("---")
        st.markdown("**Margin Impact**")
        df_open = st.session_state.get('df_open')
        current_collateral = 0
        if df_open is not None and 'Strike' in df_open.columns:
            for _, p in df_open.iterrows():
                try:
                    qty = abs(float(p.get('Quantity', p.get('Contracts', 1))))
                    s   = float(p.get('Strike', 0))
                    current_collateral += s * 100 * qty
                except Exception:
                    pass
        current_util = (current_collateral / portfolio_deposit * 100) if portfolio_deposit else 0
        new_util     = current_util + util_delta

        c1, c2, c3 = st.columns(3)
        c1.metric("Current Utilisation", f"{current_util:.1f}%")
        c2.metric("After This Trade",    f"{new_util:.1f}%", delta=f"+{util_delta:.1f}%")
        c3.metric("Capital Buffer",      _fmt_cur(portfolio_deposit * cash_buf))

        if new_util > 85:
            st.error("🔴 MARGIN ALERT: Utilisation exceeds 85% — do NOT enter")
        elif new_util > 75:
            st.warning("🟡 Caution: Utilisation > 75% — CIO review mandatory")
        elif new_util > 65:
            st.warning("⚠️ Elevated utilisation — monitor closely")
        else:
            st.success(f"✅ Utilisation healthy at {new_util:.1f}%")

        st.markdown("---")
        st.markdown("**Sector Exposure**")
        st.write(f"Sector: **{sector}**")
        open_sectors: dict = {}
        if df_open is not None and 'Sector' in df_open.columns and 'Strike' in df_open.columns:
            for _, p in df_open.iterrows():
                sec = str(p.get('Sector', 'Unknown'))
                try:
                    qty = abs(float(p.get('Quantity', p.get('Contracts', 1))))
                    s   = float(p.get('Strike', 0))
                    open_sectors[sec] = open_sectors.get(sec, 0) + s * 100 * qty
                except Exception:
                    pass
        sector_after = open_sectors.get(sector, 0) + collateral
        sector_pct   = (sector_after / portfolio_deposit * 100) if portfolio_deposit else 0
        st.metric(f"{sector} after entry", _fmt_pct(sector_pct),
                  delta=f"Cap: {sector_cap*100:.0f}%", delta_color="off")
        if sector_pct > sector_cap * 100:
            st.error(f"🔴 Sector cap breach — {sector} would be {sector_pct:.1f}% (limit {sector_cap*100:.0f}%)")

    with tab_verdict:
        st.markdown(f"### {tier_label}")

        rationale = []
        if tier == 'T1':
            rationale.append(f"✅ Guru-validated ticker ({ticker})")
            rationale.append(f"✅ IV status: {iv_s} — seller-friendly conditions")
            rationale.append(f"✅ Option Score {score} ≥ {score_t1}")
            rationale.append(f"✅ ROC {roc:.2f}% meets bucket minimum")
            rationale.append(f"✅ PoP {pop}% — probability favourable")
        elif tier == 'T2':
            rationale.append("⚠️ Passes mechanical screens but requires CIO review")
            if not is_guru:
                rationale.append(f"⚠️ {ticker} not on GURU list")
            if iv_s in ('ELEVATED', 'SPIKE'):
                rationale.append(f"⚠️ IV elevated +{iv_prem:.1f}% — investigate catalyst")
            rationale.append(f"PoP {pop}%, Score {score}")
        else:
            rationale.append("Backup candidate — use only if T1/T2 options exhausted")

        for r in rationale:
            st.write(r)

        if flags:
            st.markdown("**Flags:**")
            for flag in flags.split(' | '):
                if flag.strip():
                    st.write(f"• {flag.strip()}")

        st.markdown("---")
        st.markdown("**Execution Plan**")
        c1, c2 = st.columns(2)
        c1.info(f"**Contract:** `{occ}`")
        c2.info(f"**Limit Price:** ${limit_px:.2f} (mid − $0.05)")
        c1.info(f"**Contracts:** {contracts}")
        c2.info(f"**BTC Target:** ${btc_px:.2f} ({btc_rate*100:.0f}% of premium)")
        st.caption("Order type: Limit | Time in Force: Day | Submit within 30 min of scan")

# ─────────────────────────────────────────────
# UI — TIERED RESULTS TABLE
# ─────────────────────────────────────────────

def _render_tier_table(df: pd.DataFrame, tier: str, portfolio_deposit: float, cfg: dict):
    """Render results table + expandable deep dives for one tier."""
    if df.empty:
        st.caption("No candidates in this tier.")
        return

    tier_icon = {'T1': '🟢', 'T2': '🟡', 'T3': '⚪'}.get(tier, '—')
    btc_rates = {'T1': cfg.get('btc_t1', BTC_TARGET['T1']),
                 'T2': cfg.get('btc_t2', BTC_TARGET['T2']),
                 'T3': 0.50}
    btc_rate  = btc_rates.get(tier, 0.50)
    btc_pct   = int(btc_rate * 100)

    rows = []
    for _, row in df.iterrows():
        ticker = str(row.get('ticker', '')).upper()
        delta  = row.get('delta', 0)
        pop    = _pop_from_delta(abs(float(delta)) if pd.notna(delta) else 0)
        rows.append({
            'Ticker':              ticker,
            'Bucket':              row.get('_bucket', '—'),
            'Strike':              _fmt_cur(row.get('strike')),
            'OTM%':                _fmt_pct(row.get('pct_otm'), 1) if pd.notna(row.get('pct_otm', float('nan'))) else '—',
            'Expiry':              str(row.get('expiry', '—')),
            'DTE':                 int(row.get('dte', 0)) if pd.notna(row.get('dte')) else '—',
            'ROC%':                _fmt_pct(row.get('roc_pct'), 2),
            'Ann%':                _fmt_pct(row.get('ann_yield_pct'), 1),
            'Delta':               f"{float(delta):.2f}" if pd.notna(delta) else '—',
            'PoP':                 f"{pop}%",
            'Score':               int(row.get('score', 0)) if pd.notna(row.get('score')) else '—',
            'Sector':              str(row.get('sector', '—')),
            'Guru':                '✅' if row.get('guru') else '—',
            'IV':                  _iv_badge(row.get('iv_status', 'UNKNOWN')),
            'Contracts':           int(row.get('contracts', 0)),
            'Premium$':            _fmt_cur(row.get('prem_total')),
            f'BTC@{btc_pct}%':    (_fmt_cur(row.get('premium_mid', 0) * btc_rate)
                                    if pd.notna(row.get('premium_mid')) else '—'),
        })

    display_df = pd.DataFrame(rows)
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.markdown(f"**{tier_icon} Deep Dive Analysis**")
    for idx, (_, row) in enumerate(df.iterrows()):
        ticker = str(row.get('ticker', '')).upper()
        ann    = row.get('ann_yield_pct', 0)
        occ    = _occ_contract(ticker, row.get('expiry', ''), row.get('strike', 0))
        label  = f"{ticker} — {occ} | Ann {_fmt_pct(ann, 1)} | {_iv_badge(row.get('iv_status', 'UNKNOWN'))}"
        with st.expander(label, expanded=(idx == 0 and tier == 'T1')):
            _render_deep_dive(row, portfolio_deposit, cfg)

# ─────────────────────────────────────────────
# UI — PORTFOLIO IMPACT SUMMARY
# ─────────────────────────────────────────────

def _render_portfolio_impact(df_t1: pd.DataFrame, df_t2: pd.DataFrame,
                              portfolio_deposit: float, cfg: dict):
    """Show hypothetical margin if top T1 + T2 candidates are entered."""
    st.markdown("#### 💼 Hypothetical Portfolio Impact")
    if portfolio_deposit <= 0:
        st.info("No portfolio capital configured — load a portfolio first.")
        return

    max_pos = cfg.get('max_positions', MAX_POSITIONS)
    df_open = st.session_state.get('df_open')
    current_collateral = 0
    current_count      = 0
    if df_open is not None:
        current_count = len(df_open)
        for _, p in df_open.iterrows():
            try:
                qty = abs(float(p.get('Quantity', p.get('Contracts', 1))))
                s   = float(p.get('Strike', 0))
                current_collateral += s * 100 * qty
            except Exception:
                pass

    current_util   = current_collateral / portfolio_deposit * 100 if portfolio_deposit else 0
    new_collateral = current_collateral
    new_count      = current_count
    new_premium    = 0.0
    notes          = []

    for df, tlabel in [(df_t1, 'T1'), (df_t2, 'T2')]:
        if not df.empty:
            top = df.iloc[0]
            new_collateral += top.get('collateral', 0)
            new_count      += 1
            new_premium    += top.get('prem_total', 0)
            notes.append(
                f"{tlabel}: {str(top.get('ticker','')).upper()} "
                f"{_occ_contract(str(top.get('ticker','')).upper(), top.get('expiry',''), top.get('strike',0))} "
                f"→ {_fmt_cur(top.get('collateral', 0))} collateral, "
                f"{_fmt_cur(top.get('prem_total', 0))} premium"
            )
            if new_count >= max_pos:
                break

    new_util = new_collateral / portfolio_deposit * 100 if portfolio_deposit else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Current Positions",   current_count)
    c2.metric("After New Trades",    new_count)
    c3.metric("Current Utilisation", f"{current_util:.1f}%")
    c4.metric("Post-Entry Util",     f"{new_util:.1f}%",
              delta=f"+{new_util - current_util:.1f}%",
              delta_color="inverse" if new_util > 75 else "off")

    if notes:
        st.markdown("Included trades:")
        for n in notes:
            st.markdown(f"• {n}")

    if new_premium > 0:
        est_ann = new_premium / portfolio_deposit * (365 / 30) * 100 if portfolio_deposit else 0
        st.success(f"💰 Total premium collected: **{_fmt_cur(new_premium)}** "
                   f"| Estimated annualised on capital: **{_fmt_pct(est_ann, 1)}**")

# ─────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────
# UI — BUCKET RESULTS SECTION
# ─────────────────────────────────────────────

def _render_bucket_results(res: dict, bucket_label: str, portfolio_deposit: float, cfg: dict):
    """Render T1/T2/T3/Blocked expanders for a single bucket's scan results."""
    df_t1      = res.get('t1',      pd.DataFrame())
    df_t2      = res.get('t2',      pd.DataFrame())
    df_t3      = res.get('t3',      pd.DataFrame())
    df_blocked = res.get('blocked', pd.DataFrame())

    total = len(df_t1) + len(df_t2) + len(df_t3)
    if total == 0 and df_blocked.empty:
        st.info(
            f"No candidates passed all filters for the {bucket_label} bucket. "
            "Try relaxing thresholds in ⚙️ Scan Parameters."
        )
        return

    t1_sz  = cfg.get('t1_size',  TIER_SIZE_PCT['T1'])
    t2_sz  = cfg.get('t2_size',  TIER_SIZE_PCT['T2'])
    t3_sz  = cfg.get('t3_size',  TIER_SIZE_PCT['T3'])
    btc_t1 = cfg.get('btc_t1',   BTC_TARGET['T1'])
    btc_t2 = cfg.get('btc_t2',   BTC_TARGET['T2'])

    # T1
    with st.expander(
        f"🟢 Tier 1 — AUTO-EXECUTE ({len(df_t1)} candidates)",
        expanded=len(df_t1) > 0,
    ):
        if df_t1.empty:
            st.info("No T1 candidates — no guru-validated tickers with normal/elevated IV passed all gates.")
        else:
            st.markdown(
                f"Highest conviction. Execute within 30 minutes. "
                f"Max position size **{t1_sz*100:.0f}%** of capital. "
                f"BTC target: **{btc_t1*100:.0f}%** profit."
            )
            _render_tier_table(df_t1, 'T1', portfolio_deposit, cfg)

    # T2
    with st.expander(
        f"🟡 Tier 2 — CIO REVIEW REQUIRED ({len(df_t2)} candidates)",
        expanded=len(df_t2) > 0 and df_t1.empty,
    ):
        if df_t2.empty:
            st.info("No T2 candidates.")
        else:
            st.markdown(
                f"CIO approval required before entry. "
                f"Max position size **{t2_sz*100:.0f}%** of capital. "
                f"BTC target: **{btc_t2*100:.0f}%** profit."
            )
            _render_tier_table(df_t2, 'T2', portfolio_deposit, cfg)

    # T3
    with st.expander(
        f"⚪ Tier 3 — BACKUP / OPPORTUNISTIC ({len(df_t3)} candidates)",
        expanded=False,
    ):
        if df_t3.empty:
            st.info("No T3 candidates.")
        else:
            st.markdown(
                f"Use only if T1/T2 exhausted. "
                f"Max position size **{t3_sz*100:.0f}%** of capital."
            )
            _render_tier_table(df_t3, 'T3', portfolio_deposit, cfg)

    # Blocked
    if not df_blocked.empty:
        with st.expander(
            f"🔴 Blocked — Portfolio Constraints ({len(df_blocked)} candidates)",
            expanded=False,
        ):
            st.caption(
                "These passed all 5 gates but are blocked by sector cap or duplicate underlying."
            )
            blocked_display = []
            for _, row in df_blocked.iterrows():
                blocked_display.append({
                    'Ticker': str(row.get('ticker', '')).upper(),
                    'Strike': _fmt_cur(row.get('strike')),
                    'OTM%':   _fmt_pct(row.get('pct_otm'), 1) if pd.notna(row.get('pct_otm', float('nan'))) else '—',
                    'Expiry': str(row.get('expiry', '—')),
                    'Ann%':   _fmt_pct(row.get('ann_yield_pct', 0), 1),
                    'Reason': row.get('flags', '—'),
                })
            st.dataframe(pd.DataFrame(blocked_display), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────

def render_income_scanner():
    """Main render function — called from app.py routing."""

    # Initialise config on first load
    _init_cfg()

    st.title("🔍 Income Scanner")
    st.caption("ARGUS v6 · Cash-Secured Put Screener · 5-Gate Filter · T1/T2/T3 Tiering")

    portfolio_deposit = st.session_state.get('portfolio_deposit', 0)
    portfolio_name    = st.session_state.get('current_portfolio', '—')
    cfg               = _get_cfg()

    # Portfolio status strip
    df_open    = st.session_state.get('df_open')
    open_count = len(df_open) if df_open is not None else 0
    max_pos    = cfg.get('max_positions', MAX_POSITIONS)
    slots_free = max_pos - open_count
    cash_buf   = cfg.get('cash_buffer', CASH_BUFFER)
    available  = portfolio_deposit * (1 - cash_buf)

    col_s1, col_s2, col_s3, col_s4 = st.columns(4)
    col_s1.metric("Portfolio",        portfolio_name)
    col_s2.metric("Capital",          _fmt_cur(portfolio_deposit))
    col_s3.metric("Available",        _fmt_cur(available))
    col_s4.metric("Open Slots",       f"{slots_free} / {max_pos}", delta_color="off")

    st.divider()

    # ── CONFIG PANEL ───────────────────────────────────────────
    _render_config_panel()
    # Re-read cfg after panel (widget sync may have updated it)
    cfg = _get_cfg()

    st.divider()

    # ── INPUT SECTION ──────────────────────────────────────────
    st.subheader("📥 Data Input")
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.markdown("**Upload ThetaScanner CSV**")
        uploaded = st.file_uploader(
            "ThetaScanner Pro export (Active or Core scan)",
            type=["csv"],
            help=(
                "Export from ThetaScanner Pro. Expected columns: Symbol, Strike, Expiration, DTE, "
                "Days to ER, Mark, ROC, Annual Yield, Delta, IV, Stock IV, Option Score, Sector, "
                "Market Cap, Open Int, Volume, Type"
            ),
            key="scanner_csv_upload",
        )

    with col_right:
        st.markdown("**Or filter by tickers (optional)**")
        ticker_text = st.text_area(
            "Ticker list (comma or space separated)",
            placeholder="WMT, JPM, DVN, VALE, DAL",
            height=80,
            key="scanner_ticker_list",
            help="If provided, only these tickers from the CSV will be scanned. Leave blank for all.",
        )
        bucket = st.selectbox(
            "Bucket",
            ["Both", "Active (6–10 DTE)", "Core (30–45 DTE)"],
            key="scanner_bucket",
            help="Active = short-dated (6–10 DTE). Core = longer-dated (30–45 DTE).",
        )

        st.markdown("**Guru List (optional)**")
        guru_file = st.file_uploader(
            "Community guru list CSV",
            type=["csv"],
            key="scanner_guru_upload",
            help=(
                "Upload the community's guru list CSV "
                "(Ticker, Industry, Company, Sector, Ideal Sell Put Strike). "
                "Tickers on this list that meet all criteria are elevated to T1."
            ),
        )
        if guru_file is not None:
            _g_tickers, _g_strikes = _parse_guru_file(guru_file)
            if _g_tickers:
                st.session_state['guru_list']       = _g_tickers
                st.session_state['guru_strike_map'] = _g_strikes
                _preview = ', '.join(_g_tickers[:6])
                _more    = f' +{len(_g_tickers) - 6} more' if len(_g_tickers) > 6 else ''
                st.caption(f"Loaded **{len(_g_tickers)} tickers**: {_preview}{_more}")
            else:
                st.warning("Could not parse any tickers from that file.")
        else:
            _cur_guru = st.session_state.get('guru_list', [])
            if _cur_guru:
                st.caption(f"Guru list active: **{len(_cur_guru)} tickers** (from earlier upload this session)")
            else:
                st.caption("No guru list loaded — scan will have no T1 candidates via guru path")

    run_scan = st.button(
        "▶ Run Scan", type="primary",
        disabled=(uploaded is None and not ticker_text.strip()),
    )

    if uploaded is None and not ticker_text.strip():
        st.info("👆 Upload a ThetaScanner CSV (or enter tickers) to begin scanning.")

    # ── SCAN ENGINE ────────────────────────────────────────────
    if run_scan and uploaded is not None:
        bucket_key = 'Both'
        if 'Active' in bucket:
            bucket_key = 'Active'
        elif 'Core' in bucket:
            bucket_key = 'Core'

        with st.spinner("⚙️ Running ARGUS v6 scan..."):
            df_raw = _parse_csv(uploaded)
            if df_raw is None:
                st.stop()

            res_active, res_core, funnel_stats, rejections = _run_scan(
                df_raw, ticker_text, bucket_key
            )

        # Summary counts for success banner
        def _res_count(res, key):
            return len(res[key]) if res and key in res else 0

        n_t1 = _res_count(res_active, 't1') + _res_count(res_core, 't1')
        n_t2 = _res_count(res_active, 't2') + _res_count(res_core, 't2')
        n_t3 = _res_count(res_active, 't3') + _res_count(res_core, 't3')
        n_bl = _res_count(res_active, 'blocked') + _res_count(res_core, 'blocked')

        st.session_state['scanner_results'] = {
            'active':     res_active,
            'core':       res_core,
            'funnel':     funnel_stats,
            'rejections': rejections,
            'bucket':     bucket_key,
        }
        st.success(
            f"Scan complete — "
            f"🟢 {n_t1} T1 · 🟡 {n_t2} T2 · ⚪ {n_t3} T3 · "
            f"🔴 {n_bl} blocked · {len(rejections)} total exclusions"
        )

    # ── RESULTS ────────────────────────────────────────────────
    results = st.session_state.get('scanner_results')
    if results:
        res_active = results.get('active')
        res_core   = results.get('core')
        funnel     = results.get('funnel',     {})
        rejections = results.get('rejections', [])

        st.divider()

        # Funnel counts
        _render_funnel_summary(funnel)

        # Rejection detail (collapsed)
        _render_rejection_breakdown(rejections)

        st.divider()

        bucket_ran = results.get('bucket', 'Both')

        # ── Active bucket results ───────────────────────────────
        show_active = res_active is not None and ('Active' in bucket_ran or bucket_ran == 'Both')
        if show_active:
            st.subheader("⚡ Active Bucket (6–10 DTE)")
            _render_bucket_results(res_active, 'Active', portfolio_deposit, cfg)

        # ── Core bucket results ─────────────────────────────────
        show_core = res_core is not None and ('Core' in bucket_ran or bucket_ran == 'Both')
        if show_core:
            if show_active:
                st.divider()
            st.subheader("🔵 Core Bucket (30–45 DTE)")
            _render_bucket_results(res_core, 'Core', portfolio_deposit, cfg)

        # ── Portfolio impact (combined T1+T2 from both buckets) ─
        combined_t1 = pd.concat([
            res_active['t1'] if res_active else pd.DataFrame(),
            res_core['t1']   if res_core   else pd.DataFrame(),
        ], ignore_index=True)
        combined_t2 = pd.concat([
            res_active['t2'] if res_active else pd.DataFrame(),
            res_core['t2']   if res_core   else pd.DataFrame(),
        ], ignore_index=True)
        if not combined_t1.empty or not combined_t2.empty:
            st.divider()
            _render_portfolio_impact(combined_t1, combined_t2, portfolio_deposit, cfg)

    st.divider()
    # Footer shows ACTIVE thresholds (from cfg, not module globals)
    st.caption(
        f"Active thresholds — "
        f"Active: ROC≥{cfg.get('active_roc_min', ACTIVE_ROC_MIN):.2f}% "
        f"DTE {cfg.get('active_dte_min', ACTIVE_DTE_MIN)}–{cfg.get('active_dte_max', ACTIVE_DTE_MAX)} "
        f"Δ≤{cfg.get('active_delta_max', ACTIVE_DELTA_MAX):.2f} · "
        f"Core: ROC≥{cfg.get('core_roc_min', CORE_ROC_MIN):.2f}% "
        f"DTE {cfg.get('core_dte_min', CORE_DTE_MIN)}–{cfg.get('core_dte_max', CORE_DTE_MAX)} "
        f"Δ≤{cfg.get('core_delta_max', CORE_DELTA_MAX):.2f} · "
        f"Sector cap {cfg.get('sector_cap_pct', int(SECTOR_CAP*100))}% · "
        f"Max {cfg.get('max_positions', MAX_POSITIONS)} positions · "
        f"Reserve {cfg.get('cash_buffer_pct', int(CASH_BUFFER*100))}%"
    )
