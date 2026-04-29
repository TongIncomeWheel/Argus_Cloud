"""
Income Wheel - Streamlit Application
Main entry point for the Options Income Wheel tracker
"""
import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path

# Page config must be first Streamlit command
# Note: Title will be updated dynamically based on portfolio selection
st.set_page_config(
    page_title="ARGUS",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

from config import TICKERS, WEEKLY_TARGET_PCT, EXPIRING_SOON_DTE
from gsheet_handler import GSheetHandler, validate_data_integrity


def get_tickers_for_dropdown(portfolio: str, df_trades=None):
    """Ticker list for this portfolio: saved tickers + tickers from positions. Income Wheel falls back to config TICKERS when saved is empty."""
    base = get_tickers(portfolio)
    if not base and portfolio == "Income Wheel":
        base = list(TICKERS)
    position_tickers = []
    if df_trades is not None and not df_trades.empty and 'Ticker' in df_trades.columns:
        position_tickers = df_trades['Ticker'].dropna().astype(str).str.strip().str.upper().unique().tolist()
    combined = [t for t in (base + position_tickers) if t]
    return sorted(set(combined))
from calculations import CapitalCalculator, PremiumCalculator, QuotaCalculator, RiskCalculator, CSPTankCalculator, PMCCCalculator
from models import TradeValidator, generate_trade_id, generate_audit_id
from price_feed import PriceFeed, display_price_status, get_cached_prices
from market_data import MarketDataService as _MarketDataService
_market_data = _MarketDataService()
from persistence import get_portfolio_deposit, save_portfolio_deposit, get_capital_allocation, save_capital_allocation, get_portfolio_deposit_sgd, save_portfolio_deposit_sgd, get_fx_rate, save_fx_rate, get_pmcc_tickers, save_pmcc_tickers, get_tickers, save_tickers
from ai_chat import render_ai_chat
# Strategy Instructions module removed in v2 cleanup
from income_scanner_ui import render_income_scanner
from contract_price_lookup import render_contract_price_lookup
# CIO Report module removed in v2 cleanup

# ============================================================
# FORMATTING HELPERS - Negative Values in Red with Parentheses
# ============================================================
def format_currency(value, decimals=2, show_cents=True):
    """
    Format currency value. Negative values shown as (-$xxx) in red.
    
    Args:
        value: Numeric value to format
        decimals: Number of decimal places
        show_cents: If False, rounds to whole dollars
    
    Returns:
        Formatted string with HTML styling for negative values
    """
    if pd.isna(value) or value is None:
        return "$0.00"
    
    value = float(value)
    
    if show_cents:
        if decimals == 0:
            formatted = f"${abs(value):,.0f}"
        else:
            formatted = f"${abs(value):,.{decimals}f}"
    else:
        formatted = f"${abs(value):,.0f}"
    
    if value < 0:
        return f'<span style="color: #ff4444;">(-{formatted})</span>'
    else:
        return formatted

def format_number(value, decimals=2):
    """
    Format number. Negative values shown as (-xxx) in red.
    
    Args:
        value: Numeric value to format
        decimals: Number of decimal places
    
    Returns:
        Formatted string with HTML styling for negative values
    """
    if pd.isna(value) or value is None:
        return "0"
    
    value = float(value)
    
    if decimals == 0:
        formatted = f"{abs(value):,.0f}"
    else:
        formatted = f"{abs(value):,.{decimals}f}"
    
    if value < 0:
        return f'<span style="color: #ff4444;">(-{formatted})</span>'
    else:
        return formatted

def format_percent(value, decimals=2):
    """
    Format percentage. Negative values shown as (-xx.xx%) in red.
    
    Args:
        value: Numeric value to format as percentage
        decimals: Number of decimal places
    
    Returns:
        Formatted string with HTML styling for negative values
    """
    if pd.isna(value) or value is None:
        return "0%"
    
    value = float(value)
    formatted = f"{abs(value):,.{decimals}f}%"
    
    if value < 0:
        return f'<span style="color: #ff4444;">(-{formatted})</span>'
    else:
        return formatted

def format_currency_for_display(value, decimals=2, show_cents=True):
    """
    Format currency for display in DataFrames, captions, etc.
    Negative values shown as (-$xxx) in red.
    Returns HTML string for use with st.markdown(..., unsafe_allow_html=True)
    """
    if pd.isna(value) or value is None:
        return "$0.00"
    
    value = float(value)
    
    if show_cents:
        if decimals == 0:
            formatted = f"${abs(value):,.0f}"
        else:
            formatted = f"${abs(value):,.{decimals}f}"
    else:
        formatted = f"${abs(value):,.0f}"
    
    if value < 0:
        return f'<span style="color: #ff4444;">(-{formatted})</span>'
    else:
        return formatted

def format_number_for_display(value, decimals=2):
    """
    Format number for display. Negative values shown as (-xxx) in red.
    Returns HTML string for use with st.markdown(..., unsafe_allow_html=True)
    """
    if pd.isna(value) or value is None:
        return "0"
    
    value = float(value)
    
    if decimals == 0:
        formatted = f"{abs(value):,.0f}"
    else:
        formatted = f"{abs(value):,.{decimals}f}"
    
    if value < 0:
        return f'<span style="color: #ff4444;">(-{formatted})</span>'
    else:
        return formatted

def st_metric_with_negatives(label, value, delta=None, delta_color="normal", help_text=None, decimals=2, is_currency=False, is_percent=False, suffix=""):
    """
    Display metric with negative values shown as (-xxx) in red.
    Same font size and alignment for positive (black) and negative (red).
    """
    if pd.isna(value) or value is None:
        value = 0.0
    
    value = float(value)
    
    # Format the value
    if is_currency:
        if decimals == 0:
            formatted = f"${abs(value):,.0f}"
        else:
            formatted = f"${abs(value):,.{decimals}f}"
    elif is_percent:
        formatted = f"{abs(value):,.{decimals}f}%"
    else:
        if decimals == 0:
            formatted = f"{abs(value):,.0f}"
        else:
            formatted = f"{abs(value):,.{decimals}f}"
    
    # Add suffix if provided
    if suffix:
        formatted = f"{formatted}{suffix}"
    
    # Same layout for both: one consistent font size and weight; only color differs (black vs red)
    value_style = "color: #0f172a; font-size: 1.25rem; font-weight: 600;"
    if value < 0:
        display_val = f"(-{formatted})"
        value_style = "color: #ff4444; font-size: 1.25rem; font-weight: 600;"
    else:
        display_val = formatted
    if label or help_text:
        st.markdown(f"**{label}**" + (f" — {help_text}" if help_text else ""))
    st.markdown(f'<span style="{value_style}">{display_val}</span>', unsafe_allow_html=True)
    if delta is not None:
        st.caption(f"Δ {delta}")

def format_currency_for_dataframe(value, decimals=2, show_cents=True):
    """
    Format currency for DataFrame display (text only, no HTML).
    Negative values shown as (-$xxx) format.
    Handles both numeric values and already-formatted strings.
    """
    if pd.isna(value) or value is None:
        return "$0.00"
    
    # If value is already a string, try to extract numeric value
    if isinstance(value, str):
        # Remove currency symbols, commas, parentheses, and whitespace
        cleaned = value.replace('$', '').replace(',', '').replace('(', '').replace(')', '').strip()
        # Check if it's already formatted as negative (starts with - or has (-))
        is_negative = value.startswith('(-') or (cleaned.startswith('-'))
        try:
            value = float(cleaned)
            if is_negative:
                value = -abs(value)
        except (ValueError, AttributeError):
            # If we can't parse it, return as-is or default to 0
            return "$0.00"
    
    value = float(value)
    
    if show_cents:
        if decimals == 0:
            formatted = f"${abs(value):,.0f}"
        else:
            formatted = f"${abs(value):,.{decimals}f}"
    else:
        formatted = f"${abs(value):,.0f}"
    
    if value < 0:
        return f"(-{formatted})"
    else:
        return formatted

def format_number_for_dataframe(value, decimals=2):
    """
    Format number for DataFrame display (text only, no HTML).
    Negative values shown as (-xxx) format.
    Handles both numeric values and already-formatted strings.
    """
    if pd.isna(value) or value is None:
        return "0"
    
    # If value is already a string, try to extract numeric value
    if isinstance(value, str):
        # Remove commas, parentheses, and whitespace
        cleaned = value.replace(',', '').replace('(', '').replace(')', '').strip()
        # Check if it's already formatted as negative
        is_negative = value.startswith('(-') or (cleaned.startswith('-'))
        try:
            value = float(cleaned)
            if is_negative:
                value = -abs(value)
        except (ValueError, AttributeError):
            # If we can't parse it, return as-is or default to 0
            return "0"
    
    value = float(value)
    
    if decimals == 0:
        formatted = f"{abs(value):,.0f}"
    else:
        formatted = f"{abs(value):,.{decimals}f}"
    
    if value < 0:
        return f"(-{formatted})"
    else:
        return formatted

def style_dataframe_negatives(df, currency_columns=None, number_columns=None):
    """
    Style DataFrame to show negative values in red with parentheses.
    Returns a styled DataFrame that can be displayed with st.dataframe.
    
    Args:
        df: DataFrame to style
        currency_columns: List of column names that should be formatted as currency
        number_columns: List of column names that should be formatted as numbers
    
    Returns:
        Styled DataFrame with CSS applied
    """
    if currency_columns is None:
        currency_columns = []
    if number_columns is None:
        number_columns = []
    
    # Create a copy to avoid modifying original
    df_styled = df.copy()
    
    # Apply formatting to currency columns
    for col in currency_columns:
        if col in df_styled.columns:
            df_styled[col] = df_styled[col].apply(
                lambda x: format_currency_for_dataframe(x, decimals=0, show_cents=False) if pd.notna(x) else "$0"
            )
    
    # Apply formatting to number columns
    for col in number_columns:
        if col in df_styled.columns:
            df_styled[col] = df_styled[col].apply(
                lambda x: format_number_for_dataframe(x, decimals=2) if pd.notna(x) else "0"
            )
    
    # Apply CSS styling for negative values (red only, same size/weight as positive)
    def highlight_negatives(val):
        if isinstance(val, str) and val.startswith('(-'):
            return 'color: #ff4444;'
        return ''
    
    # Create styled DataFrame
    styled_df = df_styled.style.map(highlight_negatives, subset=currency_columns + number_columns)
    
    return styled_df

from config import INCOME_WHEEL_SHEET_ID, ACTIVE_CORE_SHEET_ID

def get_sheet_id(portfolio: str) -> str:
    """Get the Google Sheet ID for the selected portfolio"""
    if portfolio == "Active Core":
        return ACTIVE_CORE_SHEET_ID
    return INCOME_WHEEL_SHEET_ID


# ============================================================
# SESSION STATE INITIALIZATION
# ============================================================
def init_session_state():
    """Initialize session state variables"""
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    if 'df_trades' not in st.session_state:
        st.session_state.df_trades = None
    if 'df_audit' not in st.session_state:
        st.session_state.df_audit = None
    if 'df_open' not in st.session_state:
        st.session_state.df_open = None
    if 'live_prices' not in st.session_state:
        st.session_state.live_prices = {}
    if 'ibkr_connected' not in st.session_state:
        st.session_state.ibkr_connected = False


# ============================================================
# DATA LOADING
# ============================================================
@st.cache_data(ttl=60)
def load_data(portfolio: str = "Income Wheel"):
    """Load data from Google Sheets with caching for the specified portfolio"""
    sheet_id = get_sheet_id(portfolio)

    if not sheet_id:
        return None, None, [f"No Google Sheet ID configured for {portfolio}. Check .env file."]

    try:
        handler = GSheetHandler(sheet_id)
        df_trades = handler.read_data_table()
        df_audit = handler.read_audit_table()

        # Validate integrity
        errors = validate_data_integrity(df_trades, df_audit)

        return df_trades, df_audit, errors
    except Exception as e:
        return None, None, [f"Error loading {portfolio} from Google Sheets: {e}"]


def refresh_data():
    """Force refresh data from Google Sheets"""
    st.cache_data.clear()
    st.session_state.data_loaded = False
    st.rerun()


# ============================================================
# SIDEBAR
# ============================================================
def render_sidebar():
    """Render sidebar with portfolio selector, navigation and status"""
    
    # Sidebar: pure JS drag-to-resize. Streamlit 1.53 has no built-in handle.
    # Drag zone = rightmost 8px of sidebar. Max width = 1/3 of screen. No observers.
    sidebar_css = """
    <style>
        section[data-testid="stSidebar"] {
            min-width: 280px;
            width: 480px;
            flex-shrink: 0;
            position: relative;
        }
        section[data-testid="stSidebar"] > div:first-child {
            width: 100% !important;
        }
        /* Visual drag-handle strip on right edge */
        section[data-testid="stSidebar"]::after {
            content: '';
            position: absolute;
            top: 0; right: 0;
            width: 6px; height: 100%;
            cursor: col-resize;
            background: rgba(120,120,120,0.15);
            z-index: 1000;
            transition: background 0.15s;
        }
        section[data-testid="stSidebar"]:hover::after {
            background: rgba(120,120,120,0.35);
        }
        .main .block-container {
            padding-left: 1.5rem !important;
            padding-right: 1.5rem !important;
            padding-top: 2rem !important;
            max-width: 100% !important;
        }
        .stChat { width: 100% !important; }
    </style>
    <script>
    (function() {
        var MIN_W = 280;
        function maxW() { return Math.max(600, window.innerWidth - 200); }  // Up to screen width minus 200px

        function applyWidth(w) {
            var sb = document.querySelector('section[data-testid="stSidebar"]');
            if (!sb) return;
            sb.style.setProperty('width', w + 'px', 'important');
            var main = document.querySelector('.main');
            if (main) { main.style.marginLeft = w + 'px'; }
        }

        function setup() {
            var sb = document.querySelector('section[data-testid="stSidebar"]');
            if (!sb) { setTimeout(setup, 200); return; }

            // Restore saved width from localStorage — survives Streamlit rerenders
            var saved = localStorage.getItem('argus_sidebar_w');
            if (saved) {
                var w = Math.min(maxW(), Math.max(MIN_W, parseInt(saved, 10)));
                applyWidth(w);
            }

            // Avoid attaching multiple listeners on Streamlit rerenders
            if (sb._dragAttached) return;
            sb._dragAttached = true;

            sb.addEventListener('mousedown', function(e) {
                var rect = sb.getBoundingClientRect();
                // Only trigger drag if click is in the rightmost 8px
                if (e.clientX < rect.right - 8) return;
                e.preventDefault();
                var startX = e.clientX;
                var startW = rect.width;

                function onMove(ev) {
                    var newW = Math.min(maxW(), Math.max(MIN_W, startW + (ev.clientX - startX)));
                    applyWidth(newW);
                    // Persist immediately so rerenders pick it up
                    localStorage.setItem('argus_sidebar_w', newW);
                }
                function onUp() {
                    document.removeEventListener('mousemove', onMove);
                    document.removeEventListener('mouseup', onUp);
                }
                document.addEventListener('mousemove', onMove);
                document.addEventListener('mouseup', onUp);
            });
        }

        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', setup);
        } else {
            setup();
        }
        // Retry after Streamlit's initial render passes
        setTimeout(setup, 300);
        setTimeout(setup, 800);
        setTimeout(setup, 1500);
    })();
    </script>
    """

    st.markdown(sidebar_css, unsafe_allow_html=True)
    
    with st.sidebar:
        # Single portfolio (Income Wheel) — Active Core retired in favor of two-pot architecture
        portfolio = "🎡 Income Wheel"
        st.session_state.selected_portfolio = portfolio

        # Navigation (Collapsible)
        with st.expander("🧭 Navigation", expanded=False):
            page = st.radio(
                "Navigate",
                ["📊 Dashboard", "📅 Daily Helper", "📝 Entry Forms",
                 "📈 Expiry Ladder", "📉 Performance", "📋 All Positions", "⚙️ Margin Config",
                 "🔍 Income Scanner", "📡 Market Data", "🔎 Contract Lookup"],
                key="navigation_radio",
                label_visibility="collapsed"
            )
            st.session_state.current_page = page
        
        st.divider()
        
        # AI Chat (persistent across all pages) - AFTER portfolio/page selection for proper context
        try:
            portfolio_name = portfolio.replace("🎡 ", "").replace("⭐ ", "")
            portfolio_deposit = get_portfolio_deposit(portfolio_name)
            
            render_ai_chat(
                df_trades=st.session_state.df_trades,
                df_open=st.session_state.df_open,
                portfolio_deposit=portfolio_deposit,
                current_page=page,
                portfolio=portfolio_name
            )
        except Exception as e:
            st.error(f"AI Chat Error: {e}")
        
        # Strategy Selector (for Income Wheel portfolio) - Collapsible
        with st.expander("🎯 Strategy & Settings", expanded=False):
            if portfolio == "🎡 Income Wheel":
                strategy_filter = st.radio(
                    "View Strategy",
                    ["All", "WHEEL", "PMCC", "ActiveCore"],
                    key="strategy_filter",
                    help="Filter by strategy: WHEEL (CSP+CC), PMCC (LEAP+CC), ActiveCore (opportunistic income)"
                )
                # Note: st.radio with key automatically manages st.session_state.strategy_filter
            else:
                # For other portfolios, set strategy filter to "All" if not already set
                if 'strategy_filter' not in st.session_state:
                    st.session_state.strategy_filter = "All"
            
            st.divider()
            
            # Price Feed Status
            st.caption("Live Prices")
            feed = PriceFeed()
            connected = feed.connect()  # Always True for Yahoo Finance
            st.session_state.ibkr_connected = connected  # Keep name for compatibility
            
            display_price_status(connected)
            if st.button("Refresh Prices"):
                st.cache_data.clear()
                st.rerun()
            
            st.divider()
            
            # Data status
            st.caption("Data Status")
            if st.button("🔄 Refresh Data"):
                refresh_data()
            
            if st.session_state.df_trades is not None:
                df = st.session_state.df_trades
                st.caption(f"Total trades: {len(df)}")
                st.caption(f"Open: {len(df[df['Status'] == 'Open'])}")
        
        # Static reference: CSP pacing + shortcuts (minimizable at bottom of sidebar)
        with st.expander("📋 Strategy & shortcuts", expanded=False):
            st.markdown("**CSP deployment pacing**")
            st.markdown("- **Firepower:** Portfolio Deposit − Capital Locked in Stock")
            st.markdown("- **Weekly target:** Deploy ~25% of Firepower in new CSPs each week")
            st.markdown("- **Check:** Sum CSP reserved opened this week vs Weekly Target → UNDER / ON TARGET / OVER")
            st.divider()
            st.markdown("**Shortcuts**")
            st.markdown("- **Dashboard:** Capital Cockpit, P/L, BP, Liquid cash")
            st.markdown("- **Daily Helper:** MARA CC 4-week coverage, live prices")
            st.markdown("- **Entry Forms:** New trade, Close, Split, BTC, Expire, Assignment, Exercise")
            st.markdown("- **Expiry Ladder:** Options by expiry")
            st.markdown("- **All Positions:** Filter by status, type, ticker")
            st.markdown("- **Margin Config:** Deposit, allocation")
        
        return page, portfolio


# ============================================================
# DASHBOARD PAGE
# ============================================================
def render_dashboard():
    """Render main dashboard"""
    st.title("📊 Dashboard")

    df_open = st.session_state.df_open
    df_trades = st.session_state.df_trades

    if df_open is None or df_open.empty:
        st.warning("No open positions found.")
        return

    # Always use current portfolio's persisted values (never stale from previous selection)
    portfolio = st.session_state.get('current_portfolio', 'Income Wheel')

    # ── POT SELECTOR ────────────────────────────────────────────
    from unified_calculations import filter_by_pot, POT_BASE, POT_ACTIVE
    from persistence import get_pot_deposit, get_pot_capital_allocation
    _pot_col, _ = st.columns([2, 5])
    with _pot_col:
        pot_view = st.radio(
            "Pot View",
            ["All Pots", "🏛️ Base Pot", "⚡ Active Income Pot"],
            horizontal=True,
            key="dashboard_pot_view"
        )

    # Determine pot scope
    if pot_view == "🏛️ Base Pot":
        df_open = filter_by_pot(df_open, POT_BASE)
        df_trades = filter_by_pot(df_trades, POT_BASE)
        portfolio_deposit = get_pot_deposit(POT_BASE, portfolio)
        # Fallback: if Base pot not set yet, use legacy deposit (everything was Base before pots)
        if portfolio_deposit == 0:
            portfolio_deposit = get_portfolio_deposit(portfolio)
        capital_allocation_for_view = get_pot_capital_allocation(POT_BASE, portfolio)
        # Fallback: if no Base allocation yet, use legacy combined allocation
        if not capital_allocation_for_view:
            capital_allocation_for_view = get_capital_allocation(portfolio)
        pot_label = "Base Pot"
    elif pot_view == "⚡ Active Income Pot":
        df_open = filter_by_pot(df_open, POT_ACTIVE)
        df_trades = filter_by_pot(df_trades, POT_ACTIVE)
        portfolio_deposit = get_pot_deposit(POT_ACTIVE, portfolio)
        capital_allocation_for_view = get_pot_capital_allocation(POT_ACTIVE, portfolio)
        pot_label = "Active Income Pot"
    else:
        # All Pots: total of both deposits
        portfolio_deposit = get_pot_deposit(POT_BASE, portfolio) + get_pot_deposit(POT_ACTIVE, portfolio)
        if portfolio_deposit == 0:
            # Fallback to legacy single deposit
            portfolio_deposit = get_portfolio_deposit(portfolio)
        # Combined allocation = base + active
        base_alloc = get_pot_capital_allocation(POT_BASE, portfolio)
        active_alloc = get_pot_capital_allocation(POT_ACTIVE, portfolio)
        capital_allocation_for_view = {}
        for t, v in base_alloc.items():
            capital_allocation_for_view[t] = capital_allocation_for_view.get(t, 0) + v
        for t, v in active_alloc.items():
            capital_allocation_for_view[t] = capital_allocation_for_view.get(t, 0) + v
        # Fallback: if neither pot has allocations, use legacy
        if not capital_allocation_for_view:
            capital_allocation_for_view = get_capital_allocation(portfolio)
        pot_label = "All Pots"

    # Defensive: if pot has no positions, bail gracefully
    if df_open is None or df_open.empty:
        st.info(f"No open positions in {pot_label}.")
        return

    st.session_state.portfolio_deposit = portfolio_deposit
    
    # Get live prices for capital calculation (Yahoo Finance - always available)
    tickers = df_open['Ticker'].unique().tolist()
    live_prices = {}
    try:
        live_prices = get_cached_prices(tuple(tickers)) or {}
        st.session_state.live_prices = live_prices

        # Save current prices to persistent storage (for LLM reference and future API integration)
        from persistence import update_current_price
        for ticker, price in live_prices.items():
            if price is not None:
                update_current_price(ticker, price, source='yahoo', portfolio=portfolio)
    except Exception as e:
        st.warning(f"⚠️ Yahoo Finance unavailable: {e}")
        live_prices = st.session_state.get('live_prices', {}) or {}

    # Cloud fallback: yfinance often fails on Streamlit Cloud (rate-limit/blocked).
    # Backfill missing prices from the Data Table's Price_of_current_underlying_(USD)
    # and from previously-cached prices in user_settings.
    missing_tickers = [t for t in tickers if not live_prices.get(t)]
    if missing_tickers:
        from persistence import load_settings
        _settings = load_settings()
        _stored = _settings.get(f"{portfolio.lower().replace(' ', '_')}_current_prices", {})
        for t in list(missing_tickers):
            # Try stored cache first
            stored_data = _stored.get(t)
            if isinstance(stored_data, dict):
                p = stored_data.get('price')
            else:
                p = stored_data
            if p:
                try:
                    live_prices[t] = float(p)
                    missing_tickers.remove(t)
                    continue
                except (TypeError, ValueError):
                    pass
            # Try sheet column fallback
            ticker_rows = df_open[df_open['Ticker'] == t]
            if not ticker_rows.empty:
                sheet_prices = pd.to_numeric(ticker_rows['Price_of_current_underlying_(USD)'], errors='coerce').dropna()
                if not sheet_prices.empty:
                    live_prices[t] = float(sheet_prices.iloc[0])
                    missing_tickers.remove(t)

    # Coerce all prices to float, drop None
    live_prices = {t: float(p) for t, p in live_prices.items() if p is not None and p != 0}

    if live_prices:
        prices_count = len(live_prices)
        st.info(f"📊 Prices loaded for {prices_count}/{len(tickers)} tickers")
    if missing_tickers:
        st.warning(f"⚠️ Missing prices for {missing_tickers} — using $0 (some metrics will be incomplete)")
    
    # Get PMCC tickers for CSP Tank calculation
    pmcc_tickers = get_pmcc_tickers(portfolio)
    pmcc_tickers_set = set(pmcc_tickers) if pmcc_tickers else set()
    
    # Get stock average prices from Performance tab (from broker records)
    from persistence import get_stock_average_prices
    stock_avg_prices = get_stock_average_prices(portfolio)
    
    # Calculate capital usage using unified calculator
    from unified_calculations import UnifiedCapitalCalculator
    capital_data = UnifiedCapitalCalculator.calculate_capital_by_ticker(
        df_open, portfolio_deposit, stock_avg_prices, live_prices, pmcc_tickers_set
    )
    
    # Extract totals for backward compatibility with existing code
    total_stock_locked = capital_data['total']['stock_locked']
    total_csp_reserved = capital_data['total']['csp_reserved']
    total_leap_sunk = capital_data['total']['leap_sunk']
    total_committed = capital_data['total']['total_committed']
    
    # Keep old format for compatibility (if needed elsewhere)
    tank_data = {
        'locked': {
            'stock_locked': total_stock_locked,
            'true_csp_reserved': total_csp_reserved
        },
        'by_ticker': capital_data['by_ticker'],
        'health': {
            'starting_deposit': portfolio_deposit,
            'true_buying_power': capital_data['total']['remaining_bp'],
            'overleveraged': capital_data['total']['overleveraged'],
            'status': 'OVERLEVERAGED' if capital_data['total']['overleveraged'] else 'OK'
        }
    }
    pmcc_data = {
        'total': {
            'leap_sunk': total_leap_sunk,
            'remaining_buying_power': capital_data['total']['remaining_bp']
        }
    }
    
    # Show warning if capital seems too low (likely missing prices)
    total_used = tank_data['locked']['stock_locked'] + tank_data['locked']['true_csp_reserved']
    if total_used < 100000 and len(df_open[df_open['TradeType'].isin(['STOCK', 'LEAP'])]) > 10:
        missing_prices = df_open[
            (df_open['TradeType'].isin(['STOCK', 'LEAP'])) & 
            (pd.to_numeric(df_open['Price_of_current_underlying_(USD)'], errors='coerce').isna())
        ]
        if len(missing_prices) > 0:
            total_shares_missing = missing_prices.apply(
                lambda row: abs(pd.to_numeric(row.get('Open_lots', 0), errors='coerce') or 0) 
                if pd.notna(row.get('Open_lots')) 
                else abs(pd.to_numeric(row.get('Quantity', 0), errors='coerce') or 0) * 100, 
                axis=1
            ).sum()
            st.error(f"⚠️ **Warning:** {len(missing_prices)} positions with {total_shares_missing:,.0f} shares have missing prices. Prices will be fetched from Yahoo Finance, or update prices in Google Sheets to see accurate capital deployment.")
    inventory = CapitalCalculator.calculate_inventory(df_open)
    
    # Save Open_lots to persistent storage (for LLM reference and future API integration)
    # CRITICAL: Only save actual STOCK shares, NOT LEAP shares equivalent
    # LEAP positions should NOT be saved to Open_lots as they are not actual stock holdings
    from persistence import save_open_lots
    open_lots_by_ticker = {}
    positions_by_ticker = inventory.get('positions_by_ticker', {})
    for ticker, ticker_data in positions_by_ticker.items():
        # Only save actual STOCK shares (not LEAP shares equivalent)
        stock_shares = ticker_data.get('stock', 0)  # Actual stock shares only
        if stock_shares > 0:
            open_lots_by_ticker[ticker] = float(stock_shares)
    if open_lots_by_ticker:
        save_open_lots(open_lots_by_ticker, portfolio=portfolio)
    
    # Live Prices Cards
    st.subheader("📊 Live Prices")
    if live_prices:
        price_cols = st.columns(min(len(tickers), 5))
        for idx, ticker in enumerate(tickers[:5]):  # Show first 5 tickers
            with price_cols[idx % len(price_cols)]:
                price = live_prices.get(ticker)
                if price is not None:
                    st_metric_with_negatives(ticker, price, decimals=2, is_currency=True)
                else:
                    st.metric(ticker, "N/A")
    
    # ----- Capital Cockpit: summary row + 3 sections (Stock, CSP, CCs) -----
    st.subheader("💰 Capital Cockpit")
    
    # Totals from unified capital (stock_locked = stock at current price)
    total_stock_locked = capital_data['total']['stock_locked']
    total_stock_at_buy = capital_data['total'].get('stock_at_buy_price', 0.0)
    total_stock_at_current = capital_data['total'].get('stock_at_current_price', total_stock_locked)
    total_stock_pl = capital_data['total'].get('stock_pl', 0.0)
    total_csp_reserved = capital_data['total']['csp_reserved']
    total_leap_sunk = capital_data['total']['leap_sunk']
    total_committed = capital_data['total']['total_committed']
    remaining_bp = capital_data['total']['remaining_bp']
    is_overleveraged = capital_data['total']['overleveraged']
    
    # Comprehensive P&L (same source as "Net P&L (Mark-to-Market)" section) so Total P/L matches
    from pnl_calculator import PnLCalculator
    from persistence import get_spy_leap_pl
    spy_leap_pl = get_spy_leap_pl(portfolio)
    # Pass live options data (Alpaca) so LEAP P&L uses TRUE mark-to-market
    _live_options = st.session_state.get("open_positions_data", [])
    comprehensive_pnl = PnLCalculator.calculate_comprehensive_pnl(
        df_trades=df_trades,
        df_open=df_open,
        stock_avg_prices=stock_avg_prices,
        live_prices=live_prices,
        spy_leap_pl=spy_leap_pl if spy_leap_pl != 0 else None,
        live_options=_live_options,
    )
    # Realized P&L from all closed trades (CC, CSP, LEAP, STOCK)
    premium_collected_by_ticker = {}
    if df_trades is not None and not df_trades.empty:
        closed_opts = df_trades[(df_trades['Status'] == 'Closed') & (df_trades['TradeType'].isin(['CC', 'CSP', 'LEAP', 'STOCK']))].copy()
        if not closed_opts.empty:
            if 'Actual_Profit_(USD)' in closed_opts.columns:
                closed_opts['_prem'] = pd.to_numeric(closed_opts['Actual_Profit_(USD)'], errors='coerce').fillna(0)
            else:
                closed_opts['_prem'] = pd.to_numeric(closed_opts.get('OptPremium', 0), errors='coerce').fillna(0) * 100 * pd.to_numeric(closed_opts.get('Quantity', 0), errors='coerce').fillna(0)
            prem_agg = closed_opts.groupby('Ticker')['_prem'].sum()
            premium_collected_by_ticker = prem_agg.to_dict()
    total_premium_row = sum(premium_collected_by_ticker.values()) if premium_collected_by_ticker else 0.0
    total_stock_leap_pl = comprehensive_pnl['unrealized_stock_pnl']['total'] + comprehensive_pnl['unrealized_leap_pnl']['total']
    total_pl = total_premium_row + total_stock_leap_pl  # Total P/L = Total income + Total stock/LEAPs P/L
    
    # Keep tank_data / health for backward compatibility (used elsewhere)
    health = tank_data['health']
    locked = tank_data['locked']
    
    # ----- Computed values for summary -----
    # Cash-secured policy: STOCK at BUY price (not market) + LEAP sunk + CSP reserved
    # Stock cost basis = actual cash deployed; market value drives unrealized P&L only.
    total_stock_at_buy = capital_data['total'].get('stock_at_buy_price', 0.0)
    total_capital_used = total_stock_at_buy + total_leap_sunk + total_csp_reserved
    capital_held = total_stock_at_buy + total_leap_sunk
    liquid_cash = (portfolio_deposit + total_pl) - capital_held
    bp = liquid_cash - total_csp_reserved
    stock_and_leap_total = total_stock_at_buy + total_leap_sunk

    # ══════════════════════════════════════════════════════════
    # PHASE 3.1: ALERT BANNERS (always visible, top priority)
    # ══════════════════════════════════════════════════════════
    # BP negative alert
    if bp < 0:
        st.error(f"🔴 **BUYING POWER NEGATIVE: ${bp:,.0f}** — Do NOT sell new CSPs until capital is freed.")

    # Per-ticker threshold alerts (use pot-scoped allocation)
    capital_allocation = capital_allocation_for_view
    for ticker, ticker_data in capital_data['by_ticker'].items():
        ticker_total = ticker_data.get('total_committed', 0)
        ticker_cap = capital_allocation.get(ticker, 0)
        if ticker_cap > 0 and ticker_total > ticker_cap:
            pct = (ticker_total / ticker_cap * 100)
            st.warning(f"⚠️ **{ticker}** exceeds soft cap: ${ticker_total:,.0f} used / ${ticker_cap:,.0f} allocated ({pct:.0f}%)")
        # Single ticker > 30% of portfolio
        if portfolio_deposit > 0 and ticker_total > portfolio_deposit * 0.30:
            pct_port = (ticker_total / portfolio_deposit * 100)
            st.warning(f"⚠️ **{ticker}** concentration: {pct_port:.0f}% of portfolio (>${30}% threshold)")

    # Crypto cluster alert
    crypto_tickers = ['MARA', 'CRCL', 'ETHA', 'SOL']
    crypto_total = sum(capital_data['by_ticker'].get(t, {}).get('total_committed', 0) for t in crypto_tickers)
    if portfolio_deposit > 0 and crypto_total > portfolio_deposit * 0.40:
        crypto_pct = (crypto_total / portfolio_deposit * 100)
        st.error(f"🔴 **Crypto cluster at {crypto_pct:.0f}%** (MARA+CRCL+ETHA+SOL) — exceeds 40% cap. Total: ${crypto_total:,.0f}")

    # ══════════════════════════════════════════════════════════
    # PHASE 3.2: SIMPLIFIED CAPITAL SUMMARY (5 metrics, not 7+3)
    # ══════════════════════════════════════════════════════════
    # ══════════════════════════════════════════════════════════
    # SECTION A: ACCOUNT VALUE (live mark-to-market)
    # ══════════════════════════════════════════════════════════
    # NAV = Deposit + Realized P&L + Unrealized P&L
    nav = portfolio_deposit + total_pl
    nav_delta = nav - portfolio_deposit
    nav_delta_pct = (nav_delta / portfolio_deposit * 100) if portfolio_deposit > 0 else 0

    # MMF cash estimate: what's actually sitting at broker after real margin held
    # NAV - stock at market - LEAP cost - Tiger actual CSP margin
    _tiger_for_mmf = UnifiedCapitalCalculator.calculate_tiger_margin(df_open, live_prices)
    cash_idle_mmf = nav - total_stock_at_current - total_leap_sunk - _tiger_for_mmf['csp_margin']
    cash_idle_mmf = max(0, cash_idle_mmf)
    mmf_yield_annual = cash_idle_mmf * 0.05  # ~5% MMF yield estimate

    st.markdown("### 📈 Account Value (live)")
    # FX rate for SGD display
    from persistence import get_fx_rate
    _fx = get_fx_rate(portfolio) or 1.35
    nav_sgd = nav * _fx

    nav_col1, nav_col2, nav_col3, nav_col4 = st.columns(4)
    with nav_col1:
        st.metric("Net Account Value",
                   f"${nav:,.0f}",
                   delta=f"{nav_delta_pct:+.1f}% vs deposit",
                   help="Deposit + Realized P&L + Unrealized P&L (Stock + LEAP at live prices)")
        st.caption(f"≈ SGD {nav_sgd:,.0f} (at {_fx:.4f})")
    with nav_col2:
        st.metric("Stock at Market",
                   f"${total_stock_at_current:,.0f}",
                   help="Current market value of stock holdings")
    with nav_col3:
        # LEAP MTM = LEAP cost + LEAP unrealized P&L. If live options data
        # was fetched from Alpaca, this is TRUE market value. Otherwise it's
        # cost basis with intrinsic-only adjustment.
        _leap_unrealized = comprehensive_pnl['unrealized_leap_pnl']['total']
        leap_mtm = total_leap_sunk + _leap_unrealized
        _has_alpaca = bool(st.session_state.get("open_positions_data"))
        leap_label = "LEAP at Market" if _has_alpaca else "LEAP at Cost"
        leap_help = (
            "Live mid-price × 100 × contracts (Alpaca MTM)"
            if _has_alpaca else
            "Cost basis (premium paid). Click 'Refresh Open Positions' on Market Data page for live MTM."
        )
        st.metric(leap_label,
                   f"${leap_mtm:,.0f}",
                   delta=f"{_leap_unrealized:+,.0f} unrealized" if _leap_unrealized != 0 else None,
                   help=leap_help)
    with nav_col4:
        st.metric("Cash idle (MMF est.)",
                   f"${cash_idle_mmf:,.0f}",
                   delta=f"~${mmf_yield_annual:,.0f}/yr at 5%",
                   help="NAV − Stock@market − LEAP − Tiger margin held. Estimated cash earning yield in MMF. Golden figure is in your Tiger account.")

    # ── LEAP P&L TRANSPARENCY (drill-down per position) ─────────
    _leaps_in_view = df_open[df_open['TradeType'] == 'LEAP'].copy() if 'TradeType' in df_open.columns else pd.DataFrame()
    if not _leaps_in_view.empty:
        with st.expander(f"🔍 LEAP P&L breakdown ({len(_leaps_in_view)} positions) — click to verify each line vs broker", expanded=True):
            # Inline Alpaca refresh button (no need to navigate to Market Data)
            _refresh_col, _status_col = st.columns([1, 4])
            with _refresh_col:
                if st.button("🔄 Fetch Alpaca live mid-prices", key="leap_alpaca_refresh", use_container_width=True):
                    try:
                        df_options_open = df_open[df_open['TradeType'].isin(['CC', 'CSP', 'LEAP'])].copy()
                        if not df_options_open.empty:
                            with st.spinner("Fetching Alpaca options data..."):
                                fresh = _market_data.get_open_positions_data(df_options_open)
                                st.session_state.open_positions_data = fresh
                                st.toast(f"Loaded {len(fresh)} live contracts", icon="✅")
                                st.rerun()
                    except Exception as e:
                        st.error(f"Alpaca fetch failed: {e}")

            with _status_col:
                if not _has_alpaca:
                    st.warning("⚠️ Using **intrinsic-only** valuation. Click ← to fetch Alpaca live mid-prices for true MTM.")
                else:
                    st.success(f"✅ Using **live Alpaca mid-prices** for {len(_live_options)} contracts.")

            # Build live-options lookup
            _live_lookup = {}
            for c in _live_options:
                try:
                    mid = c.last_price if c.last_price > 0 else (c.bid + c.ask) / 2
                    if mid > 0:
                        _live_lookup[(c.underlying, float(c.strike), c.right, str(c.expiry))] = (mid, c.bid, c.ask, c.last_price)
                except Exception:
                    continue

            # Build breakdown — rows are the SOURCE OF TRUTH for totals
            _leap_rows = []
            _running_cost = 0.0
            _running_mtm = 0.0
            for _, r in _leaps_in_view.iterrows():
                tid = r.get('TradeID', '')
                ticker = r.get('Ticker', '')
                strike = float(pd.to_numeric(r.get('Option_Strike_Price_(USD)', 0), errors='coerce') or 0)
                premium = float(pd.to_numeric(r.get('OptPremium', 0), errors='coerce') or 0)
                qty = abs(int(pd.to_numeric(r.get('Quantity', 0), errors='coerce') or 0))
                expiry_dt = pd.to_datetime(r.get('Expiry_Date'), errors='coerce')
                expiry_str = expiry_dt.strftime('%Y-%m-%d') if pd.notna(expiry_dt) else ''
                spot = float(live_prices.get(ticker, 0) or 0)

                cost = premium * 100 * qty
                _running_cost += cost

                # Try Alpaca mid first
                live_data = _live_lookup.get((ticker, strike, 'C', expiry_str))
                if live_data:
                    mid, bid, ask, last = live_data
                    contract_value = mid
                    valuation_source = f"Alpaca: bid {bid:.2f} / ask {ask:.2f} / last {last:.2f}"
                else:
                    # Intrinsic fallback
                    contract_value = max(0, spot - strike) if spot > 0 else 0
                    valuation_source = f"Intrinsic only: max(0, {spot:.2f} − {strike:.2f})"

                mtm_value = contract_value * 100 * qty
                _running_mtm += mtm_value
                pl = mtm_value - cost
                dte = (expiry_dt - pd.Timestamp.now()).days if pd.notna(expiry_dt) else 0

                _leap_rows.append({
                    'TradeID': tid,
                    'Strike': f"${strike:,.2f}",
                    'Expiry': expiry_str,
                    'DTE': dte,
                    'Qty': qty,
                    'Cost/contract': f"${premium:,.2f}",
                    'Total Cost': f"${cost:,.0f}",
                    'Spot': f"${spot:,.2f}" if spot else "—",
                    'Current/contract': f"${contract_value:,.2f}",
                    'MTM Value': f"${mtm_value:,.0f}",
                    'P&L': f"{'+' if pl >= 0 else ''}${pl:,.0f}",
                    'Source': valuation_source,
                })

            df_leap_breakdown = pd.DataFrame(_leap_rows)
            st.dataframe(df_leap_breakdown, use_container_width=True, hide_index=True)

            # Totals — derived from rows above (guaranteed match)
            _total_pl = _running_mtm - _running_cost
            tcol1, tcol2, tcol3 = st.columns(3)
            tcol1.metric("Total LEAP Cost (Σ rows)", f"${_running_cost:,.0f}")
            tcol2.metric("Total LEAP MTM (Σ rows)", f"${_running_mtm:,.0f}")
            tcol3.metric("Total Unrealized P&L", f"${_total_pl:+,.0f}")

            st.caption(
                "Totals computed by summing the rows above — guaranteed to match. "
                "**Compare each row to your Tiger broker:** P&L column should be very close. "
                "Big drift = wrong cost basis, missing roll, or wrong expiry in the GSheet."
            )

    st.divider()

    # ══════════════════════════════════════════════════════════
    # SECTION B: SELLING CAPACITY (how much can I deploy)
    # ══════════════════════════════════════════════════════════
    _tiger = UnifiedCapitalCalculator.calculate_tiger_margin(df_open, live_prices)
    # Tiger BP = if you used broker margin instead of cash-secured for CSPs
    tiger_bp = bp + _tiger['headroom']  # cash-secured BP + headroom freed by using broker margin
    weekly_target_pct = 0.25  # 25% of available BP per week
    weekly_pacing = max(0, bp) * weekly_target_pct
    daily_pacing = weekly_pacing / 5

    st.markdown("### 💼 How Much Can I Sell?")
    bp_col1, bp_col2, bp_col3 = st.columns(3)
    with bp_col1:
        st_metric_with_negatives("Cash-Secured BP", bp, decimals=0, is_currency=True,
                                  help_text="Your discipline: max new CSPs without using broker margin")
        st.caption("**Your policy ceiling**")
    with bp_col2:
        st.metric("Tiger Margin BP (FYI)",
                   f"${tiger_bp:,.0f}",
                   help="Broker would allow this much, but charges ~6-8% interest on margin used")
        st.caption("Reference only — interest cost applies if used")
    with bp_col3:
        st.metric("This Week Target (25%)",
                   f"${weekly_pacing:,.0f}",
                   delta=f"~${daily_pacing:,.0f}/day",
                   help="25% of cash-secured BP deployed per week, paced over 5 trading days")
        st.caption("Conservative pacing")

    if bp < 0:
        st.error(f"🔴 **Buying Power negative ({'-' if bp < 0 else ''}${abs(bp):,.0f})** — "
                  "You're past your cash-secured ceiling. Free capital before opening new CSPs.")

    # Tiger margin per-position breakdown (collapsed for detail)
    with st.expander("📊 Tiger margin breakdown by position", expanded=False):
        st.caption(
            "**Tiger formula:** 30% × spot × 100 + premium received − OTM amount, capped at strike. "
            "Margin scales up to 100% in high volatility. Static 30% baseline estimate."
        )
        if _tiger['by_position']:
            _df_tiger = pd.DataFrame(_tiger['by_position'])
            _df_tiger['CashSecured'] = _df_tiger['CashSecured'].apply(lambda v: f"${v:,.0f}")
            _df_tiger['TigerMargin'] = _df_tiger['TigerMargin'].apply(lambda v: f"${v:,.0f}")
            _df_tiger['Headroom'] = _df_tiger['Headroom'].apply(lambda v: f"${v:,.0f}")
            _df_tiger['Strike'] = _df_tiger['Strike'].apply(lambda v: f"${v:.2f}")
            _df_tiger['Spot'] = _df_tiger['Spot'].apply(lambda v: f"${v:.2f}" if v > 0 else "—")
            st.dataframe(_df_tiger, use_container_width=True, hide_index=True)
        else:
            st.info("No open CSPs to display.")

    # ══════════════════════════════════════════════════════════
    # SECTION C: ALLOCATION DRILL-DOWN (% per ticker → $ → pacing)
    # ══════════════════════════════════════════════════════════
    st.markdown("### 🎯 Allocation Drill-Down (per ticker)")
    st.caption("% allocations are set in **Margin Config → Capital Allocation**. "
                "Targets below are **25% of remaining capital per week**, paced over 5 trading days.")

    # Pull deployed (committed) per ticker from capital_data — already includes Stock@buy + LEAP + CSP
    _alloc_rows = []
    _allocated_tickers = set(capital_allocation.keys())
    _all_position_tickers = set(capital_data['by_ticker'].keys())
    _explicit_tickers = sorted(_allocated_tickers, key=lambda t: (0 if t in ['MARA', 'CRCL', 'SPY'] else 1, t))

    _total_explicit_alloc = sum(capital_allocation.values())
    _others_alloc = max(0, portfolio_deposit - _total_explicit_alloc)
    _others_pct = (_others_alloc / portfolio_deposit * 100) if portfolio_deposit > 0 else 0

    # Tickers with positions but NOT in explicit allocation → fall under OTHERS
    _other_position_tickers = _all_position_tickers - _allocated_tickers
    _others_deployed = sum(
        capital_data['by_ticker'].get(t, {}).get('total_committed', 0)
        for t in _other_position_tickers
    )

    for ticker in _explicit_tickers:
        allocated = capital_allocation.get(ticker, 0)
        pct_alloc = (allocated / portfolio_deposit * 100) if portfolio_deposit > 0 else 0
        deployed = capital_data['by_ticker'].get(ticker, {}).get('total_committed', 0)
        remaining = max(0, allocated - deployed)
        pct_used = (deployed / allocated * 100) if allocated > 0 else 0
        weekly_t = remaining * 0.25
        daily_t = weekly_t / 5
        if pct_used > 100:
            status = "🔴 Over"
        elif pct_used > 85:
            status = "🟡 High"
        elif pct_used > 60:
            status = "🟠 Mid"
        else:
            status = "🟢 OK"
        _alloc_rows.append({
            'Ticker': ticker,
            'Allocation %': f"{pct_alloc:.1f}%",
            'Allocated $': f"${allocated:,.0f}",
            'Deployed $': f"${deployed:,.0f}",
            'Remaining $': f"${remaining:,.0f}",
            '% Used': f"{pct_used:.0f}%",
            'Weekly Target': f"${weekly_t:,.0f}",
            'Daily Target': f"${daily_t:,.0f}",
            'Status': status,
        })

    # OTHERS bucket
    if _others_alloc > 0 or _others_deployed > 0:
        _others_remaining = max(0, _others_alloc - _others_deployed)
        _others_pct_used = (_others_deployed / _others_alloc * 100) if _others_alloc > 0 else 0
        _others_weekly = _others_remaining * 0.25
        _others_daily = _others_weekly / 5
        if _others_alloc == 0 and _others_deployed > 0:
            _others_status = "🔴 No alloc"
        elif _others_pct_used > 100:
            _others_status = "🔴 Over"
        elif _others_pct_used > 85:
            _others_status = "🟡 High"
        elif _others_pct_used > 60:
            _others_status = "🟠 Mid"
        else:
            _others_status = "🟢 OK"
        _alloc_rows.append({
            'Ticker': f'OTHERS ({", ".join(sorted(_other_position_tickers)) if _other_position_tickers else "—"})',
            'Allocation %': f"{_others_pct:.1f}%",
            'Allocated $': f"${_others_alloc:,.0f}",
            'Deployed $': f"${_others_deployed:,.0f}",
            'Remaining $': f"${_others_remaining:,.0f}",
            '% Used': f"{_others_pct_used:.0f}%" if _others_alloc > 0 else "—",
            'Weekly Target': f"${_others_weekly:,.0f}",
            'Daily Target': f"${_others_daily:,.0f}",
            'Status': _others_status,
        })

    # TOTAL row
    _total_deployed = sum(d.get('total_committed', 0) for d in capital_data['by_ticker'].values())
    _total_remaining = max(0, portfolio_deposit - _total_deployed)
    _total_pct_used = (_total_deployed / portfolio_deposit * 100) if portfolio_deposit > 0 else 0
    _alloc_rows.append({
        'Ticker': '**TOTAL**',
        'Allocation %': '100.0%',
        'Allocated $': f"${portfolio_deposit:,.0f}",
        'Deployed $': f"${_total_deployed:,.0f}",
        'Remaining $': f"${_total_remaining:,.0f}",
        '% Used': f"{_total_pct_used:.0f}%",
        'Weekly Target': f"${_total_remaining * 0.25:,.0f}",
        'Daily Target': f"${_total_remaining * 0.25 / 5:,.0f}",
        'Status': '—',
    })

    df_alloc = pd.DataFrame(_alloc_rows)
    st.dataframe(df_alloc, use_container_width=True, hide_index=True,
                  column_config={
                      "Ticker": st.column_config.TextColumn("Ticker", width="medium"),
                      "Allocation %": st.column_config.TextColumn("Alloc %", width="small"),
                      "Allocated $": st.column_config.TextColumn("Allocated", width="small"),
                      "Deployed $": st.column_config.TextColumn("Deployed", width="small"),
                      "Remaining $": st.column_config.TextColumn("Remaining", width="small"),
                      "% Used": st.column_config.TextColumn("% Used", width="small"),
                      "Weekly Target": st.column_config.TextColumn("Week", width="small"),
                      "Daily Target": st.column_config.TextColumn("Day", width="small"),
                      "Status": st.column_config.TextColumn("Status", width="small"),
                  })
    st.caption("Status: 🟢 <60% | 🟠 60-85% | 🟡 85-100% | 🔴 >100% (over allocation)")

    # ----- CSP Reserved expandable: Open CSPs, Counters, Expiry, Income ladder (YTD, MTD, Next 4 weeks) -----
    with st.expander("CSP Reserved – Open CSPs, counters, expiry & income ladder (pace and deploy BP)", expanded=False):
        df_csp_open = df_open[(df_open['TradeType'] == 'CSP')].copy() if df_open is not None and not df_open.empty and 'TradeType' in df_open.columns else pd.DataFrame()
        if not df_csp_open.empty:
            df_csp_open = df_csp_open.copy()
            df_csp_open['Expiry_Date'] = pd.to_datetime(df_csp_open['Expiry_Date'], errors='coerce')
            strike_col = 'Option_Strike_Price_(USD)' if 'Option_Strike_Price_(USD)' in df_csp_open.columns else 'Strike'
            qty_col = 'Quantity'
            prem_col = 'OptPremium'
            df_csp_open['Reserved'] = pd.to_numeric(df_csp_open.get(strike_col, 0), errors='coerce').fillna(0) * 100 * pd.to_numeric(df_csp_open[qty_col], errors='coerce').fillna(0)
            df_csp_open['Premium_at_expiry'] = pd.to_numeric(df_csp_open.get(prem_col, 0), errors='coerce').fillna(0) * 100 * pd.to_numeric(df_csp_open[qty_col], errors='coerce').fillna(0)
            cols_show = [c for c in ['Ticker', 'TradeID', strike_col, qty_col, 'Expiry_Date', 'Reserved', 'Premium_at_expiry'] if c in df_csp_open.columns]
            st.write("**Open CSPs**")
            exp_display = df_csp_open['Expiry_Date'].dt.strftime('%Y-%m-%d') if df_csp_open['Expiry_Date'].notna().any() else df_csp_open['Expiry_Date'].astype(str)
            st.dataframe(df_csp_open[cols_show].assign(Expiry_Date=exp_display), use_container_width=True, hide_index=True)
            counters = sorted(df_csp_open['Ticker'].dropna().unique().tolist())
            st.write("**Counters:** " + ", ".join(counters) if counters else "—")
        else:
            st.write("**Open CSPs:** None.")
            counters = []
        st.write("**Income ladder (CSP only)**")
        df_trades_csp = df_trades[(df_trades['TradeType'] == 'CSP')].copy() if df_trades is not None and not df_trades.empty and 'TradeType' in df_trades.columns else pd.DataFrame()
        df_open_csp = df_csp_open.copy() if not df_csp_open.empty else pd.DataFrame()
        ytd_csp = month_csp = 0.0
        if not df_trades_csp.empty:
            df_trades_csp['Expiry_Date'] = pd.to_datetime(df_trades_csp['Expiry_Date'], errors='coerce')
            closed_csp = df_trades_csp[df_trades_csp['Status'].str.upper() == 'CLOSED']
            if not closed_csp.empty and 'Actual_Profit_(USD)' in closed_csp.columns:
                closed_csp = closed_csp.copy()
                closed_csp['_profit'] = pd.to_numeric(closed_csp['Actual_Profit_(USD)'], errors='coerce').fillna(0)
                closed_csp['_expiry_d'] = closed_csp['Expiry_Date'].dt.date
                today_d = date.today()
                ytd_csp = closed_csp[closed_csp['_expiry_d'].apply(lambda x: x.year == today_d.year if hasattr(x, 'year') else False)]['_profit'].sum()
                start_m = date(today_d.year, today_d.month, 1)
                end_m = date(today_d.year, today_d.month + 1, 1) - timedelta(days=1) if today_d.month < 12 else date(today_d.year + 1, 1, 1) - timedelta(days=1)
                month_csp = closed_csp[(closed_csp['_expiry_d'] >= start_m) & (closed_csp['_expiry_d'] <= end_m)]['_profit'].sum()
        lc1, lc2, lc3 = st.columns(3)
        with lc1:
            st.metric("CSP income YTD", f"${float(ytd_csp):,.0f}")
        with lc2:
            st.metric("CSP income MTD", f"${float(month_csp):,.0f}")
        next4_rows = []
        if not df_open_csp.empty and 'Expiry_Date' in df_open_csp.columns and 'Premium_at_expiry' in df_open_csp.columns:
            today_d = date.today()
            days_until_fri = (4 - today_d.weekday()) % 7
            next_fri = today_d + timedelta(days=days_until_fri)
            for i in range(4):
                week_end = next_fri + timedelta(days=7 * i)
                week_start = week_end - timedelta(days=6)
                mask = (df_open_csp['Expiry_Date'].dt.date >= week_start) & (df_open_csp['Expiry_Date'].dt.date <= week_end)
                exp_week = df_open_csp.loc[mask]
                prem_week = exp_week['Premium_at_expiry'].sum()
                next4_rows.append({"Week ending": week_end.strftime("%Y-%m-%d"), "CSP premium (expiring)": prem_week})
        with lc3:
            if next4_rows:
                st.write("**Next 4 weeks (expiring)**")
                st.dataframe(pd.DataFrame(next4_rows), use_container_width=True, hide_index=True)
            else:
                st.write("**Next 4 weeks:** No open CSPs expiring.")
        st.caption("Use this to pace and deploy available BP.")
    
    st.divider()
    
    # ----- Profit and Loss (by Ticker) - one line + dropdown for details -----
    # comprehensive_pnl already computed above for Total P/L in summary
    st.write("**Profit and Loss (by Ticker)**")
    
    # Summary card: Nett = Total Premium + Total P/L
    nett_pl = total_pl  # Total premium income + Total Stock/Leaps P/L (from summary)
    nett_str = f"${nett_pl:,.0f}" if nett_pl >= 0 else f"(${abs(nett_pl):,.0f})"
    nett_color = "#0f172a" if nett_pl >= 0 else "#ff4444"
    st.markdown(
        f'''
        <div style="background: #f8fafc; border-radius: 12px; padding: 1rem 1.25rem; margin: 0.75rem 0; border: 1px solid #e2e8f0;">
            <p style="color: #1e3a5f; font-size: 0.85rem; font-weight: 600; margin: 0 0 0.25rem 0;">Nett P/L</p>
            <p style="color: {nett_color}; font-size: 1.5rem; font-weight: 700; margin: 0;">{nett_str}</p>
            <p style="color: #64748b; font-size: 0.75rem; margin: 0.35rem 0 0 0;">Total Premium Income + Total P/L (Stock + LEAP)</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    
    # Realized P&L by ticker (all closed trades: CC, CSP, LEAP, STOCK)
    premium_collected_by_ticker = {}
    if df_trades is not None and not df_trades.empty:
        closed_opts = df_trades[(df_trades['Status'] == 'Closed') & (df_trades['TradeType'].isin(['CC', 'CSP', 'LEAP', 'STOCK']))].copy()
        if not closed_opts.empty:
            if 'Actual_Profit_(USD)' in closed_opts.columns:
                closed_opts['_prem'] = pd.to_numeric(closed_opts['Actual_Profit_(USD)'], errors='coerce').fillna(0)
            else:
                closed_opts['_prem'] = pd.to_numeric(closed_opts.get('OptPremium', 0), errors='coerce').fillna(0) * 100 * pd.to_numeric(closed_opts.get('Quantity', 0), errors='coerce').fillna(0)
            prem_agg = closed_opts.groupby('Ticker')['_prem'].sum()
            premium_collected_by_ticker = prem_agg.to_dict()
    
    stock_pl_by_ticker = comprehensive_pnl.get('unrealized_stock_pnl', {}).get('by_ticker', {})
    leap_pl_by_ticker = comprehensive_pnl.get('unrealized_leap_pnl', {}).get('by_ticker', {})
    all_pl_tickers = sorted(set(stock_pl_by_ticker.keys()) | set(leap_pl_by_ticker.keys()) | set(premium_collected_by_ticker.keys()))
    
    if all_pl_tickers:
        # Row total: premium collected
        total_premium_row = sum(premium_collected_by_ticker.get(t, 0.0) for t in all_pl_tickers)
        total_stock_pl_row = sum(stock_pl_by_ticker.get(t, 0.0) for t in all_pl_tickers)
        total_leap_pl_row = sum(leap_pl_by_ticker.get(t, 0.0) for t in all_pl_tickers)
        total_pl_row = total_stock_pl_row + total_leap_pl_row
        
        # One line: Premium collected by ticker + Total (before P/L)
        st.caption("Realized P&L (closed CC/CSP/LEAP/STOCK)")
        n_cols = min(len(all_pl_tickers), 8)
        prem_cols = st.columns(n_cols + 1)
        for i, ticker in enumerate(all_pl_tickers[:n_cols]):
            prem = premium_collected_by_ticker.get(ticker, 0.0)
            with prem_cols[i]:
                st_metric_with_negatives(ticker, prem, decimals=0, is_currency=True,
                                         help_text="Premium collected from closed CC/CSP")
        with prem_cols[n_cols]:
            st_metric_with_negatives("Total", total_premium_row, decimals=0, is_currency=True,
                                     help_text="Total premium collected (all tickers)")
        if len(all_pl_tickers) > 8:
            st.caption(f"Showing first 8 of {len(all_pl_tickers)} tickers. See dropdown for full list.")
        
        # One line: P/L per ticker + Total P/L
        st.caption("Unrealized P/L (Stock + LEAP)")
        n_pl = min(len(all_pl_tickers), 8)
        pl_cols = st.columns(n_pl + 1)
        for i, ticker in enumerate(all_pl_tickers[:n_pl]):
            stock_pl = stock_pl_by_ticker.get(ticker, 0.0)
            leap_pl = leap_pl_by_ticker.get(ticker, 0.0)
            total_pl = stock_pl + leap_pl
            with pl_cols[i]:
                st_metric_with_negatives(ticker, total_pl, decimals=0, is_currency=True,
                                         help_text=f"Stock P/L: ${stock_pl:,.0f} | LEAP P/L: ${leap_pl:,.0f}")
        with pl_cols[n_pl]:
            st_metric_with_negatives("Total P/L", total_pl_row, decimals=0, is_currency=True,
                                     help_text="Sum of all tickers (Stock + LEAP unrealized P/L)")
    else:
        st.caption("No premium or unrealized P/L data by ticker.")
    
    st.divider()
    
    # ══════════════════════════════════════════════════════════
    # PHASE 3.3: PER-TICKER CASH PANEL (always visible)
    # ══════════════════════════════════════════════════════════
    st.subheader("💰 Capital by Ticker")
    all_cap_tickers = sorted(capital_data['by_ticker'].keys(),
                             key=lambda t: (0 if t in ['MARA', 'CRCL', 'SPY'] else 1, t))
    if all_cap_tickers:
        cap_rows = []
        for ticker in all_cap_tickers:
            d = capital_data['by_ticker'][ticker]
            stock_val = d.get('stock_at_current_price', 0)
            leap_val = d.get('leap_sunk', 0)
            csp_val = d.get('csp_reserved', 0)
            total_val = d.get('total_committed', stock_val + leap_val + csp_val)
            soft_cap = capital_allocation.get(ticker, 0)
            remaining = soft_cap - total_val if soft_cap > 0 else 0
            pct_used = (total_val / soft_cap * 100) if soft_cap > 0 else 0
            # Status indicator
            if soft_cap == 0:
                status = "—"
            elif pct_used > 100:
                status = "🔴 Over"
            elif pct_used > 85:
                status = "🟡 High"
            elif pct_used > 60:
                status = "🟠 Mid"
            else:
                status = "🟢 OK"
            cap_rows.append({
                'Ticker': ticker,
                'Soft Cap': f"${soft_cap:,.0f}" if soft_cap > 0 else "—",
                'Stock': f"${stock_val:,.0f}" if stock_val else "$0",
                'LEAP': f"${leap_val:,.0f}" if leap_val else "$0",
                'CSP': f"${csp_val:,.0f}" if csp_val else "$0",
                'Total': f"${total_val:,.0f}",
                'Remaining': f"${remaining:,.0f}" if soft_cap > 0 else "—",
                '% Used': f"{pct_used:.0f}%" if soft_cap > 0 else "—",
                'Status': status,
            })
        df_cap = pd.DataFrame(cap_rows)
        st.dataframe(df_cap, use_container_width=True, hide_index=True,
                      column_config={
                          "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                          "Soft Cap": st.column_config.TextColumn("Soft Cap", width="small"),
                          "Stock": st.column_config.TextColumn("Stock", width="small"),
                          "LEAP": st.column_config.TextColumn("LEAP", width="small"),
                          "CSP": st.column_config.TextColumn("CSP", width="small"),
                          "Total": st.column_config.TextColumn("Total", width="small"),
                          "Remaining": st.column_config.TextColumn("Remaining", width="small"),
                          "% Used": st.column_config.TextColumn("% Used", width="small"),
                          "Status": st.column_config.TextColumn("Status", width="small"),
                      })
        st.caption("Soft Cap from Margin Config. Status: 🟢 <60% | 🟠 60-85% | 🟡 >85% | 🔴 >100%")
    st.divider()
    
    # Premium Stats (based on expiry dates)
    
    st.subheader("💵 Premium Collected (Options Only)")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        week_stats = PremiumCalculator.calculate_premium_stats(df_trades, df_open, 'week')
        st_metric_with_negatives("To Collect This Week", week_stats.get('premium_to_collect', 0), 
                                 delta="Expiring + Open", decimals=0, is_currency=True)
        collected_text = format_currency_for_display(week_stats.get('premium_collected', 0), decimals=0, show_cents=False)
        st.markdown(f"Collected: {collected_text}", unsafe_allow_html=True)
    
    with col2:
        week_stats = PremiumCalculator.calculate_premium_stats(df_trades, df_open, 'week')
        st_metric_with_negatives("Collected This Week", week_stats.get('premium_collected', 0),
                                 delta="Expired + Closed", decimals=0, is_currency=True)
    
    with col3:
        month_stats = PremiumCalculator.calculate_premium_stats(df_trades, df_open, 'month')
        st_metric_with_negatives("Month to Date", month_stats['total_premium'],
                                 delta="Expiry this month", decimals=0, is_currency=True)
    
    with col4:
        ytd_stats = PremiumCalculator.calculate_premium_stats(df_trades, df_open, 'ytd')
        st_metric_with_negatives("Year to Date", ytd_stats['total_premium'],
                                 delta="Expiry this year", decimals=0, is_currency=True)
    
    # Premium breakdown by ticker
    st.write("**Premium Breakdown by Ticker:**")
    
    # Calculate premium by ticker for each period
    ticker_premium_data = []
    all_tickers = df_trades['Ticker'].unique().tolist()
    
    for ticker in all_tickers:
        ticker_trades = df_trades[df_trades['Ticker'] == ticker].copy()
        if not df_open.empty:
            ticker_open = df_open[df_open['Ticker'] == ticker].copy()
        else:
            ticker_open = pd.DataFrame()
        
        # Week stats
        week_stats = PremiumCalculator.calculate_premium_stats(ticker_trades, ticker_open if not ticker_open.empty else None, 'week')
        # Month stats
        month_stats = PremiumCalculator.calculate_premium_stats(ticker_trades, ticker_open if not ticker_open.empty else None, 'month')
        # YTD stats
        ytd_stats = PremiumCalculator.calculate_premium_stats(ticker_trades, ticker_open if not ticker_open.empty else None, 'ytd')
        
        ticker_premium_data.append({
            'Ticker': ticker,
            'To Collect This Week': week_stats.get('premium_to_collect', 0),
            'Collected This Week': week_stats.get('premium_collected', 0),
            'Month to Date': month_stats.get('total_premium', 0),
            'Year to Date': ytd_stats.get('total_premium', 0)
        })
    
    if ticker_premium_data:
        df_premium_by_ticker = pd.DataFrame(ticker_premium_data)
        df_premium_by_ticker = df_premium_by_ticker.sort_values('Year to Date', ascending=False)
        
        # Format for display with negative value handling
        df_premium_display = df_premium_by_ticker.copy()
        # Apply styling for negative values (red color) - this will format the currency columns
        styled_df = style_dataframe_negatives(df_premium_display, 
                                               currency_columns=['To Collect This Week', 'Collected This Week', 'Month to Date', 'Year to Date'])
        
        st.dataframe(
            styled_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                "To Collect This Week": st.column_config.TextColumn("To Collect This Week", width="medium"),
                "Collected This Week": st.column_config.TextColumn("Collected This Week", width="medium"),
                "Month to Date": st.column_config.TextColumn("Month to Date", width="medium"),
                "Year to Date": st.column_config.TextColumn("Year to Date", width="medium")
            }
        )
    else:
        st.info("No premium data available by ticker.")
    
    st.divider()
    
    # Position Inventory - By Ticker breakdown with CC coverage ratio
    st.subheader("📦 Position Inventory by Ticker")
    if inventory['positions_by_ticker']:
        st.write("**By Ticker:**")
        ticker_data = []
        for ticker, counts in inventory['positions_by_ticker'].items():
            # Format CC coverage ratio
            # If ratio > 0 and <= 1.0: Stock coverage % (stock shares / CC shares needed)
            # If ratio > 1.0: CC/LEAP ratio (CC contracts / LEAPs, multiply by 100 for %)
            # If ratio < 0: Uncovered (flag value -1.0)
            if counts.get('cc_coverage_ratio') is not None:
                coverage_ratio = counts['cc_coverage_ratio']
                if coverage_ratio < 0:
                    # Uncovered CCs (no stock/LEAPs)
                    coverage_display = "Uncovered ⚠️"
                elif coverage_ratio <= 1.0:
                    # Stock coverage % (CC shares needed / stock shares)
                    # Shows what % of shares are committed to covering calls
                    coverage_pct = coverage_ratio * 100
                    if coverage_pct >= 100:
                        coverage_display = f"{coverage_pct:.0f}% ✅"
                    elif coverage_pct >= 80:
                        coverage_display = f"{coverage_pct:.0f}% ⚠️"
                    else:
                        # Under 80% means well covered with excess capacity
                        coverage_display = f"{coverage_pct:.0f}% ✅"
                else:
                    # Over 100% means uncovered (more calls than shares can cover)
                    coverage_pct = coverage_ratio * 100
                    coverage_display = f"{coverage_pct:.0f}% ❌"
            else:
                coverage_display = "N/A"
            
            # Add CSP Reserved $ from capital_data
            csp_reserved_val = capital_data['by_ticker'].get(ticker, {}).get('csp_reserved', 0)
            # Uncovered alert: CSP with no stock/LEAP
            has_underlying = counts.get('stock', 0) > 0 or counts.get('leaps', 0) > 0
            uncovered_flag = " ⚠️ Naked" if counts['csp'] > 0 and not has_underlying else ""
            ticker_data.append({
                'Ticker': ticker,
                'CC': counts['cc'],
                'CSP': counts['csp'],
                'CSP Reserved': f"${csp_reserved_val:,.0f}" if csp_reserved_val > 0 else "$0",
                'Stock (shares)': counts.get('stock', 0),
                'LEAPs (shares)': counts.get('leaps', 0),
                'Total Stock (shares)': counts.get('total_stock', counts.get('stock', 0)),
                'CC Coverage': coverage_display,
                'Notes': uncovered_flag
            })
        df_ticker = pd.DataFrame(ticker_data)
        
        # Format numbers for alignment
        df_ticker_display = df_ticker.copy()
        df_ticker_display['CC'] = df_ticker_display['CC'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
        df_ticker_display['CSP'] = df_ticker_display['CSP'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
        df_ticker_display['Stock (shares)'] = df_ticker_display['Stock (shares)'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
        df_ticker_display['LEAPs (shares)'] = df_ticker_display['LEAPs (shares)'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
        df_ticker_display['Total Stock (shares)'] = df_ticker_display['Total Stock (shares)'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
        
        # Improved visual table with styling and alignment
        st.dataframe(
            df_ticker_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                "CC": st.column_config.TextColumn("CC", width="small"),
                "CSP": st.column_config.TextColumn("CSP", width="small"),
                "CSP Reserved": st.column_config.TextColumn("CSP $", width="small"),
                "Stock (shares)": st.column_config.TextColumn("Stock", width="small"),
                "LEAPs (shares)": st.column_config.TextColumn("LEAPs", width="small"),
                "Total Stock (shares)": st.column_config.TextColumn("Total", width="small"),
                "CC Coverage": st.column_config.TextColumn("Coverage", width="small"),
                "Notes": st.column_config.TextColumn("Notes", width="small")
            }
        )
        st.caption("CC Coverage: If LEAPs exist, shows CC/LEAP ratio. Otherwise shows stock coverage % (CC shares needed / stock shares). Under 100% = fully covered ✅, Over 100% = uncovered ❌")

    st.divider()

    # ══════════════════════════════════════════════════════════
    # PHASE 7.1: PORTFOLIO GREEKS (Delta, Theta, Vega)
    # ══════════════════════════════════════════════════════════
    st.subheader("📐 Portfolio Risk Metrics")

    # Pull Greeks from cached market data (if Alpaca provided them)
    _open_positions_data = st.session_state.get("open_positions_data", [])
    portfolio_delta = 0.0
    portfolio_theta = 0.0
    portfolio_vega = 0.0
    greeks_available = False

    for contract in _open_positions_data:
        if hasattr(contract, 'delta') and contract.delta is not None:
            greeks_available = True
            # Find matching open position to get quantity
            _match = df_open[
                (df_open['Ticker'] == contract.underlying) &
                (df_open['TradeType'].isin(['CC', 'CSP']))
            ]
            for _, pos in _match.iterrows():
                _strike = float(pd.to_numeric(pos.get('Option_Strike_Price_(USD)', 0), errors='coerce') or 0)
                if abs(_strike - contract.strike) < 0.01:
                    qty = abs(int(pd.to_numeric(pos.get('Quantity', 0), errors='coerce') or 0))
                    direction = -1 if pos.get('Direction', 'Sell') == 'Sell' else 1
                    if contract.delta is not None:
                        portfolio_delta += contract.delta * qty * 100 * direction
                    if hasattr(contract, 'theta') and contract.theta is not None:
                        portfolio_theta += contract.theta * qty * 100 * direction
                    if hasattr(contract, 'vega') and contract.vega is not None:
                        portfolio_vega += contract.vega * qty * 100 * direction
                    break

    col_g1, col_g2, col_g3 = st.columns(3)
    with col_g1:
        st_metric_with_negatives("Portfolio Delta", portfolio_delta, decimals=0, is_currency=False)
        st.caption("Net delta exposure (+ = bullish, - = bearish)")
    with col_g2:
        st_metric_with_negatives("Daily Theta", portfolio_theta, decimals=0, is_currency=True)
        st.caption("Estimated daily time decay income")
    with col_g3:
        st_metric_with_negatives("Portfolio Vega", portfolio_vega, decimals=0, is_currency=False)
        st.caption("Sensitivity to 1% IV change")

    if not greeks_available:
        st.caption("Greeks require Alpaca API — click 'Refresh Open Positions Data' on Market Data page to load.")

    # ══════════════════════════════════════════════════════════
    # PHASE 7.2: ASSIGNMENT EXPOSURE
    # ══════════════════════════════════════════════════════════
    st.subheader("⚠️ Assignment Exposure")

    # For each open CSP: if ITM, calculate stock received + cost
    _open_csps = df_open[df_open['TradeType'] == 'CSP'].copy()
    assignment_rows = []
    total_assignment_cost = 0.0
    if not _open_csps.empty:
        for _, csp in _open_csps.iterrows():
            ticker = csp.get('Ticker', '')
            strike = float(pd.to_numeric(csp.get('Option_Strike_Price_(USD)', 0), errors='coerce') or 0)
            qty = abs(int(pd.to_numeric(csp.get('Quantity', 0), errors='coerce') or 0))
            current = live_prices.get(ticker, 0)
            if strike > 0 and current > 0 and current < strike:  # ITM
                cost = strike * 100 * qty
                loss = (strike - current) * 100 * qty
                total_assignment_cost += cost
                assignment_rows.append({
                    'Ticker': ticker,
                    'Strike': f"${strike:.2f}",
                    'Spot': f"${current:.2f}",
                    'Contracts': qty,
                    'Shares Received': qty * 100,
                    'Cost': f"${cost:,.0f}",
                    'Unrealized Loss': f"(${loss:,.0f})",
                })

    if assignment_rows:
        st.error(f"🔴 If all ITM puts assigned today: **{sum(r['Contracts'] for r in assignment_rows)} contracts** = **{sum(r['Shares Received'] for r in assignment_rows):,} shares** at **${total_assignment_cost:,.0f}** cost")
        st.dataframe(pd.DataFrame(assignment_rows), use_container_width=True, hide_index=True)
    else:
        st.success("✅ No ITM CSP positions — zero assignment risk")

    st.divider()


# ============================================================
# DAILY HELPER PAGE
# ============================================================
def _format_risk_label(risk: str, trade_type: str, current_price: float, strike: float) -> str:
    """Format risk label — never say 'Safe' when ITM."""
    emojis = {'HIGH': '🔴', 'MEDIUM': '🟡', 'LOW': '🟠', 'NONE': '🟢'}
    emoji = emojis.get(risk, '⚪')

    if risk != 'NONE':
        return f"{emoji} {risk}"

    # NONE risk — but check if actually ITM
    try:
        cp = float(current_price or 0)
        st = float(strike or 0)
        if cp > 0 and st > 0:
            if trade_type == 'CC' and cp > st:
                return "🟡 ITM"
            elif trade_type == 'CSP' and cp < st:
                return "🟡 ITM"
    except (ValueError, TypeError):
        pass

    return f"{emoji} OTM"


def render_daily_helper():
    """Render daily helper page"""
    st.title("📅 Daily Helper")
    
    df_open = st.session_state.df_open
    live_prices = st.session_state.live_prices
    
    if df_open is None or df_open.empty:
        st.warning("No open positions found.")
        return
    
    # Filter by strategy if selected
    strategy_filter = st.session_state.get('strategy_filter', 'All')
    if strategy_filter != 'All':
        portfolio = st.session_state.get('current_portfolio', 'Income Wheel')
        pmcc_tickers = get_pmcc_tickers(portfolio)
        pmcc_tickers_set = set(pmcc_tickers) if pmcc_tickers else set()
        
        if strategy_filter == 'PMCC':
            pmcc_mask = (
                df_open['Ticker'].isin(pmcc_tickers_set) |
                (df_open.get('StrategyType', '') == 'PMCC') |
                (df_open['TradeType'] == 'LEAP')
            )
            df_open = df_open[pmcc_mask].copy()
        elif strategy_filter == 'WHEEL':
            wheel_mask = (
                ~df_open['Ticker'].isin(pmcc_tickers_set) &
                (
                    (df_open.get('StrategyType', '') == 'WHEEL') |
                    (df_open.get('StrategyType', '').isna()) |
                    (df_open.get('StrategyType', '') == '')
                ) &
                (df_open['TradeType'] != 'LEAP')
            )
            df_open = df_open[wheel_mask].copy()
        elif strategy_filter == 'ActiveCore':
            df_open = df_open[df_open.get('StrategyType', '') == 'ActiveCore'].copy()

        if df_open.empty:
            st.info(f"ℹ️ No open positions found for {strategy_filter} strategy.")
            return

    # Get live prices (Yahoo Finance - always available)
    tickers = df_open['Ticker'].unique().tolist()
    
    try:
        live_prices = get_cached_prices(tuple(tickers))
        st.session_state.live_prices = live_prices
    except Exception as e:
        st.warning(f"⚠️ Could not fetch prices: {e}")
        live_prices = st.session_state.get('live_prices', {})

    # Fetch options data + Greeks for open positions (Mode 1 — auto-feed)
    try:
        df_options_open = df_open[df_open['TradeType'].isin(['CC', 'CSP'])].copy()
        if not df_options_open.empty:
            open_positions_data = _market_data.get_open_positions_data(df_options_open)
            st.session_state.open_positions_data = open_positions_data
    except Exception as e:
        st.warning(f"⚠️ Could not fetch options data: {e}")

    st.divider()

    # Live prices display
    st.subheader("📈 Live Prices")
    price_cols = st.columns(len(tickers))
    for i, ticker in enumerate(tickers):
        with price_cols[i]:
            price = live_prices.get(ticker)
            if price:
                st.metric(ticker, f"${price:.2f}")
            else:
                # Fallback to last known price from data source
                last_price = df_open[df_open['Ticker'] == ticker]['Price_of_current_underlying_(USD)'].iloc[0]
                st.metric(ticker, f"${last_price:.2f}", "Last known")
    
    st.divider()

    # ============================================================
    # CC Coverage Planner — All Tickers (next 5 weekly expiries)
    # ============================================================
    st.subheader("🧭 CC Coverage Planner (Next 5 Weekly Expiries)")

    try:
        from datetime import date as _date, timedelta as _timedelta

        # ── Derive CC-eligible tickers: any ticker that has STOCK in df_open ──
        _stock_rows = df_open[df_open["TradeType"] == "STOCK"].copy()
        # Use Open_lots (actual shares), NOT Quantity (contracts/lots)
        _stock_rows["Shares_num"] = pd.to_numeric(_stock_rows["Open_lots"], errors="coerce").fillna(
            pd.to_numeric(_stock_rows["Quantity"], errors="coerce").fillna(0).abs() * 100
        ).abs()
        # Per-ticker stock shares (sum in case of multiple rows)
        _ticker_shares: dict = (
            _stock_rows.groupby("Ticker")["Shares_num"].sum().to_dict()
        )
        # Also include any ticker that already has CC positions even if no STOCK row
        _cc_tickers_existing = df_open[df_open["TradeType"] == "CC"]["Ticker"].unique().tolist()
        _cc_eligible = sorted(
            set(list(_ticker_shares.keys()) + list(_cc_tickers_existing)),
            key=lambda t: (0 if t in ["MARA", "CRCL", "SPY"] else 1, t),
        )

        if not _cc_eligible:
            st.info("No CC-eligible tickers found (need STOCK or existing CC positions).")
        else:
            # Ticker selector dropdown
            _plan_col1, _plan_col2 = st.columns([2, 5])
            with _plan_col1:
                _default_idx = _cc_eligible.index("MARA") if "MARA" in _cc_eligible else 0
                _selected_planner_ticker = st.selectbox(
                    "Select ticker",
                    options=_cc_eligible,
                    index=_default_idx,
                    key="cc_planner_ticker_select",
                    label_visibility="collapsed",
                )

            # Weekly target: total shares / 100 (contracts) / 4 (weeks of coverage)
            _stock_shares_for_ticker = _ticker_shares.get(_selected_planner_ticker, 0)
            _weekly_target = int(round(_stock_shares_for_ticker / 100 / 4)) if _stock_shares_for_ticker >= 400 else 0

            with _plan_col2:
                if _stock_shares_for_ticker > 0:
                    st.caption(
                        f"**{_selected_planner_ticker}** · {int(_stock_shares_for_ticker):,} shares owned · "
                        f"Weekly target = {_weekly_target} contracts  "
                        f"*(= shares ÷ 4 ÷ 100)*"
                    )
                else:
                    st.caption(
                        f"**{_selected_planner_ticker}** · No STOCK row found — showing CC positions only. "
                        f"Set weekly target manually below."
                    )
                    _weekly_target = st.number_input(
                        "Manual weekly target (contracts)",
                        min_value=0, step=1, value=_weekly_target,
                        key="cc_planner_manual_target",
                    )

            # Build 5-week grid
            today_d = _date.today()
            days_until_fri = (4 - today_d.weekday()) % 7
            upcoming_fri = today_d + _timedelta(days=days_until_fri)
            expiry_targets = [upcoming_fri + _timedelta(days=7 * i) for i in range(5)]

            ticker_cc = df_open[
                (df_open["Ticker"] == _selected_planner_ticker) & (df_open["TradeType"] == "CC")
            ].copy()
            ticker_cc["Expiry_Date"] = pd.to_datetime(ticker_cc["Expiry_Date"], errors="coerce").dt.date
            ticker_cc["Quantity_num"] = pd.to_numeric(ticker_cc["Quantity"], errors="coerce").fillna(0).abs()

            planner_rows = []
            for exp in expiry_targets:
                # Aggregate all CCs expiring Mon–Sun of this Friday's week
                # (fixes SPY/others with non-Friday expiries being invisible)
                week_start = exp - _timedelta(days=4)   # Monday of this Friday's week
                week_end   = exp + _timedelta(days=2)   # Sunday of this Friday's week
                week_rows = ticker_cc[
                    (ticker_cc["Expiry_Date"] >= week_start) &
                    (ticker_cc["Expiry_Date"] <= week_end)
                ]
                existing_contracts = int(week_rows["Quantity_num"].sum()) if not week_rows.empty else 0
                trade_ids = ", ".join(week_rows["TradeID"].astype(str).tolist()) if not week_rows.empty else "—"
                to_sell = max(0, _weekly_target - existing_contracts)

                # Coverage = existing / target. Show as fraction, not inflated %
                if _weekly_target > 0:
                    coverage_str = f"{existing_contracts}/{_weekly_target}"
                    if existing_contracts >= _weekly_target:
                        status = "✅ Full"
                    elif existing_contracts > 0:
                        status = "🟡 Partial"
                    else:
                        status = "🔴 Empty"
                else:
                    coverage_str = f"{existing_contracts}/—"
                    status = "—" if existing_contracts == 0 else f"{existing_contracts} open"

                planner_rows.append(
                    {
                        "Week Ending (Fri)": exp.strftime("%Y-%m-%d"),
                        "Existing": existing_contracts,
                        "Target": _weekly_target,
                        "To Sell": to_sell,
                        "Coverage": coverage_str,
                        "Status": status,
                        "TradeIDs": trade_ids,
                    }
                )

            df_planner = pd.DataFrame(planner_rows)

            st.dataframe(
                df_planner,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Week Ending (Fri)": st.column_config.TextColumn("Week Ending (Fri)", width="small"),
                    "Existing":          st.column_config.NumberColumn("Existing",        width="small", format="%d"),
                    "Target":            st.column_config.NumberColumn("Target",          width="small", format="%d"),
                    "To Sell":           st.column_config.NumberColumn("To Sell",         width="small", format="%d"),
                    "Coverage":          st.column_config.TextColumn("Have/Need",         width="small"),
                    "Status":            st.column_config.TextColumn("Status",            width="small"),
                    "TradeIDs":          st.column_config.TextColumn("TradeIDs",          width="large"),
                },
            )
            st.caption(
                "All CC contracts expiring Mon–Sun of each week are aggregated under that week's Friday date. "
                "🟢 Full = at/above target · 🟡 Partial = some coverage · 🔴 Empty = nothing sold yet. "
                "Fill near-term gaps with short-DTE, build further out with longer-DTE."
            )
    except Exception as e:
        st.warning(f"Could not build CC coverage planner: {e}")
    
    # Expiring Soon
    st.subheader(f"⏰ Expiring Within {EXPIRING_SOON_DTE} Days — All Tickers")

    # ALL tickers that have options (CC or CSP) — show every one, even if nothing expiring
    _all_option_tickers_raw = df_open[df_open['TradeType'].isin(['CC', 'CSP'])]['Ticker'].unique().tolist()
    priority_tickers = ['MARA', 'CRCL', 'SPY']
    _all_option_tickers = (
        sorted([t for t in _all_option_tickers_raw if t in priority_tickers], key=lambda x: priority_tickers.index(x))
        + sorted([t for t in _all_option_tickers_raw if t not in priority_tickers])
    )

    # Calculate DTE for all options (CC, CSP), exclude STOCK and LEAP
    df_expiring = df_open[df_open['TradeType'].isin(['CC', 'CSP'])].copy()
    df_expiring['Expiry_Date'] = pd.to_datetime(df_expiring['Expiry_Date'], errors='coerce')
    df_expiring = df_expiring[df_expiring['Expiry_Date'].notna()]
    # DTE inclusive: today + expiry day both count
    df_expiring['DTE_Calc'] = (df_expiring['Expiry_Date'] - pd.Timestamp.now()).dt.days + 1
    df_expiring = df_expiring[df_expiring['DTE_Calc'] <= EXPIRING_SOON_DTE]

    if not _all_option_tickers:
        st.success(f"✅ No option positions (CC/CSP) found")
    else:
        # Add call risk and distance-to-spot to the expiring subset
        if not df_expiring.empty:
            df_expiring = RiskCalculator.calculate_call_risk(df_expiring, live_prices)
            df_expiring = df_expiring.copy()
            df_expiring['Current_Price'] = df_expiring['Ticker'].map(lambda t: live_prices.get(t, 0))
            df_expiring['Strike'] = pd.to_numeric(df_expiring['Option_Strike_Price_(USD)'], errors='coerce')
            df_expiring['Distance_to_Spot'] = df_expiring.apply(
                lambda row: (row['Current_Price'] - row['Strike']) if row['TradeType'] == 'CC'
                           else (row['Strike'] - row['Current_Price']),
                axis=1,
            )

        # Iterate ALL tickers, not just those with expiring positions
        tickers_expiring = df_expiring['Ticker'].unique().tolist() if not df_expiring.empty else []
        sorted_tickers = _all_option_tickers  # already priority-sorted above
        
        # Calculate this week's date range (Monday to Sunday) for highlighting
        today = date.today()
        start_of_week_highlight = today - timedelta(days=today.weekday())  # Monday
        end_of_week_highlight = start_of_week_highlight + timedelta(days=6)  # Sunday
        
        for ticker in sorted_tickers:
            ticker_positions = df_expiring[df_expiring['Ticker'] == ticker].copy() if not df_expiring.empty else pd.DataFrame()

            # Ticker with NO expiring positions — show a compact green row and skip the table
            if ticker_positions.empty:
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.write(f"### 📊 {ticker}")
                with col2:
                    st.success(f"✅ Nothing expiring within {EXPIRING_SOON_DTE}d")
                st.divider()
                continue

            # Sort by DTE (ascending - closest expiry first), then by Distance to Spot (descending - most ITM/risky first)
            ticker_positions = ticker_positions.sort_values(['DTE_Calc', 'Distance_to_Spot'], ascending=[True, False])

            # Calculate total quantity for this ticker
            total_qty = pd.to_numeric(ticker_positions['Quantity'], errors='coerce').fillna(0).abs().sum()

            # Show ticker header and quantity card
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write(f"### 📊 {ticker}")
            with col2:
                st.metric("Total Quantity", f"{int(total_qty):,}")
            
            # Build lookup map from live options data for P&L enrichment
            contracts_map = {}
            for _c in st.session_state.get("open_positions_data", []):
                _key = (_c.underlying, float(_c.strike), _c.right, str(_c.expiry))
                contracts_map[_key] = _c

            # Create a table for this ticker
            display_data = []
            expiry_dates_list = []  # Store expiry dates for styling
            for _, row in ticker_positions.iterrows():
                risk = row.get('CallRisk', 'NONE')
                risk_emoji = {
                    'HIGH': '🔴',
                    'MEDIUM': '🟡',
                    'LOW': '🟠',
                    'NONE': '🟢'
                }.get(risk, '⚪')
                
                distance_pct = (row['Distance_to_Spot'] / row['Strike'] * 100) if row['Strike'] > 0 else 0
                distance_str = f"{row['Distance_to_Spot']:.2f} ({distance_pct:+.1f}%)"
                
                qty = pd.to_numeric(row.get('Quantity', 0), errors='coerce') or 0
                expiry_date = pd.to_datetime(row['Expiry_Date']).strftime('%Y-%m-%d') if pd.notna(row['Expiry_Date']) else 'N/A'
                expiry_date_obj = pd.to_datetime(row['Expiry_Date']).date() if pd.notna(row['Expiry_Date']) else None
                
                # Calculate premium expected
                premium = pd.to_numeric(row.get('OptPremium', 0), errors='coerce') or 0.0
                premium_expected = premium * qty * 100
                
                # Check if expires this week
                expires_this_week = False
                if expiry_date_obj:
                    expires_this_week = start_of_week_highlight <= expiry_date_obj <= end_of_week_highlight
                
                expiry_dates_list.append(expires_this_week)
                
                # Look up live options data for Mark Price + Contract P&L
                right_code = 'C' if row['TradeType'] == 'CC' else 'P'
                expiry_key = pd.to_datetime(row['Expiry_Date']).strftime('%Y-%m-%d') if pd.notna(row['Expiry_Date']) else ''
                live_contract = contracts_map.get((row['Ticker'], float(row['Strike']), right_code, expiry_key))

                if live_contract:
                    mark_price = live_contract.last_price if live_contract.last_price > 0 else (live_contract.bid + live_contract.ask) / 2
                    # Sold option P&L: premium collected - current mark (positive = profit)
                    contract_pl = (premium - mark_price) * qty * 100 if premium > 0 else None
                    mark_str = f"${mark_price:.2f}"
                    pl_str = f"${contract_pl:+,.2f}" if contract_pl is not None else "—"
                    delta_str = f"{live_contract.delta:.3f}" if live_contract.delta is not None else "—"
                    theta_str = f"{live_contract.theta:.3f}" if live_contract.theta is not None else "—"
                else:
                    mark_str = "—"
                    pl_str = "—"
                    delta_str = "—"
                    theta_str = "—"

                display_data.append({
                    'TradeID': row['TradeID'],
                    'Type': row['TradeType'],
                    'Qty': int(qty),
                    'Expiry': expiry_date,
                    'Strike': f"${row['Strike']:.2f}",
                    'Spot': f"${row['Current_Price']:.2f}",
                    'DTE': int(row['DTE_Calc']),
                    'Dist to Spot': distance_str,
                    'Prem Sold': f"${premium:.2f}",
                    'Mark': mark_str,
                    'Contract P&L': pl_str,
                    'Δ': delta_str,
                    'Θ': theta_str,
                    'Risk': _format_risk_label(risk, row['TradeType'], row.get('Current_Price', 0), row.get('Strike', 0))
                })
            
            df_display = pd.DataFrame(display_data)
            
            # Apply styling: light blue background for rows expiring this week
            def highlight_this_week(row):
                # Check if this row expires this week (using the row index)
                row_idx = row.name
                if row_idx < len(expiry_dates_list) and expiry_dates_list[row_idx]:
                    return ['background-color: #E3F2FD'] * len(row)
                return [''] * len(row)
            
            styled_df = df_display.style.apply(highlight_this_week, axis=1)
            
            st.dataframe(
                styled_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "TradeID":       st.column_config.TextColumn("TradeID",       width="small"),
                    "Type":          st.column_config.TextColumn("Type",          width="small"),
                    "Qty":           st.column_config.NumberColumn("Qty",         width="small", format="%d"),
                    "Expiry":        st.column_config.TextColumn("Expiry",        width="small"),
                    "Strike":        st.column_config.TextColumn("Strike",        width="small"),
                    "Spot":          st.column_config.TextColumn("Spot",          width="small"),
                    "DTE":           st.column_config.NumberColumn("DTE",         width="small", format="%d"),
                    "Dist to Spot":  st.column_config.TextColumn("Dist to Spot",  width="medium"),
                    "Prem Sold":     st.column_config.TextColumn("Prem Sold",     width="small"),
                    "Mark":          st.column_config.TextColumn("Mark",          width="small"),
                    "Contract P&L":  st.column_config.TextColumn("Contract P&L",  width="small"),
                    "Δ":             st.column_config.TextColumn("Δ Delta",       width="small"),
                    "Θ":             st.column_config.TextColumn("Θ Theta",       width="small"),
                    "Risk":          st.column_config.TextColumn("Risk",          width="medium"),
                }
            )
            st.divider()
    
    st.divider()
    
    # ══════════════════════════════════════════════════════════
    # PHASE 4: DAILY ACTION PLAN — Unified per-ticker cards
    # ══════════════════════════════════════════════════════════
    st.subheader("🎯 Daily Action Plan")

    # --- Shared data ---
    portfolio = st.session_state.get('current_portfolio', 'Income Wheel')
    capital_allocation = get_capital_allocation(portfolio)
    inventory = CapitalCalculator.calculate_inventory(df_open)

    # Compute unified capital data (reuse from dashboard if available)
    from unified_calculations import UnifiedCapitalCalculator
    from persistence import get_stock_average_prices, get_pmcc_tickers
    _stock_avg = get_stock_average_prices(portfolio)
    _pmcc_set = set(get_pmcc_tickers(portfolio) or [])
    _portfolio_deposit = st.session_state.get('portfolio_deposit', 0)
    _capital_data = UnifiedCapitalCalculator.calculate_capital_by_ticker(
        df_open, _portfolio_deposit, _stock_avg, live_prices, _pmcc_set
    )

    # BP from dashboard formula (cash-secured policy: stock at BUY price)
    _total_stock_buy = _capital_data['total'].get('stock_at_buy_price', 0)
    _total_leap_sunk = _capital_data['total']['leap_sunk']
    _total_csp_reserved = _capital_data['total']['csp_reserved']

    # Need P&L for BP calc
    from pnl_calculator import PnLCalculator
    from persistence import get_spy_leap_pl
    _spy_leap_pl = get_spy_leap_pl(portfolio)
    _comp_pnl = PnLCalculator.calculate_comprehensive_pnl(
        df_trades=st.session_state.df_trades, df_open=df_open,
        stock_avg_prices=_stock_avg, live_prices=live_prices,
        spy_leap_pl=_spy_leap_pl if _spy_leap_pl != 0 else None,
        live_options=st.session_state.get("open_positions_data", []),
    )
    _prem_by_ticker = {}
    _closed_opts = st.session_state.df_trades[
        (st.session_state.df_trades['Status'] == 'Closed') &
        (st.session_state.df_trades['TradeType'].isin(['CC', 'CSP', 'LEAP', 'STOCK']))
    ].copy()
    if not _closed_opts.empty and 'Actual_Profit_(USD)' in _closed_opts.columns:
        _closed_opts['_prem'] = pd.to_numeric(_closed_opts['Actual_Profit_(USD)'], errors='coerce').fillna(0)
        _prem_by_ticker = _closed_opts.groupby('Ticker')['_prem'].sum().to_dict()
    _total_premium = sum(_prem_by_ticker.values())
    _total_stock_leap_pl = _comp_pnl['unrealized_stock_pnl']['total'] + _comp_pnl['unrealized_leap_pnl']['total']
    _total_pl = _total_premium + _total_stock_leap_pl
    # Cash-secured policy: stock at BUY price + LEAP + CSP
    _capital_held = _total_stock_buy + _total_leap_sunk
    _liquid_cash = (_portfolio_deposit + _total_pl) - _capital_held
    _bp = _liquid_cash - _total_csp_reserved
    _available_csp_capital = max(0, _bp)

    # Contracts sold this week
    now = datetime.now()
    start_of_week = now - timedelta(days=now.weekday())
    _df_trades_copy = st.session_state.df_trades.copy()
    _df_trades_copy['Date_open'] = pd.to_datetime(_df_trades_copy['Date_open'], errors='coerce')
    _this_week = _df_trades_copy[
        (_df_trades_copy['Date_open'] >= start_of_week) &
        (_df_trades_copy['TradeType'].isin(['CC', 'CSP']))
    ].copy()
    _this_week['Quantity'] = pd.to_numeric(_this_week['Quantity'], errors='coerce').fillna(0).abs()

    # Remaining trading days this week (Mon=0..Fri=4)
    _today_wd = now.weekday()
    _remaining_days = max(1, 5 - _today_wd)

    # Crypto cluster check
    _crypto_tickers = ['MARA', 'CRCL', 'ETHA', 'SOL']
    _crypto_total = sum(_capital_data['by_ticker'].get(t, {}).get('total_committed', 0) for t in _crypto_tickers)
    _crypto_pct = (_crypto_total / _portfolio_deposit * 100) if _portfolio_deposit > 0 else 0

    # BP status banner
    if _bp < 0:
        st.error(f"🔴 **Buying Power: ${_bp:,.0f}** — All CSP targets set to 0. Free capital before selling new puts.")
    else:
        st.success(f"🟢 **Buying Power: ${_bp:,.0f}** — Available for new CSPs: ${_available_csp_capital:,.0f}")

    # Priority ticker ordering
    all_tickers = sorted(df_open['Ticker'].unique().tolist(),
                         key=lambda t: (0 if t in ['MARA', 'CRCL', 'SPY'] else 1, t))

    # --- Per-ticker action cards ---
    for ticker in all_tickers:
        td = _capital_data['by_ticker'].get(ticker, {})
        inv = inventory['positions_by_ticker'].get(ticker, {})
        ticker_price = live_prices.get(ticker, 0)

        # CSP data
        csp_reserved = td.get('csp_reserved', 0)
        csp_allocated = capital_allocation.get(ticker, 0)
        csp_open_contracts = int(df_open[(df_open['Ticker'] == ticker) & (df_open['TradeType'] == 'CSP')]['Quantity'].abs().sum()) if not df_open.empty else 0
        csp_sold_week = int(_this_week[(_this_week['Ticker'] == ticker) & (_this_week['TradeType'] == 'CSP')]['Quantity'].sum())

        # CSP: how many can we sell today?
        if _bp <= 0 or ticker_price <= 0:
            csp_can_sell = 0
            csp_reason = "No BP" if _bp <= 0 else "No price"
        elif csp_allocated > 0 and csp_reserved >= csp_allocated:
            csp_can_sell = 0
            csp_reason = "At soft cap"
        else:
            # Available = min(remaining allocation, available BP) / (price * 100)
            remaining_alloc = max(0, csp_allocated - csp_reserved) if csp_allocated > 0 else _available_csp_capital
            csp_budget = min(remaining_alloc, _available_csp_capital)
            csp_can_sell = int(csp_budget / (ticker_price * 100)) if ticker_price > 0 else 0
            csp_reason = ""

        # CC data
        stock_shares = inv.get('stock', 0) + inv.get('leaps', 0)
        cc_open = inv.get('cc', 0)
        cc_coverage_ratio = inv.get('cc_coverage_ratio')
        cc_coverage_pct = (cc_coverage_ratio * 100) if cc_coverage_ratio and cc_coverage_ratio > 0 else 0
        uncovered_shares = max(0, stock_shares - (cc_open * 100))
        cc_can_sell = int(uncovered_shares / 100)
        cc_sold_week = int(_this_week[(_this_week['Ticker'] == ticker) & (_this_week['TradeType'] == 'CC')]['Quantity'].sum())

        # Badges
        if csp_can_sell == 0 and csp_reason:
            csp_badge = f"🔴 {csp_reason}"
        elif csp_can_sell == 0:
            csp_badge = "✅ Fully deployed"
        else:
            csp_badge = f"🟢 Sell up to {csp_can_sell}"

        if stock_shares == 0:
            cc_badge = "— No underlying"
        elif cc_can_sell == 0:
            cc_badge = "✅ Fully covered"
        else:
            cc_badge = f"🟢 Sell up to {cc_can_sell}"

        # Constraints
        constraints = []
        if ticker in _crypto_tickers and _crypto_pct > 40:
            constraints.append(f"Crypto cluster: {_crypto_pct:.0f}%")
        if _portfolio_deposit > 0 and td.get('total_committed', 0) > _portfolio_deposit * 0.30:
            constraints.append(f"Concentration: {td['total_committed']/_portfolio_deposit*100:.0f}%")

        # Render card
        with st.container(border=True):
            col_hdr, col_price = st.columns([3, 1])
            with col_hdr:
                st.markdown(f"**{ticker}**")
            with col_price:
                st.caption(f"${ticker_price:.2f}" if ticker_price else "—")

            col_csp, col_cc = st.columns(2)
            with col_csp:
                deployed_pct = (csp_reserved / csp_allocated * 100) if csp_allocated > 0 else 0
                alloc_str = f"${csp_allocated:,.0f}" if csp_allocated > 0 else "—"
                st.markdown(f"**CSP:** ${csp_reserved:,.0f} / {alloc_str} deployed ({deployed_pct:.0f}%) | {csp_open_contracts} open")
                st.markdown(f"Sell today: **{csp_badge}** | Sold this week: {csp_sold_week}")
            with col_cc:
                st.markdown(f"**CC:** {cc_coverage_pct:.0f}% covered | {cc_open} open / {int(stock_shares):,} shares")
                st.markdown(f"Sell today: **{cc_badge}** | Sold this week: {cc_sold_week}")

            if constraints:
                st.caption("⚠️ " + " | ".join(constraints))



# ============================================================
# ENTRY FORMS PAGE
# ============================================================
def render_entry_forms():
    """Render trade entry forms"""
    st.title("📝 Entry Forms")
    
    df_open = st.session_state.df_open
    df_trades = st.session_state.df_trades
    df_audit = st.session_state.df_audit
    
    portfolio = st.session_state.get('current_portfolio', 'Income Wheel')
    tickers_for_portfolio = get_tickers_for_dropdown(portfolio, df_trades)
    pmcc_tickers = get_pmcc_tickers(portfolio) if portfolio == "Income Wheel" else []
    pmcc_tickers_set = set(pmcc_tickers) if pmcc_tickers else set()

    # Form reset counter — incrementing this changes all widget keys so Streamlit
    # treats them as brand-new widgets and renders them with their default values.
    if 'form_reset_counter' not in st.session_state:
        st.session_state.form_reset_counter = 0
    fv = st.session_state.form_reset_counter  # short alias used in widget key= args

    # Helper function to show success and clear form fields
    def show_success_and_clear(trade_id: str = None, form_key: str = "", additional_message: str = ""):
        """Show confirmation popup and reset all forms to defaults after successful submit."""
        if trade_id:
            success_msg = f"✅ Trade submitted {trade_id}"
            if additional_message:
                success_msg += f" - {additional_message}"
        else:
            success_msg = f"✅ {additional_message}" if additional_message else "✅ Recorded successfully."
        st.toast(success_msg, icon="✅")
        st.success(success_msg)
        st.session_state.success_message = success_msg
        try:
            st.balloons()
        except Exception:
            pass
        # Increment the counter — all widget keys change on next render so every
        # widget re-initialises from its declared default value= / index=.
        st.session_state.form_reset_counter += 1
        # Also clear expire_checkboxes dict (not a keyed widget)
        st.session_state.expire_checkboxes = {}
        refresh_data()
        st.rerun()
    
    # Helper function to render strategy selector (for use in each form)
    def render_strategy_selector(form_key_suffix=""):
        """Render strategy selector for a form - returns strategy value. Shown for all portfolios; Active Core defaults to WHEEL."""
        is_active_core = portfolio in ("Active Core", "⭐ Active Core")
        default_index = 1 if is_active_core else 0  # WHEEL for Active Core, "— Select strategy —" for Income Wheel
        strategy = st.selectbox(
            "🎯 Strategy",
            ["— Select strategy —", "WHEEL", "PMCC", "ActiveCore"],
            key=f"entry_strategy_{form_key_suffix}_{fv}",
            index=default_index,
            help="WHEEL (CSP+CC), PMCC (LEAP+CC), ActiveCore (opportunistic income)"
        )
        if is_active_core and strategy == "— Select strategy —":
            return "WHEEL"
        return strategy if strategy != "— Select strategy —" else None
    
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Sell CC", "Sell CSP", "BTC", "Roll", "Expire", "Assignment"])
    
    # Get live prices for this portfolio's tickers
    live_prices = get_cached_prices(tuple(tickers_for_portfolio)) if tickers_for_portfolio else {}
    st.session_state.live_prices = live_prices
    
    def display_live_prices_cards():
        """Display live price cards for this portfolio's tickers"""
        if live_prices and tickers_for_portfolio:
            cols = st.columns(min(len(tickers_for_portfolio), 8))
            for idx, ticker in enumerate(tickers_for_portfolio[:8]):
                with cols[idx]:
                    price = live_prices.get(ticker, None)
                    if price is not None:
                        st.metric(ticker, f"${price:.2f}")
                    else:
                        st.metric(ticker, "N/A")
            if len(tickers_for_portfolio) > 8:
                st.caption(f"Showing 8 of {len(tickers_for_portfolio)} tickers.")
            st.divider()
    
    # ---- SELL CC TAB ----
    with tab1:
        st.subheader("Sell Covered Call")
        display_live_prices_cards()
        
        # Input fields (outside form for real-time calculation)
        col1, col2 = st.columns(2)
        
        with col1:
            # Strategy selector right before Ticker
            entry_strategy = render_strategy_selector("cc")
            cc_ticker = st.selectbox("Ticker", ["— Select ticker —"] + list(tickers_for_portfolio), key=f"cc_ticker_{fv}", index=0)
            cc_strike = st.number_input("Strike Price ($)", min_value=0.0, step=0.50, key=f"cc_strike_{fv}", value=0.0)
            cc_expiry = st.date_input("Expiry Date", min_value=date.today(), key=f"cc_expiry_{fv}", value=None)

        with col2:
            cc_qty = st.number_input("Contracts", min_value=0, step=1, key=f"cc_qty_{fv}", value=0)
            cc_premium = st.number_input("Premium ($)", min_value=0.0, step=0.01, key=f"cc_premium_{fv}", value=0.0)
            cc_underlying = st.number_input("Current Stock Price ($)", min_value=0.0, step=0.01, key=f"cc_underlying_{fv}", value=0.0)
        
        # Real-time premium and yield calculation (outside form)
        if cc_ticker and cc_ticker != "— Select ticker —" and cc_premium > 0 and cc_qty > 0 and cc_strike > 0 and cc_expiry:
            total_premium = cc_premium * cc_qty * 100  # Premium per contract * contracts * 100 shares
            premium_pct = (cc_premium / cc_strike) * 100  # Premium as % of strike
            
            # DTE inclusive: today + expiry day both count
            days_to_expiry = (cc_expiry - date.today()).days + 1
            if days_to_expiry > 0:
                # Capital at risk = strike * 100 * qty (for CC, capital is the stock value, but we use strike for yield calc)
                capital_at_risk = cc_strike * 100 * cc_qty
                # Annualized yield = (premium / capital) * (365 / days) * 100
                annualized_yield = (total_premium / capital_at_risk) * (365 / days_to_expiry) * 100
            else:
                annualized_yield = 0
                days_to_expiry = 0
            
            # Display calculations
            st.divider()
            st.write("**📊 Real-time Calculation:**")
            calc_col1, calc_col2, calc_col3, calc_col4 = st.columns(4)
            with calc_col1:
                st.metric("Total Premium", f"${total_premium:,.2f}")
            with calc_col2:
                st.metric("Premium %", f"{premium_pct:.2f}%")
            with calc_col3:
                st.metric("Days to Expiry", f"{days_to_expiry}")
            with calc_col4:
                if days_to_expiry > 0:
                    st.metric("Annualized Yield", f"{annualized_yield:.2f}%")
                else:
                    st.metric("Annualized Yield", "N/A")
            st.divider()
        
        # Comment field
        cc_remarks = st.text_input("Comments (optional)", key=f"cc_remarks_{fv}", placeholder="Enter any comments or notes about this trade")
        
        # Submit button (right after comments)
        submitted = st.button("Submit CC", type="primary", key="cc_submit")
        
        # Show CC Coverage and Pacing Information (consistent with Dashboard and Daily Helper)
        if df_open is not None and not df_open.empty:
            st.divider()
            st.markdown("#### 📊 Current Position & Pacing for Selected Ticker")
            
            # Get inventory for this ticker
            inventory = CapitalCalculator.calculate_inventory(df_open)
            ticker_inventory = inventory['positions_by_ticker'].get(cc_ticker, {})
            
            # Stock holdings (STOCK + LEAP)
            stock_shares = ticker_inventory.get('total_stock', 0)
            stock_only = ticker_inventory.get('stock', 0)
            leaps_shares = ticker_inventory.get('leaps', 0)
            
            # Existing CC contracts
            existing_cc = ticker_inventory.get('cc', 0)
            
            # CC Coverage Ratio
            cc_coverage_ratio = ticker_inventory.get('cc_coverage_ratio')
            
            # Calculate required shares for new + existing CCs
            required_shares = (existing_cc + cc_qty) * 100
            
            # Display coverage information
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Stock Held", f"{int(stock_shares):,} shares")
                if leaps_shares > 0:
                    st.caption(f"Stock: {int(stock_only):,} | LEAPs: {int(leaps_shares):,}")
            with col2:
                st.metric("Existing CC", f"{int(existing_cc)} contracts")
                st.caption(f"= {int(existing_cc * 100):,} shares")
            with col3:
                st.metric("New CC", f"{int(cc_qty)} contracts")
                st.caption(f"= {int(cc_qty * 100):,} shares")
            with col4:
                if cc_coverage_ratio is not None:
                    if cc_coverage_ratio < 0:
                        st.metric("CC Coverage", "Uncovered ⚠️")
                    elif cc_coverage_ratio <= 1.0:
                        # Stock coverage % (CC shares needed / stock shares)
                        coverage_pct = cc_coverage_ratio * 100
                        if coverage_pct >= 100:
                            st.metric("CC Coverage", f"{coverage_pct:.0f}% ✅")
                        elif coverage_pct >= 80:
                            st.metric("CC Coverage", f"{coverage_pct:.0f}% ⚠️")
                        else:
                            st.metric("CC Coverage", f"{coverage_pct:.0f}% ✅")
                    else:
                        # Over 100% means uncovered
                        coverage_pct = cc_coverage_ratio * 100
                        st.metric("CC Coverage", f"{coverage_pct:.0f}% ❌")
                else:
                    st.metric("CC Coverage", "N/A")
            
            # Coverage message (informational only – does not block submitting CCs)
            if stock_shares >= required_shares:
                st.success(f"✅ Stock available: {int(stock_shares):,} shares (need {required_shares:,} for {existing_cc + cc_qty} total CCs)")
            else:
                st.warning(f"⚠️ CC ratio over 100%: {int(stock_shares):,} shares (need {required_shares:,} for {existing_cc + cc_qty} total CCs). You can still submit if desired.")
            
            st.divider()
            
            # Show CCs expiring this week for selected ticker
            st.markdown("#### ⏰ CCs Expiring This Week")
            today = date.today()
            start_of_week = today - timedelta(days=today.weekday())  # Monday
            end_of_week = start_of_week + timedelta(days=6)  # Sunday
            
            expiring_ccs = df_open[
                (df_open['Ticker'] == cc_ticker) &
                (df_open['TradeType'] == 'CC') &
                (df_open['Status'] == 'Open')
            ].copy()
            
            if not expiring_ccs.empty:
                expiring_ccs['Expiry_Date'] = pd.to_datetime(expiring_ccs['Expiry_Date'], errors='coerce')
                expiring_ccs['Expiry_Date_Date'] = expiring_ccs['Expiry_Date'].dt.date
                
                this_week_ccs = expiring_ccs[
                    (expiring_ccs['Expiry_Date_Date'] >= start_of_week) &
                    (expiring_ccs['Expiry_Date_Date'] <= end_of_week)
                ].copy()
                
                if not this_week_ccs.empty:
                    this_week_ccs['OptPremium'] = pd.to_numeric(this_week_ccs['OptPremium'], errors='coerce').fillna(0)
                    this_week_ccs['Quantity'] = pd.to_numeric(this_week_ccs['Quantity'], errors='coerce').fillna(0)
                    
                    total_expiring_contracts = int(this_week_ccs['Quantity'].sum())
                    avg_premium = this_week_ccs['OptPremium'].mean() if len(this_week_ccs) > 0 else 0.0
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("CCs Expiring This Week", f"{total_expiring_contracts} contracts")
                    with col2:
                        st.metric("Average Premium", f"${avg_premium:.2f}")
                    
                    if total_expiring_contracts > 0:
                        st.info(f"ℹ️ {total_expiring_contracts} CC contract(s) expiring this week may free up {total_expiring_contracts * 100:,} shares, which can help explain coverage above 100% if they're far from spot.")
                else:
                    st.info("ℹ️ No CCs expiring this week for this ticker")
            else:
                st.info("ℹ️ No open CCs for this ticker")
            
            st.divider()
            
            # Show pacing information (from Daily Helper logic)
            st.markdown("#### 🎯 Weekly Pacing Information")
            
            # Calculate weekly and daily targets
            ticker_weekly_target = stock_shares / 4 / 100 if stock_shares > 0 else 0
            ticker_daily_target = stock_shares / 4 / 5 / 100 if stock_shares > 0 else 0
            
            # Get CC sold this week
            now = datetime.now()
            start_of_week = now - timedelta(days=now.weekday())
            df_trades_check = st.session_state.df_trades.copy()
            df_trades_check['Date_open'] = pd.to_datetime(df_trades_check['Date_open'], errors='coerce')
            this_week_trades = df_trades_check[
                (df_trades_check['Date_open'] >= start_of_week) &
                (df_trades_check['TradeType'] == 'CC') &
                (df_trades_check['Ticker'] == cc_ticker)
            ].copy()
            this_week_trades['Quantity'] = pd.to_numeric(this_week_trades['Quantity'], errors='coerce').fillna(0).abs()
            cc_sold = this_week_trades['Quantity'].sum() or 0
            
            # Calculate remaining (weekly target - sold this week)
            remaining_weekly = max(0.0, ticker_weekly_target - cc_sold)
            
            # Display pacing metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Weekly Target", f"{ticker_weekly_target:.1f} contracts")
            with col2:
                st.metric("Daily Target", f"{ticker_daily_target:.1f} contracts")
            with col3:
                st.metric("Sold This Week", f"{int(cc_sold)} contracts")
            with col4:
                st.metric("Remaining", f"{remaining_weekly:.1f} contracts")
            
            # Show impact of new trade
            if cc_qty > 0:
                new_total_sold = cc_sold + cc_qty
                new_remaining_weekly = max(0.0, ticker_weekly_target - new_total_sold)
                
                st.info(f"📈 **After this trade:** Total sold this week = {int(new_total_sold)} contracts | Remaining = {new_remaining_weekly:.1f} contracts")
            
            st.divider()
        
        if submitted:
            errs = []
            if not cc_ticker or cc_ticker == "— Select ticker —":
                errs.append("Select a ticker.")
            if not cc_strike or cc_strike <= 0:
                errs.append("Enter strike price.")
            if not cc_qty or cc_qty <= 0:
                errs.append("Enter number of contracts.")
            if not cc_premium or cc_premium <= 0:
                errs.append("Enter premium.")
            if not cc_expiry:
                errs.append("Select expiry date.")
            if not entry_strategy or entry_strategy == "— Select strategy —":
                errs.append("Select a strategy.")
            if errs:
                st.error("Please fix: " + " ".join(errs))
            else:
                # Validate (warnings only – never block on insufficient stock / CC ratio > 100%)
                valid, msg = TradeValidator.validate_sell_cc(cc_ticker, cc_qty, df_open)
                if valid and "WARNING" in msg:
                    st.warning(msg)
                elif not valid:
                    # Do not block on stock/coverage; allow submit and show warning only
                    if msg and ("insufficient" in msg.lower() or "stock" in msg.lower() or "shares" in msg.lower() or "coverage" in msg.lower()):
                        st.warning(f"⚠️ {msg}")
                        valid = True
                    else:
                        st.error(f"❌ Validation failed: {msg}")
                if valid:
                    new_trade_id = generate_trade_id(df_trades)
                    strategy_cc = entry_strategy or "WHEEL"
                    trade_data = {
                        'TradeID': new_trade_id,
                        'Ticker': cc_ticker,
                        'StrategyType': strategy_cc,
                        'Direction': 'Sell',
                        'TradeType': 'CC',
                        'Quantity': cc_qty,
                        'Option_Strike_Price_(USD)': cc_strike,
                        'Price_of_current_underlying_(USD)': cc_underlying,
                        'OptPremium': cc_premium,
                        'Opt_Premium_%': (cc_premium / cc_strike) if cc_strike else 0,
                        'Date_open': datetime.now(),
                        'Expiry_Date': cc_expiry,
                        'Remarks': cc_remarks,
                        'Status': 'Open',
                        'Open_lots': cc_qty * 100
                    }
                    
                    audit_data = {
                        'Audit ID': generate_audit_id(st.session_state.df_audit),
                        'Timestamp': datetime.now(),
                        'Action Type': 'Open',
                        'TradeID_Ref': new_trade_id,
                        'Remarks': f'Sell CC {cc_ticker} ${cc_strike} x{cc_qty}',
                        'ScriptName': 'Income Wheel App',
                        'AffectedQty': cc_qty
                    }
                    
                    try:
                        handler = GSheetHandler(st.session_state.current_sheet_id)
                        handler.append_trade(trade_data)  # Write to main Data Table
                        handler.append_audit(audit_data)  # Write to Audit Table
                        show_success_and_clear(new_trade_id, 'cc')
                    except Exception as e:
                        st.error(f"❌ Error saving trade: {e}")
                        st.error("⚠️ Trade was NOT saved. Please try again.")
    
    # ---- SELL CSP TAB ----
    with tab2:
        st.subheader("Sell Cash-Secured Put")
        display_live_prices_cards()
        
        # Input fields (outside form for real-time calculation)
        col1, col2 = st.columns(2)
        
        with col1:
            # Strategy selector right before Ticker
            entry_strategy = render_strategy_selector("csp")
            csp_ticker = st.selectbox("Ticker", ["— Select ticker —"] + list(tickers_for_portfolio), key=f"csp_ticker_{fv}", index=0)
            # Auto-detect strategy for PMCC tickers
            if portfolio == "Income Wheel" and csp_ticker and csp_ticker != "— Select ticker —":
                pmcc_tickers = get_pmcc_tickers(portfolio)
                pmcc_tickers_set = set(pmcc_tickers) if pmcc_tickers else set()
                if csp_ticker in pmcc_tickers_set:
                    st.warning(f"⚠️ {csp_ticker} is a PMCC ticker - CSPs are typically not used in PMCC strategy")
            csp_strike = st.number_input("Strike Price ($)", min_value=0.0, step=0.50, key=f"csp_strike_{fv}", value=0.0)
            csp_expiry = st.date_input("Expiry Date", min_value=date.today(), key=f"csp_expiry_{fv}", value=None)

        with col2:
            csp_qty = st.number_input("Contracts", min_value=0, step=1, key=f"csp_qty_{fv}", value=0)
            csp_premium = st.number_input("Premium ($)", min_value=0.0, step=0.01, key=f"csp_premium_{fv}", value=0.0)
            csp_underlying = st.number_input("Current Stock Price ($)", min_value=0.0, step=0.01, key=f"csp_underlying_{fv}", value=0.0)
        
        # Show cash required
        cash_required = csp_strike * 100 * csp_qty
        st.info(f"💵 Cash Required: ${cash_required:,.0f}")
        
        # Real-time premium and yield calculation (outside form)
        if csp_ticker and csp_ticker != "— Select ticker —" and csp_premium > 0 and csp_qty > 0 and csp_strike > 0 and csp_expiry:
            total_premium = csp_premium * csp_qty * 100  # Premium per contract * contracts * 100 shares
            premium_pct = (csp_premium / csp_strike) * 100  # Premium as % of strike
            
            # DTE inclusive: today + expiry day both count
            days_to_expiry = (csp_expiry - date.today()).days + 1
            if days_to_expiry > 0:
                # Capital at risk = strike * 100 * qty (cash secured)
                capital_at_risk = csp_strike * 100 * csp_qty
                # Annualized yield = (premium / capital) * (365 / days) * 100
                annualized_yield = (total_premium / capital_at_risk) * (365 / days_to_expiry) * 100
            else:
                annualized_yield = 0
                days_to_expiry = 0
            
            # Display calculations
            st.divider()
            st.write("**📊 Real-time Calculation:**")
            calc_col1, calc_col2, calc_col3, calc_col4 = st.columns(4)
            with calc_col1:
                st.metric("Total Premium", f"${total_premium:,.2f}")
            with calc_col2:
                st.metric("Premium %", f"{premium_pct:.2f}%")
            with calc_col3:
                st.metric("Days to Expiry", f"{days_to_expiry}")
            with calc_col4:
                if days_to_expiry > 0:
                    st.metric("Annualized Yield", f"{annualized_yield:.2f}%")
                else:
                    st.metric("Annualized Yield", "N/A")
            st.divider()
        
        csp_remarks = st.text_input("Remarks (optional)", key=f"csp_remarks_{fv}")
        
        # Show CSPs expiring this week for selected ticker
        if df_open is not None and not df_open.empty:
            st.divider()
            st.markdown("#### ⏰ CSPs Expiring This Week")
            today = date.today()
            start_of_week = today - timedelta(days=today.weekday())  # Monday
            end_of_week = start_of_week + timedelta(days=6)  # Sunday
            
            expiring_csps = df_open[
                (df_open['Ticker'] == csp_ticker) &
                (df_open['TradeType'] == 'CSP') &
                (df_open['Status'] == 'Open')
            ].copy()
            
            if not expiring_csps.empty:
                expiring_csps['Expiry_Date'] = pd.to_datetime(expiring_csps['Expiry_Date'], errors='coerce')
                expiring_csps['Expiry_Date_Date'] = expiring_csps['Expiry_Date'].dt.date
                
                this_week_csps = expiring_csps[
                    (expiring_csps['Expiry_Date_Date'] >= start_of_week) &
                    (expiring_csps['Expiry_Date_Date'] <= end_of_week)
                ].copy()
                
                if not this_week_csps.empty:
                    this_week_csps['OptPremium'] = pd.to_numeric(this_week_csps['OptPremium'], errors='coerce').fillna(0)
                    this_week_csps['Quantity'] = pd.to_numeric(this_week_csps['Quantity'], errors='coerce').fillna(0)
                    this_week_csps['Option_Strike_Price_(USD)'] = pd.to_numeric(this_week_csps['Option_Strike_Price_(USD)'], errors='coerce').fillna(0)
                    
                    total_expiring_contracts = int(this_week_csps['Quantity'].sum())
                    avg_premium = this_week_csps['OptPremium'].mean() if len(this_week_csps) > 0 else 0.0
                    
                    # Calculate total capital that will be freed up (strike * quantity * 100 for each position)
                    total_capital_freed = (this_week_csps['Option_Strike_Price_(USD)'] * this_week_csps['Quantity'] * 100).sum()
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("CSPs Expiring This Week", f"{total_expiring_contracts} contracts")
                    with col2:
                        st.metric("Average Premium", f"${avg_premium:.2f}")
                    
                    if total_expiring_contracts > 0:
                        st.info(f"ℹ️ {total_expiring_contracts} CSP contract(s) expiring this week may free up ${total_capital_freed:,.0f} in capital, which can help explain capital allocation if they're far from spot.")
                else:
                    st.info("ℹ️ No CSPs expiring this week for this ticker")
            else:
                st.info("ℹ️ No open CSPs for this ticker")
            
            st.divider()
        
        # Submit button
        submitted = st.button("Submit CSP", type="primary", key="csp_submit")
            
        if submitted:
            errs = []
            if not csp_ticker or csp_ticker == "— Select ticker —":
                errs.append("Select a ticker.")
            if not csp_strike or csp_strike <= 0:
                errs.append("Enter strike price.")
            if not csp_qty or csp_qty <= 0:
                errs.append("Enter number of contracts.")
            if not csp_premium or csp_premium <= 0:
                errs.append("Enter premium.")
            if not csp_expiry:
                errs.append("Select expiry date.")
            if not entry_strategy or entry_strategy == "— Select strategy —":
                errs.append("Select a strategy.")
            if errs:
                st.error("Please fix: " + " ".join(errs))
            else:
                new_trade_id = generate_trade_id(df_trades)
                strategy_csp = entry_strategy or "WHEEL"
                trade_data = {
                    'TradeID': new_trade_id,
                    'Ticker': csp_ticker,
                    'StrategyType': strategy_csp,
                    'Direction': 'Sell',
                    'TradeType': 'CSP',
                    'Quantity': csp_qty,
                    'Option_Strike_Price_(USD)': csp_strike,
                    'Price_of_current_underlying_(USD)': csp_underlying,
                    'OptPremium': csp_premium,
                    'Opt_Premium_%': (csp_premium / csp_strike) if csp_strike else 0,
                    'Date_open': datetime.now(),
                    'Expiry_Date': csp_expiry,
                    'Remarks': csp_remarks,
                    'Status': 'Open',
                    'Cash_required_per_position_(USD)': cash_required,
                    'Open_lots': csp_qty * 100
                }
                audit_data = {
                    'Audit ID': generate_audit_id(st.session_state.df_audit),
                    'Timestamp': datetime.now(),
                    'Action Type': 'Open',
                    'TradeID_Ref': new_trade_id,
                    'Remarks': f'Sell CSP {csp_ticker} ${csp_strike} x{csp_qty}',
                    'ScriptName': 'Income Wheel App',
                    'AffectedQty': csp_qty
                }
                try:
                    handler = GSheetHandler(st.session_state.current_sheet_id)
                    handler.append_trade(trade_data)  # Write to main Data Table
                    handler.append_audit(audit_data)  # Write to Audit Table
                    show_success_and_clear(new_trade_id, 'csp')
                except Exception as e:
                    st.error(f"❌ Error saving trade: {e}")
                    st.error("⚠️ Trade was NOT saved. Please try again.")
    
    # ---- BTC TAB ----
    with tab3:
        st.subheader("Buy to Close (BTC)")
        display_live_prices_cards()
        
        # Strategy selector (for reference, but BTC uses original trade's strategy)
        entry_strategy = render_strategy_selector("btc")
        
        if df_open is None or df_open.empty:
            st.warning("No open positions to close")
        else:
            # Filter to CC and CSP only
            open_options = df_open[df_open['TradeType'].isin(['CC', 'CSP'])].copy()
            
            if open_options.empty:
                st.warning("No open CC/CSP positions to close")
            else:
                # Normalize Expiry_Date for filtering
                open_options['_expiry_d'] = pd.to_datetime(open_options['Expiry_Date'], errors='coerce').dt.date
                open_options['_expiry_str'] = open_options['_expiry_d'].astype(str)
                open_options['_qty'] = pd.to_numeric(open_options['Quantity'], errors='coerce').fillna(0).astype(int)
                
                # ---- Filters: Ticker, Expiry Date, Type, Quantity ----
                st.markdown("**🔍 Filter positions**")
                fcol1, fcol2, fcol3, fcol4 = st.columns(4)
                tickers_btc = ["— Select —"] + sorted(open_options['Ticker'].dropna().unique().tolist())
                expiries_btc = ["— Select —"] + sorted(open_options['_expiry_str'].dropna().unique().tolist())
                with fcol1:
                    filter_ticker_btc = st.selectbox("Ticker", tickers_btc, key=f"btc_filter_ticker_{fv}", index=0)
                with fcol2:
                    filter_expiry_btc = st.selectbox("Expiry Date", expiries_btc, key=f"btc_filter_expiry_{fv}", index=0)
                with fcol3:
                    filter_type_btc = st.selectbox("Type", ["— Select —", "CC", "CSP"], key=f"btc_filter_type_{fv}", index=0)
                with fcol4:
                    qty_options = ["— Select —"] + ["1", "2", "3", "4", "5+"]
                    filter_qty_btc = st.selectbox("Quantity", qty_options, key=f"btc_filter_qty_{fv}", index=0)
                
                filtered_btc = open_options.copy()
                if filter_ticker_btc and filter_ticker_btc != "— Select —":
                    filtered_btc = filtered_btc[filtered_btc['Ticker'] == filter_ticker_btc]
                if filter_expiry_btc and filter_expiry_btc != "— Select —":
                    filtered_btc = filtered_btc[filtered_btc['_expiry_str'] == filter_expiry_btc]
                if filter_type_btc and filter_type_btc != "— Select —":
                    filtered_btc = filtered_btc[filtered_btc['TradeType'] == filter_type_btc]
                if filter_qty_btc and filter_qty_btc != "— Select —":
                    if filter_qty_btc == "5+":
                        filtered_btc = filtered_btc[filtered_btc['_qty'] >= 5]
                    else:
                        qty_val = int(filter_qty_btc)
                        filtered_btc = filtered_btc[filtered_btc['_qty'] == qty_val]
                
                if filtered_btc.empty:
                    st.warning("No positions match the selected filters. Adjust filters or choose '— Select —'.")
                else:
                    # Position selector OUTSIDE form so changing it triggers rerun and Close Qty gets correct max (1..quantity)
                    position_options = ["— Select position —"] + filtered_btc.apply(
                        lambda r: f"{r['TradeID']} - {r['TradeType']} {r['Ticker']} ${r['Option_Strike_Price_(USD)']} ({r['Quantity']} contracts)",
                        axis=1
                    ).tolist()
                    selected = st.selectbox("Select Position to Close", position_options, index=0, key=f"btc_position_select_{fv}")
                    
                    quantity = 0
                    trade_id = None
                    original = None
                    if selected and selected != "— Select position —":
                        trade_id = selected.split(' - ')[0].strip()
                        match = filtered_btc[filtered_btc['TradeID'].astype(str).str.strip() == trade_id]
                        if not match.empty:
                            original = match.iloc[0]
                            quantity = int(pd.to_numeric(original['Quantity'], errors='coerce') or 0)
                            quantity = max(1, quantity)
                    btc_close_qty_key = f"btc_close_qty_{fv}"
                    if quantity >= 1 and btc_close_qty_key in st.session_state:
                        old_q = st.session_state[btc_close_qty_key]
                        if old_q is None or old_q < 1 or old_q > quantity:
                            st.session_state[btc_close_qty_key] = quantity

                    max_close = quantity if quantity >= 1 else 1
                    default_close = quantity if quantity >= 1 else 1

                    with st.form(f"btc_form_{fv}"):
                        st.markdown("**Enter BTC details**")
                        col_btc1, col_btc2 = st.columns(2)
                        with col_btc1:
                            close_qty = st.number_input(
                                "Close Qty (contracts)",
                                min_value=1,
                                max_value=max_close,
                                value=default_close,
                                step=1,
                                key=f"btc_close_qty_{fv}",
                                help="Choose how many contracts to close (1 to %s for this position)." % max_close
                            )
                        with col_btc2:
                            btc_price = st.number_input("BTC Price (per contract, $)", min_value=0.0, step=0.01, key=f"btc_price_{fv}")

                        is_partial_choice = False
                        if quantity > 0 and close_qty < quantity:
                            is_partial_choice = st.checkbox(
                                "Partial close — split into (A) closed and (B) open",
                                key=f"btc_partial_confirm_{fv}",
                                help="Check to split: closed part (A) and open part (B)."
                            )
                        
                        if original is not None:
                            st.write(f"**Original Premium:** ${original['OptPremium']:.2f} · **Position Qty:** {quantity} contract(s)")
                            original_premium = pd.to_numeric(original['OptPremium'], errors='coerce') or 0.0
                            profit = (original_premium - btc_price) * 100 * close_qty
                            if btc_price > 0:
                                if profit >= 0:
                                    st.success(f"💰 P&L (this close): ${profit:,.2f}")
                                else:
                                    st.error(f"📉 P&L (this close): ${profit:,.2f}")
                                st.caption(f"Formula: (${original_premium:.2f} - ${btc_price:.2f}) × 100 × {close_qty} = ${profit:,.2f}")
                            if close_qty < quantity and not is_partial_choice:
                                st.warning(f"Close qty ({close_qty}) < position ({quantity}). Check **Partial close** to split, or set Close Qty to {quantity} for full close.")
                            elif close_qty == quantity:
                                st.info("Full close — entire position will be closed (no split).")
                        else:
                            st.caption("Select a position above, then enter Close Qty and BTC Price.")
                        
                        submitted = st.form_submit_button("Close Position (BTC)", type="primary")
                    
                    if submitted:
                        if not selected or selected == "— Select position —" or trade_id is None or original is None:
                            st.error("Please select a position to close.")
                        else:
                            valid, msg = TradeValidator.validate_btc(trade_id, df_open)
                            if not valid:
                                st.error(f"❌ {msg}")
                            else:
                                quantity = int(pd.to_numeric(original['Quantity'], errors='coerce') or 0)
                                close_qty = int(close_qty) if close_qty is not None else quantity
                                close_qty = min(max(1, close_qty), quantity)
                                remaining_qty = quantity - close_qty
                                original_premium = pd.to_numeric(original['OptPremium'], errors='coerce') or 0.0
                                profit = (original_premium - btc_price) * 100 * close_qty
                                
                                # Full close: close_qty == quantity → no split, no (A)/(B)
                                # Partial close: close_qty < quantity and user must have confirmed with checkbox
                                if remaining_qty > 0 and not st.session_state.get(f"btc_partial_confirm_{fv}", False):
                                    st.error("Close Qty is less than position size. Check **Partial close — split into (A) closed and (B) open** to proceed, or set Close Qty to the full position size for a full close.")
                                else:
                                    is_partial = remaining_qty > 0
                                    audit_data = {
                                        'Audit ID': generate_audit_id(st.session_state.df_audit),
                                        'Timestamp': datetime.now(),
                                        'Action Type': 'BTC',
                                        'TradeID_Ref': trade_id,
                                        'Remarks': f'BTC at ${btc_price:.2f}, P&L: ${profit:.2f}' + (f' (partial: {close_qty}/{quantity})' if is_partial else ''),
                                        'ScriptName': 'Income Wheel App',
                                        'AffectedQty': close_qty
                                    }
                                    try:
                                        handler = GSheetHandler(st.session_state.current_sheet_id)
                                        if is_partial:
                                            # Partial: rename to (A) closed, create (B) open
                                            trade_id_a = f"{trade_id}(A)"
                                            trade_id_b = f"{trade_id}(B)"
                                            audit_data['TradeID_Ref'] = f"{trade_id_a}, {trade_id_b}"
                                            updates_a = {
                                                'TradeID': trade_id_a,
                                                'Quantity': close_qty,
                                                'Open_lots': close_qty * 100,
                                                'Status': 'Closed',
                                                'Date_closed': datetime.now(),
                                                'Close_Price': btc_price,
                                                'Actual_Profit_(USD)': profit
                                            }
                                            handler.update_trade(trade_id, updates_a)
                                            row_b = original.to_dict()
                                            for k in list(row_b.keys()):
                                                if k.startswith('_'):
                                                    del row_b[k]
                                            row_b['TradeID'] = trade_id_b
                                            row_b['Quantity'] = remaining_qty
                                            row_b['Open_lots'] = remaining_qty * 100
                                            row_b['Status'] = 'Open'
                                            row_b.pop('Date_closed', None)
                                            row_b.pop('Close_Price', None)
                                            row_b.pop('Actual_Profit_(USD)', None)
                                            if 'Date_open' in row_b and pd.isna(row_b.get('Date_open')):
                                                row_b['Date_open'] = original.get('Date_open')
                                            handler.append_trade(row_b)
                                            show_success_and_clear(trade_id_a, 'btc', f"Closed {trade_id_a}; {trade_id_b} open with {remaining_qty} contract(s)")
                                        else:
                                            # Full close: keep TradeID, just close the position
                                            updates = {
                                                'Status': 'Closed',
                                                'Date_closed': datetime.now(),
                                                'Close_Price': btc_price,
                                                'Actual_Profit_(USD)': profit
                                            }
                                            handler.update_trade(trade_id, updates)
                                            show_success_and_clear(trade_id, 'btc', f"Full close: {trade_id}")
                                        handler.append_audit(audit_data)
                                    except Exception as e:
                                        st.error(f"❌ Error saving trade: {e}")
                                        st.error("⚠️ Trade was NOT saved. Please try again.")
    
    # ---- ROLL TAB ----
    with tab4:
        st.subheader("Roll Position")
        display_live_prices_cards()
        
        if df_open is None or df_open.empty:
            st.warning("No open positions to roll")
        else:
            open_options = df_open[df_open['TradeType'].isin(['CC', 'CSP'])].copy()
            
            if open_options.empty:
                st.warning("No open CC/CSP positions to roll")
            else:
                # Normalize Expiry_Date for filtering (may be datetime or string)
                open_options['_expiry_d'] = pd.to_datetime(open_options['Expiry_Date'], errors='coerce').dt.date
                open_options['_expiry_str'] = open_options['_expiry_d'].astype(str)
                
                # ---- Filters: Ticker, Option Type, Expiry Date (dropdowns) ----
                st.markdown("**🔍 Filter positions available for roll**")
                fcol1, fcol2, fcol3 = st.columns(3)
                tickers_available = ["— Select —"] + sorted(open_options['Ticker'].dropna().unique().tolist())
                expiries_available = ["— Select —"] + sorted(open_options['_expiry_str'].dropna().unique().tolist())
                
                with fcol1:
                    filter_ticker = st.selectbox("Ticker", tickers_available, key=f"roll_filter_ticker_{fv}", index=0)
                with fcol2:
                    filter_type = st.selectbox("Option Type", ["— Select —", "CC", "CSP"], key=f"roll_filter_type_{fv}", index=0)
                with fcol3:
                    filter_expiry = st.selectbox("Expiry Date", expiries_available, key=f"roll_filter_expiry_{fv}", index=0)
                
                # Apply filters (only when user chose something other than placeholder)
                filtered = open_options.copy()
                if filter_ticker and filter_ticker != "— Select —":
                    filtered = filtered[filtered['Ticker'] == filter_ticker]
                if filter_type and filter_type != "— Select —":
                    filtered = filtered[filtered['TradeType'] == filter_type]
                if filter_expiry and filter_expiry != "— Select —":
                    filtered = filtered[filtered['_expiry_str'] == filter_expiry]
                
                if filtered.empty:
                    st.warning("No positions match the selected filters. Adjust filters or choose 'All'.")
                else:
                    # Strategy selector
                    entry_strategy = render_strategy_selector("roll")
                    # Select position to roll (from filtered list); blank default
                    position_options = ["— Select position —"] + filtered.apply(
                        lambda r: f"{r['TradeID']} - {r['TradeType']} {r['Ticker']} ${r['Option_Strike_Price_(USD)']} exp {r['_expiry_str']}",
                        axis=1
                    ).tolist()
                    selected = st.selectbox("Select Position to Roll", position_options, key=f"roll_position_select_{fv}", index=0)
                    if selected and selected != "— Select position —":
                        old_trade_id = selected.split(' - ')[0]
                        original = filtered[filtered['TradeID'] == old_trade_id].iloc[0]
                        quantity = int(pd.to_numeric(original['Quantity'], errors='coerce') or 0)
                        original_premium = float(pd.to_numeric(original['OptPremium'], errors='coerce') or 0.0)
                        orig_strike = float(pd.to_numeric(original['Option_Strike_Price_(USD)'], errors='coerce') or 0.0)
                        st.write(f"**Rolling:** {original['TradeType']} {original['Ticker']}")
                        st.write(f"**Current Strike:** ${orig_strike:.2f}")
                        st.write(f"**Current Expiry:** {original['_expiry_str']}")
                        st.write(f"**Quantity:** {quantity} contract(s)")
                        st.divider()
                        col1, col2 = st.columns(2)
                        with col1:
                            new_strike = st.number_input("New Strike ($)", min_value=0.0, step=0.50, key=f"roll_new_strike_{fv}", value=0.0)
                            new_expiry = st.date_input("New Expiry", min_value=date.today(), key=f"roll_new_expiry_{fv}", value=None)
                        with col2:
                            btc_cost = st.number_input("BTC Cost (to close old, $)", min_value=0.0, step=0.01, key=f"roll_btc_cost_{fv}", value=0.0)
                            new_premium = st.number_input("New Premium ($)", min_value=0.0, step=0.01, key=f"roll_new_premium_{fv}", value=0.0)
                        quantity_to_roll = st.number_input(
                            "Quantity to roll (contracts)",
                            min_value=0,
                            max_value=max(1, quantity),
                            value=0,
                            step=1,
                            key=f"roll_quantity_{fv}"
                        )
                        old_position_profit = (original_premium - btc_cost) * 100 * quantity_to_roll
                        total_premium_received = original_premium * 100 * quantity_to_roll
                        # Net Credit/Debit = original premium + new premium - cost to close (includes all cash flows)
                        net_credit = (original_premium + new_premium - btc_cost) * 100 * quantity_to_roll
                        st.divider()
                        st.markdown("#### 📊 Roll Calculations (live)")
                        st.markdown("**💰 Premium Received (P&L):**")
                        col_prem1, col_prem2, col_prem3 = st.columns(3)
                        with col_prem1:
                            st.metric("Old Premium Received", f"${original_premium:.2f}")
                        with col_prem2:
                            st.metric("Quantity", f"{quantity_to_roll} contract(s)")
                        with col_prem3:
                            st.metric("**Total Premium Received**", f"**${total_premium_received:,.2f}**")
                        st.caption(f"Premium received when sold: ${original_premium:.2f} × 100 × {quantity_to_roll} = ${total_premium_received:,.2f}")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.markdown("**Old Position P&L (Being Closed):**")
                            if old_position_profit >= 0:
                                st.success(f"💰 Profit: ${old_position_profit:,.2f}")
                            else:
                                st.error(f"📉 Loss: ${old_position_profit:,.2f}")
                            st.caption(f"Formula: (${original_premium:.2f} received - ${btc_cost:.2f} paid) × 100 × {quantity_to_roll} = ${old_position_profit:,.2f}")
                            st.info(f"✅ This P&L (${old_position_profit:,.2f}) will be recorded as **Actual_Profit_(USD)** on the old position.")
                        with col2:
                            st.markdown("**Net Credit/Debit from Roll:**")
                            if net_credit >= 0:
                                st.success(f"💵 Net Credit: ${net_credit:,.2f}")
                            else:
                                st.warning(f"💸 Net Debit: ${abs(net_credit):,.2f}")
                            st.caption(f"Formula: (${original_premium:.2f} orig + ${new_premium:.2f} new − ${btc_cost:.2f} close) × 100 × {quantity_to_roll} = ${net_credit:,.2f}")
                            st.info("💡 Original premium received + new premium − cost to close old = net credit/debit.")
                        submitted = st.button("Execute Roll", type="primary", key="roll_submit_btn")
                    else:
                        st.caption("Select a position above to roll.")
                        submitted = False
                
                if submitted and not filtered.empty and selected and selected != "— Select position —":
                        roll_errs = []
                        if not new_strike or new_strike <= 0:
                            roll_errs.append("Enter new strike.")
                        if not new_expiry:
                            roll_errs.append("Select new expiry.")
                        if new_premium is None or new_premium < 0:
                            roll_errs.append("Enter new premium.")
                        if not quantity_to_roll or quantity_to_roll < 1:
                            roll_errs.append("Enter quantity to roll (at least 1).")
                        if roll_errs:
                            st.error("Please fix: " + " ".join(roll_errs))
                        else:
                            valid, msg = TradeValidator.validate_roll(old_trade_id, new_expiry, df_open)
                            if not valid:
                                st.error(f"❌ {msg}")
                            else:
                                new_trade_id = generate_trade_id(df_trades)
                                _qty_save = quantity_to_roll  # BUG-04 fix: use quantity_to_roll, not original['Quantity']
                                _orig_prem = float(pd.to_numeric(original['OptPremium'], errors='coerce') or 0.0)
                                orig_strike = float(pd.to_numeric(original['Option_Strike_Price_(USD)'], errors='coerce') or 0.0)  # BUG-06 fix: guarded conversion
                                old_position_profit = (_orig_prem - btc_cost) * 100 * _qty_save
                                net_credit_save = (_orig_prem + new_premium - btc_cost) * 100 * _qty_save
                                orig_strat = original.get('StrategyType') or entry_strategy or 'WHEEL'
                                if orig_strat == "— Select strategy —":
                                    orig_strat = 'WHEEL'

                                try:
                                    handler = GSheetHandler(st.session_state.current_sheet_id)
                                    # BUG-01 fix: use atomic_transaction for all 3 operations
                                    ops = [
                                        {'type': 'update_trade', 'data': {
                                            'trade_id': old_trade_id,
                                            'updates': {
                                                'Status': 'Closed',
                                                'Date_closed': datetime.now(),
                                                'Close_Price': btc_cost,
                                                'Actual_Profit_(USD)': old_position_profit,
                                                'Remarks': f"Rolled to {new_trade_id}"
                                            }
                                        }},
                                        {'type': 'append_trade', 'data': {
                                            'TradeID': new_trade_id,
                                            'Ticker': original['Ticker'],
                                            'StrategyType': orig_strat,
                                            'Direction': 'Sell',
                                            'TradeType': original['TradeType'],
                                            'Quantity': _qty_save,
                                            'Option_Strike_Price_(USD)': new_strike,
                                            'Price_of_current_underlying_(USD)': original['Price_of_current_underlying_(USD)'],
                                            'OptPremium': new_premium,
                                            'Date_open': datetime.now(),
                                            'Expiry_Date': new_expiry,
                                            'Status': 'Open',
                                            'Remarks': f"Rolled from {old_trade_id}",
                                            'Open_lots': _qty_save * 100
                                        }},
                                        {'type': 'append_audit', 'data': {
                                            'Audit ID': generate_audit_id(st.session_state.df_audit),
                                            'Timestamp': datetime.now(),
                                            'Action Type': 'Roll',
                                            'TradeID_Ref': f"{old_trade_id}, {new_trade_id}",
                                            'Remarks': f"Net credit: ${net_credit_save:.2f}",
                                            'ScriptName': 'Income Wheel App',
                                            'AffectedQty': _qty_save
                                        }}
                                    ]
                                    handler.atomic_transaction(ops)
                                    show_success_and_clear(new_trade_id, 'roll', f"Rolled from {old_trade_id}")
                                except Exception as e:
                                    st.error(f"❌ Error saving trade: {e}")
                                st.error("⚠️ Trade was NOT saved. Please try again.")
    
    # ---- EXPIRE TAB ----
    with tab5:
        st.subheader("Expire Options (Worthless)")
        display_live_prices_cards()
        st.info("💡 Use this for options expiring worthless (both CC and CSP). Options will be closed at $0.00 and full premium recorded as profit. No stock positions are created or closed.")
        
        # Strategy selector at top
        entry_strategy = render_strategy_selector("expire")
        
        if df_open is None or df_open.empty:
            st.warning("No open positions")
        else:
            # Filter to ONLY OPEN options expiring in past 30 days and next 15 days
            today = date.today()
            thirty_days_ago = today - timedelta(days=30)
            fifteen_days_later = today + timedelta(days=15)
            
            # Explicitly filter for OPEN status and CC/CSP types
            open_options = df_open[
                (df_open['Status'].str.upper() == 'OPEN') &
                (df_open['TradeType'].isin(['CC', 'CSP']))
            ].copy()
            
            if open_options.empty:
                st.warning("No open option positions found")
            else:
                # Convert expiry dates
                open_options['Expiry_Date'] = pd.to_datetime(open_options['Expiry_Date'], errors='coerce')
                open_options['Expiry_Date_Date'] = open_options['Expiry_Date'].dt.date
                
                # Filter to options expiring in past 30 days and next 15 days
                expiring_options = open_options[
                    (open_options['Expiry_Date_Date'] >= thirty_days_ago) &
                    (open_options['Expiry_Date_Date'] <= fifteen_days_later) &
                    (open_options['Expiry_Date_Date'].notna())
                ].copy()
                
                if expiring_options.empty:
                    st.warning("No open options expiring in the past 30 days or next 15 days")
                else:
                    # Sort by ticker, then expiry date
                    expiring_options = expiring_options.sort_values(['Ticker', 'Expiry_Date_Date'])
                    
                    # Initialize session state for checkboxes if not exists
                    if 'expire_checkboxes' not in st.session_state:
                        st.session_state.expire_checkboxes = {}
                    
                    st.markdown("#### Options Expiring (Past 30 Days & Next 15 Days) - Open Positions Only")
                    st.caption("Select options that will expire worthless. Total premium will be calculated automatically.")
                    
                    # Group by ticker for better display
                    tickers_expiring = sorted(expiring_options['Ticker'].unique().tolist())
                    
                    for ticker in tickers_expiring:
                        ticker_positions = expiring_options[expiring_options['Ticker'] == ticker].copy()
                        
                        st.markdown(f"**{ticker}**")
                        
                        # Create table for this ticker
                        display_data = []
                        for _, row in ticker_positions.iterrows():
                            trade_id = row['TradeID']
                            expiry_date = row['Expiry_Date_Date']
                            # DTE inclusive: today + expiry day both count
                            dte = (expiry_date - today).days + 1
                            premium = pd.to_numeric(row['OptPremium'], errors='coerce') or 0.0
                            quantity = int(row['Quantity'])
                            full_premium = premium * quantity * 100
                            
                            # Format DTE (negative = past expiry, positive = future)
                            if dte < 0:
                                dte_str = f"-{abs(dte)}d (Past)"
                            elif dte == 0:
                                dte_str = "Today"
                            else:
                                dte_str = f"{dte}d"
                            
                            # Get checkbox state
                            checkbox_key = f"expire_{trade_id}"
                            is_selected = st.session_state.expire_checkboxes.get(trade_id, False)
                            
                            display_data.append({
                                'Select': is_selected,
                                'TradeID': trade_id,
                                'Type': row['TradeType'],
                                'Strike': f"${row['Option_Strike_Price_(USD)']:.2f}",
                                'Quantity': quantity,
                                'Expiry': expiry_date.strftime('%Y-%m-%d'),
                                'DTE': dte_str,
                                'Premium/Contract': f"${premium:.2f}",
                                'Total Premium': f"${full_premium:,.2f}"
                            })
                        
                        df_ticker = pd.DataFrame(display_data)
                        
                        # Display with checkboxes
                        for idx, row_data in df_ticker.iterrows():
                            trade_id = row_data['TradeID']
                            checkbox_key = f"expire_{trade_id}"
                            
                            col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([0.3, 1.2, 0.8, 1, 0.8, 1, 1, 1.2])
                            
                            with col1:
                                is_selected = st.checkbox(
                                    "",
                                    key=checkbox_key,
                                    value=st.session_state.expire_checkboxes.get(trade_id, False)
                                )
                                st.session_state.expire_checkboxes[trade_id] = is_selected
                            
                            with col2:
                                st.write(row_data['TradeID'])
                            with col3:
                                st.write(row_data['Type'])
                            with col4:
                                st.write(row_data['Strike'])
                            with col5:
                                st.write(f"{int(row_data['Quantity'])}")
                            with col6:
                                st.write(row_data['Expiry'])
                            with col7:
                                st.write(row_data['DTE'])
                            with col8:
                                st.write(f"{row_data['Premium/Contract']} → **{row_data['Total Premium']}**")
                        
                        st.divider()
                    
                    # Collect selected trade IDs and calculate live total premium
                    selected_trade_ids = [
                        trade_id for trade_id in expiring_options['TradeID'].tolist()
                        if st.session_state.expire_checkboxes.get(trade_id, False)
                    ]
                    
                    # Calculate live total premium (updates as checkboxes change)
                    total_premium = 0.0
                    if selected_trade_ids:
                        for trade_id in selected_trade_ids:
                            original = expiring_options[expiring_options['TradeID'] == trade_id].iloc[0]
                            premium = pd.to_numeric(original['OptPremium'], errors='coerce') or 0.0
                            quantity = pd.to_numeric(original['Quantity'], errors='coerce')
                            if pd.isna(quantity) or quantity <= 0:
                                continue  # Skip invalid quantities in calculation
                            quantity = int(quantity)
                            full_premium = premium * quantity * 100
                            total_premium += full_premium
                    
                    # Display live total premium calculation
                    st.markdown("---")
                    if selected_trade_ids:
                        st.markdown(f"#### Selected: {len(selected_trade_ids)} position(s)")
                        
                        # Show breakdown with premium received details
                        st.markdown("**💰 Premium Received Breakdown:**")
                        for trade_id in selected_trade_ids:
                            original = expiring_options[expiring_options['TradeID'] == trade_id].iloc[0]
                            premium = pd.to_numeric(original['OptPremium'], errors='coerce') or 0.0
                            quantity = pd.to_numeric(original['Quantity'], errors='coerce')
                            if pd.isna(quantity) or quantity <= 0:
                                st.warning(f"⚠️ {trade_id}: Invalid Quantity ({original.get('Quantity', 'N/A')})")
                                continue
                            quantity = int(quantity)
                            full_premium = premium * quantity * 100
                            
                            st.write(f"- **{trade_id}**: {original['TradeType']} {original['Ticker']} | Premium: ${premium:.2f}/contract × {quantity} contracts × 100 = **${full_premium:,.2f}**")
                        
                        # Live total premium display
                        st.success(f"💰 **Total Premium Received: ${total_premium:,.2f}** (Full premium, positions closed at $0.00)")
                        st.info(f"✅ This total premium (${total_premium:,.2f}) will be recorded as **Actual_Profit_(USD)** for each position when expired.")
                        
                        # Expire button
                        if st.button("🚀 Expire Selected Options", type="primary", key="expire_button"):
                            success_count = 0
                            error_count = 0
                            
                            for trade_id in selected_trade_ids:
                                try:
                                    original = expiring_options[expiring_options['TradeID'] == trade_id].iloc[0]
                                    
                                    handler = GSheetHandler(st.session_state.current_sheet_id)
                                    
                                    # Calculate full premium (actual profit from expiring worthless)
                                    premium = pd.to_numeric(original['OptPremium'], errors='coerce') or 0.0
                                    quantity = pd.to_numeric(original['Quantity'], errors='coerce')
                                    if pd.isna(quantity) or quantity <= 0:
                                        st.error(f"❌ {trade_id}: Invalid Quantity ({original.get('Quantity', 'N/A')})")
                                        error_count += 1
                                        continue
                                    quantity = int(quantity)
                                    full_premium = premium * quantity * 100  # Full premium = actual profit
                                    
                                    # Close option position at $0.00 (expire worthless means full premium collected)
                                    handler.update_trade(trade_id, {
                                        'Status': 'Closed',
                                        'Date_closed': datetime.now(),
                                        'Close_Price': 0.00,  # Expire closes at $0.00
                                        'Actual_Profit_(USD)': full_premium,  # Full premium is the profit
                                        'Remarks': f"{original['TradeType']} Expired Worthless"
                                    })
                                    
                                    # Audit entry (no stock positions created/closed for expire)
                                    handler.append_audit({
                                        'Audit ID': generate_audit_id(st.session_state.df_audit),
                                        'Timestamp': datetime.now(),
                                        'Action Type': 'Expire',
                                        'TradeID_Ref': trade_id,
                                        'Remarks': f"{original['TradeType']} Expired Worthless",
                                        'ScriptName': 'Income Wheel App',
                                        'AffectedQty': quantity
                                    })
                                    
                                    success_count += 1
                                except Exception as e:
                                    st.error(f"❌ Error expiring {trade_id}: {e}")
                                    error_count += 1
                            
                            # Show summary and clear if all successful
                            if error_count == 0 and success_count > 0:
                                show_success_and_clear(None, 'expire', f"Successfully expired {success_count} position(s)")
                            elif success_count > 0:
                                st.warning(f"⚠️ {success_count} position(s) expired, {error_count} failed. Please check errors above.")
                                refresh_data()
                            else:
                                st.error(f"❌ Failed to expire any positions. Please check errors above.")
                            
                            if success_count > 0:
                                st.success(f"✅ Successfully expired {success_count} position(s)")
                                refresh_data()
                            if error_count > 0:
                                st.warning(f"⚠️ {error_count} position(s) had errors")
    
    # ---- ASSIGNMENT TAB ----
    with tab6:
        st.subheader("Assignment & Exercise")
        display_live_prices_cards()
        
        # Create two sub-tabs for Assignment (CSP) and Exercise (CC)
        sub_tab1, sub_tab2 = st.tabs(["📥 Assignment (CSP)", "📤 Exercise (CC)"])
        
        # ---- ASSIGNMENT SUB-TAB (CSP) ----
        with sub_tab1:
            st.markdown("#### CSP Assignment")
            
            if df_open is None or df_open.empty:
                st.warning("No open positions")
            else:
                # Filter to CSP only
                open_csps = df_open[df_open['TradeType'] == 'CSP']
                
                if open_csps.empty:
                    st.warning("No open CSP positions")
                else:
                    # Strategy selector before position selector
                    entry_strategy = render_strategy_selector("assignment")
                    position_options = ["— Select position —"] + open_csps.apply(
                        lambda r: f"{r['TradeID']} - CSP {r['Ticker']} ${r['Option_Strike_Price_(USD)']}",
                        axis=1
                    ).tolist()
                    selected = st.selectbox("Select CSP Position", position_options, key=f"assignment_csp_select_{fv}", index=0)
                    if selected and selected != "— Select position —":
                        trade_id = selected.split(' - ')[0]
                        original = open_csps[open_csps['TradeID'] == trade_id].iloc[0]
                        premium = pd.to_numeric(original['OptPremium'], errors='coerce') or 0.0
                        quantity = int(original['Quantity'])
                        full_premium = premium * quantity * 100
                        col_left, col_right = st.columns(2)
                        with col_left:
                            st.markdown("#### 📥 Assignment Details")
                            st.info(f"**You will BUY** {int(quantity * 100)} shares of **{original['Ticker']}** at **${original['Option_Strike_Price_(USD)']:.2f}** per share")
                            st.caption("💡 Assignment for CSPs: You will BUY shares when the put is assigned.")
                        with col_right:
                            st.markdown("#### 💰 Premium Received")
                            st.metric("Premium per Contract", f"${premium:.2f}")
                            st.metric("Quantity", f"{quantity} contracts")
                            st.metric("**Total Premium Received**", f"**${full_premium:,.2f}**")
                            st.caption(f"Formula: ${premium:.2f} × 100 × {quantity} = ${full_premium:,.2f}")
                            st.caption(f"✅ This premium will be recorded as **Actual_Profit_(USD)** when assigned.")
                        st.divider()
                        with st.form(f"assignment_csp_form_{fv}"):
                            submitted = st.form_submit_button("Confirm Assignment", type="primary")
                    else:
                        st.caption("Select a position above.")
                        submitted = False
                        trade_id = None
                        original = None
                        
                    if submitted and selected and selected != "— Select position —":
                        valid, msg = TradeValidator.validate_exercise_csp(trade_id, df_open)
                        
                        if not valid:
                            st.error(f"❌ {msg}")
                        else:
                            try:
                                handler = GSheetHandler(st.session_state.current_sheet_id)
                                
                                # Calculate full premium (actual profit from assignment)
                                premium = pd.to_numeric(original['OptPremium'], errors='coerce') or 0.0
                                quantity = int(original['Quantity'])
                                full_premium = premium * quantity * 100
                                
                                # Close option position
                                handler.update_trade(trade_id, {
                                    'Status': 'Closed',
                                    'Date_closed': datetime.now(),
                                    'Close_Price': 0.00,
                                    'Actual_Profit_(USD)': full_premium,
                                    'Remarks': "CSP Assigned - Bought stock"
                                })
                                
                                # Create stock position
                                stock_trade_id = generate_trade_id(df_trades)
                                stock_trade = {
                                    'TradeID': stock_trade_id,
                                    'Ticker': original['Ticker'],
                                    'StrategyType': entry_strategy or 'WHEEL',
                                    'Direction': 'Buy',
                                    'TradeType': 'STOCK',
                                    'Quantity': original['Quantity'] * 100,
                                    'Option_Strike_Price_(USD)': original['Option_Strike_Price_(USD)'],
                                    'Price_of_current_underlying_(USD)': original['Option_Strike_Price_(USD)'],
                                    'Date_open': datetime.now(),
                                    'Status': 'Open',
                                    'Remarks': f"Assigned from {trade_id}"
                                }
                                handler.append_trade(stock_trade)
                                
                                # Audit entry
                                handler.append_audit({
                                    'Audit ID': generate_audit_id(st.session_state.df_audit),
                                    'Timestamp': datetime.now(),
                                    'Action Type': 'Exercise',
                                    'TradeID_Ref': f"{trade_id}, {stock_trade_id}",
                                    'Remarks': "CSP Assigned - Bought stock",
                                    'ScriptName': 'Income Wheel App',
                                    'AffectedQty': original['Quantity']
                                })
                                
                                show_success_and_clear(stock_trade_id, 'assignment', f"Assigned from {trade_id}")
                            except Exception as e:
                                st.error(f"❌ Error saving trade: {e}")
                                st.error("⚠️ Trade was NOT saved. Please try again.")
        
        # ---- EXERCISE SUB-TAB (CC) ----
        with sub_tab2:
            st.markdown("#### CC Exercise")
            
            if df_open is None or df_open.empty:
                st.warning("No open positions")
            else:
                # Filter to CC only
                open_ccs = df_open[df_open['TradeType'] == 'CC']
                
                if open_ccs.empty:
                    st.warning("No open CC positions")
                else:
                    entry_strategy = render_strategy_selector("exercise")
                    position_options = ["— Select position —"] + open_ccs.apply(
                        lambda r: f"{r['TradeID']} - CC {r['Ticker']} ${r['Option_Strike_Price_(USD)']}",
                        axis=1
                    ).tolist()
                    selected = st.selectbox("Select CC Position", position_options, key=f"exercise_cc_select_{fv}", index=0)
                    if selected and selected != "— Select position —":
                        trade_id = selected.split(' - ')[0]
                        original = open_ccs[open_ccs['TradeID'] == trade_id].iloc[0]
                        premium = pd.to_numeric(original['OptPremium'], errors='coerce') or 0.0
                        quantity = int(original['Quantity'])
                        full_premium = premium * quantity * 100
                        col_left, col_right = st.columns(2)
                        with col_left:
                            st.markdown("#### 📤 Exercise Details")
                            st.info(f"**You will SELL/REMOVE** {int(quantity * 100)} shares of **{original['Ticker']}** at **${original['Option_Strike_Price_(USD)']:.2f}** per share")
                            st.caption("💡 Exercise for CCs: You will SELL/REMOVE shares when the call is exercised.")
                        with col_right:
                            st.markdown("#### 💰 Premium Received")
                            st.metric("Premium per Contract", f"${premium:.2f}")
                            st.metric("Quantity", f"{quantity} contracts")
                            st.metric("**Total Premium Received**", f"**${full_premium:,.2f}**")
                            st.caption(f"Formula: ${premium:.2f} × 100 × {quantity} = ${full_premium:,.2f}")
                            st.caption(f"✅ This premium will be recorded as **Actual_Profit_(USD)** when exercised.")
                        st.divider()
                        with st.form(f"exercise_cc_form_{fv}"):
                            submitted = st.form_submit_button("Confirm Exercise", type="primary")
                    else:
                        st.caption("Select a position above.")
                        submitted = False
                        trade_id = None
                        original = None
                        
                    if submitted and selected and selected != "— Select position —":
                        valid, msg = TradeValidator.validate_exercise_cc(trade_id, df_open)
                        
                        if not valid:
                            st.error(f"❌ {msg}")
                        else:
                            try:
                                handler = GSheetHandler(st.session_state.current_sheet_id)
                                
                                # Calculate full premium (actual profit from exercise)
                                premium = pd.to_numeric(original['OptPremium'], errors='coerce') or 0.0
                                quantity = int(original['Quantity'])
                                full_premium = premium * quantity * 100
                                
                                # Close option position
                                handler.update_trade(trade_id, {
                                    'Status': 'Closed',
                                    'Date_closed': datetime.now(),
                                    'Close_Price': 0.00,
                                    'Actual_Profit_(USD)': full_premium,
                                    'Remarks': "CC Called - Sold stock"
                                })
                                
                                # Close stock position
                                stock_trade_id = None  # BUG-05 fix: init before if-block
                                stock_positions = df_open[
                                    (df_open['Ticker'] == original['Ticker']) &
                                    (df_open['TradeType'] == 'STOCK')
                                ]
                                if not stock_positions.empty:
                                    stock_trade_id = stock_positions.iloc[0]['TradeID']
                                    # BUG-10 fix: calculate and record stock P&L
                                    strike_price = float(pd.to_numeric(original['Option_Strike_Price_(USD)'], errors='coerce') or 0)
                                    from persistence import get_stock_average_prices
                                    stock_avg_prices = get_stock_average_prices(st.session_state.get('current_portfolio', 'Income Wheel'))
                                    cost_basis = float(stock_avg_prices.get(original['Ticker'], strike_price))
                                    shares_sold = int(pd.to_numeric(original['Quantity'], errors='coerce') or 0) * 100
                                    stock_profit = (strike_price - cost_basis) * shares_sold
                                    handler.update_trade(stock_trade_id, {
                                        'Status': 'Closed',
                                        'Date_closed': datetime.now(),
                                        'Close_Price': strike_price,
                                        'Actual_Profit_(USD)': stock_profit,
                                        'Remarks': f"Called away by {trade_id}"
                                    })
                                    audit_ref = f"{trade_id}, {stock_trade_id}"
                                else:
                                    audit_ref = trade_id
                                
                                # Audit entry
                                handler.append_audit({
                                    'Audit ID': generate_audit_id(st.session_state.df_audit),
                                    'Timestamp': datetime.now(),
                                    'Action Type': 'Exercise',
                                    'TradeID_Ref': audit_ref,
                                    'Remarks': "CC Called - Sold stock",
                                    'ScriptName': 'Income Wheel App',
                                    'AffectedQty': original['Quantity']
                                })
                                
                                stock_id_msg = f"Stock position {stock_trade_id} closed (P&L: ${stock_profit:,.2f})" if stock_trade_id else "No stock position found"
                                show_success_and_clear(trade_id, 'exercise', stock_id_msg)
                            except Exception as e:
                                st.error(f"❌ Error saving trade: {e}")
                                st.error("⚠️ Trade was NOT saved. Please try again.")
    
    # Display Audit Log (Last 50 Transactions) - this portfolio only (exclude other portfolio's audit refs)
    st.divider()
    if df_audit is not None and not df_audit.empty:
        st.subheader("📋 Recent Trade Activity (Last 50 Transactions)")
        tradeid_col_audit = None
        for col in df_audit.columns:
            if 'tradeid' in col.lower() or 'trade_id' in col.lower():
                tradeid_col_audit = col
                break
        if df_trades is not None and not df_trades.empty and tradeid_col_audit and tradeid_col_audit in df_audit.columns:
            valid_trade_ids = set(df_trades['TradeID'].astype(str).str.strip())
            def _ref_matches(s):
                if not s or s not in valid_trade_ids:
                    base = s.replace('T-', 'T').replace('-', '')
                    for tid in valid_trade_ids:
                        t = str(tid).strip()
                        if t == s or t == base or s == t or (base and t.startswith(s)) or (base and s.startswith(t)):
                            return True
                    return False
                return True
            def ref_in_trades(ref):
                if pd.isna(ref): return False
                s = str(ref).strip()
                # Roll (and similar) rows have "T-001, T-002" – keep if any ref is in this portfolio
                for part in s.replace(';', ',').split(','):
                    part = part.strip()
                    if part and _ref_matches(part):
                        return True
                return False
            df_audit = df_audit[df_audit[tradeid_col_audit].apply(ref_in_trades)].copy()
        if df_audit.empty:
            st.info("ℹ️ No trade activity for this portfolio.")
        else:
            timestamp_col = None
            for col in df_audit.columns:
                if 'time' in col.lower() or 'stamp' in col.lower():
                    timestamp_col = col
                    break
            if timestamp_col:
                df_audit_sorted = df_audit.sort_values(timestamp_col, ascending=False).head(50)
            else:
                df_audit_sorted = df_audit.tail(50).iloc[::-1]
            if df_trades is not None and not df_trades.empty:
                tradeid_col = None
                for col in df_audit_sorted.columns:
                    if 'tradeid' in col.lower() or 'trade_id' in col.lower():
                        tradeid_col = col
                        break
                trades_detail_cols = ['TradeID', 'TradeType', 'Direction', 'Ticker', 'Quantity', 'Option_Strike_Price_(USD)']
                trades_detail_cols = [c for c in trades_detail_cols if c in df_trades.columns]
                if tradeid_col and trades_detail_cols:
                    merged = df_audit_sorted.merge(
                        df_trades[trades_detail_cols],
                        left_on=tradeid_col,
                        right_on='TradeID',
                        how='left',
                        suffixes=('', '_trades')
                    )
                    # For Roll etc.: ref may be "T-001, T-002" so merge won't match; fill from first ID in ref
                    lookup_cols = [c for c in ['Ticker', 'Quantity', 'Option_Strike_Price_(USD)'] if c in df_trades.columns]
                    if lookup_cols and tradeid_col in merged.columns:
                        ref_col = tradeid_col
                        has_ticker = 'Ticker' in merged.columns
                        missing = (merged['Ticker'].isna() if has_ticker else merged[ref_col].notna()) & merged[ref_col].notna()
                        if missing.any():
                            tid_lookup = df_trades.set_index('TradeID')[lookup_cols].to_dict('index')
                            for idx in merged.index[missing]:
                                ref_val = merged.at[idx, ref_col]
                                if pd.isna(ref_val) or ref_val == '':
                                    continue
                                first_id = str(ref_val).replace(';', ',').split(',')[0].strip()
                                if first_id and first_id in tid_lookup:
                                    info = tid_lookup[first_id]
                                    for k in lookup_cols:
                                        if k in merged.columns:
                                            merged.at[idx, k] = info.get(k, '')
                elif tradeid_col:
                    merged = df_audit_sorted.merge(
                        df_trades[['TradeID', 'TradeType', 'Direction']],
                        left_on=tradeid_col,
                        right_on='TradeID',
                        how='left'
                    )
                    merged['Ticker'] = ''
                    merged['Quantity'] = ''
                    merged['Option_Strike_Price_(USD)'] = ''
                else:
                    merged = df_audit_sorted.copy()
                    merged['TradeType'] = None
                    merged['Direction'] = None
                    merged['Ticker'] = ''
                    merged['Quantity'] = ''
                    merged['Option_Strike_Price_(USD)'] = ''
            else:
                merged = df_audit_sorted.copy()
                merged['TradeType'] = None
                merged['Direction'] = None
                merged['Ticker'] = ''
                merged['Quantity'] = ''
                merged['Option_Strike_Price_(USD)'] = ''
            
            # Select columns to display (include Ticker, Qty, Strike)
            display_cols = []
            col_mapping = {}
            for col in merged.columns:
                col_lower = col.lower()
                if 'audit id' in col_lower:
                    display_cols.append(col)
                    col_mapping[col] = 'Audit ID'
                elif 'tradeid' in col_lower and 'ref' in col_lower:
                    display_cols.append(col)
                    col_mapping[col] = 'Trade ID'
                elif col == 'Ticker':
                    display_cols.append(col)
                    col_mapping[col] = 'Ticker'
                elif col == 'Quantity':
                    display_cols.append(col)
                    col_mapping[col] = 'Qty'
                elif col == 'Option_Strike_Price_(USD)':
                    display_cols.append(col)
                    col_mapping[col] = 'Strike ($)'
                elif 'affectedqty' in col_lower or 'affected_qty' in col_lower:
                    display_cols.append(col)
                    col_mapping[col] = 'Affected Qty'
                elif 'tradetype' in col_lower:
                    display_cols.append(col)
                    col_mapping[col] = 'Trade Type'
                elif 'direction' in col_lower:
                    display_cols.append(col)
                    col_mapping[col] = 'Direction'
                elif 'remarks' in col_lower or 'comments' in col_lower:
                    display_cols.append(col)
                    col_mapping[col] = 'Comments'
                elif 'action' in col_lower and 'type' in col_lower:
                    display_cols.append(col)
                    col_mapping[col] = 'Action'
                elif timestamp_col and col == timestamp_col:
                    display_cols.append(col)
                    col_mapping[col] = 'Timestamp'
            if display_cols:
                df_display = merged[display_cols].copy()
                df_display = df_display.rename(columns=col_mapping)
                if 'Strike ($)' in df_display.columns:
                    def _fmt_strike(x):
                        if pd.isna(x) or str(x).strip() == '':
                            return ''
                        try:
                            return f"${float(x):.2f}"
                        except (ValueError, TypeError):
                            return str(x)
                    df_display['Strike ($)'] = df_display['Strike ($)'].apply(_fmt_strike)
                if 'Qty' in df_display.columns:
                    df_display['Qty'] = pd.to_numeric(df_display['Qty'], errors='coerce')
                if 'Timestamp' in df_display.columns:
                    try:
                        df_display['Timestamp'] = pd.to_datetime(df_display['Timestamp'], errors='coerce')
                        df_display['Timestamp'] = df_display['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    except Exception:
                        pass
                # Convert all columns to string to avoid Arrow mixed-type errors
                df_display = df_display.fillna('').astype(str)
                preferred_order = ['Timestamp', 'Audit ID', 'Trade ID', 'Action', 'Trade Type', 'Ticker', 'Qty', 'Strike ($)', 'Affected Qty', 'Direction', 'Comments']
                existing_cols = [col for col in preferred_order if col in df_display.columns]
                remaining_cols = [col for col in df_display.columns if col not in preferred_order]
                df_display = df_display[existing_cols + remaining_cols]
                column_config = {}
                for c in df_display.columns:
                    if c == 'Comments':
                        column_config[c] = st.column_config.TextColumn(c, width="large")
                    elif c == 'Timestamp':
                        column_config[c] = st.column_config.TextColumn(c, width="medium")
                    else:
                        column_config[c] = st.column_config.TextColumn(c, width="small")
                st.dataframe(df_display, use_container_width=True, hide_index=True, column_config=column_config)
            else:
                st.info("ℹ️ Audit log structure not recognized. Showing raw data:")
                st.dataframe(df_audit_sorted.head(50), use_container_width=True, hide_index=True)
    else:
        st.info("ℹ️ No audit log entries found.")


# ============================================================
# EXPIRY LADDER PAGE
# ============================================================
def render_expiry_ladder():
    """Render expiry ladder visualization"""
    st.title("📈 Expiry Ladder")
    
    df_open = st.session_state.df_open
    df_trades = st.session_state.df_trades
    
    # Filter by strategy if selected
    strategy_filter = st.session_state.get('strategy_filter', 'All')
    if strategy_filter != 'All':
        portfolio = st.session_state.get('current_portfolio', 'Income Wheel')
        pmcc_tickers = get_pmcc_tickers(portfolio)
        pmcc_tickers_set = set(pmcc_tickers) if pmcc_tickers else set()
        
        if df_open is not None and not df_open.empty:
            if strategy_filter == 'PMCC':
                pmcc_mask = (
                    df_open['Ticker'].isin(pmcc_tickers_set) |
                    (df_open.get('StrategyType', '') == 'PMCC') |
                    (df_open['TradeType'] == 'LEAP')
                )
                df_open = df_open[pmcc_mask].copy()
            elif strategy_filter == 'WHEEL':
                wheel_mask = (
                    ~df_open['Ticker'].isin(pmcc_tickers_set) &
                    (
                        (df_open.get('StrategyType', '') == 'WHEEL') |
                        (df_open.get('StrategyType', '').isna()) |
                        (df_open.get('StrategyType', '') == '')
                    ) &
                    (df_open['TradeType'] != 'LEAP')
                )
                df_open = df_open[wheel_mask].copy()
            elif strategy_filter == 'ActiveCore':
                df_open = df_open[df_open.get('StrategyType', '') == 'ActiveCore'].copy()

        if df_trades is not None and not df_trades.empty:
            if strategy_filter == 'PMCC':
                pmcc_mask = (
                    df_trades['Ticker'].isin(pmcc_tickers_set) |
                    (df_trades.get('StrategyType', '') == 'PMCC') |
                    (df_trades['TradeType'] == 'LEAP')
                )
                df_trades = df_trades[pmcc_mask].copy()
            elif strategy_filter == 'WHEEL':
                wheel_mask = (
                    ~df_trades['Ticker'].isin(pmcc_tickers_set) &
                    (
                        (df_trades.get('StrategyType', '') == 'WHEEL') |
                        (df_trades.get('StrategyType', '').isna()) |
                        (df_trades.get('StrategyType', '') == '')
                    ) &
                    (df_trades['TradeType'] != 'LEAP')
                )
                df_trades = df_trades[wheel_mask].copy()
            elif strategy_filter == 'ActiveCore':
                df_trades = df_trades[df_trades.get('StrategyType', '') == 'ActiveCore'].copy()

    # Get all option positions (open + closed) for 5 years back and forward
    today = date.today()
    start_date = date(today.year - 5, 1, 1)
    end_date = date(today.year + 5, 12, 31)
    
    # Combine open and closed options
    all_options = []
    
    # Add open positions
    if df_open is not None and not df_open.empty:
        open_options = df_open[df_open['TradeType'].isin(['CC', 'CSP'])].copy()
        open_options['Status'] = 'Open'
        all_options.append(open_options)
    
    # Add closed positions
    if df_trades is not None and not df_trades.empty:
        closed_options = df_trades[
            (df_trades['TradeType'].isin(['CC', 'CSP'])) &
            (df_trades['Status'].str.lower() == 'closed')
        ].copy()
        all_options.append(closed_options)
    
    if not all_options:
        st.warning("No option positions found")
        return
    
    # Combine all options
    df_all_options = pd.concat(all_options, ignore_index=True)
    
    # Convert expiry and open date to datetime
    df_all_options['Expiry_Date'] = pd.to_datetime(df_all_options['Expiry_Date'], errors='coerce')
    df_all_options['Date_open'] = pd.to_datetime(df_all_options.get('Date_open'), errors='coerce')
    
    # Filter to 5 years range (by expiry)
    df_all_options = df_all_options[
        (df_all_options['Expiry_Date'].dt.date >= start_date) &
        (df_all_options['Expiry_Date'].dt.date <= end_date)
    ].copy()
    
    if df_all_options.empty:
        st.warning("No option positions in the 5-year range")
        return
    
    # Calculate Friday date for each expiry (options expire on Friday)
    def get_friday_of_week(exp_date):
        if pd.isna(exp_date):
            return pd.NaT
        weekday = exp_date.weekday()  # Monday=0, Sunday=6
        if weekday <= 4:  # Monday to Friday
            return exp_date + timedelta(days=(4 - weekday))
        else:  # Saturday or Sunday - get previous Friday
            return exp_date - timedelta(days=(weekday - 4))
    
    df_all_options['Expiry_Friday'] = df_all_options['Expiry_Date'].apply(get_friday_of_week)
    df_all_options['Friday_Date'] = df_all_options['Expiry_Friday'].dt.strftime('%Y-%m-%d')
    
    # Expiry-based: Year, Month, Week (for ladder and expiry-month filter)
    df_all_options['Year'] = df_all_options['Expiry_Friday'].dt.year
    df_all_options['Month'] = df_all_options['Expiry_Friday'].dt.month
    df_all_options['Week'] = df_all_options['Expiry_Friday'].dt.isocalendar().week
    df_all_options['Month_Name'] = df_all_options['Expiry_Friday'].dt.strftime('%B')
    df_all_options['Year_Month_Str'] = df_all_options['Year'].astype(str) + '-' + df_all_options['Month'].astype(str).str.zfill(2)
    
    # Open-date-based: for "filter by open month"
    df_all_options['Open_Year'] = df_all_options['Date_open'].dt.year
    df_all_options['Open_Month'] = df_all_options['Date_open'].dt.month
    df_all_options['Open_Year_Month_Str'] = df_all_options['Open_Year'].astype(str) + '-' + df_all_options['Open_Month'].astype(str).str.zfill(2)
    
    # Calculate premium
    df_all_options['OptPremium'] = pd.to_numeric(df_all_options['OptPremium'], errors='coerce').fillna(0)
    df_all_options['Quantity'] = pd.to_numeric(df_all_options['Quantity'], errors='coerce').fillna(0)
    df_all_options['Total_Premium'] = df_all_options['OptPremium'] * df_all_options['Quantity'] * 100
    
    # Calculate Actual_Profit_(USD) for closed positions - ALWAYS use Actual_Profit_(USD) for closed CC/CSP
    if 'Actual_Profit_(USD)' in df_all_options.columns:
        df_all_options['Actual_Profit_(USD)'] = pd.to_numeric(df_all_options['Actual_Profit_(USD)'], errors='coerce').fillna(0)
    else:
        df_all_options['Actual_Profit_(USD)'] = 0.0
    
    # For closed positions, ALWAYS use Actual_Profit_(USD) (even if negative or 0) - this includes BTC losses
    # For open positions, use Total_Premium (expected premium)
    df_all_options['Premium_Collected'] = df_all_options.apply(
        lambda row: row['Actual_Profit_(USD)'] if row['Status'] == 'Closed' and row['TradeType'] in ['CC', 'CSP'] else (row['Total_Premium'] if row['Status'] == 'Open' else 0.0),
        axis=1
    )
    
    # ===== FILTERS =====
    st.markdown("### 🔍 Filters")
    month_filter_basis = st.radio(
        "**Month filter refers to:**",
        ["Expiry month", "Open month"],
        horizontal=True,
        key="ladder_month_basis",
        help="Expiry month = when the option expires; Open month = when the trade was opened."
    )
    use_expiry_for_filter = (month_filter_basis == "Expiry month")
    year_col = 'Year' if use_expiry_for_filter else 'Open_Year'
    month_col = 'Month' if use_expiry_for_filter else 'Open_Month'
    
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    available_years = sorted(df_all_options[year_col].dropna().unique().tolist(), reverse=True)
    available_years = [y for y in available_years if y is not None and not (isinstance(y, float) and pd.isna(y))]
    
    with filter_col1:
        selected_year = st.selectbox("**Filter by Year**", ["All"] + available_years, key="ladder_year_filter")
    
    with filter_col2:
        month_names = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
                      7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"}
        if selected_year != "All":
            yr = int(selected_year) if isinstance(selected_year, str) and selected_year != "All" else selected_year
            available_months = sorted(df_all_options[df_all_options[year_col] == yr][month_col].dropna().unique().tolist(), reverse=True)
        else:
            available_months = sorted(df_all_options[month_col].dropna().unique().tolist(), reverse=True)
        available_months = [m for m in available_months if m is not None and not (isinstance(m, float) and pd.isna(m))]
        month_options = ["All"] + [f"{month_names.get(int(m), str(m))} ({m})" for m in available_months]
        selected_month_str = st.selectbox("**Filter by Month**", month_options, key="ladder_month_filter")
        selected_month = None if selected_month_str == "All" else int(selected_month_str.split("(")[1].split(")")[0])
    
    with filter_col3:
        available_tickers = sorted(df_all_options['Ticker'].dropna().unique().tolist())
        selected_ticker = st.selectbox("**Filter by Ticker**", ["All"] + available_tickers, key="ladder_ticker_filter")
    
    # Apply filters (year and month use chosen basis: expiry or open)
    df_filtered = df_all_options.copy()
    if selected_year != "All":
        yr = int(selected_year) if isinstance(selected_year, str) else selected_year
        df_filtered = df_filtered[df_filtered[year_col] == yr]
    if selected_month is not None:
        df_filtered = df_filtered[df_filtered[month_col] == selected_month]
    if selected_ticker != "All":
        df_filtered = df_filtered[df_filtered['Ticker'] == selected_ticker]
    
    if df_filtered.empty:
        st.warning("No positions match the selected filters")
        return
    
    # ===== SUMMARY BY MONTH =====
    st.markdown("### 📊 Summary by Month")
    st.caption("Totals grouped by month. Use the filter above to restrict by year/ticker; month filter applies to " + ("expiry" if use_expiry_for_filter else "open") + " date.")
    
    # By expiry month
    def agg_expiry(g):
        open_mask = g['Status'] == 'Open'
        closed_mask = g['Status'] == 'Closed'
        return pd.Series({
            'Contracts': g['Quantity'].sum(),
            'Premium_Expected': g.loc[open_mask, 'Total_Premium'].sum(),
            'Premium_Collected': g.loc[closed_mask, 'Premium_Collected'].sum()
        })
    summary_expiry = df_filtered.groupby('Year_Month_Str').apply(agg_expiry).reset_index()
    summary_expiry = summary_expiry.sort_values('Year_Month_Str', ascending=False)
    summary_expiry['Premium_Expected'] = summary_expiry['Premium_Expected'].fillna(0)
    summary_expiry['Premium_Collected'] = summary_expiry['Premium_Collected'].fillna(0)
    summary_expiry['Contracts'] = summary_expiry['Contracts'].astype(int)
    st.markdown("**By expiry month**")
    st.dataframe(
        summary_expiry.style.format({'Premium_Expected': '${:,.2f}', 'Premium_Collected': '${:,.2f}'}, subset=['Premium_Expected', 'Premium_Collected']),
        use_container_width=True,
        hide_index=True
    )
    
    # By open month (only if we have open dates)
    if df_filtered['Date_open'].notna().any():
        def agg_open(g):
            open_mask = g['Status'] == 'Open'
            closed_mask = g['Status'] == 'Closed'
            return pd.Series({
                'Contracts': g['Quantity'].sum(),
                'Premium_Expected': g.loc[open_mask, 'Total_Premium'].sum(),
                'Premium_Collected': g.loc[closed_mask, 'Premium_Collected'].sum()
            })
        summary_open = df_filtered[df_filtered['Open_Year_Month_Str'].notna()].groupby('Open_Year_Month_Str').apply(agg_open).reset_index()
        summary_open = summary_open.rename(columns={'Open_Year_Month_Str': 'Year_Month_Str'})
        summary_open = summary_open.sort_values('Year_Month_Str', ascending=False)
        summary_open['Premium_Expected'] = summary_open['Premium_Expected'].fillna(0)
        summary_open['Premium_Collected'] = summary_open['Premium_Collected'].fillna(0)
        summary_open['Contracts'] = summary_open['Contracts'].astype(int)
        st.markdown("**By open month**")
        st.dataframe(
            summary_open.style.format({'Premium_Expected': '${:,.2f}', 'Premium_Collected': '${:,.2f}'}, subset=['Premium_Expected', 'Premium_Collected']),
            use_container_width=True,
            hide_index=True
        )
    
    st.divider()
    
    # ===== 12-MONTH PREMIUM COLLECTED TREND =====
    st.markdown("### 📊 Premium Collected Trend (Last 12 Months)")
    
    # View filter (aggregated vs by ticker)
    view_mode = st.radio(
        "**View Mode:**",
        ["Aggregated", "By Ticker"],
        horizontal=True,
        key="trend_view_mode"
    )
    
    # Get last 12 months of data - only closed CC/CSP trades
    twelve_months_ago = today - timedelta(days=365)
    df_trend = df_all_options[
        (df_all_options['Expiry_Date'].dt.date >= twelve_months_ago) &
        (df_all_options['Status'] == 'Closed') &
        (df_all_options['TradeType'].isin(['CC', 'CSP']))
    ].copy()
    
    # Filter by ticker for trend if selected
    if selected_ticker != "All":
        df_trend = df_trend[df_trend['Ticker'] == selected_ticker]
    
    # Debug: Show what's being counted
    if st.checkbox("🔍 Show Debug Info", key="ladder_debug"):
        st.markdown("#### Debug: Trades Included in Trend")
        debug_cols = ['TradeID', 'Ticker', 'TradeType', 'Status', 'Expiry_Date', 'Actual_Profit_(USD)', 'Premium_Collected', 'OptPremium', 'Quantity']
        debug_cols = [c for c in debug_cols if c in df_trend.columns]
        st.dataframe(
            df_trend[debug_cols].sort_values('Expiry_Date', ascending=False),
            use_container_width=True,
            hide_index=True
        )
        st.caption(f"Total trades: {len(df_trend)} | Total Premium Collected: ${df_trend['Premium_Collected'].sum():,.2f}")
    
    if not df_trend.empty:
        # Group by month and ticker (if by ticker view) or just month (if aggregated)
        df_trend['Year_Month'] = df_trend['Expiry_Date'].dt.to_period('M')
        df_trend['Year_Month_Str'] = df_trend['Year_Month'].astype(str)
        
        if view_mode == "By Ticker":
            # Group by month and ticker for stacked chart
            monthly_premium_by_ticker = df_trend.groupby(['Year_Month_Str', 'Ticker']).agg({
                'Premium_Collected': 'sum'
            }).reset_index()
            monthly_premium_by_ticker = monthly_premium_by_ticker.sort_values('Year_Month_Str')
            
            # Pivot for stacked bar chart
            monthly_pivot = monthly_premium_by_ticker.pivot(
                index='Year_Month_Str',
                columns='Ticker',
                values='Premium_Collected'
            ).fillna(0)
            
            # Display stacked column chart
            import plotly.express as px
            fig = px.bar(
                monthly_premium_by_ticker,
                x='Year_Month_Str',
                y='Premium_Collected',
                color='Ticker',
                labels={'Year_Month_Str': 'Month', 'Premium_Collected': 'Premium Collected ($)', 'Ticker': 'Ticker'},
                title=f"Premium Collected by Month (Stacked by Ticker) {'- ' + selected_ticker if selected_ticker != 'All' else '- All Tickers'}",
                barmode='stack'
            )
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show summary stats by ticker
            st.markdown("#### 📊 Summary by Ticker (Last 12 Months)")
            ticker_totals = df_trend.groupby('Ticker').agg({
                'Premium_Collected': 'sum'
            }).reset_index()
            ticker_totals = ticker_totals.sort_values('Premium_Collected', ascending=False)
            ticker_totals['Premium_Collected'] = ticker_totals['Premium_Collected'].apply(lambda x: f"${x:,.2f}")
            ticker_totals.columns = ['Ticker', 'Total Premium (12M)']
            st.dataframe(ticker_totals, use_container_width=True, hide_index=True)
            
        else:
            # Aggregated view - group by month only
            monthly_premium = df_trend.groupby('Year_Month_Str').agg({
                'Premium_Collected': 'sum'
            }).reset_index()
            monthly_premium = monthly_premium.sort_values('Year_Month_Str')
            
            # Display simple bar chart
            import plotly.express as px
            fig = px.bar(
                monthly_premium,
                x='Year_Month_Str',
                y='Premium_Collected',
                labels={'Year_Month_Str': 'Month', 'Premium_Collected': 'Premium Collected ($)'},
                title=f"Premium Collected by Month (Aggregated) {'- ' + selected_ticker if selected_ticker != 'All' else '- All Tickers'}"
            )
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        
        # Show summary stats (same for both views)
        total_12m = df_trend['Premium_Collected'].sum()
        avg_monthly = df_trend.groupby('Year_Month_Str')['Premium_Collected'].sum().mean()
        best_month = df_trend.groupby('Year_Month_Str')['Premium_Collected'].sum().max()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Premium (12M)", f"${total_12m:,.2f}")
        with col2:
            st.metric("Average Monthly", f"${avg_monthly:,.2f}")
        with col3:
            st.metric("Best Month", f"${best_month:,.2f}")
        
        # Show comparison with Dashboard calculation
        st.markdown("#### 📊 Comparison with Dashboard")
        st.caption("Note: Dashboard MTD/YTD filters by expiry date in the period. This trend shows all closed trades in last 12 months.")
        
        # Calculate total all-time premium collected (for comparison)
        all_closed = df_all_options[
            (df_all_options['Status'] == 'Closed') &
            (df_all_options['TradeType'].isin(['CC', 'CSP']))
        ].copy()
        total_all_time = all_closed['Premium_Collected'].sum()
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total All-Time Premium Collected", f"${total_all_time:,.2f}")
        with col2:
            st.metric("Last 12 Months Premium", f"${total_12m:,.2f}")
    else:
        st.info("No premium collected data for the last 12 months")
    
    st.divider()
    
    # ===== PREMIUM EXPECTED (Next 2 Months) =====
    st.markdown("### 💰 Premium Expected (Next 2 Months)")
    
    # Get open positions expiring in next 2 months
    two_months_later = today + timedelta(days=60)
    df_expected = df_all_options[
        (df_all_options['Expiry_Date'].dt.date >= today) &
        (df_all_options['Expiry_Date'].dt.date <= two_months_later) &
        (df_all_options['Status'] == 'Open') &
        (df_all_options['TradeType'].isin(['CC', 'CSP']))
    ].copy()
    
    if not df_expected.empty:
        # Group by Friday date (week) and calculate total premium expected
        df_expected['Expiry_Friday'] = df_expected['Expiry_Date'].apply(get_friday_of_week)
        df_expected['Friday_Date'] = df_expected['Expiry_Friday'].dt.strftime('%Y-%m-%d')
        
        # Calculate premium expected per week
        weekly_expected = df_expected.groupby('Friday_Date').agg({
            'Total_Premium': 'sum'
        }).reset_index()
        weekly_expected.columns = ['Week', 'Premium Expected']
        weekly_expected = weekly_expected.sort_values('Week')
        
        # Also group by ticker for stacked view option
        weekly_expected_by_ticker = df_expected.groupby(['Friday_Date', 'Ticker']).agg({
            'Total_Premium': 'sum'
        }).reset_index()
        weekly_expected_by_ticker.columns = ['Week', 'Ticker', 'Premium Expected']
        weekly_expected_by_ticker = weekly_expected_by_ticker.sort_values('Week')
        
        # View mode for expected premium
        expected_view_mode = st.radio(
            "**View Mode:**",
            ["Aggregated", "By Ticker"],
            horizontal=True,
            key="expected_premium_view_mode"
        )
        
        import plotly.express as px
        
        if expected_view_mode == "By Ticker":
            # Stacked column chart by ticker
            fig_expected = px.bar(
                weekly_expected_by_ticker,
                x='Week',
                y='Premium Expected',
                color='Ticker',
                labels={'Week': 'Expiry Week (Friday)', 'Premium Expected': 'Premium Expected ($)', 'Ticker': 'Ticker'},
                title="Premium Expected by Week (Next 2 Months) - By Ticker",
                barmode='stack'
            )
            fig_expected.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig_expected, use_container_width=True)
            
            # Summary by ticker
            ticker_expected_summary = df_expected.groupby('Ticker').agg({
                'Total_Premium': 'sum'
            }).reset_index()
            ticker_expected_summary.columns = ['Ticker', 'Total Expected (2M)']
            ticker_expected_summary = ticker_expected_summary.sort_values('Total Expected (2M)', ascending=False)
            ticker_expected_summary['Total Expected (2M)'] = ticker_expected_summary['Total Expected (2M)'].apply(lambda x: f"${x:,.2f}")
            st.markdown("#### 📊 Summary by Ticker (Next 2 Months)")
            st.dataframe(ticker_expected_summary, use_container_width=True, hide_index=True)
        else:
            # Aggregated view
            fig_expected = px.bar(
                weekly_expected,
                x='Week',
                y='Premium Expected',
                labels={'Week': 'Expiry Week (Friday)', 'Premium Expected': 'Premium Expected ($)'},
                title="Premium Expected by Week (Next 2 Months) - Aggregated"
            )
            fig_expected.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig_expected, use_container_width=True)
        
        # Summary metrics
        total_expected_2m = df_expected['Total_Premium'].sum()
        avg_weekly_expected = weekly_expected['Premium Expected'].mean()
        max_weekly_expected = weekly_expected['Premium Expected'].max()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Expected (2M)", f"${total_expected_2m:,.2f}")
        with col2:
            st.metric("Average Weekly", f"${avg_weekly_expected:,.2f}")
        with col3:
            st.metric("Peak Week", f"${max_weekly_expected:,.2f}")
    else:
        st.info("No open positions expiring in the next 2 months")
    
    st.divider()
    
    # ===== EXPIRY LADDER BY WEEK =====
    st.markdown("### 📅 Expiry Ladder by Week")
    st.caption("Grouped by expiry week (Friday). Use filters at the top to restrict by year, month (expiry or open), and ticker.")
    
    if df_filtered.empty:
        st.warning("No positions match the selected filters")
        return
    
    # Sort by Year, Month, Week, Ticker, TradeType (descending - latest date first)
    df_filtered = df_filtered.sort_values(['Year', 'Month', 'Week', 'Ticker', 'TradeType'], ascending=[False, False, False, True, True])
    
    # Group by Friday date (week) - sort groups by date descending (latest first)
    friday_groups = df_filtered.groupby('Friday_Date')
    
    # Sort Friday dates in descending order (latest date first)
    sorted_friday_dates = sorted(friday_groups.groups.keys(), reverse=True)
    
    # Display each week group with details (latest date first)
    for friday_date in sorted_friday_dates:
        week_data = friday_groups.get_group(friday_date)
        # Sort within week by Ticker, TradeType
        week_data_sorted = week_data.sort_values(['Ticker', 'TradeType'])
        
        # Calculate summary for this week
        total_contracts = int(week_data_sorted['Quantity'].sum())
        total_premium_expected = week_data_sorted[week_data_sorted['Status'] == 'Open']['Total_Premium'].sum()
        total_premium_collected = week_data_sorted[week_data_sorted['Status'] == 'Closed']['Premium_Collected'].sum()
        
        # Calculate summary by ticker
        ticker_summary_data = []
        for ticker in week_data_sorted['Ticker'].unique():
            ticker_data = week_data_sorted[week_data_sorted['Ticker'] == ticker]
            contracts = int(ticker_data['Quantity'].sum())
            expected = ticker_data[ticker_data['Status'] == 'Open']['Total_Premium'].sum()
            collected = ticker_data[ticker_data['Status'] == 'Closed']['Premium_Collected'].sum()
            ticker_summary_data.append({
                'Ticker': ticker,
                'Contracts': contracts,
                'Expected': expected,
                'Collected': collected
            })
        ticker_summary = pd.DataFrame(ticker_summary_data)
        
        # Display week header with summary
        with st.expander(f"📅 **{friday_date}** - {total_contracts} contracts | Expected: ${total_premium_expected:,.2f} | Collected: ${total_premium_collected:,.2f}"):
            # Show summary by ticker
            if not ticker_summary.empty:
                st.markdown("#### Summary by Ticker")
                ticker_display = ticker_summary.copy()
                ticker_display['Contracts'] = ticker_display['Contracts'].apply(lambda x: f"{int(x)}")
                ticker_display['Expected'] = ticker_display['Expected'].apply(lambda x: f"${x:,.2f}")
                ticker_display['Collected'] = ticker_display['Collected'].apply(lambda x: f"${x:,.2f}")
                st.dataframe(
                    ticker_display,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                        "Contracts": st.column_config.TextColumn("Contracts", width="small"),
                        "Expected": st.column_config.TextColumn("Expected", width="medium"),
                        "Collected": st.column_config.TextColumn("Collected", width="medium")
                    }
                )
                st.divider()
            
            # Prepare display data
            display_data = []
            for _, row in week_data_sorted.iterrows():
                display_data.append({
                    'TradeID': row.get('TradeID', 'N/A'),
                    'Ticker': row.get('Ticker', 'N/A'),
                    'Type': row.get('TradeType', 'N/A'),
                    'Status': row.get('Status', 'N/A'),
                    'Strike': f"${row.get('Option_Strike_Price_(USD)', 0):.2f}",
                    'Quantity': int(row.get('Quantity', 0)),
                    'Premium': f"${row.get('OptPremium', 0):.2f}",
                    'Total Premium': f"${row.get('Total_Premium', 0):,.2f}",
                    'Premium Collected': f"${row.get('Premium_Collected', 0):,.2f}" if row.get('Status') == 'Closed' else "N/A",
                    'Expiry Date': row['Expiry_Date'].strftime('%Y-%m-%d') if pd.notna(row['Expiry_Date']) else 'N/A'
                })
            
            df_display = pd.DataFrame(display_data)
            
            st.markdown("#### Detailed Positions")
            st.dataframe(
                df_display,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "TradeID": st.column_config.TextColumn("TradeID", width="small"),
                    "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                    "Type": st.column_config.TextColumn("Type", width="small"),
                    "Status": st.column_config.TextColumn("Status", width="small"),
                    "Strike": st.column_config.TextColumn("Strike", width="small"),
                    "Quantity": st.column_config.NumberColumn("Quantity", width="small"),
                    "Premium": st.column_config.TextColumn("Premium", width="small"),
                    "Total Premium": st.column_config.TextColumn("Total Premium", width="medium"),
                    "Premium Collected": st.column_config.TextColumn("Premium Collected", width="medium"),
                    "Expiry Date": st.column_config.TextColumn("Expiry Date", width="small")
                }
            )


# ============================================================
# ANALYTICS PAGE
# ============================================================
def render_performance():
    """Render performance page with P&L breakdown by ticker"""
    st.title("📊 Performance")
    
    df_trades = st.session_state.df_trades
    df_open = st.session_state.df_open
    
    if df_trades is None or df_trades.empty:
        st.warning("No trades found")
        return
    
    # Filter by strategy if selected
    strategy_filter = st.session_state.get('strategy_filter', 'All')
    if strategy_filter != 'All':
        portfolio = st.session_state.get('current_portfolio', 'Income Wheel')
        pmcc_tickers = get_pmcc_tickers(portfolio)
        pmcc_tickers_set = set(pmcc_tickers) if pmcc_tickers else set()
        
        if strategy_filter == 'PMCC':
            pmcc_mask = (
                df_trades['Ticker'].isin(pmcc_tickers_set) |
                (df_trades.get('StrategyType', '') == 'PMCC') |
                (df_trades['TradeType'] == 'LEAP')
            )
            df_trades = df_trades[pmcc_mask].copy()
            if df_open is not None and not df_open.empty:
                pmcc_mask_open = (
                    df_open['Ticker'].isin(pmcc_tickers_set) |
                    (df_open.get('StrategyType', '') == 'PMCC') |
                    (df_open['TradeType'] == 'LEAP')
                )
                df_open = df_open[pmcc_mask_open].copy()
        elif strategy_filter == 'WHEEL':
            wheel_mask = (
                ~df_trades['Ticker'].isin(pmcc_tickers_set) &
                (
                    (df_trades.get('StrategyType', '') == 'WHEEL') |
                    (df_trades.get('StrategyType', '').isna()) |
                    (df_trades.get('StrategyType', '') == '')
                ) &
                (df_trades['TradeType'] != 'LEAP')
            )
            df_trades = df_trades[wheel_mask].copy()
            if df_open is not None and not df_open.empty:
                wheel_mask_open = (
                    ~df_open['Ticker'].isin(pmcc_tickers_set) &
                    (
                        (df_open.get('StrategyType', '') == 'WHEEL') |
                        (df_open.get('StrategyType', '').isna()) |
                        (df_open.get('StrategyType', '') == '')
                    ) &
                    (df_open['TradeType'] != 'LEAP')
                )
                df_open = df_open[wheel_mask_open].copy()
        elif strategy_filter == 'ActiveCore':
            df_trades = df_trades[df_trades.get('StrategyType', '') == 'ActiveCore'].copy()
            if df_open is not None and not df_open.empty:
                df_open = df_open[df_open.get('StrategyType', '') == 'ActiveCore'].copy()

        if df_trades.empty:
            st.info(f"ℹ️ No trades found for {strategy_filter} strategy.")
            return
    
    # Get live prices
    from price_feed import get_cached_prices
    all_tickers = df_trades['Ticker'].unique().tolist()
    if df_open is not None and not df_open.empty:
        all_tickers.extend(df_open['Ticker'].unique().tolist())
    all_tickers = sorted(list(set(all_tickers)))
    live_prices = get_cached_prices(all_tickers)
    
    # Load persisted values
    from persistence import get_stock_average_prices, save_stock_average_prices, get_spy_leap_pl, save_spy_leap_pl
    portfolio = st.session_state.get('current_portfolio', 'Income Wheel')
    stock_avg_prices = get_stock_average_prices(portfolio)
    spy_leap_pl = get_spy_leap_pl(portfolio)
    
    # ===== PREMIUM COLLECTED BY TICKER =====
    st.subheader("💰 Realized P&L by Ticker")

    # Get all closed trades (CC, CSP, LEAP, STOCK) — include LEAP rolls/closes
    df_closed_options = df_trades[
        (df_trades['Status'].str.lower() == 'closed') &
        (df_trades['TradeType'].isin(['CC', 'CSP', 'LEAP', 'STOCK']))
    ].copy()

    # Use Actual_Profit_(USD) for all closed trades
    if 'Actual_Profit_(USD)' in df_closed_options.columns:
        df_closed_options['Actual_Profit_(USD)'] = pd.to_numeric(df_closed_options['Actual_Profit_(USD)'], errors='coerce').fillna(0)
        # Use Actual_Profit_(USD) directly (even if 0, as it represents the actual P&L)
        df_closed_options['Premium_Collected'] = df_closed_options['Actual_Profit_(USD)']
    else:
        # Fallback: if column doesn't exist, calculate from OptPremium (shouldn't happen in normal operation)
        df_closed_options['OptPremium'] = pd.to_numeric(df_closed_options['OptPremium'], errors='coerce').fillna(0)
        df_closed_options['Quantity'] = pd.to_numeric(df_closed_options['Quantity'], errors='coerce').fillna(0)
        df_closed_options['Premium_Collected'] = df_closed_options['OptPremium'] * 100 * df_closed_options['Quantity']
        st.warning("⚠️ Actual_Profit_(USD) column not found. Using OptPremium calculation as fallback.")
    
    # Group by ticker
    premium_by_ticker = df_closed_options.groupby('Ticker').agg({
        'Premium_Collected': 'sum'
    }).reset_index()
    premium_by_ticker.columns = ['Ticker', 'Premium_Collected']
    
    # ===== STOCK POSITIONS =====
    st.subheader("📈 Stock Positions (Live Prices)")
    
    # Get stock positions (STOCK and LEAP)
    stock_positions = []
    if df_open is not None and not df_open.empty:
        stock_df = df_open[df_open['TradeType'].isin(['STOCK', 'LEAP'])].copy()
        for _, row in stock_df.iterrows():
            ticker = row['Ticker']
            trade_type = row['TradeType']
            quantity = pd.to_numeric(row.get('Open_lots', row.get('Quantity', 0)), errors='coerce') or 0
            if trade_type == 'LEAP':
                quantity = abs(quantity)  # LEAPs are long positions
            
            stock_positions.append({
                'Ticker': ticker,
                'Type': trade_type,
                'Quantity': quantity,
                'Live_Price': live_prices.get(ticker, 0.0)
            })
    
    # Create performance dataframe
    all_tickers_perf = sorted(list(set(premium_by_ticker['Ticker'].tolist() + [p['Ticker'] for p in stock_positions])))
    
    performance_data = []
    for ticker in all_tickers_perf:
        # Premium collected
        premium = premium_by_ticker[premium_by_ticker['Ticker'] == ticker]['Premium_Collected'].sum()
        
        # Stock position info
        ticker_stocks = [p for p in stock_positions if p['Ticker'] == ticker]
        total_shares = sum(p['Quantity'] for p in ticker_stocks)
        live_price = ticker_stocks[0]['Live_Price'] if ticker_stocks else live_prices.get(ticker, 0.0)
        
        # Get average price (from persistence or calculate)
        avg_price = stock_avg_prices.get(ticker, 0.0)
        
        # Calculate stock P&L
        # Special handling for SPY LEAP P&L
        if ticker == 'SPY':
            # Check if there are LEAPs
            spy_leaps = [p for p in ticker_stocks if p['Type'] == 'LEAP']
            spy_regular = [p for p in ticker_stocks if p['Type'] == 'STOCK']
            
            if spy_leaps and spy_leap_pl != 0:
                # Use manual LEAP P&L if entered
                leap_pl = spy_leap_pl
            elif spy_leaps and avg_price > 0:
                # Calculate LEAP P&L from average price if available
                spy_leap_shares = sum(p['Quantity'] for p in spy_leaps)
                leap_pl = (live_price - avg_price) * spy_leap_shares
            else:
                leap_pl = 0.0
            
            # Calculate regular stock P&L
            if spy_regular and avg_price > 0:
                spy_regular_shares = sum(p['Quantity'] for p in spy_regular)
                regular_pl = (live_price - avg_price) * spy_regular_shares
            else:
                regular_pl = 0.0
            
            stock_pl = leap_pl + regular_pl
        elif avg_price > 0 and total_shares > 0:
            stock_pl = (live_price - avg_price) * total_shares
        else:
            stock_pl = 0.0
        
        # Total P&L
        total_pl = premium + stock_pl
        
        performance_data.append({
            'Ticker': ticker,
            'Premium_Collected': premium,
            'Stock_Shares': total_shares,
            'Avg_Price': avg_price,
            'Live_Price': live_price,
            'Stock_PL': stock_pl,
            'Total_PL': total_pl
        })
    
    df_performance = pd.DataFrame(performance_data)
    
    # ===== USER INPUTS FOR STOCK AVERAGE PRICES =====
    st.markdown("#### ⚙️ Stock Average Prices (Manual Entry)")
    st.caption("Enter average cost basis for each ticker (from broker). Values are persisted.")
    
    # Create editable table for average prices
    avg_price_data = []
    for ticker in sorted(all_tickers_perf):
        current_avg = stock_avg_prices.get(ticker, 0.0)
        avg_price_data.append({
            'Ticker': ticker,
            'Average_Price': current_avg
        })
    
    df_avg_prices = pd.DataFrame(avg_price_data)
    
    edited_avg_prices = st.data_editor(
        df_avg_prices,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Ticker": st.column_config.TextColumn("Ticker", width="small", disabled=True),
            "Average_Price": st.column_config.NumberColumn("Average Price ($)", width="medium", min_value=0.0, step=0.01)
        },
        key="avg_prices_editor"
    )
    
    # Save average prices on change
    if not edited_avg_prices.equals(df_avg_prices):
        new_avg_prices = {}
        for _, row in edited_avg_prices.iterrows():
            if row['Average_Price'] > 0:
                new_avg_prices[row['Ticker']] = float(row['Average_Price'])
        save_stock_average_prices(new_avg_prices, portfolio)
        stock_avg_prices = new_avg_prices
        st.toast("Average prices saved. Form cleared.", icon="✅")
        st.success("✅ Average prices saved!")
        st.rerun()
    
    # ===== SPY LEAP P&L MANUAL ENTRY =====
    st.markdown("#### 🎯 SPY LEAP P&L (Manual Entry)")
    st.caption("Enter SPY LEAP P&L manually (non-linear calculations from broker). Value is persisted.")
    
    col1, col2 = st.columns([2, 1])
    with col1:
        new_spy_leap_pl = st.number_input(
            "SPY LEAP P&L ($)",
            value=spy_leap_pl,
            step=0.01,
            format="%.2f",
            key="spy_leap_pl_input"
        )
    with col2:
        if st.button("💾 Save SPY LEAP P&L", key="save_spy_leap"):
            save_spy_leap_pl(new_spy_leap_pl, portfolio)
            st.toast("SPY LEAP P&L saved.", icon="✅")
            st.success("✅ SPY LEAP P&L saved!")
            st.rerun()
    
    st.divider()
    
    # ===== PERFORMANCE TABLE =====
    st.markdown("#### 📊 Performance Breakdown by Ticker")
    
    # Recalculate with updated values
    for idx, row in df_performance.iterrows():
        ticker = row['Ticker']
        avg_price = stock_avg_prices.get(ticker, 0.0)
        ticker_stocks = [p for p in stock_positions if p['Ticker'] == ticker]
        live_price = row['Live_Price']
        
        # Calculate stock P&L
        if ticker == 'SPY':
            # Check if there are LEAPs
            spy_leaps = [p for p in ticker_stocks if p['Type'] == 'LEAP']
            spy_regular = [p for p in ticker_stocks if p['Type'] == 'STOCK']
            
            if spy_leaps and spy_leap_pl != 0:
                # Use manual LEAP P&L if entered
                leap_pl = spy_leap_pl
            elif spy_leaps and avg_price > 0:
                # Calculate LEAP P&L from average price if available
                spy_leap_shares = sum(p['Quantity'] for p in spy_leaps)
                leap_pl = (live_price - avg_price) * spy_leap_shares
            else:
                leap_pl = 0.0
            
            # Calculate regular stock P&L
            if spy_regular and avg_price > 0:
                spy_regular_shares = sum(p['Quantity'] for p in spy_regular)
                regular_pl = (live_price - avg_price) * spy_regular_shares
            else:
                regular_pl = 0.0
            
            stock_pl = leap_pl + regular_pl
        elif avg_price > 0:
            total_shares = sum(p['Quantity'] for p in ticker_stocks)
            if total_shares > 0:
                stock_pl = (live_price - avg_price) * total_shares
            else:
                stock_pl = 0.0
        else:
            stock_pl = 0.0
        
        df_performance.at[idx, 'Avg_Price'] = avg_price
        df_performance.at[idx, 'Stock_PL'] = stock_pl
        df_performance.at[idx, 'Total_PL'] = row['Premium_Collected'] + stock_pl
    
    # Format for display (guard: empty or missing columns e.g. Active Core without options data)
    df_display = df_performance.copy()
    if df_display.empty or 'Premium_Collected' not in df_display.columns:
        df_display = pd.DataFrame(columns=['Ticker', 'Premium Collected', 'Stock Shares', 'Avg Price', 'Live Price', 'Stock P&L', 'Total P&L'])
        st.dataframe(df_display, use_container_width=True, hide_index=True)
        total_premium = total_stock_pl = total_pl = 0.0
    else:
        df_display['Premium_Collected'] = df_display['Premium_Collected'].apply(lambda x: f"${x:,.2f}")
        df_display['Stock_Shares'] = df_display['Stock_Shares'].apply(lambda x: f"{int(x):,}" if x > 0 else "0")
        df_display['Avg_Price'] = df_display['Avg_Price'].apply(lambda x: f"${x:.2f}" if x > 0 else "N/A")
        df_display['Live_Price'] = df_display['Live_Price'].apply(lambda x: f"${x:.2f}" if x > 0 else "N/A")
        df_display['Stock_PL'] = df_display['Stock_PL'].apply(lambda x: f"${x:,.2f}")
        df_display['Total_PL'] = df_display['Total_PL'].apply(lambda x: f"${x:,.2f}")
        df_display.columns = ['Ticker', 'Premium Collected', 'Stock Shares', 'Avg Price', 'Live Price', 'Stock P&L', 'Total P&L']
        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                "Premium Collected": st.column_config.TextColumn("Premium Collected", width="medium"),
                "Stock Shares": st.column_config.TextColumn("Stock Shares", width="small"),
                "Avg Price": st.column_config.TextColumn("Avg Price", width="small"),
                "Live Price": st.column_config.TextColumn("Live Price", width="small"),
                "Stock P&L": st.column_config.TextColumn("Stock P&L", width="medium"),
                "Total P&L": st.column_config.TextColumn("Total P&L", width="medium")
            }
        )
        total_premium = df_performance['Premium_Collected'].sum()
        total_stock_pl = df_performance['Stock_PL'].sum()
        total_pl = df_performance['Total_PL'].sum()
    
    # ===== PORTFOLIO TOTAL =====
    st.divider()
    st.markdown("#### 🎯 Portfolio Total P&L")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Premium Collected", f"${total_premium:,.2f}")
    with col2:
        st.metric("Total Stock P&L", f"${total_stock_pl:,.2f}")
    with col3:
        st.metric("**Total Portfolio P&L**", f"**${total_pl:,.2f}**",
                 delta=f"${total_pl:,.2f}" if total_pl != 0 else None)

    # ══════════════════════════════════════════════════════════
    # PORTFOLIO GROWTH CHARTS
    # ══════════════════════════════════════════════════════════
    st.divider()
    st.subheader("📈 Portfolio Growth")

    df_closed = df_trades[
        (df_trades['Status'].str.lower() == 'closed') &
        (df_trades['TradeType'].isin(['CC', 'CSP', 'LEAP', 'STOCK']))
    ].copy()

    if not df_closed.empty and 'Date_closed' in df_closed.columns:
        df_closed['Date_closed'] = pd.to_datetime(df_closed['Date_closed'], errors='coerce')
        df_closed['Actual_Profit_(USD)'] = pd.to_numeric(df_closed['Actual_Profit_(USD)'], errors='coerce').fillna(0)
        df_closed = df_closed[df_closed['Date_closed'].notna()].sort_values('Date_closed')

        if not df_closed.empty:
            # ── Time frame selector ──────────────────────────────
            _min_date = df_closed['Date_closed'].min().date()
            _max_date = df_closed['Date_closed'].max().date()
            _today = date.today()

            _tf_col1, _tf_col2, _tf_col3 = st.columns([2, 2, 3])
            with _tf_col1:
                _tf_preset = st.selectbox(
                    "Time frame",
                    ["All Time", "YTD", "Last 12 Months", "Last 6 Months", "Last 3 Months", "Last Month", "Custom"],
                    key="perf_timeframe"
                )
            # Resolve preset to date range
            if _tf_preset == "YTD":
                _tf_start = date(_today.year, 1, 1)
                _tf_end = _today
            elif _tf_preset == "Last 12 Months":
                _tf_start = _today - timedelta(days=365)
                _tf_end = _today
            elif _tf_preset == "Last 6 Months":
                _tf_start = _today - timedelta(days=182)
                _tf_end = _today
            elif _tf_preset == "Last 3 Months":
                _tf_start = _today - timedelta(days=91)
                _tf_end = _today
            elif _tf_preset == "Last Month":
                _tf_start = _today - timedelta(days=30)
                _tf_end = _today
            elif _tf_preset == "Custom":
                with _tf_col2:
                    _tf_start = st.date_input("From", value=_min_date, min_value=_min_date, max_value=_max_date, key="perf_from")
                with _tf_col3:
                    _tf_end = st.date_input("To", value=_today, min_value=_min_date, key="perf_to")
            else:  # All Time
                _tf_start = _min_date
                _tf_end = _today

            # Filter to selected time frame
            df_closed = df_closed[
                (df_closed['Date_closed'].dt.date >= _tf_start) &
                (df_closed['Date_closed'].dt.date <= _tf_end)
            ].copy()

            st.caption(f"Showing: **{_tf_start.strftime('%d %b %Y')}** to **{_tf_end.strftime('%d %b %Y')}** ({len(df_closed)} closed trades)")
            import plotly.graph_objects as go

            _deposit = st.session_state.get('portfolio_deposit', 0)
            colors = {'MARA': '#f59e0b', 'CRCL': '#10b981', 'SPY': '#6366f1', 'COIN': '#ef4444'}

            # ── Chart 1: Portfolio Equity Curve ──────────────────────
            st.markdown("#### Portfolio Value Over Time")
            st.caption("Starting from deposit, each realized P&L event moves the equity curve up or down.")

            daily_pl = df_closed.groupby(df_closed['Date_closed'].dt.date)['Actual_Profit_(USD)'].sum().reset_index()
            daily_pl.columns = ['Date', 'Daily_PL']
            daily_pl['Cumulative_PL'] = daily_pl['Daily_PL'].cumsum()
            daily_pl['Portfolio_Value'] = _deposit + daily_pl['Cumulative_PL']

            # Add starting point (deposit day = day before first trade)
            first_date = daily_pl['Date'].iloc[0]
            import datetime as _dt
            start_row = pd.DataFrame([{
                'Date': first_date - _dt.timedelta(days=1),
                'Daily_PL': 0, 'Cumulative_PL': 0, 'Portfolio_Value': _deposit
            }])
            daily_pl = pd.concat([start_row, daily_pl], ignore_index=True)

            fig_equity = go.Figure()

            # Portfolio value line (primary)
            fig_equity.add_trace(go.Scatter(
                x=daily_pl['Date'], y=daily_pl['Portfolio_Value'],
                mode='lines+markers', name='Portfolio Value',
                line=dict(width=3, color='#2563eb'),
                fill='tozeroy', fillcolor='rgba(37,99,235,0.08)',
                hovertemplate='%{x}<br>Portfolio: $%{y:,.0f}<extra></extra>'
            ))

            # Deposit baseline
            fig_equity.add_hline(
                y=_deposit, line_dash="dash", line_color="#94a3b8", opacity=0.7,
                annotation_text=f"Deposit: ${_deposit:,.0f}",
                annotation_position="bottom right"
            )

            # Colour the area above/below deposit
            fig_equity.update_layout(
                height=420,
                xaxis_title="Date", yaxis_title="Portfolio Value ($)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode="x unified",
                yaxis=dict(tickformat="$,.0f"),
            )
            st.plotly_chart(fig_equity, use_container_width=True)

            # ── Chart 2: Cumulative P&L by Ticker ────────────────────
            st.markdown("#### Cumulative P&L by Ticker")
            st.caption("How each ticker has contributed to total realized P&L over time.")

            fig_ticker = go.Figure()
            for ticker in sorted(df_closed['Ticker'].unique()):
                t_data = df_closed[df_closed['Ticker'] == ticker].copy()
                t_daily = t_data.groupby(t_data['Date_closed'].dt.date)['Actual_Profit_(USD)'].sum().reset_index()
                t_daily.columns = ['Date', 'PL']
                t_daily['Cumulative'] = t_daily['PL'].cumsum()
                fig_ticker.add_trace(go.Scatter(
                    x=t_daily['Date'], y=t_daily['Cumulative'],
                    mode='lines+markers', name=ticker,
                    line=dict(width=2, color=colors.get(ticker, '#94a3b8')),
                    hovertemplate=f'{ticker}<br>' + '%{x}<br>Cumulative: $%{y:,.0f}<extra></extra>'
                ))

            # Total line
            total_daily = df_closed.groupby(df_closed['Date_closed'].dt.date)['Actual_Profit_(USD)'].sum().reset_index()
            total_daily.columns = ['Date', 'PL']
            total_daily['Cumulative'] = total_daily['PL'].cumsum()
            fig_ticker.add_trace(go.Scatter(
                x=total_daily['Date'], y=total_daily['Cumulative'],
                mode='lines', name='TOTAL',
                line=dict(width=3, color='#1e293b', dash='solid'),
            ))

            fig_ticker.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
            fig_ticker.update_layout(
                height=380,
                xaxis_title="Date", yaxis_title="Cumulative P&L ($)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode="x unified",
                yaxis=dict(tickformat="$,.0f"),
            )
            st.plotly_chart(fig_ticker, use_container_width=True)

            # ── Chart 3: Monthly P&L bars + portfolio value line ─────
            st.markdown("#### Monthly P&L + Portfolio Growth")

            df_closed['Month'] = df_closed['Date_closed'].dt.to_period('M')
            monthly_total = df_closed.groupby('Month')['Actual_Profit_(USD)'].sum().reset_index()
            monthly_total['Month_str'] = monthly_total['Month'].astype(str)
            monthly_total['Cumulative'] = monthly_total['Actual_Profit_(USD)'].cumsum()
            monthly_total['Portfolio'] = _deposit + monthly_total['Cumulative']

            fig_monthly = go.Figure()

            # P&L bars (green/red)
            fig_monthly.add_trace(go.Bar(
                x=monthly_total['Month_str'], y=monthly_total['Actual_Profit_(USD)'],
                name='Monthly P&L',
                marker_color=['#22c55e' if v >= 0 else '#ef4444' for v in monthly_total['Actual_Profit_(USD)']],
                opacity=0.8,
                yaxis='y',
                hovertemplate='%{x}<br>P&L: $%{y:,.0f}<extra></extra>'
            ))

            # Portfolio value line on secondary axis
            fig_monthly.add_trace(go.Scatter(
                x=monthly_total['Month_str'], y=monthly_total['Portfolio'],
                mode='lines+markers', name='Portfolio Value',
                line=dict(width=3, color='#2563eb'),
                yaxis='y2',
                hovertemplate='%{x}<br>Portfolio: $%{y:,.0f}<extra></extra>'
            ))

            fig_monthly.update_layout(
                height=380,
                xaxis_title="Month",
                yaxis=dict(title="Monthly P&L ($)", tickformat="$,.0f", side='left'),
                yaxis2=dict(title="Portfolio Value ($)", tickformat="$,.0f", side='right', overlaying='y'),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode="x unified",
            )
            st.plotly_chart(fig_monthly, use_container_width=True)

            # ── Chart 4: Weekly income trend ─────────────────────────
            st.markdown("#### Weekly Income Trend")

            df_closed['Week'] = df_closed['Date_closed'].dt.to_period('W')
            weekly = df_closed.groupby('Week')['Actual_Profit_(USD)'].sum().reset_index()
            weekly['Week_str'] = weekly['Week'].apply(lambda w: w.start_time.strftime('%Y-%m-%d'))
            weekly['Rolling_4W'] = weekly['Actual_Profit_(USD)'].rolling(4, min_periods=1).mean()

            fig_weekly = go.Figure()
            fig_weekly.add_trace(go.Bar(
                x=weekly['Week_str'], y=weekly['Actual_Profit_(USD)'],
                name='Weekly P&L',
                marker_color=['#22c55e' if v >= 0 else '#ef4444' for v in weekly['Actual_Profit_(USD)']],
                opacity=0.7
            ))
            fig_weekly.add_trace(go.Scatter(
                x=weekly['Week_str'], y=weekly['Rolling_4W'],
                mode='lines', name='4-week avg',
                line=dict(width=3, color='#f59e0b')
            ))
            fig_weekly.update_layout(
                height=320,
                xaxis_title="Week Starting", yaxis_title="P&L ($)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                yaxis=dict(tickformat="$,.0f"),
            )
            fig_weekly.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
            st.plotly_chart(fig_weekly, use_container_width=True)

        else:
            st.info("No closed trades with dates found for charting.")
    else:
        st.info("No closed option trades found for charts.")


# ============================================================
# ALL POSITIONS PAGE
# ============================================================
def render_all_positions():
    """Render all positions table"""
    st.title("📋 All Positions")
    
    df_trades = st.session_state.df_trades
    
    if df_trades is None or df_trades.empty:
        st.warning("No trades found")
        return
    
    # Filter by strategy if selected
    strategy_filter = st.session_state.get('strategy_filter', 'All')
    if strategy_filter != 'All':
        portfolio = st.session_state.get('current_portfolio', 'Income Wheel')
        pmcc_tickers = get_pmcc_tickers(portfolio)
        pmcc_tickers_set = set(pmcc_tickers) if pmcc_tickers else set()
        
        if strategy_filter == 'PMCC':
            pmcc_mask = (
                df_trades['Ticker'].isin(pmcc_tickers_set) |
                (df_trades.get('StrategyType', '') == 'PMCC') |
                (df_trades['TradeType'] == 'LEAP')
            )
            df_trades = df_trades[pmcc_mask].copy()
        elif strategy_filter == 'WHEEL':
            wheel_mask = (
                ~df_trades['Ticker'].isin(pmcc_tickers_set) &
                (
                    (df_trades.get('StrategyType', '') == 'WHEEL') |
                    (df_trades.get('StrategyType', '').isna()) |
                    (df_trades.get('StrategyType', '') == '')
                ) &
                (df_trades['TradeType'] != 'LEAP')
            )
            df_trades = df_trades[wheel_mask].copy()
        elif strategy_filter == 'ActiveCore':
            df_trades = df_trades[df_trades.get('StrategyType', '') == 'ActiveCore'].copy()

        if df_trades.empty:
            st.info(f"ℹ️ No trades found for {strategy_filter} strategy.")
            return
    
    # Convert Expiry_Date to datetime for filtering
    df_trades = df_trades.copy()
    if 'Expiry_Date' in df_trades.columns:
        df_trades['Expiry_Date'] = pd.to_datetime(df_trades['Expiry_Date'], errors='coerce')
        df_trades['Expiry_Year'] = df_trades['Expiry_Date'].dt.year
        df_trades['Expiry_Month'] = df_trades['Expiry_Date'].dt.month
    
    # ===== FILTERS =====
    st.markdown("### 🔍 Filters")
    filter_col1, filter_col2, filter_col3, filter_col4, filter_col5 = st.columns(5)
    
    with filter_col1:
        status_filter = st.selectbox("**Status**", ["All", "Open", "Closed"], key="all_pos_status")
    
    with filter_col2:
        type_filter = st.selectbox("**Type**", ["All", "CC", "CSP", "STOCK", "LEAP"], key="all_pos_type")
    
    with filter_col3:
        ticker_filter = st.selectbox("**Ticker**", ["All"] + sorted(df_trades['Ticker'].unique().tolist()), key="all_pos_ticker")
    
    with filter_col4:
        # Expiry Year filter
        if 'Expiry_Year' in df_trades.columns:
            available_years = sorted([y for y in df_trades['Expiry_Year'].dropna().unique().tolist() if pd.notna(y)], reverse=True)
            expiry_year_filter = st.selectbox("**Expiry Year**", ["All"] + [str(int(y)) for y in available_years], key="all_pos_year")
        else:
            expiry_year_filter = "All"
    
    with filter_col5:
        # Expiry Month filter
        if 'Expiry_Month' in df_trades.columns and expiry_year_filter != "All":
            year_int = int(expiry_year_filter)
            available_months = sorted([m for m in df_trades[df_trades['Expiry_Year'] == year_int]['Expiry_Month'].dropna().unique().tolist() if pd.notna(m)], reverse=True)
            month_names = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
                          7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"}
            month_options = ["All"] + [f"{month_names[int(m)]} ({int(m)})" for m in available_months]
            expiry_month_filter_str = st.selectbox("**Expiry Month**", month_options, key="all_pos_month")
            expiry_month_filter = None if expiry_month_filter_str == "All" else int(expiry_month_filter_str.split("(")[1].split(")")[0])
        elif 'Expiry_Month' in df_trades.columns:
            available_months = sorted([m for m in df_trades['Expiry_Month'].dropna().unique().tolist() if pd.notna(m)], reverse=True)
            month_names = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
                          7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"}
            month_options = ["All"] + [f"{month_names[int(m)]} ({int(m)})" for m in available_months]
            expiry_month_filter_str = st.selectbox("**Expiry Month**", month_options, key="all_pos_month")
            expiry_month_filter = None if expiry_month_filter_str == "All" else int(expiry_month_filter_str.split("(")[1].split(")")[0])
        else:
            expiry_month_filter = None
    
    # Apply filters
    df_filtered = df_trades.copy()
    
    if status_filter != "All":
        df_filtered = df_filtered[df_filtered['Status'].str.lower() == status_filter.lower()]
    if type_filter != "All":
        df_filtered = df_filtered[df_filtered['TradeType'] == type_filter]
    if ticker_filter != "All":
        df_filtered = df_filtered[df_filtered['Ticker'] == ticker_filter]
    if expiry_year_filter != "All" and 'Expiry_Year' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['Expiry_Year'] == int(expiry_year_filter)]
    if expiry_month_filter is not None and 'Expiry_Month' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['Expiry_Month'] == expiry_month_filter]
    
    st.write(f"Showing {len(df_filtered)} of {len(df_trades)} trades")
    st.divider()
    
    # ===== COLUMN SELECTION =====
    st.markdown("### 📊 Column Selection")
    st.caption("Select which columns to display from the data schema")
    
    # Get all available columns from the dataframe (exclude helper columns)
    all_columns = [col for col in df_filtered.columns if col not in ['Expiry_Year', 'Expiry_Month']]
    
    # Default columns (always include TradeID)
    default_cols = ['TradeID', 'Ticker', 'TradeType', 'Status', 'Quantity', 'Date_open', 'Expiry_Date']
    default_cols = [col for col in default_cols if col in all_columns]
    
    # Column selection with multiselect
    selected_columns = st.multiselect(
        "Select columns to display:",
        options=all_columns,
        default=default_cols,
        key="all_pos_columns"
    )
    
    # Ensure TradeID is always included if available
    if 'TradeID' in all_columns and 'TradeID' not in selected_columns:
        selected_columns.insert(0, 'TradeID')
    
    if not selected_columns:
        st.warning("Please select at least one column to display")
        return
    
    # Filter to only selected columns that exist in dataframe
    display_cols = [col for col in selected_columns if col in df_filtered.columns]
    
    if not display_cols:
        st.warning("No valid columns selected")
        return
    
    # Display table (with Delete checkbox column)
    st.markdown("### 📋 Positions Table")
    st.caption("Check **Delete** for rows to remove from the Data Table, then confirm and click Delete selected.")
    
    # Ensure TradeID is in display for delete to work
    if 'TradeID' not in display_cols:
        display_cols = ['TradeID'] + [c for c in display_cols if c != 'TradeID']
    
    # Sort dataframe
    if 'Date_open' in display_cols:
        sort_col = 'Date_open'
    elif 'TradeID' in display_cols:
        sort_col = 'TradeID'
    else:
        sort_col = display_cols[0]
    df_display = df_filtered[display_cols].sort_values(sort_col, ascending=False).copy()
    
    # Add Delete checkbox column as first column
    df_display.insert(0, "Delete", False)
    
    edited = st.data_editor(
        df_display,
        use_container_width=True,
        hide_index=True,
        column_config={"Delete": st.column_config.CheckboxColumn("Delete", help="Select to delete this row from the Data Table")},
        disabled=[c for c in df_display.columns if c != "Delete"],
        key="all_positions_delete_editor",
    )
    
    # Get selected TradeIDs (where Delete is True)
    selected_mask = edited["Delete"] == True  # noqa: E712
    trade_ids_to_delete = edited.loc[selected_mask, "TradeID"].astype(str).str.strip().unique().tolist() if selected_mask.any() else []
    
    if trade_ids_to_delete:
        st.warning(f"Selected {len(trade_ids_to_delete)} position(s) to delete: {', '.join(trade_ids_to_delete[:10])}{' ...' if len(trade_ids_to_delete) > 10 else ''}")
        confirm = st.checkbox(
            "I understand these rows will be permanently removed from the Data Table",
            value=False,
            key="all_pos_confirm_delete",
        )
        if st.button("🗑️ Delete selected position(s)", type="primary", use_container_width=True, key="all_pos_btn_delete", disabled=not confirm):
            try:
                sheet_id = st.session_state.get("current_sheet_id", "")
                if not sheet_id:
                    st.error("No Google Sheet configured. Check .env and switch portfolio.")
                else:
                    handler = GSheetHandler(sheet_id)
                    handler.delete_trades(trade_ids_to_delete)
                    refresh_data()
                    st.toast("Position(s) deleted.", icon="✅")
                    st.success(f"Deleted: {', '.join(trade_ids_to_delete[:15])}{' ...' if len(trade_ids_to_delete) > 15 else ''}")
                    st.rerun()
            except Exception as e:
                st.error(f"Delete failed: {e}")
    
    # Export button
    st.divider()
    csv = df_filtered[display_cols].to_csv(index=False)
    st.download_button("📥 Export to CSV", csv, "income_wheel_export.csv", "text/csv", key="export_all_positions")


# ============================================================
# MARGIN CONFIGURATION PAGE
# ============================================================
def render_margin_config():
    """Render margin configuration page - simple form by ticker and trade type"""
    st.title("⚙️ Margin Configuration")
    
    # Initialize session state (load from persistence) - ALWAYS load to ensure persistence
    portfolio = st.session_state.get('current_portfolio', 'Income Wheel')
    st.session_state.capital_allocation = get_capital_allocation(portfolio)
    st.session_state.portfolio_deposit = get_portfolio_deposit(portfolio)
    
    # Load open positions for PMCC configuration
    df_open = st.session_state.get('df_open', pd.DataFrame())
    
    # Load SGD and FX rate from persistence
    if 'portfolio_deposit_sgd' not in st.session_state:
        st.session_state.portfolio_deposit_sgd = get_portfolio_deposit_sgd(portfolio)
    if 'sgd_usd_fx_rate' not in st.session_state:
        st.session_state.sgd_usd_fx_rate = get_fx_rate(portfolio)
    
    # Refresh button
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("🔄 Refresh Data", type="primary", use_container_width=True):
            refresh_data()
            st.rerun()
    
    st.divider()
    
    # Ticker list for this portfolio (Income Wheel and Active Core each have their own)
    st.subheader("📋 Ticker List (Entry Forms Dropdown)")
    st.caption("Tickers you add here appear in the CC/CSP ticker dropdown in Entry Forms. Tickers from your current positions are always included. Income Wheel and Active Core have separate lists.")
    saved_tickers = get_tickers(portfolio)
    if not saved_tickers and portfolio == "Income Wheel":
        st.info("No custom tickers saved for this portfolio. Entry Forms use config default + positions. Add tickers below to build your list.")
    df_open_margin = st.session_state.get('df_open', pd.DataFrame())
    df_trades_margin = st.session_state.get('df_trades', pd.DataFrame())
    position_tickers = []
    if df_trades_margin is not None and not df_trades_margin.empty and 'Ticker' in df_trades_margin.columns:
        position_tickers = sorted(set(df_trades_margin['Ticker'].dropna().astype(str).str.strip().str.upper().unique().tolist()))
    full_list = get_tickers_for_dropdown(portfolio, df_trades_margin)
    
    add_col, _ = st.columns([1, 3])
    with add_col:
        new_ticker = st.text_input("Add ticker", key="margin_config_new_ticker", placeholder="e.g. AAPL").strip().upper()
        if st.button("➕ Add ticker", key="margin_config_add_ticker"):
            if new_ticker:
                updated = sorted(set(saved_tickers + [new_ticker]))
                save_tickers(updated, portfolio)
                st.toast(f"Added {new_ticker}", icon="✅")
                st.rerun()
            else:
                st.warning("Enter a ticker symbol.")
    
    if saved_tickers:
        st.markdown("**Saved tickers (remove to drop from list):**")
        for t in saved_tickers:
            c1, c2 = st.columns([3, 1])
            with c1:
                st.text(t)
            with c2:
                if st.button("Remove", key=f"margin_remove_ticker_{t}"):
                    updated = [x for x in saved_tickers if x != t]
                    save_tickers(updated, portfolio)
                    st.toast(f"Removed {t}", icon="✅")
                    st.rerun()
    if position_tickers and full_list:
        st.caption(f"Tickers from positions (always in dropdown): {', '.join(position_tickers)}")
    
    st.divider()
    
    # ── POT DEPOSITS (Base + Active Income) ──────────────────────
    st.subheader("💰 Pot Deposits")
    st.caption(
        "Two pots: **Base** (WHEEL/PMCC strategies) and **Active Income** (ActiveCore strategy). "
        "Pot is auto-derived from StrategyType on each trade. Total = sum of both pots."
    )

    from persistence import (
        get_pot_deposit, save_pot_deposit,
        get_pot_deposit_sgd, save_pot_deposit_sgd,
        get_pot_capital_allocation, save_pot_capital_allocation,
    )

    if 'sgd_usd_fx_rate' not in st.session_state:
        st.session_state.sgd_usd_fx_rate = get_fx_rate(portfolio)

    # FX rate input (shared)
    _fx_col, _ = st.columns([1, 3])
    with _fx_col:
        sgd_usd_fx_rate = st.number_input(
            "SGD/USD FX Rate",
            min_value=0.01,
            value=float(st.session_state.sgd_usd_fx_rate),
            step=0.01,
            format="%.4f",
            help="1 USD = X SGD (e.g., 1.35)",
            key="sgd_usd_fx_rate_input"
        )

    if sgd_usd_fx_rate != st.session_state.sgd_usd_fx_rate:
        st.session_state.sgd_usd_fx_rate = sgd_usd_fx_rate
        save_fx_rate(sgd_usd_fx_rate, portfolio)

    # Auto-migrate: if pot deposits are unset but legacy deposit exists, seed Base pot
    _existing_base_sgd = get_pot_deposit_sgd('Base', portfolio)
    _existing_base_usd = get_pot_deposit('Base', portfolio)
    _existing_active_sgd = get_pot_deposit_sgd('Active', portfolio)
    _existing_active_usd = get_pot_deposit('Active', portfolio)
    if _existing_base_sgd == 0 and _existing_active_sgd == 0:
        # Migrate from legacy
        _legacy_sgd = get_portfolio_deposit_sgd(portfolio)
        _legacy_usd = get_portfolio_deposit(portfolio)
        if _legacy_sgd > 0 or _legacy_usd > 0:
            save_pot_deposit_sgd('Base', _legacy_sgd, portfolio)
            save_pot_deposit('Base', _legacy_usd, portfolio)
            _existing_base_sgd = _legacy_sgd
            _existing_base_usd = _legacy_usd
            st.info(f"ℹ️ Migrated legacy portfolio deposit (${_legacy_usd:,.0f}) into Base Pot. Adjust splits below.")

    # Two-column layout: Base | Active
    pot_col_base, pot_col_active = st.columns(2)

    with pot_col_base:
        st.markdown("**🏛️ Base Pot** (WHEEL + PMCC)")
        _base_sgd = st.number_input(
            "Base Pot (SGD)",
            min_value=0.0,
            value=float(_existing_base_sgd),
            step=1000.0,
            help="Cash deposited into the Base Pot (Wheel + PMCC)",
            key="pot_base_sgd_input"
        )
        _base_usd = _base_sgd / sgd_usd_fx_rate if sgd_usd_fx_rate > 0 else 0.0
        st.metric("Base Pot (USD)", f"${_base_usd:,.0f}")
        # Save if changed
        if _base_sgd != get_pot_deposit_sgd('Base', portfolio):
            save_pot_deposit_sgd('Base', _base_sgd, portfolio)
        if _base_usd != get_pot_deposit('Base', portfolio):
            save_pot_deposit('Base', _base_usd, portfolio)

    with pot_col_active:
        st.markdown("**⚡ Active Income Pot** (ActiveCore)")
        _active_sgd = st.number_input(
            "Active Pot (SGD)",
            min_value=0.0,
            value=float(_existing_active_sgd),
            step=1000.0,
            help="Cash deposited into the Active Income Pot (ActiveCore)",
            key="pot_active_sgd_input"
        )
        _active_usd = _active_sgd / sgd_usd_fx_rate if sgd_usd_fx_rate > 0 else 0.0
        st.metric("Active Pot (USD)", f"${_active_usd:,.0f}")
        if _active_sgd != get_pot_deposit_sgd('Active', portfolio):
            save_pot_deposit_sgd('Active', _active_sgd, portfolio)
        if _active_usd != get_pot_deposit('Active', portfolio):
            save_pot_deposit('Active', _active_usd, portfolio)

    # Total
    total_deposit_usd = _base_usd + _active_usd
    total_deposit_sgd = _base_sgd + _active_sgd
    st.metric("**Total Portfolio (USD)**",
               f"${total_deposit_usd:,.0f}",
               delta=f"SGD {total_deposit_sgd:,.0f}",
               help="Sum of both pots — drives all dashboard calculations")

    # Maintain backward compatibility: write total to legacy keys (only if non-zero, never wipe)
    if total_deposit_usd > 0 and total_deposit_usd != st.session_state.get('portfolio_deposit', 0):
        st.session_state.portfolio_deposit = total_deposit_usd
        save_portfolio_deposit(total_deposit_usd, portfolio)
    if total_deposit_sgd > 0 and total_deposit_sgd != st.session_state.get('portfolio_deposit_sgd', 0):
        st.session_state.portfolio_deposit_sgd = total_deposit_sgd
        save_portfolio_deposit_sgd(total_deposit_sgd, portfolio)

    st.divider()

    # ── PER-POT CAPITAL ALLOCATION ──────────────────────────────
    st.subheader("💵 Capital Allocation by Pot & Ticker")
    st.caption("Each pot has its own allocation. % is of that pot's deposit. Add tickers as needed.")

    df_open_for_alloc = st.session_state.df_open
    existing_tickers = sorted(df_open_for_alloc['Ticker'].unique().tolist()) if (df_open_for_alloc is not None and not df_open_for_alloc.empty) else []

    def _render_pot_allocation_editor(pot_name: str, pot_deposit: float, key_prefix: str):
        """Render allocation editor for one pot."""
        alloc = get_pot_capital_allocation(pot_name, portfolio).copy()
        # Add tickers from positions if missing (only those in this pot)
        for t in existing_tickers:
            if t not in alloc:
                alloc[t] = 0.0

        rows = []
        for t in sorted(alloc.keys()):
            cap = alloc.get(t, 0.0)
            pct = (cap / pot_deposit * 100) if pot_deposit > 0 else 0.0
            rows.append({'Ticker': t, 'Allocation %': pct, 'Capital Allocated ($)': cap})

        if not rows:
            st.info(f"No tickers configured for {pot_name} pot. Add tickers below.")
            rows = [{'Ticker': '', 'Allocation %': 0.0, 'Capital Allocated ($)': 0.0}]

        df_a = pd.DataFrame(rows)
        edited = st.data_editor(
            df_a,
            column_config={
                "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                "Allocation %": st.column_config.NumberColumn("% of Pot", min_value=0.0, max_value=100.0, step=0.1, format="%.1f%%"),
                "Capital Allocated ($)": st.column_config.NumberColumn("Allocated $", min_value=0.0, step=1000.0, format="$%d"),
            },
            hide_index=True, use_container_width=True, num_rows="dynamic",
            key=f"{key_prefix}_alloc_editor"
        )

        # Process edits: if % changed, recalc $; if $ changed, recalc %
        prev_key = f"{key_prefix}_prev_pct"
        prev_pcts = st.session_state.get(prev_key, {t: r['Allocation %'] for t, r in zip(df_a['Ticker'], rows)})
        updated_alloc = {}
        for _, row in edited.iterrows():
            t = str(row['Ticker']).strip().upper()
            if not t:
                continue
            pct = float(row['Allocation %'] or 0)
            cap = float(row['Capital Allocated ($)'] or 0)
            prev = prev_pcts.get(t, 0.0)
            # If % changed, $ follows
            if abs(pct - prev) > 0.01:
                cap = (pct / 100.0) * pot_deposit
            updated_alloc[t] = cap
        st.session_state[prev_key] = {t: (v / pot_deposit * 100 if pot_deposit > 0 else 0) for t, v in updated_alloc.items()}

        if updated_alloc != alloc:
            save_pot_capital_allocation(pot_name, updated_alloc, portfolio)
            st.toast(f"{pot_name} pot allocation saved.", icon="✅")

        # Total %
        if pot_deposit > 0:
            total_pct = sum(v / pot_deposit * 100 for v in updated_alloc.values())
            if total_pct > 100:
                st.warning(f"⚠️ {pot_name} total: {total_pct:.1f}% (exceeds 100%)")
            else:
                st.caption(f"{pot_name} total: {total_pct:.1f}% allocated, {100-total_pct:.1f}% unallocated (OTHERS)")

        return updated_alloc

    pot_alloc_col1, pot_alloc_col2 = st.columns(2)
    with pot_alloc_col1:
        st.markdown(f"**🏛️ Base Pot — ${_base_usd:,.0f}**")
        _base_alloc = _render_pot_allocation_editor('Base', _base_usd, 'base')
    with pot_alloc_col2:
        st.markdown(f"**⚡ Active Income Pot — ${_active_usd:,.0f}**")
        _active_alloc = _render_pot_allocation_editor('Active', _active_usd, 'active')

    # Maintain backward compatibility: combined allocation = base + active
    combined = {}
    for t, v in _base_alloc.items():
        combined[t] = combined.get(t, 0) + v
    for t, v in _active_alloc.items():
        combined[t] = combined.get(t, 0) + v
    save_capital_allocation(combined, portfolio)
    st.session_state.capital_allocation = combined
    
    st.divider()
    
    # PMCC Configuration Section
    st.subheader("🔄 PMCC Configuration")
    st.write("Mark tickers that use PMCC (Poor Man's Covered Call) logic. These will be excluded from CSP Tank calculations.")
    
    # Get current PMCC tickers
    portfolio = st.session_state.get('current_portfolio', 'Income Wheel')
    pmcc_tickers = get_pmcc_tickers(portfolio)
    pmcc_tickers_set = set(pmcc_tickers) if pmcc_tickers else set()
    
    # Get all unique tickers from open positions
    if not df_open.empty:
        all_tickers = sorted(set(df_open['Ticker'].unique()))
    else:
        all_tickers = []
    
    if all_tickers:
        # Create checkboxes for each ticker
        pmcc_checkboxes = {}
        cols = st.columns(min(4, len(all_tickers)))  # Max 4 columns
        
        for idx, ticker in enumerate(all_tickers):
            col_idx = idx % 4
            with cols[col_idx]:
                pmcc_checkboxes[ticker] = st.checkbox(
                    f"**{ticker}**",
                    value=ticker in pmcc_tickers_set,
                    key=f"pmcc_{ticker}",
                    help=f"Mark {ticker} as using PMCC logic"
                )
        
        # Save PMCC flags
        new_pmcc_tickers = {ticker for ticker, checked in pmcc_checkboxes.items() if checked}
        if new_pmcc_tickers != pmcc_tickers_set:
            save_pmcc_tickers(new_pmcc_tickers, portfolio)
            st.toast("PMCC configuration saved.", icon="✅")
            st.success("✅ PMCC configuration saved!")
            st.rerun()
        
        if pmcc_tickers_set:
            st.info(f"ℹ️ **PMCC Tickers:** {', '.join(sorted(pmcc_tickers_set))} - LEAP cost is the only capital tied up; short CCs are covered by the LEAP (no additional margin).")
    else:
        st.info("ℹ️ No open positions found. PMCC configuration will be available once you have open positions.")

    st.divider()

    # ── Capital Policy Reference ────────────────────────────────
    st.subheader("📋 Capital Policy")
    st.markdown(
        "**Cash-secured policy** — capital used by position type:\n\n"
        "- **CSP** = `strike × 100 × contracts` (full cash collateral)\n"
        "- **STOCK** = `shares × avg_buy_price` (cost basis — what you paid)\n"
        "- **LEAP (PMCC)** = `premium × 100 × contracts` (sunk premium)\n"
        "- **CC on STOCK** = `$0` (covered by shares)\n"
        "- **CC on LEAP (PMCC)** = `$0` (covered by LEAP — Tiger charges no extra margin)\n\n"
        "**Buying Power** = Deposit + Realized P&L − (Stock at cost + LEAP sunk + CSP reserved).\n\n"
        "See **Dashboard → Tiger Broker Margin** expander for the broker's actual margin estimate "
        "and headroom vs your cash-secured policy."
    )
    


# ============================================================
# MAIN
# ============================================================
def main():
    """Main application entry point"""
    init_session_state()
    
    # Render sidebar and get selected page and portfolio
    page, portfolio = render_sidebar()
    
    # Get portfolio name (without emoji for data loading)
    portfolio_name = "Active Core" if portfolio == "⭐ Active Core" else "Income Wheel"
    
    # Load data for selected portfolio
    df_trades, df_audit, errors = load_data(portfolio_name)
    
    if errors and df_trades is None:
        st.error(f"❌ {errors[0]}")
        st.info(f"💡 **Google Sheets connection failed** for **{portfolio_name}**.\n\n"
               f"**Check:**\n"
               f"1. The Sheet ID is set correctly in your `.env` file\n"
               f"2. The service account has access to the Google Sheet\n"
               f"3. The `credentials.json` file exists and is valid")
        return
    
    # Store in session
    st.session_state.df_trades = df_trades
    st.session_state.df_audit = df_audit
    st.session_state.df_open = df_trades[df_trades['Status'] == 'Open'] if df_trades is not None else None
    st.session_state.data_loaded = True
    st.session_state.current_portfolio = portfolio_name
    st.session_state.current_sheet_id = get_sheet_id(portfolio_name)
    st.session_state.portfolio_deposit = get_portfolio_deposit(portfolio_name)
    # Show confirmation after form submit (so user sees it after rerun and fresh data)
    if st.session_state.get('success_message'):
        msg = st.session_state.success_message
        st.toast(msg, icon="✅")
        st.success(msg)
        del st.session_state.success_message

    # Show data integrity warnings only for this portfolio (skip "Audit references missing trades" for Active Core when audit has other portfolio's IDs)
    if errors:
        for err in errors:
            if "Audit references missing trades" in err and portfolio_name == "Active Core":
                continue
            st.warning(f"⚠️ Data integrity warning: {err}")
    
    # Render selected page (all pages work for both portfolios)
    if page == "📊 Dashboard":
        render_dashboard()
    elif page == "📅 Daily Helper":
        render_daily_helper()
    elif page == "📝 Entry Forms":
        render_entry_forms()
    elif page == "📈 Expiry Ladder":
        render_expiry_ladder()
    elif page == "📉 Performance":
        render_performance()
    elif page == "📋 All Positions":
        render_all_positions()
    elif page == "⚙️ Margin Config":
        render_margin_config()
    elif page == "🔍 Income Scanner":
        render_income_scanner()
    elif page == "📡 Market Data":
        render_market_data_panel()
    elif page == "🔎 Contract Lookup":
        render_contract_price_lookup()


def render_market_data_panel():
    """
    Standalone Market Data query panel (Mode 2B).
    Allows user to query equity prices, options data, and historical OHLCV
    independently of the live positions feed.
    """
    st.header("📡 Market Data")

    # ------------------------------------------------------------------
    # Service Status Banner
    # ------------------------------------------------------------------
    alpaca_ok = _market_data.alpaca_available
    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        st.success("🟢 yfinance — Equity + Options Chain (15 min delay)")
    with col_s2:
        if alpaca_ok:
            st.success("🟢 Alpaca — Greeks Δ Γ Θ enabled")
        else:
            st.warning("🟡 Alpaca — Greeks disabled (add keys to .env)")
    with col_s3:
        st.success("🟢 Stooq — Historical OHLCV available")

    st.divider()

    tab_equity, tab_options, tab_history = st.tabs(["Equity Quote", "Options Chain", "Historical OHLCV"])

    # ------------------------------------------------------------------
    # Tab 1 — Equity Quote
    # ------------------------------------------------------------------
    with tab_equity:
        st.subheader("Live Equity Quote")
        ticker_input = st.text_input("Ticker symbol", placeholder="e.g. MARA, SPY", key="md_equity_ticker").upper().strip()
        if st.button("Get Quote", key="md_equity_btn") and ticker_input:
            with st.spinner(f"Fetching {ticker_input}..."):
                quotes = _market_data.get_equity_prices([ticker_input])
            if ticker_input in quotes:
                q = quotes[ticker_input]
                c1, c2, c3 = st.columns(3)
                c1.metric("Price", f"${q.price:.2f}")
                c2.metric("Prev Close", f"${q.prev_close:.2f}")
                change_pct = ((q.price - q.prev_close) / q.prev_close * 100) if q.prev_close else 0
                c3.metric("Change", f"{change_pct:+.2f}%")
                st.caption(f"As of {q.timestamp.strftime('%Y-%m-%d %H:%M:%S')} (15-min delay)")
            else:
                st.warning(f"Could not retrieve price for {ticker_input}.")

    # ------------------------------------------------------------------
    # Tab 2 — Options Chain
    # ------------------------------------------------------------------
    with tab_options:

        # ── Section A: Open Positions ──────────────────────────────────
        st.subheader("📋 Open Positions — Live Options Data")
        st.caption("Bid / Ask / Last / IV and Greeks for your current open CC and CSP positions.")

        if st.button("Refresh Open Positions Data", key="md_options_btn"):
            try:
                df_trades = st.session_state.get("df_trades")
                if df_trades is not None and not df_trades.empty:
                    df_open_opts = df_trades[
                        (df_trades["Status"] == "Open") &
                        (df_trades["TradeType"].isin(["CC", "CSP"]))
                    ].copy()
                    with st.spinner("Fetching options data..."):
                        contracts = _market_data.get_open_positions_data(df_open_opts)
                    st.session_state.open_positions_data = contracts
                else:
                    st.warning("No trade data loaded.")
            except Exception as exc:
                st.error(f"Error fetching options data: {exc}")

        contracts = st.session_state.get("open_positions_data", [])
        if contracts:
            # ── Build premium / qty lookup from open trade rows ──────────
            _prem_lookup: dict = {}
            _df_trades_src = st.session_state.get("df_trades")
            if _df_trades_src is not None and not _df_trades_src.empty:
                _req_cols = ["Status", "TradeType", "Ticker", "Option_Strike_Price_(USD)", "Expiry_Date"]
                if all(_rc in _df_trades_src.columns for _rc in _req_cols):
                    _open_opts = _df_trades_src[
                        (_df_trades_src["Status"].str.upper() == "OPEN") &
                        (_df_trades_src["TradeType"].isin(["CC", "CSP"]))
                    ]
                    for _, _r in _open_opts.iterrows():
                        try:
                            _right = "C" if _r["TradeType"] == "CC" else "P"
                            _expiry = pd.to_datetime(_r["Expiry_Date"]).strftime("%Y-%m-%d")
                            _strike = float(_r["Option_Strike_Price_(USD)"])
                            _prem = float(pd.to_numeric(_r.get("OptPremium", 0), errors="coerce") or 0)
                            _qty = float(pd.to_numeric(_r.get("Quantity", 0), errors="coerce") or 0)
                            _lkey = (str(_r["Ticker"]).upper(), _strike, _right, _expiry)
                            # Accumulate across multiple open trades on the same contract
                            # (same ticker/strike/expiry can have >1 trade row)
                            if _lkey in _prem_lookup:
                                _prev_prem_rcvd, _prev_qty = _prem_lookup[_lkey]
                                _prem_lookup[_lkey] = (
                                    _prev_prem_rcvd + _prem * _qty * 100,
                                    _prev_qty + _qty,
                                )
                            else:
                                # Store total $ premium received and total contracts
                                _prem_lookup[_lkey] = (_prem * _qty * 100, _qty)
                        except Exception:
                            pass

            rows = []
            for c in contracts:
                # Mark price: use last if valid, else mid
                _mark = c.last_price if c.last_price > 0 else (c.bid + c.ask) / 2
                # Look up aggregated premium / qty from trade data
                _key = (c.underlying, float(c.strike), c.right, str(c.expiry))
                if _key in _prem_lookup:
                    _total_prem_rcvd, _total_qty = _prem_lookup[_key]
                    # P&L = total premium received − current value of all contracts
                    pl_num = _total_prem_rcvd - _mark * _total_qty * 100 if _total_prem_rcvd > 0 else None
                else:
                    pl_num = None
                pl_str = f"${pl_num:+,.2f}" if pl_num is not None else "—"
                rows.append({
                    "Contract": c.contract_symbol,
                    "Underlying": c.underlying,
                    "P&L": pl_str,
                    "Strike": f"${c.strike:.2f}",
                    "Expiry": str(c.expiry),
                    "Type": "Call" if c.right == "C" else "Put",
                    "Bid": f"${c.bid:.2f}",
                    "Ask": f"${c.ask:.2f}",
                    "Last": f"${c.last_price:.2f}",
                    "IV": f"{c.implied_volatility:.1%}" if c.implied_volatility else "—",
                    "Δ Delta": f"{c.delta:.3f}" if c.delta is not None else "—",
                    "Γ Gamma": f"{c.gamma:.4f}" if c.gamma is not None else "—",
                    "Θ Theta": f"{c.theta:.3f}" if c.theta is not None else "—",
                })

            # Apply green / red bold styling to P&L column
            def _style_pl(val):
                if val == "—":
                    return ""
                try:
                    num = float(str(val).replace("$", "").replace(",", ""))
                    if num > 0:
                        return "color: green; font-weight: bold"
                    elif num < 0:
                        return "color: red; font-weight: bold"
                except Exception:
                    pass
                return ""

            _df_positions = pd.DataFrame(rows)
            _styled = _df_positions.style.map(_style_pl, subset=["P&L"])
            st.dataframe(_styled, use_container_width=True, hide_index=True)
        else:
            st.info("Click 'Refresh Open Positions Data' to load current positions.")

        st.divider()

        # ── Section B: Option Lookup ───────────────────────────────────
        st.subheader("🔍 Option Lookup")
        st.caption("Search any option contract by ticker, expiry, type and strike.")

        col_lt, col_lcp = st.columns([2, 1])
        with col_lt:
            lookup_ticker = st.text_input("Ticker", placeholder="e.g. MARA, SPY", key="opt_lookup_ticker").upper().strip()
        with col_lcp:
            lookup_right_label = st.radio("Type", ["Call (C)", "Put (P)"], key="opt_lookup_right", horizontal=True)

        if lookup_ticker:
            try:
                import yfinance as _yf_lookup
                available_expiries = list(_yf_lookup.Ticker(lookup_ticker).options)
            except Exception:
                available_expiries = []

            if available_expiries:
                col_lexp, col_lstrike, col_lbtn = st.columns([2, 1, 1])
                with col_lexp:
                    lookup_expiry = st.selectbox("Expiry Date", available_expiries, key="opt_lookup_expiry")
                with col_lstrike:
                    lookup_strike = st.number_input("Strike ($)", min_value=0.0, step=0.5, format="%.2f", key="opt_lookup_strike")
                with col_lbtn:
                    st.write("")
                    st.write("")
                    search_clicked = st.button("🔍 Search", key="opt_lookup_btn", use_container_width=True)

                if search_clicked and lookup_strike > 0:
                    right_code = "C" if "Call" in lookup_right_label else "P"
                    trade_type = "CC" if right_code == "C" else "CSP"
                    fake_df = pd.DataFrame([{
                        "Ticker": lookup_ticker,
                        "Option_Strike_Price_(USD)": lookup_strike,
                        "Expiry_Date": lookup_expiry,
                        "TradeType": trade_type,
                        "Status": "Open"
                    }])
                    with st.spinner(f"Looking up {lookup_ticker} {lookup_expiry} {'Call' if right_code == 'C' else 'Put'} ${lookup_strike:.2f}…"):
                        results = _market_data.get_open_positions_data(fake_df)

                    if results:
                        c = results[0]
                        # ── Result Card ──
                        with st.container(border=True):
                            st.markdown(f"### `{c.contract_symbol}`")
                            st.caption(f"{c.underlying} · {'Call' if c.right == 'C' else 'Put'} · Strike ${c.strike:.2f} · Expires {c.expiry}")
                            st.divider()
                            col1, col2, col3, col4 = st.columns(4)
                            col1.metric("Last Price", f"${c.last_price:.2f}")
                            col2.metric("Bid", f"${c.bid:.2f}")
                            col3.metric("Ask", f"${c.ask:.2f}")
                            col4.metric("IV", f"{c.implied_volatility:.1%}" if c.implied_volatility else "—")
                            if alpaca_ok:
                                st.divider()
                                col5, col6, col7, col8 = st.columns(4)
                                col5.metric("Δ Delta", f"{c.delta:.3f}" if c.delta is not None else "—")
                                col6.metric("Γ Gamma", f"{c.gamma:.4f}" if c.gamma is not None else "—")
                                col7.metric("Θ Theta / day", f"{c.theta:.3f}" if c.theta is not None else "—")
                                col8.metric("Timestamp", c.timestamp.strftime("%H:%M:%S"))
                    else:
                        st.warning("No contract found. Verify the strike is exact and expiry is valid for this ticker.")
                elif search_clicked and lookup_strike == 0:
                    st.warning("Please enter a strike price greater than 0.")
            else:
                if lookup_ticker:
                    st.warning(f"Could not load expiry dates for **{lookup_ticker}**. Check ticker symbol.")

    # ------------------------------------------------------------------
    # Tab 3 — Historical OHLCV
    # ------------------------------------------------------------------
    with tab_history:
        st.subheader("Historical OHLCV")
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            hist_ticker = st.text_input("Ticker", placeholder="e.g. MARA, SPY", key="md_hist_ticker").upper().strip()
        with col2:
            period = st.selectbox("Period", [30, 60, 90, 180, 365], index=2, key="md_hist_period")
        with col3:
            freq = st.selectbox("Frequency", ["daily", "monthly"], key="md_hist_freq")

        if st.button("Get History", key="md_hist_btn") and hist_ticker:
            with st.spinner(f"Fetching {hist_ticker} {freq} data ({period} days)..."):
                bars = _market_data.get_historical_ohlcv(hist_ticker, period_days=period, frequency=freq)

            if bars:
                import plotly.graph_objects as go
                df_bars = pd.DataFrame([
                    {"Date": b.date, "Open": b.open, "High": b.high,
                     "Low": b.low, "Close": b.close, "Volume": b.volume}
                    for b in bars
                ])
                fig = go.Figure(data=[go.Candlestick(
                    x=df_bars["Date"], open=df_bars["Open"],
                    high=df_bars["High"], low=df_bars["Low"], close=df_bars["Close"]
                )])
                fig.update_layout(
                    title=f"{hist_ticker} — {freq.title()} OHLCV",
                    xaxis_title="Date", yaxis_title="Price (USD)",
                    xaxis_rangeslider_visible=False, height=400
                )
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df_bars, use_container_width=True, hide_index=True)
            else:
                st.warning(f"No data returned for {hist_ticker}. Check ticker symbol.")


if __name__ == "__main__":
    main()
