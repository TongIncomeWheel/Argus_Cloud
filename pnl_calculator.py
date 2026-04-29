"""
P&L Calculator - Comprehensive Profit & Loss Tracking
Calculates realized, unrealized, and net P&L for complete portfolio view
"""
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Dict, Optional, Tuple
from data_access import DataAccess
from data_schema import get_field_name
import logging

logger = logging.getLogger(__name__)


class PnLCalculator:
    """Calculate comprehensive P&L for portfolio"""
    
    @staticmethod
    def calculate_realized_pnl(df_trades: pd.DataFrame) -> float:
        """
        Calculate realized P&L from closed trades
        
        Args:
            df_trades: All trades DataFrame
        
        Returns:
            Total realized P&L (float)
        """
        if df_trades is None or df_trades.empty:
            return 0.0
        
        # Get closed trades
        closed_trades = df_trades[df_trades[get_field_name('status')] == 'Closed'].copy()
        
        if closed_trades.empty:
            return 0.0
        
        # Sum Actual_Profit_(USD) for all closed trades
        profit_col = get_field_name('realized_profit', 'pnl')
        if profit_col not in closed_trades.columns:
            return 0.0
        
        realized_pnl = pd.to_numeric(closed_trades[profit_col], errors='coerce').fillna(0).sum()
        return float(realized_pnl)
    
    @staticmethod
    def calculate_unrealized_stock_pnl(
        df_open: pd.DataFrame,
        stock_avg_prices: Dict[str, float],
        live_prices: Dict[str, float]
    ) -> Dict[str, float]:
        """
        Calculate unrealized P&L for stock positions
        
        Args:
            df_open: Open positions DataFrame
            stock_avg_prices: Average entry prices (cost basis) by ticker
            live_prices: Current market prices by ticker
        
        Returns:
            Dict with 'total', 'by_ticker' keys
        """
        if df_open is None or df_open.empty:
            return {'total': 0.0, 'by_ticker': {}}
        
        # Filter to STOCK positions only
        trade_type_col = get_field_name('trade_type', 'identity')
        stock_positions = df_open[df_open[trade_type_col] == 'STOCK'].copy()
        
        if stock_positions.empty:
            return {'total': 0.0, 'by_ticker': {}}
        
        by_ticker = {}
        total_unrealized = 0.0
        
        # Group by ticker
        ticker_col = get_field_name('ticker', 'identity')
        for ticker in stock_positions[ticker_col].unique():
            ticker_positions = stock_positions[stock_positions[ticker_col] == ticker]
            
            # Get total shares for ticker
            total_shares = 0.0
            for idx in ticker_positions.index:
                pos_idx = ticker_positions.index.get_loc(idx)
                shares = DataAccess.get_shares_for_stock(ticker_positions, pos_idx)
                total_shares += shares
            
            if total_shares == 0:
                continue
            
            # Get cost basis (average inventory price) — guard against None/string
            try:
                cost_basis = float(stock_avg_prices.get(ticker, 0.0) or 0.0)
            except (TypeError, ValueError):
                cost_basis = 0.0
            if cost_basis == 0:
                continue

            # Get current market price — guard against None/string
            try:
                current_price = float(live_prices.get(ticker, 0.0) or 0.0)
            except (TypeError, ValueError):
                current_price = 0.0
            if current_price == 0:
                continue

            try:
                total_shares = float(total_shares or 0)
            except (TypeError, ValueError):
                total_shares = 0.0

            # Calculate unrealized P&L
            unrealized_pl = (current_price - cost_basis) * total_shares
            by_ticker[ticker] = unrealized_pl
            total_unrealized += unrealized_pl
        
        return {
            'total': total_unrealized,
            'by_ticker': by_ticker
        }
    
    @staticmethod
    def calculate_unrealized_leap_pnl(
        df_open: pd.DataFrame,
        live_prices: Dict[str, float],
        spy_leap_pl: Optional[float] = None,
        live_options: Optional[list] = None,
    ) -> Dict[str, float]:
        """
        Calculate unrealized P&L for LEAP positions using live mark-to-market
        when Alpaca data is available, falling back to intrinsic-only.

        Priority order per LEAP:
          1. Live mid-price from Alpaca (if `live_options` has a match) — TRUE MTM
          2. Manual spy_leap_pl override (SPY only) — kept for back-compat
          3. Intrinsic value: max(0, spot - strike) * 100 * qty — conservative

        Args:
            df_open: Open positions DataFrame
            live_prices: Current market prices by ticker
            spy_leap_pl: Manual SPY LEAP P&L entry (back-compat)
            live_options: List of OptionsContract objects from Alpaca
                          (passed via st.session_state.open_positions_data)

        Returns:
            Dict with 'total', 'by_ticker' keys
        """
        if df_open is None or df_open.empty:
            return {'total': 0.0, 'by_ticker': {}}

        trade_type_col = get_field_name('trade_type', 'identity')
        leap_positions = df_open[df_open[trade_type_col] == 'LEAP'].copy()

        if leap_positions.empty:
            return {'total': 0.0, 'by_ticker': {}}

        # Build live-options lookup: (ticker, strike, right, expiry) -> mid_price
        live_lookup = {}
        if live_options:
            for c in live_options:
                try:
                    mid = c.last_price if c.last_price > 0 else (c.bid + c.ask) / 2
                    if mid > 0:
                        key = (c.underlying, float(c.strike), c.right, str(c.expiry))
                        live_lookup[key] = mid
                except Exception:
                    continue

        by_ticker = {}
        total_unrealized = 0.0

        ticker_col = get_field_name('ticker', 'identity')
        for ticker in leap_positions[ticker_col].unique():
            ticker_leaps = leap_positions[leap_positions[ticker_col] == ticker]
            ticker_leap_pl = 0.0
            used_live_mtm_for_ticker = False

            for idx in ticker_leaps.index:
                pos_idx = ticker_leaps.index.get_loc(idx)

                premium_per_share = DataAccess.get_trade_field(
                    ticker_leaps, pos_idx, 'premium_per_share', 'options', 0
                )
                contracts = DataAccess.get_contracts_for_option(ticker_leaps, pos_idx)

                if premium_per_share == 0 or contracts == 0:
                    continue

                leap_cost = premium_per_share * 100 * contracts
                contracts = float(pd.to_numeric(contracts, errors='coerce') or 0)

                strike = float(pd.to_numeric(DataAccess.get_trade_field(
                    ticker_leaps, pos_idx, 'strike', 'options', 0
                ), errors='coerce') or 0)
                if strike == 0:
                    continue

                # Try Alpaca live mid-price first (LEAPs are long calls)
                expiry_raw = DataAccess.get_trade_field(ticker_leaps, pos_idx, 'expiry_date', 'options', None)
                expiry_str = pd.to_datetime(expiry_raw, errors='coerce').strftime('%Y-%m-%d') if pd.notna(pd.to_datetime(expiry_raw, errors='coerce')) else ''
                live_key = (ticker, strike, 'C', expiry_str)
                live_mid = live_lookup.get(live_key)

                if live_mid and live_mid > 0:
                    # TRUE mark-to-market
                    leap_mtm_value = live_mid * 100 * contracts
                    leap_pl = leap_mtm_value - leap_cost
                    used_live_mtm_for_ticker = True
                else:
                    # Fallback to intrinsic
                    current_price = float(pd.to_numeric(live_prices.get(ticker, 0), errors='coerce') or 0)
                    if current_price == 0:
                        continue
                    intrinsic_value = max(0, current_price - strike) * 100 * contracts
                    leap_pl = intrinsic_value - leap_cost

                ticker_leap_pl += leap_pl

            # If user provided manual spy_leap_pl AND we couldn't compute MTM, honor it
            if ticker == 'SPY' and not used_live_mtm_for_ticker and spy_leap_pl is not None and spy_leap_pl != 0:
                ticker_leap_pl = spy_leap_pl

            if ticker_leap_pl != 0:
                by_ticker[ticker] = ticker_leap_pl
                total_unrealized += ticker_leap_pl

        return {
            'total': total_unrealized,
            'by_ticker': by_ticker
        }
    
    @staticmethod
    def calculate_comprehensive_pnl(
        df_trades: pd.DataFrame,
        df_open: pd.DataFrame,
        stock_avg_prices: Dict[str, float],
        live_prices: Dict[str, float],
        spy_leap_pl: Optional[float] = None,
        live_options: Optional[list] = None,
    ) -> Dict:
        """
        Calculate comprehensive P&L breakdown
        
        Returns:
        {
            'realized_pnl': float,
            'unrealized_stock_pnl': {'total': float, 'by_ticker': dict},
            'unrealized_leap_pnl': {'total': float, 'by_ticker': dict},
            'total_unrealized_pnl': float,
            'net_pnl': float
        }
        """
        # Realized P&L (from closed trades)
        realized_pnl = PnLCalculator.calculate_realized_pnl(df_trades)
        
        # Unrealized Stock P&L
        unrealized_stock = PnLCalculator.calculate_unrealized_stock_pnl(
            df_open, stock_avg_prices, live_prices
        )
        
        # Unrealized LEAP P&L (pass live_options for true MTM via Alpaca)
        unrealized_leap = PnLCalculator.calculate_unrealized_leap_pnl(
            df_open, live_prices, spy_leap_pl, live_options
        )
        
        # Total unrealized
        total_unrealized = unrealized_stock['total'] + unrealized_leap['total']
        
        # Net P&L (Mark-to-Market)
        net_pnl = realized_pnl + total_unrealized
        
        return {
            'realized_pnl': realized_pnl,
            'unrealized_stock_pnl': unrealized_stock,
            'unrealized_leap_pnl': unrealized_leap,
            'total_unrealized_pnl': total_unrealized,
            'net_pnl': net_pnl
        }
    
    @staticmethod
    def calculate_csp_allocation_vs_strategy(
        portfolio_deposit: float,
        stock_locked: float,
        csp_reserved: float,
        target_pct: float = 0.25
    ) -> Dict:
        """
        Calculate CSP allocation vs strategy target
        
        Args:
            portfolio_deposit: Starting capital
            stock_locked: Capital locked in stock
            csp_reserved: Capital reserved for CSPs
            target_pct: Target percentage of firepower (default 25%)
        
        Returns:
        {
            'firepower': float,
            'target_csp': float,
            'actual_csp': float,
            'allocation_pct': float,
            'target_pct': float,
            'over_under': float,
            'status': str
        }
        """
        # Firepower = Portfolio Deposit - Stock Locked
        firepower = portfolio_deposit - stock_locked
        
        # Target CSP deployment
        target_csp = firepower * target_pct
        
        # Actual CSP deployment
        actual_csp = csp_reserved
        
        # Allocation percentage
        allocation_pct = (actual_csp / firepower * 100) if firepower > 0 else 0.0
        
        # Over/Under
        over_under = actual_csp - target_csp
        
        # Status
        if allocation_pct <= target_pct * 100:
            status = "UNDER"
        elif allocation_pct <= target_pct * 100 * 1.2:  # Within 20% of target
            status = "ON_TARGET"
        else:
            status = "OVER"
        
        return {
            'firepower': firepower,
            'target_csp': target_csp,
            'actual_csp': actual_csp,
            'allocation_pct': allocation_pct,
            'target_pct': target_pct * 100,
            'over_under': over_under,
            'status': status
        }

    @staticmethod
    def calculate_csp_weekly_pacing(
        portfolio_deposit: float,
        stock_locked: float,
        df_open: pd.DataFrame,
        target_pct: float = 0.25,
        current_week_start: Optional[date] = None
    ) -> Dict:
        """
        Weekly CSP deployment pacing: deploy ~25% of Available Firepower in new CSPs each week.

        Returns:
            firepower, weekly_target, csp_opened_this_week (reserved $), status, over_under
        """
        today = date.today()
        if current_week_start is None:
            current_week_start = today - timedelta(days=today.weekday())
        end_of_week = current_week_start + timedelta(days=6)

        firepower = portfolio_deposit - stock_locked
        weekly_target = firepower * target_pct if firepower > 0 else 0.0

        csp_opened_this_week = 0.0
        if df_open is not None and not df_open.empty and 'TradeType' in df_open.columns:
            csp_open = df_open[df_open['TradeType'] == 'CSP'].copy()
            if not csp_open.empty and 'Date_open' in csp_open.columns:
                csp_open['Date_open'] = pd.to_datetime(csp_open['Date_open'], errors='coerce')
                csp_open['_date'] = csp_open['Date_open'].dt.date
                csp_open = csp_open.dropna(subset=['_date'])
                this_week = csp_open[
                    (csp_open['_date'] >= current_week_start) &
                    (csp_open['_date'] <= end_of_week)
                ]
                strike_col = get_field_name('strike', 'options')
                if strike_col not in this_week.columns:
                    strike_col = 'Option_Strike_Price_(USD)' if 'Option_Strike_Price_(USD)' in this_week.columns else None
                qty_col = get_field_name('quantity', 'position')
                if qty_col not in this_week.columns:
                    qty_col = 'Quantity'
                if strike_col and strike_col in this_week.columns and qty_col in this_week.columns:
                    strike = pd.to_numeric(this_week[strike_col], errors='coerce').fillna(0)
                    qty = pd.to_numeric(this_week[qty_col], errors='coerce').fillna(0)
                    csp_opened_this_week = (strike * 100 * qty).sum()

        over_under = csp_opened_this_week - weekly_target
        if weekly_target <= 0:
            status = "ON_TARGET"
        elif csp_opened_this_week >= weekly_target * 0.9 and csp_opened_this_week <= weekly_target * 1.2:
            status = "ON_TARGET"
        elif csp_opened_this_week < weekly_target:
            status = "UNDER"
        else:
            status = "OVER"

        return {
            'firepower': firepower,
            'weekly_target': weekly_target,
            'csp_opened_this_week': csp_opened_this_week,
            'over_under': over_under,
            'status': status,
            'target_pct': target_pct * 100
        }
