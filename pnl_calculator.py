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
            
            # Get cost basis (average inventory price)
            cost_basis = stock_avg_prices.get(ticker, 0.0)
            if cost_basis == 0:
                # Fallback to live price (no P/L if no cost basis)
                continue
            
            # Get current market price
            current_price = live_prices.get(ticker, 0.0)
            if current_price == 0:
                # Can't calculate P/L without current price
                continue
            
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
        spy_leap_pl: Optional[float] = None
    ) -> Dict[str, float]:
        """
        Calculate unrealized P&L for LEAP positions
        
        Args:
            df_open: Open positions DataFrame
            live_prices: Current market prices by ticker
            spy_leap_pl: Manual SPY LEAP P&L entry (if available)
        
        Returns:
            Dict with 'total', 'by_ticker' keys
        """
        if df_open is None or df_open.empty:
            return {'total': 0.0, 'by_ticker': {}}
        
        # Filter to LEAP positions only
        trade_type_col = get_field_name('trade_type', 'identity')
        leap_positions = df_open[df_open[trade_type_col] == 'LEAP'].copy()
        
        if leap_positions.empty:
            return {'total': 0.0, 'by_ticker': {}}
        
        by_ticker = {}
        total_unrealized = 0.0
        
        # Group by ticker
        ticker_col = get_field_name('ticker', 'identity')
        for ticker in leap_positions[ticker_col].unique():
            ticker_leaps = leap_positions[leap_positions[ticker_col] == ticker]
            
            # Special handling for SPY LEAP (use manual entry if available)
            if ticker == 'SPY' and spy_leap_pl is not None:
                by_ticker[ticker] = spy_leap_pl
                total_unrealized += spy_leap_pl
                continue
            
            # Calculate LEAP P&L for other tickers
            ticker_leap_pl = 0.0
            for idx in ticker_leaps.index:
                pos_idx = ticker_leaps.index.get_loc(idx)
                
                # Get LEAP cost
                premium_per_share = DataAccess.get_trade_field(
                    ticker_leaps, pos_idx, 'premium_per_share', 'options', 0
                )
                contracts = DataAccess.get_contracts_for_option(ticker_leaps, pos_idx)
                
                if premium_per_share == 0 or contracts == 0:
                    # Missing premium data - can't calculate
                    continue
                
                leap_cost = premium_per_share * 100 * contracts
                
                # Get current underlying price
                current_price = live_prices.get(ticker, 0.0)
                if current_price == 0:
                    continue
                
                # Get strike
                strike = DataAccess.get_trade_field(
                    ticker_leaps, pos_idx, 'strike', 'options', 0
                )
                
                if strike == 0:
                    continue
                
                # Calculate intrinsic value (conservative - ignores time value)
                intrinsic_value = max(0, current_price - strike) * 100 * contracts
                leap_pl = intrinsic_value - leap_cost
                ticker_leap_pl += leap_pl
            
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
        spy_leap_pl: Optional[float] = None
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
        
        # Unrealized LEAP P&L
        unrealized_leap = PnLCalculator.calculate_unrealized_leap_pnl(
            df_open, live_prices, spy_leap_pl
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
