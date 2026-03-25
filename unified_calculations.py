"""
Unified Calculations - Single Source of Truth
Consolidates all capital and pacing calculations
"""

import pandas as pd
from datetime import datetime, date, timedelta
from typing import Dict, Optional, Set
from data_access import DataAccess
from data_schema import get_field_name
from config import WEEKLY_TARGET_PCT, TRADING_DAYS_PER_WEEK


class UnifiedCapitalCalculator:
    """Single source of truth for all capital calculations"""
    
    @staticmethod
    def calculate_capital_by_ticker(
        df_open: pd.DataFrame,
        portfolio_deposit: float,
        stock_avg_prices: Optional[Dict[str, float]] = None,
        live_prices: Optional[Dict[str, float]] = None,
        pmcc_tickers: Optional[Set[str]] = None
    ) -> Dict:
        """
        Calculate capital usage by ticker and total.

        Stock is reported at buy price (cost basis) and at current price (market value),
        with P/L = stock_at_current - stock_at_buy. stock_locked = stock_at_current
        so remaining BP and overleveraged use today's capital in use.
        
        Returns:
        {
            'by_ticker': {
                'TICKER': {
                    'stock_locked': float,  # = stock_at_current_price
                    'stock_at_buy_price': float,
                    'stock_at_current_price': float,
                    'stock_pl': float,
                    'csp_reserved': float,
                    'leap_sunk': float,
                    'total_committed': float
                }
            },
            'total': {
                'stock_locked': float,
                'stock_at_buy_price': float,
                'stock_at_current_price': float,
                'stock_pl': float,
                'csp_reserved': float,
                'leap_sunk': float,
                'total_committed': float,
                'remaining_bp': float,
                'overleveraged': bool
            }
        }
        """
        if pmcc_tickers is None:
            pmcc_tickers = set()
        
        if stock_avg_prices is None:
            stock_avg_prices = {}
        
        if live_prices is None:
            live_prices = {}
        
        # Filter to open positions only
        df_open_filtered = DataAccess.filter_open_positions(df_open)
        
        ticker_col = get_field_name('ticker', 'identity')
        trade_type_col = get_field_name('trade_type', 'identity')

        if df_open_filtered.empty:
            return {
                'by_ticker': {},
                'total': {
                    'stock_locked': 0.0,
                    'stock_at_buy_price': 0.0,
                    'stock_at_current_price': 0.0,
                    'stock_pl': 0.0,
                    'csp_reserved': 0.0,
                    'leap_sunk': 0.0,
                    'total_committed': 0.0,
                    'remaining_bp': portfolio_deposit,
                    'overleveraged': False
                }
            }

        # Get all unique tickers
        all_tickers = sorted(df_open_filtered[ticker_col].unique())

        by_ticker = {}
        total_stock_locked = 0.0
        total_stock_at_buy = 0.0
        total_stock_at_current = 0.0
        total_csp_reserved = 0.0
        total_leap_sunk = 0.0

        for ticker in all_tickers:
            ticker_data = {}

            # Separate PMCC vs non-PMCC logic
            if ticker in pmcc_tickers:
                # PMCC ticker: Only calculate LEAP sunk (CCs are covered)
                leap_sunk = DataAccess.get_leap_sunk(ticker, df_open_filtered)
                ticker_data = {
                    'stock_locked': 0.0,
                    'stock_at_buy_price': 0.0,
                    'stock_at_current_price': 0.0,
                    'stock_pl': 0.0,
                    'csp_reserved': 0.0,
                    'leap_sunk': leap_sunk,
                    'total_committed': leap_sunk
                }
                total_leap_sunk += leap_sunk
            else:
                # Non-PMCC ticker: stock at buy vs current, CSP reserved
                shares = DataAccess.get_stock_shares(ticker, df_open_filtered)
                # Excel fallback price (first STOCK row for this ticker)
                ticker_stock = df_open_filtered[
                    (df_open_filtered[ticker_col] == ticker) &
                    (df_open_filtered[trade_type_col] == 'STOCK')
                ]
                excel_price = 0.0
                if not ticker_stock.empty:
                    first_idx = ticker_stock.index[0]
                    pos_idx = ticker_stock.index.get_loc(first_idx)
                    p = DataAccess.get_trade_field(
                        ticker_stock, pos_idx, 'current_price', 'position', 0
                    )
                    if pd.notna(p) and p > 0:
                        excel_price = float(p)
                price_buy = stock_avg_prices.get(ticker) or excel_price or 0.0
                if price_buy and price_buy > 0:
                    price_buy = float(price_buy)
                else:
                    price_buy = 0.0
                price_current = live_prices.get(ticker) or excel_price or 0.0
                if price_current and price_current > 0:
                    price_current = float(price_current)
                else:
                    price_current = 0.0

                stock_at_buy = shares * price_buy
                stock_at_current = shares * price_current
                stock_pl = stock_at_current - stock_at_buy

                # stock_locked = market value (current) so remaining BP and overleveraged use today's capital
                stock_locked = stock_at_current
                csp_reserved = DataAccess.get_csp_reserved(ticker, df_open_filtered)

                ticker_data = {
                    'stock_locked': stock_locked,
                    'stock_at_buy_price': stock_at_buy,
                    'stock_at_current_price': stock_at_current,
                    'stock_pl': stock_pl,
                    'csp_reserved': csp_reserved,
                    'leap_sunk': 0.0,
                    'total_committed': stock_locked + csp_reserved
                }
                total_stock_locked += stock_locked
                total_stock_at_buy += stock_at_buy
                total_stock_at_current += stock_at_current
                total_csp_reserved += csp_reserved

            by_ticker[ticker] = ticker_data

        total_stock_pl = total_stock_at_current - total_stock_at_buy
        total_committed = total_stock_locked + total_csp_reserved + total_leap_sunk
        remaining_bp = portfolio_deposit - total_committed
        overleveraged = remaining_bp < 0

        return {
            'by_ticker': by_ticker,
            'total': {
                'stock_locked': total_stock_locked,
                'stock_at_buy_price': total_stock_at_buy,
                'stock_at_current_price': total_stock_at_current,
                'stock_pl': total_stock_pl,
                'csp_reserved': total_csp_reserved,
                'leap_sunk': total_leap_sunk,
                'total_committed': total_committed,
                'remaining_bp': remaining_bp,
                'overleveraged': overleveraged
            }
        }


class UnifiedPacingCalculator:
    """Single source of truth for daily/weekly pacing calculations"""
    
    @staticmethod
    def calculate_pacing(
        df_trades: pd.DataFrame,
        df_open: pd.DataFrame,
        portfolio_deposit: float,
        current_week_start: Optional[date] = None
    ) -> Dict:
        """
        Calculate weekly and daily pacing targets and progress
        
        Returns:
        {
            'weekly_target_capital': float,
            'weekly_target_premium': float,
            'daily_target_premium': float,
            'premium_collected_this_week': float,
            'premium_collected_today': float,
            'progress_pct': float,
            'remaining_premium': float,
            'days_left_in_week': int,
            'suggested_daily_premium': float
        }
        """
        # Calculate total deployed capital (for pacing target)
        capital_data = UnifiedCapitalCalculator.calculate_capital_by_ticker(
            df_open, portfolio_deposit
        )
        total_deployed_capital = capital_data['total']['total_committed']
        
        # Weekly target capital (25% of deployed)
        weekly_target_capital = total_deployed_capital * WEEKLY_TARGET_PCT
        
        # Estimated premium rate (2% per week - adjustable)
        estimated_premium_pct = 0.02
        weekly_target_premium = weekly_target_capital * estimated_premium_pct
        
        # Daily target
        daily_target_premium = weekly_target_premium / TRADING_DAYS_PER_WEEK
        
        # Calculate progress
        if current_week_start is None:
            now = datetime.now()
            current_week_start = (now - timedelta(days=now.weekday())).date()
        
        # Filter trades from this week
        date_open_col = get_field_name('date_open', 'position')
        trade_type_col = get_field_name('trade_type', 'identity')
        
        df_trades[date_open_col] = pd.to_datetime(df_trades[date_open_col], errors='coerce')
        this_week_trades = df_trades[
            (df_trades[date_open_col].dt.date >= current_week_start) &
            (df_trades[trade_type_col].isin(['CC', 'CSP']))
        ].copy()
        
        # Calculate premium collected this week
        premium_collected_this_week = 0.0
        for idx in this_week_trades.index:
            premium = DataAccess.get_total_premium(this_week_trades, idx)
            premium_collected_this_week += premium
        
        # Premium collected today
        today = date.today()
        today_trades = this_week_trades[
            this_week_trades[date_open_col].dt.date == today
        ].copy()
        
        premium_collected_today = 0.0
        for idx in today_trades.index:
            premium = DataAccess.get_total_premium(today_trades, idx)
            premium_collected_today += premium
        
        # Progress metrics
        remaining_premium = max(0, weekly_target_premium - premium_collected_this_week)
        progress_pct = (
            (premium_collected_this_week / weekly_target_premium * 100) 
            if weekly_target_premium > 0 else 0
        )
        
        # Days left in week
        now = datetime.now()
        days_until_friday = (4 - now.weekday()) % 7  # 4 = Friday
        days_left_in_week = max(1, days_until_friday + 1)
        
        suggested_daily_premium = (
            remaining_premium / days_left_in_week 
            if days_left_in_week > 0 else 0
        )
        
        return {
            'weekly_target_capital': weekly_target_capital,
            'weekly_target_premium': weekly_target_premium,
            'daily_target_premium': daily_target_premium,
            'premium_collected_this_week': premium_collected_this_week,
            'premium_collected_today': premium_collected_today,
            'progress_pct': progress_pct,
            'remaining_premium': remaining_premium,
            'days_left_in_week': days_left_in_week,
            'suggested_daily_premium': suggested_daily_premium
        }
