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


# ─────────────────────────────────────────────
# POT MAPPING — derived from StrategyType
# Base Pot:   WHEEL, PMCC, blank/NaN
# Active Pot: ActiveCore
# ─────────────────────────────────────────────
POT_BASE = "Base"
POT_ACTIVE = "Active"


def get_pot_for_strategy(strategy_type) -> str:
    """Return 'Base' or 'Active' based on StrategyType value."""
    if strategy_type is None:
        return POT_BASE
    s = str(strategy_type).strip()
    if s.lower() in ('activecore', 'active', 'active core'):
        return POT_ACTIVE
    return POT_BASE


def filter_by_pot(df: pd.DataFrame, pot: str) -> pd.DataFrame:
    """Filter a trades DataFrame to a single pot ('Base' or 'Active')."""
    if df is None or df.empty or 'StrategyType' not in df.columns:
        return df
    if pot == POT_ACTIVE:
        return df[df['StrategyType'].astype(str).str.strip().str.lower().isin(['activecore', 'active', 'active core'])].copy()
    else:  # Base
        return df[~df['StrategyType'].astype(str).str.strip().str.lower().isin(['activecore', 'active', 'active core'])].copy()


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

                # stock_locked = cost basis (buy price) — this is the cash deployed.
                # Market value drives unrealized P&L (separate metric), not BP.
                stock_locked = stock_at_buy
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
        # Full deployment tracking: Stock + LEAP + CSP = total capital used
        # No leverage policy: remaining_bp should be >= 0 at all times
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

    @staticmethod
    def calculate_tiger_margin(df_open: pd.DataFrame, live_prices: Optional[Dict[str, float]] = None) -> Dict:
        """
        Estimate Tiger Brokers' actual CSP margin requirement (vs. cash-secured policy).

        Tiger's formula (per their docs):
          margin = max(30% × spot × 100 + premium × 100 − OTM_amount, 0)
          capped at strike × 100 (i.e. cannot exceed full cash-secured)
          Note: scales up to 100% in high vol — this is a static 30% baseline.

        Returns:
          {
            'csp_margin': float,        # Sum across all open CSPs
            'csp_cash_secured': float,  # Full cash-secured (strike × 100 × qty)
            'headroom': float,          # Cash freed if Tiger margin used instead
            'by_position': list[dict]   # Per-CSP breakdown for transparency
          }
        """
        if live_prices is None:
            live_prices = {}

        df_open_filtered = DataAccess.filter_open_positions(df_open)
        if df_open_filtered.empty:
            return {'csp_margin': 0.0, 'csp_cash_secured': 0.0, 'headroom': 0.0, 'by_position': []}

        trade_type_col = get_field_name('trade_type', 'identity')
        ticker_col = get_field_name('ticker', 'identity')

        csps = df_open_filtered[df_open_filtered[trade_type_col] == 'CSP']
        if csps.empty:
            return {'csp_margin': 0.0, 'csp_cash_secured': 0.0, 'headroom': 0.0, 'by_position': []}

        total_tiger = 0.0
        total_cs = 0.0
        rows = []

        for _, row in csps.iterrows():
            ticker = row[ticker_col]
            strike = float(pd.to_numeric(row.get('Option_Strike_Price_(USD)', 0), errors='coerce') or 0)
            qty = abs(float(pd.to_numeric(row.get('Quantity', 0), errors='coerce') or 0))
            premium = float(pd.to_numeric(row.get('OptPremium', 0), errors='coerce') or 0)
            spot = float(live_prices.get(ticker, 0) or 0)

            if strike <= 0 or qty <= 0:
                continue

            cs_full = strike * 100 * qty  # 100% cash-secured

            if spot <= 0:
                # No spot — fall back to cash-secured (conservative)
                tiger = cs_full
            else:
                spot_value = spot * 100 * qty
                premium_total = premium * 100 * qty
                otm_amount = max(0.0, spot - strike) * 100 * qty
                tiger = max(spot_value * 0.30 + premium_total - otm_amount, 0.0)
                tiger = min(tiger, cs_full)  # cap at cash-secured

            total_tiger += tiger
            total_cs += cs_full
            rows.append({
                'TradeID': row.get('TradeID', ''),
                'Ticker': ticker,
                'Strike': strike,
                'Spot': spot,
                'Contracts': int(qty),
                'CashSecured': cs_full,
                'TigerMargin': tiger,
                'Headroom': cs_full - tiger,
            })

        return {
            'csp_margin': total_tiger,
            'csp_cash_secured': total_cs,
            'headroom': total_cs - total_tiger,
            'by_position': rows,
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

        # Phase 7.4: weekly premium target = weekly_target_capital * actual_yield
        # Use portfolio_deposit * WEEKLY_TARGET_PCT * 0.5% as a reasonable target
        # (0.5% of weekly capital deployed = ~25% annualized, typical for CSP income)
        weekly_target_premium = weekly_target_capital * 0.005
        
        # Daily target
        daily_target_premium = weekly_target_premium / TRADING_DAYS_PER_WEEK
        
        # Calculate progress
        if current_week_start is None:
            now = datetime.now()
            current_week_start = (now - timedelta(days=now.weekday())).date()
        
        # Filter trades from this week
        date_open_col = get_field_name('date_open', 'position')
        trade_type_col = get_field_name('trade_type', 'identity')
        
        # Phase 8.4: use .copy() to avoid mutating caller's DataFrame
        df_trades = df_trades.copy()
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
