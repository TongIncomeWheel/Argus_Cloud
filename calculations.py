"""
Business logic calculations for Income Wheel
"""
from datetime import datetime, date, timedelta
import pandas as pd
from typing import Dict, Set, Optional
from config import (
    WEEKLY_TARGET_PCT, 
    TRADING_DAYS_PER_WEEK,
    CALL_RISK_HIGH_DTE,
    CALL_RISK_MEDIUM_BUFFER,
    MARGIN_REQUIREMENT_PCT
)


class CapitalCalculator:
    """Calculate capital deployment metrics"""
    
    @staticmethod
    def _is_pmcc_cc(ticker: str, df_open: pd.DataFrame) -> bool:
        """
        Check if a CC position is part of a PMCC (Poor Man's Covered Call).
        PMCC = LEAP + CC for the same ticker.
        
        Args:
            ticker: Ticker symbol
            df_open: DataFrame of open positions
        
        Returns:
            True if this ticker has both LEAP and CC positions (PMCC)
        """
        ticker_positions = df_open[df_open['Ticker'] == ticker]
        has_leap = (ticker_positions['TradeType'] == 'LEAP').any()
        has_cc = (ticker_positions['TradeType'] == 'CC').any()
        return has_leap and has_cc
    
    @staticmethod
    def calculate_deployed_capital(df_open: pd.DataFrame, live_prices: dict = None) -> dict:
        """
        Calculate total capital deployed across all positions
        
        Args:
            df_open: DataFrame of open positions
            live_prices: Optional dict of {ticker: price} from IBKR for fallback when Price_of_current_underlying_(USD) is NaN
        
        Returns:
            dict with:
                - total_deployed: Total capital at risk
                - csp_capital: Capital in CSP positions
                - stock_capital: Capital in stock positions
                - cc_capital: Capital in CC (should be 0, covered by stock)
                - margin_used: Estimated margin requirement (will be overridden by user input)
        """
        if df_open.empty:
            return {
                "total_deployed": 0.0,
                "csp_capital": 0.0,
                "stock_capital": 0.0,
                "cc_capital": 0.0,
                "margin_used": 0.0
            }
        
        if live_prices is None:
            live_prices = {}
        
        # CSP capital = strike * 100 * qty for all open CSPs
        csp_positions = df_open[df_open['TradeType'] == 'CSP'].copy()
        # Handle NaN values
        csp_positions['Option_Strike_Price_(USD)'] = pd.to_numeric(csp_positions['Option_Strike_Price_(USD)'], errors='coerce').fillna(0)
        csp_positions['Quantity'] = pd.to_numeric(csp_positions['Quantity'], errors='coerce').fillna(0)
        csp_capital = (csp_positions['Option_Strike_Price_(USD)'] * 100 * csp_positions['Quantity']).sum()
        
        # Stock capital = current/entry price * qty for all stock positions (includes STOCK + LEAP)
        # Price_of_current_underlying_(USD) represents the price at which stock was acquired or current value
        # For STOCK/LEAP: Use Open_lots (actual shares) if available, else Quantity * 100
        # Use live_prices as fallback when Price_of_current_underlying_(USD) is NaN
        stock_positions = df_open[df_open['TradeType'] == 'STOCK'].copy()
        leaps_positions = df_open[df_open['TradeType'] == 'LEAP'].copy()
        
        # Handle NaN values for STOCK - use live prices as fallback
        stock_positions['Price_of_current_underlying_(USD)'] = pd.to_numeric(stock_positions['Price_of_current_underlying_(USD)'], errors='coerce')
        # Fill NaN with live prices if available
        for idx, row in stock_positions.iterrows():
            if pd.isna(stock_positions.at[idx, 'Price_of_current_underlying_(USD)']):
                ticker = row['Ticker']
                if ticker in live_prices and live_prices[ticker] is not None:
                    stock_positions.at[idx, 'Price_of_current_underlying_(USD)'] = live_prices[ticker]
                else:
                    stock_positions.at[idx, 'Price_of_current_underlying_(USD)'] = 0.0
        
        if 'Open_lots' in stock_positions.columns:
            stock_positions['Shares'] = pd.to_numeric(stock_positions['Open_lots'], errors='coerce').fillna(0)
        else:
            stock_positions['Shares'] = pd.to_numeric(stock_positions['Quantity'], errors='coerce').fillna(0) * 100
        stock_capital = (stock_positions['Price_of_current_underlying_(USD)'] * stock_positions['Shares']).sum()
        
        # LEAP capital = contract premium paid (OptPremium × Quantity)
        # For buying LEAPs, capital is the premium paid for the contracts, not underlying stock value
        # OptPremium is the total premium per contract (not per share), so we don't multiply by 100
        # Margin is 100% of the contract cost
        leaps_positions['OptPremium'] = pd.to_numeric(leaps_positions['OptPremium'], errors='coerce').fillna(0)
        leaps_positions['Quantity'] = pd.to_numeric(leaps_positions['Quantity'], errors='coerce').fillna(0)
        # Use absolute value for quantity (long positions)
        if 'Direction' in leaps_positions.columns:
            leaps_positions['Quantity'] = leaps_positions['Quantity'].abs()  # Long positions should be positive
        leaps_capital = (leaps_positions['OptPremium'] * leaps_positions['Quantity']).sum()
        
        # Total stock capital includes both STOCK and LEAP
        stock_capital = stock_capital + leaps_capital
        
        # CC capital calculation:
        # - Regular CC (covered by stock): capital = 0
        # - PMCC CC (LEAP + CC): capital = strike * 100 * qty (requires capital/margin)
        cc_positions = df_open[df_open['TradeType'] == 'CC'].copy()
        cc_capital = 0.0
        
        for _, cc_row in cc_positions.iterrows():
            ticker = cc_row['Ticker']
            # Check if this CC is part of a PMCC
            if CapitalCalculator._is_pmcc_cc(ticker, df_open):
                # PMCC CC requires capital: strike * 100 * qty
                strike = pd.to_numeric(cc_row.get('Option_Strike_Price_(USD)', 0), errors='coerce') or 0
                qty = pd.to_numeric(cc_row.get('Quantity', 0), errors='coerce') or 0
                cc_capital += strike * 100 * qty
            # Regular CC (covered by stock) has capital = 0, so no addition needed
        
        # Total deployed
        total_deployed = csp_capital + stock_capital + cc_capital
        
        # Margin used is now calculated dynamically based on user input, not a fixed percentage here
        margin_used = 0.0
        
        return {
            "total_deployed": total_deployed,
            "csp_capital": csp_capital,
            "stock_capital": stock_capital,
            "cc_capital": cc_capital,
            "margin_used": margin_used
        }
    
    @staticmethod
    def calculate_inventory(df_open: pd.DataFrame) -> dict:
        """
        Calculate position inventory by type
        
        Returns:
            dict with counts:
                - cc_count: Number of open CC contracts
                - csp_count: Number of open CSP contracts
                - stock_shares: Total stock shares held (includes STOCK + LEAP)
                - positions_by_ticker: Dict of {ticker: {cc, csp, stock, leaps, cc_coverage_ratio}}
        """
        if df_open.empty:
            return {
                "cc_count": 0,
                "csp_count": 0,
                "stock_shares": 0,
                "positions_by_ticker": {}
            }
        
        # Ensure 'Quantity' is numeric, coercing errors to NaN, then fill NaN with 0
        df_open = df_open.copy()
        df_open['Quantity'] = pd.to_numeric(df_open['Quantity'], errors='coerce').fillna(0)
        
        # Filter to only Open status (double-check)
        df_open = df_open[df_open['Status'] == 'Open']
        
        # CC Quantity might be negative (short positions), use absolute value
        cc_count = abs(df_open[df_open['TradeType'] == 'CC']['Quantity'].sum() or 0)
        csp_count = df_open[df_open['TradeType'] == 'CSP']['Quantity'].sum() or 0
        # Stock shares includes both STOCK and LEAP (LEAPs are long-term stock positions)
        # For STOCK/LEAP: Quantity is in "lots" (1 lot = 100 shares), Open_lots is actual shares
        # Use Open_lots for accurate share count
        stock_positions = df_open[df_open['TradeType'] == 'STOCK']
        leaps_positions = df_open[df_open['TradeType'] == 'LEAP']
        
        # Use Open_lots if available, otherwise fall back to Quantity * 100
        if 'Open_lots' in stock_positions.columns:
            stock_shares = pd.to_numeric(stock_positions['Open_lots'], errors='coerce').fillna(0).sum() or 0
        else:
            stock_shares = (stock_positions['Quantity'] * 100).sum() or 0
        
        # For LEAPs: Use absolute value since Direction="Buy" means long position
        # Even if Quantity/Open_lots is negative, if Direction="Buy", it's a long position
        if 'Open_lots' in leaps_positions.columns:
            leaps_shares = pd.to_numeric(leaps_positions['Open_lots'], errors='coerce').fillna(0)
            # Check Direction - if "Buy", use absolute value (long position)
            if 'Direction' in leaps_positions.columns:
                leaps_shares = leaps_shares.abs()  # Long positions should be positive
            leaps_shares = leaps_shares.sum() or 0
        else:
            leaps_qty = pd.to_numeric(leaps_positions['Quantity'], errors='coerce').fillna(0)
            # Check Direction - if "Buy", use absolute value (long position)
            if 'Direction' in leaps_positions.columns:
                leaps_qty = leaps_qty.abs()  # Long positions should be positive
            leaps_shares = (leaps_qty * 100).sum() or 0
        
        total_stock_shares = stock_shares + leaps_shares
        
        # Convert to int (already handled NaN above)
        cc_count = int(cc_count) if not pd.isna(cc_count) else 0
        csp_count = int(csp_count) if not pd.isna(csp_count) else 0
        stock_shares = int(stock_shares) if not pd.isna(stock_shares) else 0
        leaps_shares = int(leaps_shares) if not pd.isna(leaps_shares) else 0
        total_stock_shares = int(total_stock_shares) if not pd.isna(total_stock_shares) else 0
        
        # By ticker
        positions_by_ticker = {}
        for ticker in df_open['Ticker'].unique():
            ticker_data = df_open[df_open['Ticker'] == ticker]
            # CC Quantity is negative (short positions), use absolute value
            cc_qty = abs(ticker_data[ticker_data['TradeType'] == 'CC']['Quantity'].sum() or 0)
            csp_qty = ticker_data[ticker_data['TradeType'] == 'CSP']['Quantity'].sum() or 0
            
            # For STOCK/LEAP: Use Open_lots (actual shares) if available, else Quantity * 100
            stock_data = ticker_data[ticker_data['TradeType'] == 'STOCK']
            leaps_data = ticker_data[ticker_data['TradeType'] == 'LEAP']
            
            if 'Open_lots' in stock_data.columns:
                stock_shares = pd.to_numeric(stock_data['Open_lots'], errors='coerce').fillna(0).sum() or 0
            else:
                stock_shares = (stock_data['Quantity'] * 100).sum() or 0
            
            # For LEAPs: Use absolute value since Direction="Buy" means long position
            if 'Open_lots' in leaps_data.columns:
                leaps_shares = pd.to_numeric(leaps_data['Open_lots'], errors='coerce').fillna(0)
                # LEAPs are long positions - always use absolute value (treat like stock)
                leaps_shares = leaps_shares.abs()  # Long positions should be positive
                leaps_shares = leaps_shares.sum() or 0
            else:
                leaps_qty = pd.to_numeric(leaps_data['Quantity'], errors='coerce').fillna(0)
                # LEAPs are long positions - always use absolute value (treat like stock)
                leaps_qty = leaps_qty.abs()  # Long positions should be positive
                leaps_shares = (leaps_qty * 100).sum() or 0
            
            # Also get Quantity for display (in lots)
            stock_qty_lots = stock_data['Quantity'].sum() or 0
            leaps_qty_lots = leaps_data['Quantity'].sum() or 0
            
            # Handle NaN
            if pd.isna(cc_qty):
                cc_qty = 0
            if pd.isna(csp_qty):
                csp_qty = 0
            if pd.isna(stock_shares):
                stock_shares = 0
            if pd.isna(leaps_shares):
                leaps_shares = 0
            if pd.isna(stock_qty_lots):
                stock_qty_lots = 0
            if pd.isna(leaps_qty_lots):
                leaps_qty_lots = 0
            
            # Calculate CC coverage ratio
            # Priority: If LEAPs exist, use CC/LEAP ratio
            # Otherwise: If stock exists, show stock coverage % (stock shares / CC shares needed)
            leaps_qty_abs = abs(int(leaps_qty_lots)) if leaps_qty_lots != 0 else 0
            cc_qty_int = int(cc_qty) if cc_qty > 0 else 0
            
            if cc_qty_int == 0:
                cc_coverage_ratio = None  # No CCs, so no coverage ratio
            elif leaps_qty_abs > 0:
                # Has LEAPs: show CC/LEAP ratio (multiply by 100 for %)
                cc_coverage_ratio = cc_qty_int / leaps_qty_abs  # Ratio (CC contracts / LEAPs)
            elif stock_shares > 0:
                # Has CCs and stock but no LEAPs: show coverage % (CC shares needed / stock shares)
                # This shows what % of your shares are committed to covering calls
                cc_shares_needed = cc_qty_int * 100
                cc_coverage_ratio = cc_shares_needed / stock_shares  # Coverage ratio (1.0 = 100% of shares used)
            else:
                # Has CCs but no stock/LEAPs: uncovered
                cc_coverage_ratio = -1.0  # Flag for "uncovered"
            
            # Total stock shares (STOCK + LEAP combined)
            total_shares = int(stock_shares) + int(leaps_shares)
            
            positions_by_ticker[ticker] = {
                "cc": int(cc_qty),
                "csp": int(csp_qty),
                "stock": int(stock_shares),  # Actual shares, not lots
                "stock_lots": int(stock_qty_lots),  # Quantity in lots for reference
                "leaps": int(leaps_shares),  # Actual shares, not lots
                "leaps_lots": int(leaps_qty_lots),  # Quantity in lots for reference
                "total_stock": total_shares,  # STOCK + LEAP combined (in shares)
                "cc_coverage_ratio": cc_coverage_ratio
            }
        
        return {
            "cc_count": int(cc_count),
            "csp_count": int(csp_count),
            "stock_shares": int(total_stock_shares),  # Includes LEAPs
            "positions_by_ticker": positions_by_ticker
        }


class PremiumCalculator:
    """Calculate premium and yield metrics"""
    
    @staticmethod
    def calculate_premium_stats(df_trades: pd.DataFrame, df_open: pd.DataFrame = None, period: str = 'week') -> dict:
        """
        Calculate premium stats based on expiry dates
        
        Args:
            df_trades: All trades dataframe
            df_open: Open positions dataframe (for "to be collected")
            period: 'week', 'month', or 'ytd'
        
        Returns:
            dict with premium stats
        """
        now = datetime.now()
        today = date.today()
        
        # Ensure dates are datetime
        df_trades = df_trades.copy()
        df_trades['Expiry_Date'] = pd.to_datetime(df_trades['Expiry_Date'], errors='coerce')
        df_trades['Date_open'] = pd.to_datetime(df_trades['Date_open'], errors='coerce')
        df_trades['Date_closed'] = pd.to_datetime(df_trades['Date_closed'], errors='coerce')
        
        if period == 'week':
            # Current week (Monday to Sunday)
            start_of_week = today - timedelta(days=today.weekday())
            end_of_week = start_of_week + timedelta(days=6)
            
            # Premium to be collected this week (Expiring + Open)
            if df_open is not None:
                df_open_copy = df_open.copy()
                df_open_copy['Expiry_Date'] = pd.to_datetime(df_open_copy['Expiry_Date'], errors='coerce')
                # Filter out NaT dates first
                df_open_valid = df_open_copy[df_open_copy['Expiry_Date'].notna()].copy()
                # Convert to date for comparison
                df_open_valid['Expiry_Date_Date'] = df_open_valid['Expiry_Date'].dt.date
                expiring_this_week = df_open_valid[
                    (df_open_valid['Expiry_Date_Date'] >= start_of_week) &
                    (df_open_valid['Expiry_Date_Date'] <= end_of_week) &
                    (df_open_valid['TradeType'].isin(['CC', 'CSP']))
                ].copy()
                expiring_this_week['OptPremium'] = pd.to_numeric(expiring_this_week['OptPremium'], errors='coerce').fillna(0)
                expiring_this_week['Quantity'] = pd.to_numeric(expiring_this_week['Quantity'], errors='coerce').fillna(0)
                premium_to_collect = (expiring_this_week['OptPremium'] * 100 * expiring_this_week['Quantity']).sum()
            else:
                premium_to_collect = 0.0
            
            # Premium collected this week (Expiring this week + Closed)
            # Filter out NaT dates first
            df_trades_week = df_trades.copy()
            df_trades_week['Expiry_Date'] = pd.to_datetime(df_trades_week['Expiry_Date'], errors='coerce')
            df_trades_valid = df_trades_week[df_trades_week['Expiry_Date'].notna()].copy()
            # Convert to date for comparison
            df_trades_valid['Expiry_Date_Date'] = df_trades_valid['Expiry_Date'].dt.date
            
            closed_this_week = df_trades_valid[
                (df_trades_valid['Expiry_Date_Date'] >= start_of_week) &
                (df_trades_valid['Expiry_Date_Date'] <= end_of_week) &
                (df_trades_valid['Status'].str.lower() == 'closed') &
                (df_trades_valid['TradeType'].isin(['CC', 'CSP']))
            ].copy()
            
            # Use Actual_Profit_(USD) if available (includes BTC losses), otherwise calculate from OptPremium
            if 'Actual_Profit_(USD)' in closed_this_week.columns:
                closed_this_week['Actual_Profit_(USD)'] = pd.to_numeric(closed_this_week['Actual_Profit_(USD)'], errors='coerce').fillna(0)
                # Use Actual_Profit_(USD) directly (even if 0 or negative)
                closed_this_week['Profit_Used'] = closed_this_week['Actual_Profit_(USD)']
            else:
                # Fallback: calculate from OptPremium
                closed_this_week['OptPremium'] = pd.to_numeric(closed_this_week['OptPremium'], errors='coerce').fillna(0)
                closed_this_week['Quantity'] = pd.to_numeric(closed_this_week['Quantity'], errors='coerce').fillna(0)
                closed_this_week['Profit_Used'] = closed_this_week['OptPremium'] * 100 * closed_this_week['Quantity']
            
            premium_collected = closed_this_week['Profit_Used'].sum()
            
            return {
                "period": period,
                "premium_to_collect": premium_to_collect,
                "premium_collected": premium_collected,
                "total_premium": premium_to_collect + premium_collected,
                "trade_count": len(expiring_this_week) if df_open is not None else 0
            }
        
        elif period == 'month':
            # Month to Date (based on expiry date this month)
            start_of_month = date(today.year, today.month, 1)
            end_of_month = date(today.year, today.month + 1, 1) - timedelta(days=1) if today.month < 12 else date(today.year + 1, 1, 1) - timedelta(days=1)
            
            # Premium to be collected this month (Expiring + Open)
            if df_open is not None:
                df_open_copy = df_open.copy()
                df_open_copy['Expiry_Date'] = pd.to_datetime(df_open_copy['Expiry_Date'], errors='coerce')
                expiring_this_month = df_open_copy[
                    (df_open_copy['Expiry_Date'].dt.date >= start_of_month) &
                    (df_open_copy['Expiry_Date'].dt.date <= end_of_month) &
                    (df_open_copy['TradeType'].isin(['CC', 'CSP']))
                ].copy()
                expiring_this_month['OptPremium'] = pd.to_numeric(expiring_this_month['OptPremium'], errors='coerce').fillna(0)
                expiring_this_month['Quantity'] = pd.to_numeric(expiring_this_month['Quantity'], errors='coerce').fillna(0)
                premium_to_collect = (expiring_this_month['OptPremium'] * 100 * expiring_this_month['Quantity']).sum()
            else:
                premium_to_collect = 0.0
            
            # Premium collected this month: Only closed trades that expired this month
            # Simple data extraction: Status='closed' AND Expiry_Date in this month
            # Ensure we have a fresh copy and filter out NaT dates first
            df_trades_month = df_trades.copy()
            df_trades_month['Expiry_Date'] = pd.to_datetime(df_trades_month['Expiry_Date'], errors='coerce')
            df_trades_valid = df_trades_month[df_trades_month['Expiry_Date'].notna()].copy()
            
            # Convert to date for comparison
            df_trades_valid['Expiry_Date_Date'] = df_trades_valid['Expiry_Date'].dt.date
            
            closed_this_month = df_trades_valid[
                (df_trades_valid['Status'].str.lower() == 'closed') &
                (df_trades_valid['Expiry_Date_Date'] >= start_of_month) &
                (df_trades_valid['Expiry_Date_Date'] <= end_of_month) &
                (df_trades_valid['TradeType'].isin(['CC', 'CSP']))
            ].copy()
            
            # Remove duplicates by TradeID to avoid double counting
            closed_this_month = closed_this_month.drop_duplicates(subset=['TradeID'], keep='first')
            
            # Extract premium, quantity, and Actual_Profit_(USD), handle NaN
            closed_this_month['OptPremium'] = pd.to_numeric(closed_this_month['OptPremium'], errors='coerce').fillna(0)
            closed_this_month['Quantity'] = pd.to_numeric(closed_this_month['Quantity'], errors='coerce').fillna(0)
            
            # Use Actual_Profit_(USD) if available (includes BTC losses), otherwise calculate from OptPremium
            if 'Actual_Profit_(USD)' in closed_this_month.columns:
                closed_this_month['Actual_Profit_(USD)'] = pd.to_numeric(closed_this_month['Actual_Profit_(USD)'], errors='coerce').fillna(0)
                # Use Actual_Profit_(USD) if it's not zero, otherwise fall back to OptPremium calculation
                closed_this_month['Profit_Used'] = closed_this_month.apply(
                    lambda row: row['Actual_Profit_(USD)'] if row['Actual_Profit_(USD)'] != 0 else (row['OptPremium'] * 100 * row['Quantity']),
                    axis=1
                )
            else:
                # Fallback: calculate from OptPremium
                closed_this_month['Profit_Used'] = closed_this_month['OptPremium'] * 100 * closed_this_month['Quantity']
            
            # Calculate total premium collected (includes losses from BTC)
            premium_collected = closed_this_month['Profit_Used'].sum()
            
            return {
                "period": period,
                "premium_to_collect": premium_to_collect,
                "premium_collected": premium_collected,
                "total_premium": premium_collected,  # Month to Date = only collected (closed)
                "trade_count": len(closed_this_month),
                "avg_premium_per_trade": premium_collected / len(closed_this_month) if len(closed_this_month) > 0 else 0
            }
        
        elif period == 'ytd':
            # YTD (Expiry this year - only closed trades)
            # Simple data extraction: Status='closed' AND Expiry_Date in this year
            start_of_year = date(today.year, 1, 1)
            end_of_year = date(today.year, 12, 31)
            
            # Ensure we have a fresh copy and filter out NaT dates first
            df_trades_ytd = df_trades.copy()
            df_trades_ytd['Expiry_Date'] = pd.to_datetime(df_trades_ytd['Expiry_Date'], errors='coerce')
            df_trades_valid = df_trades_ytd[df_trades_ytd['Expiry_Date'].notna()].copy()
            
            # Convert to date for comparison
            df_trades_valid['Expiry_Date_Date'] = df_trades_valid['Expiry_Date'].dt.date
            
            ytd_trades = df_trades_valid[
                (df_trades_valid['Status'].str.lower() == 'closed') &
                (df_trades_valid['Expiry_Date_Date'] >= start_of_year) &
                (df_trades_valid['Expiry_Date_Date'] <= end_of_year) &
                (df_trades_valid['TradeType'].isin(['CC', 'CSP']))
            ].copy()
            
            # Remove duplicates by TradeID to avoid double counting
            ytd_trades = ytd_trades.drop_duplicates(subset=['TradeID'], keep='first')
            
            # Extract premium, quantity, and Actual_Profit_(USD), handle NaN
            ytd_trades['OptPremium'] = pd.to_numeric(ytd_trades['OptPremium'], errors='coerce').fillna(0)
            ytd_trades['Quantity'] = pd.to_numeric(ytd_trades['Quantity'], errors='coerce').fillna(0)
            
            # Use Actual_Profit_(USD) if available (includes BTC losses), otherwise calculate from OptPremium
            if 'Actual_Profit_(USD)' in ytd_trades.columns:
                ytd_trades['Actual_Profit_(USD)'] = pd.to_numeric(ytd_trades['Actual_Profit_(USD)'], errors='coerce').fillna(0)
                # Use Actual_Profit_(USD) if it's not zero, otherwise fall back to OptPremium calculation
                ytd_trades['Profit_Used'] = ytd_trades.apply(
                    lambda row: row['Actual_Profit_(USD)'] if row['Actual_Profit_(USD)'] != 0 else (row['OptPremium'] * 100 * row['Quantity']),
                    axis=1
                )
            else:
                # Fallback: calculate from OptPremium
                ytd_trades['Profit_Used'] = ytd_trades['OptPremium'] * 100 * ytd_trades['Quantity']
            
            # Calculate total premium collected (includes losses from BTC)
            total_premium = ytd_trades['Profit_Used'].sum()
            
            return {
                "period": period,
                "total_premium": total_premium,
                "trade_count": len(ytd_trades),
                "avg_premium_per_trade": total_premium / len(ytd_trades) if len(ytd_trades) > 0 else 0
            }
        
        else:
            raise ValueError(f"Invalid period: {period}")
    
    @staticmethod
    def calculate_yield_pa(premium: float, capital: float, days: int) -> float:
        """
        Calculate annualized yield percentage
        
        Args:
            premium: Premium collected
            capital: Capital deployed
            days: Number of days the trade is held
        
        Returns:
            Annualized yield %
        """
        if capital == 0 or days == 0:
            return 0.0
        
        daily_yield = premium / capital
        annual_yield = (daily_yield / days) * 365
        
        return annual_yield * 100  # Return as percentage


class QuotaCalculator:
    """Calculate daily/weekly selling quotas
    
    DEPRECATED: Use UnifiedPacingCalculator instead.
    This class is kept for backward compatibility.
    """
    
    @staticmethod
    def calculate_weekly_quota(total_deployed_capital: float) -> dict:
        """
        DEPRECATED: Use UnifiedPacingCalculator.calculate_pacing() instead.
        """
        from unified_calculations import UnifiedPacingCalculator
        from datetime import date
        
        # Create dummy DataFrames for compatibility
        import pandas as pd
        df_trades = pd.DataFrame()
        df_open = pd.DataFrame()
        
        # Use unified calculator
        pacing_data = UnifiedPacingCalculator.calculate_pacing(
            df_trades, df_open, total_deployed_capital * 4  # Approximate portfolio deposit
        )
        
        return {
            'weekly_target_capital': pacing_data['weekly_target_capital'],
            'weekly_target_premium': pacing_data['weekly_target_premium'],
            'daily_target_premium': pacing_data['daily_target_premium']
        }
        """
        Calculate weekly selling quota
        
        User sells 25% of deployed capital per week
        
        Returns:
            dict with:
                - weekly_target_premium: Total premium to collect this week
                - daily_target_premium: Daily target (weekly / 5)
                - weekly_target_capital: Capital to deploy this week (25% of total)
        """
        weekly_target_capital = total_deployed_capital * WEEKLY_TARGET_PCT
        
        # Assuming ~2% premium per week (adjustable)
        # This is approximate - user should adjust based on actual market conditions
        estimated_premium_pct = 0.02
        weekly_target_premium = weekly_target_capital * estimated_premium_pct
        
        daily_target_premium = weekly_target_premium / TRADING_DAYS_PER_WEEK
        
        return {
            "weekly_target_capital": weekly_target_capital,
            "weekly_target_premium": weekly_target_premium,
            "daily_target_premium": daily_target_premium
        }
    
    @staticmethod
    def calculate_weekly_progress(df_trades: pd.DataFrame, total_deployed_capital: float) -> dict:
        """
        Calculate progress towards weekly quota
        
        Returns:
            dict with:
                - quota: Weekly quota info
                - sold_this_week_premium: Premium collected this week
                - sold_this_week_count: Number of contracts sold this week
                - remaining_premium: Remaining to hit quota
                - progress_pct: Progress percentage
                - days_left_in_week: Trading days left this week
        """
        quota = QuotaCalculator.calculate_weekly_quota(total_deployed_capital)
        
        # Get this week's trades
        now = datetime.now()
        start_of_week = now - timedelta(days=now.weekday())
        
        df_trades['Date_open'] = pd.to_datetime(df_trades['Date_open'])
        this_week = df_trades[
            (df_trades['Date_open'] >= start_of_week) &
            (df_trades['TradeType'].isin(['CC', 'CSP']))
        ]
        
        sold_this_week_premium = (this_week['OptPremium'] * 100 * this_week['Quantity']).sum()
        sold_this_week_count = len(this_week)
        
        remaining_premium = max(0, quota['weekly_target_premium'] - sold_this_week_premium)
        progress_pct = (sold_this_week_premium / quota['weekly_target_premium'] * 100) if quota['weekly_target_premium'] > 0 else 0
        
        # Days left in week (excluding weekend)
        days_until_friday = (4 - now.weekday()) % 7  # 4 = Friday
        days_left_in_week = max(1, days_until_friday + 1)  # At least 1
        
        return {
            "quota": quota,
            "sold_this_week_premium": sold_this_week_premium,
            "sold_this_week_count": sold_this_week_count,
            "remaining_premium": remaining_premium,
            "progress_pct": progress_pct,
            "days_left_in_week": days_left_in_week,
            "suggested_daily_premium": remaining_premium / days_left_in_week
        }


class RiskCalculator:
    """Calculate risk indicators"""
    
    @staticmethod
    def calculate_call_risk(df_open: pd.DataFrame, live_prices: dict) -> pd.DataFrame:
        """
        Calculate call risk for open CC positions
        
        Args:
            df_open: Open positions dataframe
            live_prices: Dict of {ticker: current_price}
        
        Returns:
            DataFrame with call risk indicators added
        """
        df = df_open.copy()
        df['CallRisk'] = 'NONE'
        
        for idx in df.index:
            trade_type = df.loc[idx, 'TradeType']
            ticker = df.loc[idx, 'Ticker']
            strike = pd.to_numeric(df.loc[idx, 'Option_Strike_Price_(USD)'], errors='coerce')
            dte = df.loc[idx, 'DTE']
            
            # Get current price
            current_price = live_prices.get(ticker) if live_prices else None
            if current_price is None:
                current_price = pd.to_numeric(df.loc[idx, 'Price_of_current_underlying_(USD)'], errors='coerce')
            
            if pd.isna(dte) or pd.isna(strike) or pd.isna(current_price) or strike == 0:
                continue
            
            # CC Risk Logic: Risk when stock price is ABOVE strike (ITM)
            if trade_type == 'CC':
                # High risk: ITM and close to expiry
                if current_price > strike and dte <= CALL_RISK_HIGH_DTE:
                    df.loc[idx, 'CallRisk'] = 'HIGH'
                # Medium risk: Near strike and close to expiry
                elif current_price > (strike * CALL_RISK_MEDIUM_BUFFER) and dte <= CALL_RISK_HIGH_DTE:
                    df.loc[idx, 'CallRisk'] = 'MEDIUM'
                # Low risk: ITM but time left
                elif current_price > strike:
                    df.loc[idx, 'CallRisk'] = 'LOW'
            
            # CSP Risk Logic: Risk when stock price is BELOW strike (ITM for puts)
            elif trade_type == 'CSP':
                # Calculate moneyness (how deep ITM)
                moneyness_pct = ((strike - current_price) / strike) * 100 if strike > 0 else 0
                
                # HIGH RISK: Deep ITM (10%+ below strike) OR ITM with low DTE (7 days or less)
                # Deep ITM CSPs are very likely to be assigned regardless of DTE
                if current_price < strike:
                    if moneyness_pct >= 10.0:  # Deep ITM (10%+ below strike)
                        df.loc[idx, 'CallRisk'] = 'HIGH'
                    elif dte <= 7:  # ITM and very close to expiry (7 days or less)
                        df.loc[idx, 'CallRisk'] = 'HIGH'
                    elif moneyness_pct >= 5.0 and dte <= 14:  # Moderately ITM (5-10%) and close to expiry
                        df.loc[idx, 'CallRisk'] = 'HIGH'
                    elif moneyness_pct >= 5.0:  # Moderately ITM (5-10%) but more time
                        df.loc[idx, 'CallRisk'] = 'MEDIUM'
                    elif dte <= 14:  # Slightly ITM (<5%) but close to expiry
                        df.loc[idx, 'CallRisk'] = 'MEDIUM'
                    else:  # Slightly ITM with time left
                        df.loc[idx, 'CallRisk'] = 'LOW'
                # OTM CSPs (stock above strike) - generally safe, but monitor if close to strike
                elif current_price <= (strike * 1.05) and dte <= CALL_RISK_HIGH_DTE:
                    # Stock is within 5% above strike with low DTE - medium risk of going ITM
                    df.loc[idx, 'CallRisk'] = 'MEDIUM'
                else:
                    # OTM with buffer - safe
                    df.loc[idx, 'CallRisk'] = 'NONE'
        
        return df
    
    @staticmethod
    def calculate_dte(expiry_date: date) -> int:
        """Calculate days to expiry (inclusive: today + expiry day both count, e.g. 19 Feb → 20 Feb = 2 DTE)."""
        if pd.isna(expiry_date):
            return 0
        
        if isinstance(expiry_date, str):
            expiry_date = pd.to_datetime(expiry_date).date()
        
        today = date.today()
        return (expiry_date - today).days + 1


class PMCCCalculator:
    """Calculate PMCC (Poor Man's Covered Call) capital and risk"""
    
    @staticmethod
    def calculate_pmcc_by_ticker(
        df_open: pd.DataFrame,
        starting_deposit: float,
        total_csp_reserved: float,
        live_prices: Optional[Dict[str, float]] = None,
        pmcc_tickers: Optional[Set[str]] = None
    ) -> Dict:
        """
        Calculate PMCC capital commitments and remaining buying power.
        
        PMCC Logic:
        1. Identify PMCC pairs: Match Short Calls (CC) with Long LEAPs of same ticker
        2. LEAP Sunk Capital = Sum(OptPremium * Qty) where OptPremium is total premium per contract
        3. CCs require no capital charge (covered by stock/LEAPs)
           From cash-secured perspective, CCs don't lock additional capital
        4. Total PMCC Capital = LEAP_Sunk only
        5. Remaining BP = Starting_Deposit - Total_CSP_Reserved - LEAP_Sunk
        5. Overleverage if Remaining_BP < 0
        
        Args:
            df_open: DataFrame of open positions
            starting_deposit: Initial portfolio deposit (Capital)
            total_csp_reserved: Total CSP reserved capital from CSP Tank Logic
            live_prices: Optional dict of {ticker: price} for current prices
            pmcc_tickers: Set of tickers that use PMCC logic
        
        Returns:
            dict with structure:
            {
                'by_ticker': {
                    'TICKER': {
                        'leap_sunk': float,
                        'pmcc_reserved': float,
                        'total_committed': float,
                        'leap_positions': [...],
                        'cc_positions': [...],
                        'matched_pairs': [...]
                    }
                },
                'total': {
                    'leap_sunk': float,
                    'pmcc_reserved': float,
                    'total_committed': float,
                    'remaining_buying_power': float,
                    'overleveraged': bool
                }
            }
        """
        if pmcc_tickers is None:
            pmcc_tickers = set()
        
        # Filter for PMCC tickers only and ensure only Open positions
        pmcc_df = df_open[
            (df_open['Ticker'].isin(pmcc_tickers)) & 
            (df_open['Status'] == 'Open')
        ].copy()
        
        if pmcc_df.empty:
            return {
                'by_ticker': {},
                'total': {
                    'leap_sunk': 0.0,
                    'pmcc_reserved': 0.0,
                    'total_committed': 0.0,
                    'remaining_buying_power': starting_deposit - total_csp_reserved,
                    'overleveraged': (starting_deposit - total_csp_reserved) < 0
                }
            }
        
        by_ticker = {}
        total_leap_sunk = 0.0
        total_pmcc_reserved = 0.0
        
        # Group by ticker
        for ticker in sorted(pmcc_tickers):
            ticker_positions = pmcc_df[pmcc_df['Ticker'] == ticker].copy()
            
            # Get LEAP positions (long calls)
            leap_positions = ticker_positions[ticker_positions['TradeType'] == 'LEAP'].copy()
            # Get CC positions (short calls)
            cc_positions = ticker_positions[ticker_positions['TradeType'] == 'CC'].copy()
            
            # Calculate LEAP Sunk Capital (capital locked to buy LEAPs)
            # OptPremium is stored as per share, so we need to multiply by 100 to get per contract
            # LEAP_Sunk = OptPremium (per share) × 100 × Qty
            # This is capital that allows us to exercise and hold stock
            leap_sunk = 0.0
            for _, row in leap_positions.iterrows():
                premium = pd.to_numeric(row.get('OptPremium', 0), errors='coerce') or 0
                qty = pd.to_numeric(row.get('Quantity', 0), errors='coerce') or 0
                # Use absolute value for quantity (long positions)
                qty = abs(qty)
                
                # OptPremium is per share, multiply by 100 to get contract price
                # Then multiply by quantity to get total sunk capital
                leap_sunk += premium * 100 * qty
            
            # CCs require no capital charge from our cash-secured perspective
            # They are covered by stock/LEAPs, so we don't count them in capital calculations
            # Even if broker charges margin, it doesn't change our cash-secured exposure calculation
            pmcc_reserved = 0.0
            
            if leap_sunk > 0:
                by_ticker[ticker] = {
                    'leap_sunk': leap_sunk,
                    'total_committed': leap_sunk  # Only LEAP sunk, CCs don't require capital
                }
                
                total_leap_sunk += leap_sunk
                total_pmcc_reserved += pmcc_reserved
        
        # Calculate Remaining Buying Power
        # Only LEAP sunk counts (CCs are covered, no additional capital needed)
        total_committed = total_leap_sunk
        remaining_bp = starting_deposit - total_csp_reserved - total_committed
        overleveraged = remaining_bp < 0
        
        return {
            'by_ticker': by_ticker,
                'total': {
                    'leap_sunk': total_leap_sunk,
                    'pmcc_reserved': 0.0,  # CCs don't require capital (kept for backward compatibility)
                    'total_committed': total_leap_sunk,  # Only LEAP sunk
                    'remaining_buying_power': remaining_bp,
                    'overleveraged': overleveraged
                }
        }


class CSPTankCalculator:
    """Calculate CSP Tank Logic - True Buying Power with Tiger Vault (MMF) support
    
    DEPRECATED: Use UnifiedCapitalCalculator instead.
    This class is kept for backward compatibility.
    """
    
    @staticmethod
    def calculate_csp_tank_by_ticker(
        df_open: pd.DataFrame,
        starting_deposit: float,
        live_prices: Optional[Dict[str, float]] = None,
        pmcc_tickers: Optional[Set[str]] = None,
        stock_avg_prices: Optional[Dict[str, float]] = None
    ) -> Dict:
        """
        DEPRECATED: Use UnifiedCapitalCalculator.calculate_capital_by_ticker() instead.
        This method wraps the unified calculator for backward compatibility.
        """
        from unified_calculations import UnifiedCapitalCalculator
        
        # Use unified calculator
        capital_data = UnifiedCapitalCalculator.calculate_capital_by_ticker(
            df_open, starting_deposit, stock_avg_prices, live_prices, pmcc_tickers
        )
        
        # Convert to old format for backward compatibility
        return {
            'locked': {
                'stock_locked': capital_data['total']['stock_locked'],
                'true_csp_reserved': capital_data['total']['csp_reserved']
            },
            'by_ticker': {
                ticker: {
                    'stock_locked': data['stock_locked'],
                    'csp_reserved': data['csp_reserved'],
                    'total_used': data['total_committed']
                }
                for ticker, data in capital_data['by_ticker'].items()
                if ticker not in (pmcc_tickers or set())
            },
            'health': {
                'starting_deposit': starting_deposit,
                'true_buying_power': capital_data['total']['remaining_bp'],
                'overleveraged': capital_data['total']['overleveraged'],
                'status': 'OVERLEVERAGED' if capital_data['total']['overleveraged'] else 'OK'
            },
            'pmcc_tickers': {}
        }
        """
        Calculate CSP Tank Logic - Simplified.
        
        Simple Logic:
        1. Stock Locked = Sum(Shares * Current_Market_Price) for STOCK positions
        2. True CSP Reserved = Sum(Put_Strike * 100 * Contracts) for CSP positions
        3. True Buying Power (TBP) = Starting_Deposit - Stock_Locked - True_CSP_Reserved
        4. Overleverage: If TBP < 0, flag as "OVERLEVERAGED"
        
        Args:
            df_open: DataFrame of open positions
            starting_deposit: Initial portfolio deposit (Capital)
            live_prices: Optional dict of {ticker: price} for current prices
            pmcc_tickers: Set of tickers that use PMCC logic (excluded from CSP Tank)
        
        Returns:
            dict with structure:
            {
                'locked': {
                    'stock_locked': float,
                    'true_csp_reserved': float
                },
                'by_ticker': {
                    'TICKER1': {
                        'stock_locked': float,
                        'csp_reserved': float,
                        'total_used': float
                    },
                    ...
                },
                'health': {
                    'starting_deposit': float,
                    'true_buying_power': float,
                    'overleveraged': bool,
                    'status': str  # "OK" or "OVERLEVERAGED"
                },
                'pmcc_tickers': {
                    'SPY': {
                        'note': 'PMCC logic to be provided later'
                    }
                }
            }
        """
        # Initialize return structure
        result = {
            'locked': {
                'stock_locked': 0.0,
                'true_csp_reserved': 0.0
            },
            'by_ticker': {},
            'health': {
                'starting_deposit': starting_deposit,
                'true_buying_power': starting_deposit,
                'overleveraged': False,
                'status': 'OK'
            },
            'pmcc_tickers': {}
        }
        
        if df_open.empty:
            return result
        
        if live_prices is None:
            live_prices = {}
        
        if pmcc_tickers is None:
            pmcc_tickers = set()
        
        if stock_avg_prices is None:
            stock_avg_prices = {}
        
        # Filter out PMCC tickers and ensure only Open positions
        non_pmcc_df = df_open[
            (~df_open['Ticker'].isin(pmcc_tickers)) & 
            (df_open['Status'] == 'Open')
        ].copy()
        pmcc_df = df_open[
            (df_open['Ticker'].isin(pmcc_tickers)) & 
            (df_open['Status'] == 'Open')
        ].copy()
        
        # Get all unique tickers (non-PMCC)
        tickers = sorted(non_pmcc_df['Ticker'].unique()) if not non_pmcc_df.empty else []
        
        for ticker in tickers:
            ticker_positions = non_pmcc_df[non_pmcc_df['Ticker'] == ticker].copy()
            
            # Calculate Stock Locked (STOCK positions only)
            # Aggregate all shares for this ticker, then use ONE current market price
            stock_positions = ticker_positions[ticker_positions['TradeType'] == 'STOCK'].copy()
            total_shares = 0.0
            
            # Sum all shares across all STOCK positions for this ticker
            for _, row in stock_positions.iterrows():
                # Get shares - for STOCK positions, shares are stored directly
                # Priority: Open_lots (if exists and valid) > Quantity (already in shares for STOCK)
                shares = 0.0
                
                # Try Open_lots first (this is the number of shares)
                if 'Open_lots' in row.index and pd.notna(row['Open_lots']):
                    shares = abs(pd.to_numeric(row['Open_lots'], errors='coerce') or 0)
                
                # If Open_lots is 0 or missing, try Quantity (for STOCK, Quantity is already in shares, not contracts)
                if shares == 0 and 'Quantity' in row.index:
                    qty = pd.to_numeric(row.get('Quantity', 0), errors='coerce') or 0
                    shares = abs(qty)  # For STOCK, Quantity is already shares, don't multiply by 100
                
                total_shares += shares
            
            # Get average entry price from Performance tab (preferred - from broker records)
            # Fallback to live prices, then entry prices from trade records
            current_price = 0.0
            if ticker in stock_avg_prices and stock_avg_prices[ticker] > 0:
                # Use average entry price from Performance tab (most accurate - from broker)
                current_price = stock_avg_prices[ticker]
            elif ticker in live_prices and live_prices[ticker] is not None:
                # Fallback to live prices
                current_price = live_prices[ticker]
            else:
                # Last fallback: use the most recent entry price from any STOCK position
                if not stock_positions.empty:
                    prices = pd.to_numeric(stock_positions['Price_of_current_underlying_(USD)'], errors='coerce')
                    prices = prices[prices > 0]
                    if not prices.empty:
                        current_price = prices.iloc[-1]  # Use last non-zero price
            
            # Calculate stock locked: total shares × current market price
            stock_locked = total_shares * current_price if total_shares > 0 and current_price > 0 else 0.0
            
            # Calculate True CSP Reserved (CSP positions - 100% cash secured)
            csp_positions = ticker_positions[ticker_positions['TradeType'] == 'CSP'].copy()
            csp_reserved = 0.0
            
            for _, row in csp_positions.iterrows():
                strike = pd.to_numeric(row.get('Option_Strike_Price_(USD)', 0), errors='coerce') or 0
                qty = pd.to_numeric(row.get('Quantity', 0), errors='coerce') or 0
                csp_reserved += strike * 100 * qty
            
            # Store per-ticker data
            result['by_ticker'][ticker] = {
                'stock_locked': stock_locked,
                'csp_reserved': csp_reserved,
                'total_used': stock_locked + csp_reserved,
                'positions': {
                    'stock': stock_positions.to_dict('records') if not stock_positions.empty else [],
                    'csp': csp_positions.to_dict('records') if not csp_positions.empty else []
                }
            }
            
            # Accumulate totals
            result['locked']['stock_locked'] += stock_locked
            result['locked']['true_csp_reserved'] += csp_reserved
        
        # Calculate True Buying Power (Simple: Capital - Locked - Reserved)
        result['health']['true_buying_power'] = (
            starting_deposit 
            - result['locked']['stock_locked'] 
            - result['locked']['true_csp_reserved']
        )
        
        # Check Overleverage
        result['health']['overleveraged'] = result['health']['true_buying_power'] < 0
        
        # Set status
        if result['health']['overleveraged']:
            result['health']['status'] = 'OVERLEVERAGED'
        else:
            result['health']['status'] = 'OK'
        
        # PMCC tickers info
        if not pmcc_df.empty:
            pmcc_ticker_list = sorted(pmcc_df['Ticker'].unique())
            for ticker in pmcc_ticker_list:
                result['pmcc_tickers'][ticker] = {
                    'note': 'PMCC logic to be provided later'
                }
        
        return result
