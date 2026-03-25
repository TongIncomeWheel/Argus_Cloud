"""
Unified Data Access Layer
Provides consistent access to trade data with proper field interpretation
"""

import pandas as pd
from typing import Dict, Optional, List
from data_schema import (
    get_field_name, 
    get_logical_name,
    FIELD_INTERPRETATIONS,
    CALCULATION_FORMULAS
)


class DataAccess:
    """Unified data access layer with consistent field mapping"""
    
    @staticmethod
    def get_trade_field(df: pd.DataFrame, row_idx, logical_name: str, 
                        category: Optional[str] = None, default=None):
        """
        Get a field value from DataFrame using logical name
        
        Args:
            df: DataFrame
            row_idx: Row index (can be positional int or index label)
            logical_name: Logical field name (e.g., 'trade_id')
            category: Optional category hint
            default: Default value if field missing or NaN
        
        Returns:
            Field value or default
        """
        excel_column = get_field_name(logical_name, category)
        
        if excel_column not in df.columns:
            return default
        
        try:
            # Try using loc first (works with index labels)
            if row_idx in df.index:
                value = df.loc[row_idx, excel_column]
            else:
                # Fallback to iloc (positional index)
                value = df.iloc[row_idx][excel_column]
        except (IndexError, KeyError):
            return default
        
        if pd.isna(value):
            return default
        
        return value
    
    @staticmethod
    def get_shares_for_stock(df: pd.DataFrame, row_idx: int) -> float:
        """
        Get shares for a STOCK position
        Priority: Open_lots > Quantity
        
        Returns:
            Number of shares (float)
        """
        # Try Open_lots first (preferred for stock)
        open_lots = DataAccess.get_trade_field(df, row_idx, 'open_lots', 'position', 0)
        if pd.notna(open_lots) and open_lots != 0:
            return abs(float(open_lots))
        
        # Fallback to Quantity
        quantity = DataAccess.get_trade_field(df, row_idx, 'quantity', 'position', 0)
        if pd.notna(quantity):
            return abs(float(quantity))
        
        return 0.0
    
    @staticmethod
    def get_contracts_for_option(df: pd.DataFrame, row_idx: int) -> float:
        """
        Get number of contracts for an option position
        
        Returns:
            Number of contracts (float)
        """
        quantity = DataAccess.get_trade_field(df, row_idx, 'quantity', 'position', 0)
        if pd.notna(quantity):
            return abs(float(quantity))
        return 0.0
    
    @staticmethod
    def get_stock_price(ticker: str, df: pd.DataFrame, row_idx: int,
                       stock_avg_prices: Optional[Dict[str, float]] = None,
                       live_prices: Optional[Dict[str, float]] = None) -> float:
        """
        Get stock price with priority:
        1. stock_avg_prices (Performance tab - most accurate)
        2. live_prices (Yahoo Finance)
        3. Price_of_current_underlying_(USD) from DataFrame
        
        Returns:
            Stock price (float)
        """
        # Priority 1: Average price from Performance tab
        if stock_avg_prices and ticker in stock_avg_prices:
            price = stock_avg_prices[ticker]
            if price and price > 0:
                return float(price)
        
        # Priority 2: Live prices
        if live_prices and ticker in live_prices:
            price = live_prices[ticker]
            if price and price > 0:
                return float(price)
        
        # Priority 3: DataFrame value
        current_price = DataAccess.get_trade_field(df, row_idx, 'current_price', 'position', 0)
        if pd.notna(current_price) and current_price > 0:
            return float(current_price)
        
        return 0.0
    
    @staticmethod
    def get_premium_per_contract(df: pd.DataFrame, row_idx: int) -> float:
        """
        Get premium per contract (not per share)
        OptPremium is per share, so multiply by 100
        
        Returns:
            Premium per contract (float)
        """
        premium_per_share = DataAccess.get_trade_field(df, row_idx, 'premium_per_share', 'options', 0)
        if pd.notna(premium_per_share):
            return float(premium_per_share) * 100.0
        return 0.0
    
    @staticmethod
    def get_total_premium(df: pd.DataFrame, row_idx: int) -> float:
        """
        Get total premium for a position
        Formula: premium_per_share * 100 * quantity
        
        Returns:
            Total premium (float)
        """
        premium_per_share = DataAccess.get_trade_field(df, row_idx, 'premium_per_share', 'options', 0)
        quantity = DataAccess.get_contracts_for_option(df, row_idx)
        
        if pd.notna(premium_per_share) and quantity > 0:
            return float(premium_per_share) * 100.0 * quantity
        return 0.0
    
    @staticmethod
    def get_stock_shares(ticker: str, df: pd.DataFrame) -> float:
        """
        Get total stock shares for a ticker (all STOCK positions).
        Used for stock_at_buy vs stock_at_current capital calculations.
        """
        ticker_col = get_field_name('ticker', 'identity')
        trade_type_col = get_field_name('trade_type', 'identity')
        ticker_positions = df[df[ticker_col] == ticker].copy()
        stock_positions = ticker_positions[ticker_positions[trade_type_col] == 'STOCK'].copy()
        if stock_positions.empty:
            return 0.0
        total_shares = 0.0
        for idx in stock_positions.index:
            pos_idx = stock_positions.index.get_loc(idx)
            shares = DataAccess.get_shares_for_stock(stock_positions, pos_idx)
            total_shares += shares
        return total_shares

    @staticmethod
    def get_stock_locked(ticker: str, df: pd.DataFrame, 
                        stock_avg_prices: Optional[Dict[str, float]] = None,
                        live_prices: Optional[Dict[str, float]] = None) -> float:
        """
        Calculate stock locked capital for a ticker
        Aggregates all STOCK positions for the ticker
        
        Returns:
            Total stock locked capital (float)
        """
        ticker_positions = df[df[get_field_name('ticker')] == ticker].copy()
        stock_positions = ticker_positions[ticker_positions[get_field_name('trade_type')] == 'STOCK'].copy()
        
        if stock_positions.empty:
            return 0.0
        
        total_shares = 0.0
        for idx in stock_positions.index:
            # Convert index label to positional index
            pos_idx = stock_positions.index.get_loc(idx)
            shares = DataAccess.get_shares_for_stock(stock_positions, pos_idx)
            total_shares += shares
        
        if total_shares == 0:
            return 0.0
        
        # Get price (with priority) - use first row for fallback to Excel price
        # Priority: stock_avg_prices > live_prices > Excel
        price = 0.0
        if stock_avg_prices and ticker in stock_avg_prices:
            price = stock_avg_prices[ticker]
            if price and price > 0:
                price = float(price)
        elif live_prices and ticker in live_prices:
            price = live_prices[ticker]
            if price and price > 0:
                price = float(price)
        else:
            # Fallback to Excel price from first position
            if not stock_positions.empty:
                first_idx = stock_positions.index[0]
                pos_idx = stock_positions.index.get_loc(first_idx)
                excel_price = DataAccess.get_trade_field(stock_positions, pos_idx, 'current_price', 'position', 0)
                if pd.notna(excel_price) and excel_price > 0:
                    price = float(excel_price)
        
        return total_shares * price
    
    @staticmethod
    def get_csp_reserved(ticker: str, df: pd.DataFrame) -> float:
        """
        Calculate CSP reserved capital for a ticker
        Formula: strike * 100 * quantity
        
        Returns:
            Total CSP reserved capital (float)
        """
        ticker_positions = df[df[get_field_name('ticker')] == ticker].copy()
        csp_positions = ticker_positions[ticker_positions[get_field_name('trade_type')] == 'CSP'].copy()
        
        if csp_positions.empty:
            return 0.0
        
        total_reserved = 0.0
        for idx in csp_positions.index:
            # Convert index label to positional index
            pos_idx = csp_positions.index.get_loc(idx)
            strike = DataAccess.get_trade_field(csp_positions, pos_idx, 'strike', 'options', 0)
            quantity = DataAccess.get_contracts_for_option(csp_positions, pos_idx)
            
            if pd.notna(strike) and strike > 0 and quantity > 0:
                total_reserved += float(strike) * 100.0 * quantity
        
        return total_reserved
    
    @staticmethod
    def get_leap_sunk(ticker: str, df: pd.DataFrame) -> float:
        """
        Calculate LEAP sunk capital for a ticker
        Formula: premium_per_share * 100 * quantity
        
        Returns:
            Total LEAP sunk capital (float)
        """
        ticker_positions = df[df[get_field_name('ticker')] == ticker].copy()
        leap_positions = ticker_positions[ticker_positions[get_field_name('trade_type')] == 'LEAP'].copy()
        
        if leap_positions.empty:
            return 0.0
        
        total_sunk = 0.0
        for idx in leap_positions.index:
            # Use positional index for iloc
            pos_idx = leap_positions.index.get_loc(idx)
            premium_per_share = DataAccess.get_trade_field(leap_positions, pos_idx, 'premium_per_share', 'options', 0)
            quantity = DataAccess.get_contracts_for_option(leap_positions, pos_idx)
            
            if pd.notna(premium_per_share) and premium_per_share > 0 and quantity > 0:
                # OptPremium is per share, multiply by 100 for contract cost
                total_sunk += float(premium_per_share) * 100.0 * quantity
        
        return total_sunk
    
    @staticmethod
    def filter_open_positions(df: pd.DataFrame) -> pd.DataFrame:
        """Filter DataFrame to only open positions"""
        status_col = get_field_name('status', 'identity')
        if status_col not in df.columns:
            return df.copy()
        return df[df[status_col] == 'Open'].copy()
    
    @staticmethod
    def filter_by_trade_type(df: pd.DataFrame, trade_types: List[str]) -> pd.DataFrame:
        """Filter DataFrame by trade types"""
        trade_type_col = get_field_name('trade_type', 'identity')
        if trade_type_col not in df.columns:
            return pd.DataFrame()
        return df[df[trade_type_col].isin(trade_types)].copy()
    
    @staticmethod
    def filter_by_ticker(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Filter DataFrame by ticker"""
        ticker_col = get_field_name('ticker', 'identity')
        if ticker_col not in df.columns:
            return pd.DataFrame()
        return df[df[ticker_col] == ticker].copy()
