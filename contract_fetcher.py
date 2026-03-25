"""
Contract Fetcher Module
Filters open positions and enriches them with contract details for IBKR API queries
Separate from IBKR connection logic - pure data preparation
"""
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional

from config import TICKERS

# Lazy import IBKR to avoid event loop issues in Streamlit
def _get_ibkr_imports():
    """Lazy import IBKR modules to avoid event loop issues"""
    try:
        import nest_asyncio
        nest_asyncio.apply()
        from ib_insync import Stock, Option, Contract
        return Stock, Option, Contract
    except Exception as e:
        # If import fails, return None - app can work without IBKR
        return None, None, None


class ContractFetcher:
    """
    Fetches and enriches open option positions for IBKR API queries
    
    Responsibilities:
    1. Filter open positions from Data Table
    2. Enrich with contract details (strike, expiry, right, exchange)
    3. Format into IBKR Contract objects
    4. Group by ticker for efficient batch queries
    """
    
    def __init__(self, df_trades: pd.DataFrame):
        """
        Initialize with trades dataframe
        
        Args:
            df_trades: Full trades dataframe from Google Sheets
        """
        self.df_trades = df_trades
        self.df_open = None
        self._refresh_open_positions()
    
    def _refresh_open_positions(self):
        """Refresh open positions cache"""
        if self.df_trades is not None and not self.df_trades.empty:
            self.df_open = self.df_trades[
                (self.df_trades['Status'] == 'Open') &
                (self.df_trades['TradeType'].isin(['CC', 'CSP']))
            ].copy()
        else:
            self.df_open = pd.DataFrame()
    
    def get_open_options(self) -> pd.DataFrame:
        """
        Get all open option positions (CC and CSP only)
        
        Returns:
            DataFrame with open option positions
        """
        self._refresh_open_positions()
        return self.df_open.copy()
    
    def get_options_by_ticker(self, ticker: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        Get open options grouped by ticker
        
        Args:
            ticker: If provided, return only that ticker. Otherwise return all.
        
        Returns:
            Dict of {ticker: DataFrame}
        """
        self._refresh_open_positions()
        
        if self.df_open.empty:
            return {}
        
        if ticker:
            filtered = self.df_open[self.df_open['Ticker'] == ticker.upper()]
            return {ticker.upper(): filtered} if not filtered.empty else {}
        
        # Group by ticker
        result = {}
        for t in self.df_open['Ticker'].unique():
            result[t] = self.df_open[self.df_open['Ticker'] == t]
        
        return result
    
    def enrich_with_contract_details(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich positions with contract details needed for IBKR queries
        
        Adds columns:
        - contract_symbol: Stock symbol
        - contract_strike: Strike price
        - contract_expiry: Expiry date in IBKR format (YYYYMMDD)
        - contract_right: 'C' for Call, 'P' for Put
        - contract_multiplier: Always 100 for standard options
        - contract_exchange: 'SMART' (IBKR's smart routing)
        - contract_currency: 'USD'
        
        Args:
            df: DataFrame with open positions
        
        Returns:
            Enriched DataFrame
        """
        enriched = df.copy()
        
        # Contract symbol (ticker)
        enriched['contract_symbol'] = enriched['Ticker'].str.upper()
        
        # Strike price
        enriched['contract_strike'] = enriched['Option_Strike_Price_(USD)']
        
        # Expiry date in IBKR format (YYYYMMDD)
        enriched['contract_expiry'] = pd.to_datetime(enriched['Expiry_Date']).dt.strftime('%Y%m%d')
        
        # Right: 'C' for Call (CC), 'P' for Put (CSP)
        enriched['contract_right'] = enriched['TradeType'].map({
            'CC': 'C',
            'CSP': 'P'
        })
        
        # Standard option contract details
        enriched['contract_multiplier'] = 100
        enriched['contract_exchange'] = 'SMART'
        enriched['contract_currency'] = 'USD'
        
        # Contract key for deduplication (same contract may appear multiple times)
        enriched['contract_key'] = (
            enriched['contract_symbol'] + '_' +
            enriched['contract_expiry'] + '_' +
            enriched['contract_strike'].astype(str) + '_' +
            enriched['contract_right']
        )
        
        return enriched
    
    def create_ibkr_contracts(self, df: Optional[pd.DataFrame] = None) -> List:
        """
        Create IBKR Contract objects from enriched positions
        
        Args:
            df: Optional DataFrame (if None, uses all open positions)
        
        Returns:
            List of IBKR Contract objects ready for API queries
        """
        # Lazy import IBKR classes
        Stock, Option, Contract = _get_ibkr_imports()
        if Option is None:
            return []  # IBKR not available
        
        if df is None:
            df = self.get_open_options()
        
        if df.empty:
            return []
        
        # Enrich with contract details
        enriched = self.enrich_with_contract_details(df)
        
        # Remove duplicates (same contract may have multiple entries)
        unique_contracts = enriched.drop_duplicates(subset=['contract_key'])
        
        contracts = []
        for _, row in unique_contracts.iterrows():
            try:
                # Create Option contract
                contract = Option(
                    symbol=row['contract_symbol'],
                    lastTradeDateOrContractMonth=row['contract_expiry'],
                    strike=float(row['contract_strike']),
                    right=row['contract_right'],
                    exchange=row['contract_exchange'],
                    currency=row['contract_currency']
                )
                contracts.append(contract)
            except Exception as e:
                # Log error but continue
                print(f"Error creating contract for {row.get('TradeID', 'Unknown')}: {e}")
                continue
        
        return contracts
    
    def get_contracts_by_ticker(self, ticker: str) -> List:
        """
        Get IBKR contracts for a specific ticker
        
        Args:
            ticker: Ticker symbol
        
        Returns:
            List of Contract objects for that ticker
        """
        ticker_positions = self.get_options_by_ticker(ticker)
        
        if ticker not in ticker_positions:
            return []
        
        return self.create_ibkr_contracts(ticker_positions[ticker])
    
    def get_stock_contracts(self) -> List:
        """
        Get IBKR Stock contracts for all unique tickers in open positions
        
        Returns:
            List of Stock Contract objects for underlying prices
        """
        # Lazy import IBKR classes
        Stock, Option, Contract = _get_ibkr_imports()
        if Stock is None:
            return []  # IBKR not available
        
        self._refresh_open_positions()
        
        if self.df_open.empty:
            return []
        
        unique_tickers = self.df_open['Ticker'].unique()
        
        contracts = []
        for ticker in unique_tickers:
            try:
                contract = Stock(ticker, 'SMART', 'USD')
                contracts.append(contract)
            except Exception as e:
                print(f"Error creating stock contract for {ticker}: {e}")
                continue
        
        return contracts
    
    def get_contract_summary(self) -> pd.DataFrame:
        """
        Get summary of contracts to query
        
        Returns:
            DataFrame with contract summary (for debugging/logging)
        """
        self._refresh_open_positions()
        
        if self.df_open.empty:
            return pd.DataFrame()
        
        enriched = self.enrich_with_contract_details(self.df_open)
        
        # Group by contract_key to see unique contracts
        summary = enriched.groupby('contract_key').agg({
            'TradeID': 'count',
            'contract_symbol': 'first',
            'contract_expiry': 'first',
            'contract_strike': 'first',
            'contract_right': 'first',
            'Quantity': 'sum'
        }).reset_index()
        
        summary.columns = [
            'ContractKey', 'PositionCount', 'Symbol', 'Expiry', 
            'Strike', 'Right', 'TotalContracts'
        ]
        
        return summary
    
    def validate_contracts(self) -> Dict[str, List[str]]:
        """
        Validate contracts before sending to IBKR
        
        Returns:
            Dict with 'errors' and 'warnings' lists
        """
        errors = []
        warnings = []
        
        self._refresh_open_positions()
        
        if self.df_open.empty:
            return {'errors': [], 'warnings': ['No open positions to validate']}
        
        enriched = self.enrich_with_contract_details(self.df_open)
        
        # Check for missing required fields
        required_fields = [
            'contract_symbol', 'contract_strike', 'contract_expiry', 
            'contract_right', 'Expiry_Date'
        ]
        
        for field in required_fields:
            missing = enriched[enriched[field].isna()]
            if not missing.empty:
                errors.append(f"Missing {field} for TradeIDs: {missing['TradeID'].tolist()}")
        
        # Check for invalid expiry dates (past dates)
        today = date.today()
        # Only check if Expiry_Date column exists and has valid datetime values
        if 'Expiry_Date' in enriched.columns:
            expiry_dates = pd.to_datetime(enriched['Expiry_Date'], errors='coerce')
            valid_expiry = expiry_dates.notna()
            if valid_expiry.any():
                past_expiry = enriched[valid_expiry & (expiry_dates.dt.date < today)]
                if not past_expiry.empty:
                    errors.append(f"Past expiry dates for TradeIDs: {past_expiry['TradeID'].tolist()}")
        
        # Check for invalid strikes
        invalid_strikes = enriched[enriched['contract_strike'] <= 0]
        if not invalid_strikes.empty:
            errors.append(f"Invalid strike prices for TradeIDs: {invalid_strikes['TradeID'].tolist()}")
        
        # Check for invalid right values
        invalid_rights = enriched[~enriched['contract_right'].isin(['C', 'P'])]
        if not invalid_rights.empty:
            errors.append(f"Invalid contract right for TradeIDs: {invalid_rights['TradeID'].tolist()}")
        
        # Warnings for expiring soon
        if 'Expiry_Date' in enriched.columns:
            expiry_dates = pd.to_datetime(enriched['Expiry_Date'], errors='coerce')
            valid_expiry = expiry_dates.notna()
            if valid_expiry.any():
                # Calculate days to expiry properly
                expiry_dates_valid = expiry_dates[valid_expiry].dt.date
                days_to_expiry = [(ed - today).days for ed in expiry_dates_valid]
                expiring_soon_mask = pd.Series([d <= 7 for d in days_to_expiry], index=enriched[valid_expiry].index)
                expiring_soon = enriched[valid_expiry][expiring_soon_mask]
                if not expiring_soon.empty:
                    warnings.append(f"Contracts expiring within 7 days: {expiring_soon['TradeID'].tolist()}")
        
        return {'errors': errors, 'warnings': warnings}
