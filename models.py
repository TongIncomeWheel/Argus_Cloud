"""
Data models and validation for Income Wheel trades
"""
from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, Literal
import pandas as pd


@dataclass
class Trade:
    """Represents a single trade entry"""
    trade_id: str
    ticker: str
    strategy_type: str  # Wheel, PMCC, etc.
    direction: str  # Sell/Buy
    trade_type: Literal["CC", "CSP", "STOCK", "LEAP"]
    quantity: int
    strike_price: float
    underlying_price: float
    premium: float
    premium_pct: float
    date_open: date
    expiry_date: Optional[date]
    status: Literal["Open", "Closed"]
    remarks: Optional[str] = None
    date_closed: Optional[date] = None
    close_price: Optional[float] = None
    dte: Optional[int] = None
    actual_profit: Optional[float] = None
    
    @property
    def cash_required(self) -> float:
        """Calculate cash required for this position"""
        if self.trade_type == "CSP":
            return self.strike_price * 100 * self.quantity
        elif self.trade_type == "CC":
            return 0  # Covered by stock
        elif self.trade_type == "STOCK":
            return self.underlying_price * self.quantity
        return 0
    
    @property
    def margin_required(self) -> float:
        """Calculate margin required (for CSP)"""
        if self.trade_type == "CSP":
            from config import MARGIN_REQUIREMENT_PCT
            return self.cash_required * MARGIN_REQUIREMENT_PCT
        return 0
    
    @property
    def total_premium(self) -> float:
        """Total premium collected/paid"""
        return self.premium * 100 * self.quantity
    
    @property
    def is_open(self) -> bool:
        """Check if position is open"""
        return self.status == "Open"


@dataclass
class AuditEntry:
    """Represents an audit log entry"""
    audit_id: str
    timestamp: datetime
    action_type: Literal["Open", "BTC", "Roll", "Expire", "Exercise"]
    trade_id_ref: str  # Can be "T-123" or "T-123, T-124" for rolls
    remarks: str
    script_name: str
    affected_qty: int


class TradeValidator:
    """Validates trade operations"""
    
    @staticmethod
    def validate_btc(trade_id: str, df_open: pd.DataFrame) -> tuple[bool, str]:
        """Validate BTC operation"""
        if trade_id not in df_open['TradeID'].values:
            return False, f"TradeID {trade_id} not found in open positions"
        return True, "OK"
    
    @staticmethod
    def validate_roll(old_trade_id: str, new_expiry: date, df_open: pd.DataFrame) -> tuple[bool, str]:
        """Validate Roll operation"""
        if old_trade_id not in df_open['TradeID'].values:
            return False, f"TradeID {old_trade_id} not found in open positions"
        
        old_trade = df_open[df_open['TradeID'] == old_trade_id].iloc[0]
        old_expiry = pd.to_datetime(old_trade['Expiry_Date']).date()
        
        if new_expiry <= old_expiry:
            return False, f"New expiry {new_expiry} must be after old expiry {old_expiry}"
        
        return True, "OK"
    
    @staticmethod
    def validate_sell_cc(ticker: str, qty: int, df_open: pd.DataFrame) -> tuple[bool, str]:
        """Validate Sell CC operation - warnings only, no hard cap (user can override)"""
        import pandas as pd
        
        # Get existing open CC positions for this ticker
        existing_cc_qty = df_open[
            (df_open['Ticker'] == ticker) & 
            (df_open['TradeType'] == 'CC') &
            (df_open['Status'] == 'Open')
        ]['Quantity'].sum()
        
        # Get available stock - use Open_lots (actual shares) if available, else Quantity * 100
        stock_positions = df_open[
            (df_open['Ticker'] == ticker) & 
            (df_open['TradeType'] == 'STOCK') &
            (df_open['Status'] == 'Open')
        ]
        
        if 'Open_lots' in stock_positions.columns:
            stock_qty = pd.to_numeric(stock_positions['Open_lots'], errors='coerce').fillna(0).sum()
        else:
            stock_qty = (stock_positions['Quantity'] * 100).sum()
        
        # Total CC contracts needed (existing + new)
        total_cc_contracts = existing_cc_qty + qty
        required_shares = total_cc_contracts * 100
        
        # Never block on insufficient stock / over 100% CC ratio – warn only so user can still submit
        if stock_qty < required_shares:
            coverage_pct = (required_shares / stock_qty * 100) if stock_qty > 0 else float('inf')
            if stock_qty > 0:
                return True, f"⚠️ WARNING: Coverage will exceed 100% ({coverage_pct:.1f}%). Have {int(stock_qty):,} shares, need {int(required_shares):,} (existing CC: {int(existing_cc_qty)} contracts + new: {qty} contracts). You can proceed if desired."
            else:
                return True, f"⚠️ WARNING: No stock available. Need {int(required_shares):,} shares (existing CC: {int(existing_cc_qty)} contracts + new: {qty} contracts). You can proceed if desired."
        
        return True, "OK"
    
    @staticmethod
    def validate_exercise_csp(trade_id: str, df_open: pd.DataFrame) -> tuple[bool, str]:
        """Validate CSP exercise - will create stock position"""
        if trade_id not in df_open['TradeID'].values:
            return False, f"TradeID {trade_id} not found in open positions"
        
        trade = df_open[df_open['TradeID'] == trade_id].iloc[0]
        
        if trade['TradeType'] != 'CSP':
            return False, f"TradeID {trade_id} is not a CSP (found {trade['TradeType']})"
        
        return True, "OK"
    
    @staticmethod
    def validate_exercise_cc(trade_id: str, df_open: pd.DataFrame) -> tuple[bool, str]:
        """Validate CC exercise - will close stock position"""
        if trade_id not in df_open['TradeID'].values:
            return False, f"TradeID {trade_id} not found in open positions"
        
        trade = df_open[df_open['TradeID'] == trade_id].iloc[0]
        
        if trade['TradeType'] != 'CC':
            return False, f"TradeID {trade_id} is not a CC (found {trade['TradeType']})"
        
        # Check if we have stock to deliver
        ticker = trade['Ticker']
        qty_needed = trade['Quantity'] * 100  # CC contracts * 100 = shares needed
        
        # For STOCK: Use Open_lots (actual shares) if available, else Quantity * 100
        stock_positions = df_open[
            (df_open['Ticker'] == ticker) & 
            (df_open['TradeType'] == 'STOCK') &
            (df_open['Status'] == 'Open')
        ]
        
        if 'Open_lots' in stock_positions.columns:
            stock_qty = pd.to_numeric(stock_positions['Open_lots'], errors='coerce').fillna(0).sum()
        else:
            stock_qty = (stock_positions['Quantity'] * 100).sum()
        
        if stock_qty < qty_needed:
            return False, f"Insufficient stock for assignment. Have {stock_qty}, need {qty_needed}"
        
        return True, "OK"


def generate_trade_id(df_trades: pd.DataFrame) -> str:
    """Generate next TradeID in sequence"""
    if df_trades.empty:
        return "T-1"
    
    # Extract numeric part from TradeID (format: T-123)
    # Handle NaN values from non-matching patterns
    extracted = df_trades['TradeID'].str.extract(r'T-(\d+)')[0]
    # Drop NaN values and convert to int
    existing_ids = extracted.dropna().astype(int)
    
    if existing_ids.empty:
        # No valid trade IDs found, start from 1
        return "T-1"
    
    next_id = existing_ids.max() + 1
    
    return f"T-{next_id}"


def generate_audit_id(df_audit: pd.DataFrame) -> str:
    """Generate next AuditID in sequence"""
    if df_audit.empty:
        return "A-1"
    
    # Extract numeric part from Audit ID (format: A-123)
    # Handle NaN values from non-matching patterns
    extracted = df_audit['Audit ID'].str.extract(r'A-(\d+)')[0]
    # Drop NaN values and convert to int
    existing_ids = extracted.dropna().astype(int)
    
    if existing_ids.empty:
        # No valid audit IDs found, start from 1
        return "A-1"
    
    next_id = existing_ids.max() + 1
    
    return f"A-{next_id}"
