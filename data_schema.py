"""
Data Schema Definition - Single Source of Truth
Maps logical field names to actual Excel column names
"""

from typing import Dict, Optional
import pandas as pd

# Schema version for migration tracking
SCHEMA_VERSION = "1.0.0"

# Field mappings: logical_name -> excel_column_name
SCHEMA = {
    'identity': {
        'trade_id': 'TradeID',
        'ticker': 'Ticker',
        'trade_type': 'TradeType',
        'strategy_type': 'StrategyType',
        'status': 'Status',
        'direction': 'Direction'
    },
    'options': {
        'strike': 'Option_Strike_Price_(USD)',
        'premium_per_share': 'OptPremium',
        'expiry_date': 'Expiry_Date',
        'dte': 'DTE'
    },
    'position': {
        'quantity': 'Quantity',
        'open_lots': 'Open_lots',
        'current_price': 'Price_of_current_underlying_(USD)',
        'date_open': 'Date_open',
        'date_closed': 'Date_closed',
        'close_price': 'Close_Price'
    },
    'pnl': {
        'realized_profit': 'Actual_Profit_(USD)',
        'total_premium': 'Total_Premium',
        'stock_pnl': 'Stock PNL'
    },
    'metadata': {
        'remarks': 'Remarks'
    }
}

# Field interpretations - critical for calculations
FIELD_INTERPRETATIONS = {
    'premium_per_share': {
        'description': 'Premium per share (not per contract)',
        'calculation_note': 'Multiply by 100 to get per contract, then by Quantity for total',
        'example': 'OptPremium = 0.50 means $0.50 per share = $50 per contract'
    },
    'quantity': {
        'description': 'Number of contracts for options (CC, CSP, LEAP), shares for stock (STOCK)',
        'calculation_note': 'CRITICAL: For options (CC/CSP/LEAP): Quantity = contracts (1 contract = 100 shares). For stock (STOCK): Quantity = shares. Always check Open_lots first for STOCK positions.',
        'example': 'Quantity = 5 for CSP means 5 contracts = 500 shares obligation. Quantity = 500 for STOCK means 500 shares.'
    },
    'open_lots': {
        'description': 'Actual shares held (for STOCK positions only) - ALWAYS use this for stock share count',
        'calculation_note': 'CRITICAL: Open_lots is the PRIMARY source for stock shares. If Open_lots exists and is non-zero, use it. Only fall back to Quantity if Open_lots is missing or zero. For options, Open_lots may be set but represents shares equivalent (Quantity * 100).',
        'example': 'Open_lots = 15,900 for STOCK means 15,900 shares held. Open_lots = 12,000 for CC means 12,000 shares equivalent (120 contracts * 100).'
    },
    'current_price': {
        'description': 'Stock price at entry or current market price',
        'calculation_note': 'Priority: 1) stock_avg_prices (Performance tab), 2) live_prices, 3) Price_of_current_underlying_(USD)',
        'example': 'Price_of_current_underlying_(USD) = 112.50'
    }
}

# Calculation formulas
CALCULATION_FORMULAS = {
    'csp_reserved': {
        'formula': 'strike * 100 * quantity',
        'description': 'CSP Reserved Capital = Strike Price × 100 × Number of Contracts',
        'fields': ['strike', 'quantity'],
        'note': 'Quantity is in contracts. 1 contract = 100 shares obligation.'
    },
    'leap_sunk': {
        'formula': 'premium_per_share * 100 * quantity',
        'description': 'LEAP Sunk Capital = Premium per Share × 100 × Number of Contracts',
        'fields': ['premium_per_share', 'quantity'],
        'note': 'OptPremium is per share, so multiply by 100 for contract cost. Quantity is in contracts.'
    },
    'stock_locked': {
        'formula': 'shares * price',
        'description': 'Stock Locked Capital = Shares × Price',
        'fields': ['shares', 'price'],
        'note': 'Shares from Open_lots (preferred) or Quantity. Price from stock_avg_prices > live_prices > current_price. For STOCK: Open_lots is actual shares. For LEAP: Open_lots is shares equivalent (contracts * 100).'
    },
    'total_premium': {
        'formula': 'premium_per_share * 100 * quantity',
        'description': 'Total Premium = Premium per Share × 100 × Number of Contracts',
        'fields': ['premium_per_share', 'quantity'],
        'note': 'Quantity is in contracts for options. OptPremium is per share.'
    },
    'cc_coverage': {
        'formula': 'cc_shares_needed / total_stock_shares',
        'description': 'CC Coverage Ratio = (CC Contracts × 100) / (Stock Shares + LEAP Shares)',
        'fields': ['cc_contracts', 'stock_shares', 'leap_shares'],
        'note': 'If ratio < 1.0: Covered. If ratio > 1.0: Uncovered. If LEAPs exist, use CC/LEAP contract ratio instead.'
    }
}


def get_field_name(logical_name: str, category: Optional[str] = None) -> str:
    """
    Get Excel column name from logical field name
    
    Args:
        logical_name: Logical field name (e.g., 'trade_id')
        category: Optional category ('identity', 'options', 'position', 'pnl', 'metadata')
    
    Returns:
        Excel column name (e.g., 'TradeID')
    """
    if category:
        return SCHEMA.get(category, {}).get(logical_name, logical_name)
    
    # Search all categories
    for cat_schema in SCHEMA.values():
        if logical_name in cat_schema:
            return cat_schema[logical_name]
    
    return logical_name


def get_logical_name(excel_column: str) -> Optional[str]:
    """
    Get logical field name from Excel column name
    
    Args:
        excel_column: Excel column name (e.g., 'TradeID')
    
    Returns:
        Logical field name (e.g., 'trade_id') or None if not found
    """
    for category, fields in SCHEMA.items():
        for logical, excel in fields.items():
            if excel == excel_column:
                return logical
    return None


def validate_schema(df: pd.DataFrame) -> Dict[str, any]:
    """
    Validate DataFrame against schema
    
    Returns:
        dict with 'valid': bool, 'missing_fields': list, 'extra_fields': list
    """
    required_fields = []
    for category_fields in SCHEMA.values():
        required_fields.extend(category_fields.values())
    
    missing_fields = [f for f in required_fields if f not in df.columns]
    extra_fields = [f for f in df.columns if f not in required_fields]
    
    return {
        'valid': len(missing_fields) == 0,
        'missing_fields': missing_fields,
        'extra_fields': extra_fields,
        'schema_version': SCHEMA_VERSION
    }


def get_field_interpretation(field_name: str) -> Optional[Dict]:
    """Get interpretation details for a field"""
    return FIELD_INTERPRETATIONS.get(field_name)


def get_calculation_formula(calculation_name: str) -> Optional[Dict]:
    """Get formula details for a calculation"""
    return CALCULATION_FORMULAS.get(calculation_name)
