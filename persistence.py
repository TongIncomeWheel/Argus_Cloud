"""
Persistence layer for portfolio deposit and margin percentages
Saves to JSON file so values persist across app refreshes
Supports multiple portfolios (Income Wheel, Active Core, etc.)
"""
import json
from pathlib import Path
from typing import Dict, Optional, Set, List
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

PERSISTENCE_FILE = Path(__file__).parent / "data" / "user_settings.json"
CHAT_HISTORY_FILE = Path(__file__).parent / "data" / "ai_chat_history.json"
CHAT_RETENTION_DAYS = 10  # Keep chat history for 10 days


def get_portfolio_key(portfolio: str = "Income Wheel") -> str:
    """Get the key prefix for portfolio-specific settings"""
    portfolio_key_map = {
        "Income Wheel": "income_wheel",
        "Active Core": "active_core"
    }
    return portfolio_key_map.get(portfolio, portfolio.lower().replace(" ", "_"))


def load_settings() -> Dict:
    """Load user settings from JSON file"""
    if PERSISTENCE_FILE.exists():
        try:
            with open(PERSISTENCE_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load settings: {e}")
            return {}
    return {}


def save_settings(settings: Dict):
    """Save user settings to JSON file"""
    try:
        PERSISTENCE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(PERSISTENCE_FILE, 'w') as f:
            json.dump(settings, f, indent=2)
        logger.info(f"Settings saved to {PERSISTENCE_FILE}")
    except Exception as e:
        logger.error(f"Could not save settings: {e}")


def get_portfolio_deposit(portfolio: str = "Income Wheel") -> float:
    """Get saved portfolio deposit in USD for the specified portfolio. No cross-portfolio fallback."""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_portfolio_deposit_usd"
    if portfolio_key == "income_wheel":
        return float(settings.get(key, settings.get('portfolio_deposit_usd', settings.get('portfolio_deposit', 0.0))))
    return float(settings.get(key, 0.0))


def save_portfolio_deposit(usd_value: float, portfolio: str = "Income Wheel"):
    """Save portfolio deposit in USD for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_portfolio_deposit_usd"
    settings[key] = usd_value
    save_settings(settings)


def get_portfolio_deposit_sgd(portfolio: str = "Income Wheel") -> float:
    """Get saved portfolio deposit in SGD for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_portfolio_deposit_sgd"
    return float(settings.get(key, 0.0))


def save_portfolio_deposit_sgd(sgd_value: float, portfolio: str = "Income Wheel"):
    """Save portfolio deposit in SGD for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_portfolio_deposit_sgd"
    settings[key] = sgd_value
    save_settings(settings)


def get_fx_rate(portfolio: str = "Income Wheel") -> float:
    """Get saved SGD/USD FX rate for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_sgd_usd_fx_rate"
    return float(settings.get(key, settings.get('sgd_usd_fx_rate', 1.35)))  # Default to 1.35 if not set


def save_fx_rate(fx_rate: float, portfolio: str = "Income Wheel"):
    """Save SGD/USD FX rate for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_sgd_usd_fx_rate"
    settings[key] = fx_rate
    save_settings(settings)


def get_margin_percentages(portfolio: str = "Income Wheel") -> Dict[str, float]:
    """
    Get saved margin percentages for the specified portfolio
    
    NOTE: Margin percentages are stored but NOT USED in calculations.
    They are kept for reference only. All margin calculations are disabled.
    """
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_margin_percentages"
    return settings.get(key, settings.get('margin_percentages', {}))  # Fallback to old key for backward compatibility


def save_margin_percentages(margin_percentages: Dict[str, float], portfolio: str = "Income Wheel"):
    """
    Save margin percentages for the specified portfolio
    
    NOTE: Margin percentages are stored but NOT USED in calculations.
    They are kept for reference only. All margin calculations are disabled.
    """
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_margin_percentages"
    settings[key] = margin_percentages
    save_settings(settings)


def get_capital_allocation(portfolio: str = "Income Wheel") -> Dict[str, float]:
    """Get saved capital allocation by ticker for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_capital_allocation"
    return settings.get(key, settings.get('capital_allocation', {}))  # Fallback to old key for backward compatibility


def save_capital_allocation(capital_allocation: Dict[str, float], portfolio: str = "Income Wheel"):
    """Save capital allocation by ticker for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_capital_allocation"
    settings[key] = capital_allocation
    save_settings(settings)


def get_stock_average_prices(portfolio: str = "Income Wheel") -> Dict[str, float]:
    """Get saved stock average prices by ticker for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_stock_average_prices"
    return settings.get(key, settings.get('stock_average_prices', {}))  # Fallback to old key for backward compatibility


def save_stock_average_prices(stock_average_prices: Dict[str, float], portfolio: str = "Income Wheel"):
    """Save stock average prices by ticker for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_stock_average_prices"
    settings[key] = stock_average_prices
    save_settings(settings)


def get_spy_leap_pl(portfolio: str = "Income Wheel") -> float:
    """Get saved SPY LEAP P&L (manual entry) for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_spy_leap_pl"
    return float(settings.get(key, settings.get('spy_leap_pl', 0.0)))  # Fallback to old key for backward compatibility


def save_spy_leap_pl(pl_value: float, portfolio: str = "Income Wheel"):
    """Save SPY LEAP P&L (manual entry) for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_spy_leap_pl"
    settings[key] = pl_value
    save_settings(settings)


def get_pmcc_tickers(portfolio: str = "Income Wheel") -> Set[str]:
    """
    Get set of tickers that use PMCC logic for the specified portfolio.
    
    Args:
        portfolio: Portfolio name (default: "Income Wheel")
    
    Returns:
        Set of ticker symbols that use PMCC logic
    """
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_pmcc_tickers"
    ticker_list = settings.get(key, [])
    return set(ticker_list) if ticker_list else set()


def save_pmcc_tickers(pmcc_tickers: Set[str], portfolio: str = "Income Wheel"):
    """
    Save PMCC ticker flags for the specified portfolio.
    
    Args:
        pmcc_tickers: Set of ticker symbols that use PMCC logic
        portfolio: Portfolio name (default: "Income Wheel")
    """
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_pmcc_tickers"
    # Convert set to list for JSON serialization
    settings[key] = sorted(list(pmcc_tickers))
    save_settings(settings)


def get_tickers(portfolio: str = "Income Wheel") -> List[str]:
    """
    Get saved ticker list for the specified portfolio.
    Returns the list stored in settings; may be empty if never set.
    """
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_tickers"
    out = settings.get(key, [])
    return list(out) if isinstance(out, list) else []


def save_tickers(tickers: List[str], portfolio: str = "Income Wheel"):
    """Save ticker list for the specified portfolio (sorted, unique)."""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_tickers"
    settings[key] = sorted([str(t).strip().upper() for t in tickers if str(t).strip()])
    save_settings(settings)


def get_settled_usd_cash(portfolio: str = "Income Wheel") -> float:
    """Get settled USD cash balance for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_settled_usd_cash"
    return float(settings.get(key, 0.0))


def save_settled_usd_cash(settled_cash: float, portfolio: str = "Income Wheel"):
    """Save settled USD cash balance for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_settled_usd_cash"
    settings[key] = settled_cash
    save_settings(settings)


def get_tiger_vault_balance(portfolio: str = "Income Wheel") -> float:
    """Get Tiger Vault (MMF) balance for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_tiger_vault_balance"
    return float(settings.get(key, 0.0))


def save_tiger_vault_balance(vault_balance: float, portfolio: str = "Income Wheel"):
    """Save Tiger Vault (MMF) balance for the specified portfolio"""
    settings = load_settings()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_tiger_vault_balance"
    settings[key] = vault_balance
    save_settings(settings)


# ============================================================
# AI CHAT HISTORY PERSISTENCE (Model-Agnostic, 7-10 Day Retention)
# ============================================================

def load_chat_history() -> List[Dict]:
    """
    Load chat history from JSON file
    Automatically filters out messages older than retention period
    Returns empty list if file doesn't exist or is invalid
    """
    if not CHAT_HISTORY_FILE.exists():
        return []
    
    try:
        with open(CHAT_HISTORY_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Get all messages
        messages = data.get('messages', [])
        
        # Filter by retention period (remove messages older than retention_days)
        cutoff_date = datetime.now() - timedelta(days=CHAT_RETENTION_DAYS)
        filtered_messages = []
        
        for msg in messages:
            timestamp_str = msg.get('timestamp', '')
            if timestamp_str:
                try:
                    msg_date = datetime.fromisoformat(timestamp_str)
                    if msg_date >= cutoff_date:
                        filtered_messages.append(msg)
                except (ValueError, TypeError):
                    # Invalid timestamp, skip this message
                    continue
            else:
                # No timestamp, keep it (assume recent)
                filtered_messages.append(msg)
        
        # If we filtered out old messages, save the cleaned version
        if len(filtered_messages) < len(messages):
            save_chat_history(filtered_messages)
            logger.info(f"Cleaned chat history: removed {len(messages) - len(filtered_messages)} old messages")
        
        return filtered_messages
        
    except Exception as e:
        logger.warning(f"Could not load chat history: {e}")
        return []


def save_chat_history(messages: List[Dict]):
    """
    Save chat history to JSON file
    Automatically filters out messages older than retention period before saving
    
    Args:
        messages: List of message dicts with 'role', 'content', 'timestamp'
    """
    try:
        # Filter by retention period
        cutoff_date = datetime.now() - timedelta(days=CHAT_RETENTION_DAYS)
        filtered_messages = []
        
        for msg in messages:
            timestamp_str = msg.get('timestamp', '')
            if timestamp_str:
                try:
                    msg_date = datetime.fromisoformat(timestamp_str)
                    if msg_date >= cutoff_date:
                        filtered_messages.append(msg)
                except (ValueError, TypeError):
                    # Invalid timestamp, skip this message
                    continue
            else:
                # No timestamp, add current timestamp and keep
                msg['timestamp'] = datetime.now().isoformat()
                filtered_messages.append(msg)
        
        # Save to file
        CHAT_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = {
            'last_updated': datetime.now().isoformat(),
            'retention_days': CHAT_RETENTION_DAYS,
            'message_count': len(filtered_messages),
            'messages': filtered_messages
        }
        
        with open(CHAT_HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Chat history saved: {len(filtered_messages)} messages (retention: {CHAT_RETENTION_DAYS} days)")
        
    except Exception as e:
        logger.error(f"Could not save chat history: {e}")


def clear_chat_history():
    """Clear all chat history from persistent storage"""
    try:
        if CHAT_HISTORY_FILE.exists():
            CHAT_HISTORY_FILE.unlink()
        logger.info("Chat history cleared")
    except Exception as e:
        logger.error(f"Could not clear chat history: {e}")


def get_chat_history_stats() -> Dict:
    """Get statistics about stored chat history"""
    if not CHAT_HISTORY_FILE.exists():
        return {
            'total_messages': 0,
            'oldest_message': None,
            'newest_message': None,
            'retention_days': CHAT_RETENTION_DAYS
        }
    
    try:
        messages = load_chat_history()
        if not messages:
            return {
                'total_messages': 0,
                'oldest_message': None,
                'newest_message': None,
                'retention_days': CHAT_RETENTION_DAYS
            }
        
        timestamps = [msg.get('timestamp') for msg in messages if msg.get('timestamp')]
        timestamps = [datetime.fromisoformat(ts) for ts in timestamps if ts]
        
        return {
            'total_messages': len(messages),
            'oldest_message': min(timestamps).isoformat() if timestamps else None,
            'newest_message': max(timestamps).isoformat() if timestamps else None,
            'retention_days': CHAT_RETENTION_DAYS
        }
    except Exception as e:
        logger.warning(f"Could not get chat history stats: {e}")
        return {
            'total_messages': 0,
            'oldest_message': None,
            'newest_message': None,
            'retention_days': CHAT_RETENTION_DAYS
        }


# ============================================================
# OPEN LOTS & MARKET PRICES PERSISTENCE (For LLM Reference & Future API Integration)
# ============================================================

MARKET_DATA_FILE = Path(__file__).parent / "data" / "market_data.json"


def load_market_data() -> Dict:
    """Load market data (Open_lots and current prices) from JSON file"""
    if not MARKET_DATA_FILE.exists():
        return {
            'open_lots': {},  # {ticker: shares}
            'current_prices': {},  # {ticker: {'price': float, 'timestamp': str, 'source': str}}
            'last_updated': None
        }
    
    try:
        with open(MARKET_DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Ensure structure exists
        if 'open_lots' not in data:
            data['open_lots'] = {}
        if 'current_prices' not in data:
            data['current_prices'] = {}
        
        return data
    except Exception as e:
        logger.warning(f"Could not load market data: {e}")
        return {
            'open_lots': {},
            'current_prices': {},
            'last_updated': None
        }


def save_market_data(market_data: Dict):
    """Save market data (Open_lots and current prices) to JSON file"""
    try:
        MARKET_DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        market_data['last_updated'] = datetime.now().isoformat()
        
        with open(MARKET_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(market_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Market data saved to {MARKET_DATA_FILE}")
    except Exception as e:
        logger.error(f"Could not save market data: {e}")


def get_open_lots(portfolio: str = "Income Wheel") -> Dict[str, float]:
    """
    Get Open_lots (shares held) by ticker for the specified portfolio
    
    Returns:
        Dict of {ticker: shares} - e.g., {'MARA': 15900.0, 'CRCL': 500.0}
    """
    data = load_market_data()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_open_lots"
    return data.get(key, data.get('open_lots', {}))


def save_open_lots(open_lots: Dict[str, float], portfolio: str = "Income Wheel"):
    """
    Save Open_lots (shares held) by ticker for the specified portfolio
    
    Args:
        open_lots: Dict of {ticker: shares} - e.g., {'MARA': 15900.0, 'CRCL': 500.0}
        portfolio: Portfolio name (default: "Income Wheel")
    """
    data = load_market_data()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_open_lots"
    data[key] = open_lots
    save_market_data(data)


def get_current_prices(portfolio: str = "Income Wheel") -> Dict[str, Dict]:
    """
    Get current market prices by ticker for the specified portfolio
    
    Returns:
        Dict of {ticker: {'price': float, 'timestamp': str, 'source': str}}
        Example: {'MARA': {'price': 16.48, 'timestamp': '2026-01-26T13:00:00', 'source': 'yahoo'}}
    """
    data = load_market_data()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_current_prices"
    return data.get(key, data.get('current_prices', {}))


def save_current_prices(prices: Dict[str, Dict], portfolio: str = "Income Wheel"):
    """
    Save current market prices by ticker for the specified portfolio
    
    Args:
        prices: Dict of {ticker: {'price': float, 'timestamp': str, 'source': str}}
        portfolio: Portfolio name (default: "Income Wheel")
    """
    data = load_market_data()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_current_prices"
    data[key] = prices
    save_market_data(data)


def update_current_price(ticker: str, price: float, source: str = "yahoo", portfolio: str = "Income Wheel"):
    """
    Update current price for a single ticker
    
    Args:
        ticker: Ticker symbol
        price: Current market price
        source: Price source ('yahoo', 'ibkr', 'manual', etc.)
        portfolio: Portfolio name (default: "Income Wheel")
    """
    data = load_market_data()
    portfolio_key = get_portfolio_key(portfolio)
    key = f"{portfolio_key}_current_prices"
    
    if key not in data:
        data[key] = {}
    
    data[key][ticker] = {
        'price': float(price),
        'timestamp': datetime.now().isoformat(),
        'source': source
    }
    
    save_market_data(data)


def get_market_data_summary(portfolio: str = "Income Wheel") -> Dict:
    """Get summary of market data for LLM reference"""
    open_lots = get_open_lots(portfolio)
    current_prices = get_current_prices(portfolio)
    
    summary = {}
    for ticker in set(list(open_lots.keys()) + list(current_prices.keys())):
        summary[ticker] = {
            'open_lots': open_lots.get(ticker, 0),
            'current_price': current_prices.get(ticker, {}).get('price'),
            'price_timestamp': current_prices.get(ticker, {}).get('timestamp'),
            'price_source': current_prices.get(ticker, {}).get('source', 'unknown')
        }
    
    return summary


# ─────────────────────────────────────────────
# DAILY CIO REPORT PERSISTENCE
# ─────────────────────────────────────────────

DAILY_REPORT_FILE = Path(__file__).parent / "data" / "daily_report.json"


def save_daily_report(report_data: dict) -> None:
    """
    Save the generated CIO daily report to disk.
    report_data keys: markdown, generated_at, portfolio, model
    """
    try:
        DAILY_REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(DAILY_REPORT_FILE, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        logger.info("Daily report saved to %s", DAILY_REPORT_FILE)
    except Exception as e:
        logger.error("Failed to save daily report: %s", e)


def load_daily_report() -> Optional[dict]:
    """
    Load the last saved CIO daily report from disk.
    Returns None if no report has been saved yet.
    """
    if not DAILY_REPORT_FILE.exists():
        return None
    try:
        with open(DAILY_REPORT_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error("Failed to load daily report: %s", e)
        return None
