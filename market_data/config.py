"""
Configuration for the Market Data Service.
Reads Alpaca API keys from .env file (local) or st.secrets (cloud).
"""
import os
import sys
from pathlib import Path

# Load .env from project root if python-dotenv is available
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(_env_path)
except ImportError:
    pass

# Import get_secret from config (add parent to path if needed)
_parent = str(Path(__file__).parent.parent)
if _parent not in sys.path:
    sys.path.insert(0, _parent)
from config import get_secret

# Alpaca API credentials (free account — no broker dependency)
ALPACA_API_KEY: str = get_secret("ALPACA_API_KEY")
ALPACA_SECRET_KEY: str = get_secret("ALPACA_SECRET_KEY")

# Cache TTL in seconds (default 5 minutes — matches Yahoo Finance delay)
CACHE_TTL_SECONDS: int = int(get_secret("MARKET_DATA_CACHE_TTL", "300"))
