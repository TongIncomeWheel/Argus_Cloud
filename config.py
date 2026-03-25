"""
Configuration for ARGUS App
Supports both local (.env) and Streamlit Cloud (st.secrets) deployment.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def get_secret(key: str, default: str = "") -> str:
    """Get a secret from Streamlit Cloud secrets first, then fall back to .env."""
    try:
        import streamlit as st
        val = st.secrets.get(key, None)
        if val is not None:
            return str(val)
    except (ImportError, FileNotFoundError, AttributeError):
        pass
    return os.getenv(key, default)


# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
BACKUP_DIR = DATA_DIR / "backups"
LOGS_DIR = BASE_DIR / "logs"

# Ensure directories exist (handles ephemeral filesystems)
for _d in [DATA_DIR, BACKUP_DIR, LOGS_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# Google Sheets
GSHEET_CREDENTIALS_PATH = str(BASE_DIR / "gsheet_credentials.json")
INCOME_WHEEL_SHEET_ID = get_secret("INCOME_WHEEL_SHEET_ID")
ACTIVE_CORE_SHEET_ID = get_secret("ACTIVE_CORE_SHEET_ID")

# Tickers to track (configurable)
TICKERS = ["MARA", "SPY", "CRCL", "ETHA", "SOL"]

# Trading parameters
WEEKLY_TARGET_PCT = 0.25  # 25% of capital deployed per week
TRADING_DAYS_PER_WEEK = 5

# IBKR TWS settings
IBKR_HOST = "127.0.0.1"
IBKR_PORT = 7497  # 7497 for live, 7496 for paper
IBKR_CLIENT_ID = 100

# Risk thresholds
CALL_RISK_HIGH_DTE = 7  # Days to expiry to flag high call risk
CALL_RISK_MEDIUM_BUFFER = 0.98  # Price within 2% of strike = medium risk
EXPIRING_SOON_DTE = 14  # Flag positions expiring within 14 days (2 weeks)

# Capital calculation
MARGIN_REQUIREMENT_PCT = 0.20  # 20% margin requirement for CSP

# Backup settings
BACKUP_RETENTION_DAYS = 7

# Logging
LOG_LEVEL = "INFO"
