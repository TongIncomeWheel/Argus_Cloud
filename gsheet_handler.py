"""
Google Sheets read/write operations — drop-in replacement for ExcelHandler.
Uses gspread + service-account auth to read/write Google Sheets instead of
local .xlsx files.
"""
import json
import logging
import re
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Dict

import gspread
import pandas as pd

from config import GSHEET_CREDENTIALS_PATH, BACKUP_DIR, BACKUP_RETENTION_DAYS, LOGS_DIR

# ---------------------------------------------------------------------------
# Logging (same pattern as excel_handler.py)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / 'income_wheel.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Date columns that need pd.to_datetime coercion
# ---------------------------------------------------------------------------
_DATE_COLUMNS = ['Date_open', 'Date_closed', 'Expiry_Date']

# Phase 8.3: Whitelist of columns that should be numeric (not heuristic)
_NUMERIC_COLUMNS = [
    'Quantity', 'Open_lots', 'Option_Strike_Price_(USD)',
    'Price_of_current_underlying_(USD)', 'OptPremium', 'Opt_Premium_%',
    'Close_Price', 'Actual_Profit_(USD)', 'Cash_required_per_position_(USD)',
    'AffectedQty', 'Cost_Basis', 'BTC_Price', 'Net_Credit', 'PnL',
]


# ---------------------------------------------------------------------------
# Helper: convert gspread records to a clean DataFrame
# ---------------------------------------------------------------------------
def _records_to_dataframe(records: list, sheet_label: str = "") -> pd.DataFrame:
    """Convert list-of-dicts from gspread.get_all_records() into a DataFrame.

    Handles:
    - Empty record list (returns empty DF with no columns — caller will
      still get a valid DataFrame).
    - Empty-string cells -> NaN
    - Numeric coercion for columns that should be numbers
    - Date parsing for known date columns
    """
    if not records:
        logger.info(f"No data rows in {sheet_label} (headers-only or empty)")
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Replace empty strings and placeholder dashes with NaN
    df.replace(['', '-', '—', 'N/A', 'n/a', '#N/A'], pd.NA, inplace=True)

    # Coerce date columns
    for col in _DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Phase 8.3: Coerce only whitelisted numeric columns (not heuristic)
    for col in _NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # TradeID should always be a string
    if 'TradeID' in df.columns:
        df['TradeID'] = df['TradeID'].astype(str)

    return df


def _serialize_value(value):
    """Convert a Python value to something gspread/Google Sheets can accept."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ''
    # BUG-16 fix: catch NaT/NaT-like before strftime — pd.NaT, NaT Timestamps
    try:
        if pd.isna(value):
            return ''
    except (TypeError, ValueError):
        pass
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.strftime('%Y-%m-%d')
    # numpy int / float -> plain Python types
    try:
        import numpy as np
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            return float(value)
    except ImportError:
        pass
    return value


# ===================================================================
# GSheetHandler
# ===================================================================
class GSheetHandler:
    """Handle all Google-Sheets read/write operations.

    Public API mirrors ExcelHandler so it can be a drop-in replacement.
    """

    def __init__(self, sheet_id: str):
        self.sheet_id = sheet_id
        self.backup_dir = BACKUP_DIR
        self._header_cache: Dict[str, list] = {}  # Phase 8.1: cached headers

        # Authenticate: local JSON file first, then Streamlit Cloud secrets
        from pathlib import Path as _Path
        _creds_path = _Path(GSHEET_CREDENTIALS_PATH)
        if _creds_path.exists():
            gc = gspread.service_account(filename=str(_creds_path))
        else:
            try:
                import streamlit as st
                gc = gspread.service_account_from_dict(dict(st.secrets["gsheet_credentials"]))
            except Exception as e:
                raise FileNotFoundError(
                    f"No gsheet_credentials.json and st.secrets failed: {e}"
                )
        self.spreadsheet = gc.open_by_key(sheet_id)
        logger.info(f"Opened Google Sheet: {self.spreadsheet.title} ({sheet_id})")

    def _get_headers(self, ws) -> list:
        """Phase 8.1: Return cached headers for a worksheet (avoids redundant API calls)."""
        ws_title = ws.title
        if ws_title not in self._header_cache:
            self._header_cache[ws_title] = ws.row_values(1)
        return self._header_cache[ws_title]

    def _invalidate_header_cache(self, ws_title: str = None):
        """Clear header cache (call if columns are added/removed)."""
        if ws_title:
            self._header_cache.pop(ws_title, None)
        else:
            self._header_cache.clear()

    @staticmethod
    def _retry(func, *args, max_retries=3, **kwargs):
        """Phase 8.2: Retry with exponential backoff for transient API errors."""
        import time
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except gspread.exceptions.APIError as e:
                if e.response.status_code == 429 and attempt < max_retries - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Rate limited (429), retrying in {wait}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait)
                else:
                    raise
            except Exception:
                raise

    # ------------------------------------------------------------------
    # Readers
    # ------------------------------------------------------------------
    def read_data_table(self) -> pd.DataFrame:
        """Read 'Data Table' worksheet -> DataFrame."""
        try:
            ws = self.spreadsheet.worksheet("Data Table")
            records = ws.get_all_records()
            df = _records_to_dataframe(records, sheet_label="Data Table")
            logger.info(f"Loaded {len(df)} trades from Data Table")
            return df
        except Exception as e:
            logger.error(f"Error reading Data Table: {e}")
            raise

    def read_audit_table(self) -> pd.DataFrame:
        """Read 'Audit_Table' worksheet -> DataFrame."""
        try:
            ws = self.spreadsheet.worksheet("Audit_Table")
            records = ws.get_all_records()
            df = _records_to_dataframe(records, sheet_label="Audit_Table")
            logger.info(f"Loaded {len(df)} audit entries")
            return df
        except Exception as e:
            logger.error(f"Error reading Audit Table: {e}")
            raise

    def read_daily_helper(self) -> pd.DataFrame:
        """Read 'Daily Helper' worksheet -> DataFrame."""
        try:
            ws = self.spreadsheet.worksheet("Daily Helper")
            records = ws.get_all_records()
            df = _records_to_dataframe(records, sheet_label="Daily Helper")
            logger.info("Loaded Daily Helper data")
            return df
        except Exception as e:
            logger.error(f"Error reading Daily Helper: {e}")
            raise

    def load_all_data(self) -> dict:
        """Load all sheets at once.

        Returns:
            dict with 'trades', 'audit', 'daily_helper' DataFrames
        """
        return {
            'trades': self.read_data_table(),
            'audit': self.read_audit_table(),
            'daily_helper': self.read_daily_helper()
        }

    # ------------------------------------------------------------------
    # Writers
    # ------------------------------------------------------------------
    def append_trade(self, trade_data: dict) -> bool:
        """Insert a new trade row at the TOP of Data Table (row 2).

        Also auto-populates the 'Sorter' column with the TradeID so
        the most recent trades always appear first and are easy to find.

        Args:
            trade_data: dict whose keys match the Data Table column headers.

        Returns:
            True on success.
        """
        try:
            ws = self.spreadsheet.worksheet("Data Table")
            headers = self._get_headers(ws)

            # Phase 8: backup before append too
            self._backup_worksheet_json(ws, "Data Table")

            # Auto-fill Sorter column with numeric part of TradeID for easy sorting
            trade_id = trade_data.get('TradeID', '')
            if 'Sorter' in headers and 'Sorter' not in trade_data:
                import re as _re
                _num_match = _re.search(r'T-(\d+)', str(trade_id))
                trade_data['Sorter'] = int(_num_match.group(1)) if _num_match else trade_id

            row = [_serialize_value(trade_data.get(h, '')) for h in headers]
            self._retry(ws.insert_row, row, index=2, value_input_option='USER_ENTERED')
            logger.info(f"Inserted trade at top: {trade_id}")
            return True
        except Exception as e:
            logger.error(f"Error inserting trade: {e}")
            raise

    def update_trade(self, trade_id: str, updates: dict) -> bool:
        """Update an existing trade in Data Table by TradeID.

        Args:
            trade_id: The TradeID string to locate.
            updates:  dict of {column_name: new_value}.

        Returns:
            True on success.
        """
        try:
            ws = self.spreadsheet.worksheet("Data Table")

            # Create a JSON backup before destructive operation
            self._backup_worksheet_json(ws, "Data Table")

            headers = self._get_headers(ws)
            row_idx = self._find_trade_row(ws, trade_id)
            if row_idx is None:
                raise ValueError(f"TradeID {trade_id} not found in Data Table")

            # Build batch update cells
            cells_to_update = []
            for col_name, value in updates.items():
                if col_name not in headers:
                    logger.warning(f"Column '{col_name}' not in headers — skipping")
                    continue
                col_idx = headers.index(col_name) + 1  # 1-based
                cells_to_update.append(
                    gspread.Cell(row_idx, col_idx, _serialize_value(value))
                )

            if cells_to_update:
                self._retry(ws.update_cells, cells_to_update, value_input_option='USER_ENTERED')

            logger.info(f"Updated trade: {trade_id} with {updates}")
            return True
        except Exception as e:
            logger.error(f"Error updating trade: {e}")
            raise

    def delete_trades(self, trade_ids: list) -> bool:
        """Delete one or more trades from Data Table by TradeID.

        Deletes rows bottom-up to preserve row indices.
        """
        if not trade_ids:
            return True
        try:
            ws = self.spreadsheet.worksheet("Data Table")

            # Create a JSON backup before destructive operation
            self._backup_worksheet_json(ws, "Data Table")

            trade_ids_str = {str(t).strip() for t in trade_ids}

            # Get all TradeID values (column A typically, but find by header)
            headers = ws.row_values(1)
            if 'TradeID' not in headers:
                raise ValueError("TradeID column not found in Data Table headers")
            trade_col_idx = headers.index('TradeID') + 1  # 1-based

            all_values = ws.col_values(trade_col_idx)  # includes header at [0]

            rows_to_delete = []
            for i, val in enumerate(all_values):
                if i == 0:
                    continue  # skip header
                if str(val).strip() in trade_ids_str:
                    rows_to_delete.append(i + 1)  # 1-based row number

            if not rows_to_delete:
                logger.warning(f"No rows matched TradeIDs: {trade_ids}")
                return True

            # Delete bottom-up to keep indices stable
            for row_idx in sorted(rows_to_delete, reverse=True):
                ws.delete_rows(row_idx)

            logger.info(f"Deleted {len(rows_to_delete)} trade(s): {trade_ids}")
            return True
        except Exception as e:
            logger.error(f"Error deleting trades: {e}")
            raise

    def append_audit(self, audit_data: dict) -> bool:
        """Insert an audit entry at the TOP of Audit_Table (row 2).

        Args:
            audit_data: dict whose keys match Audit_Table headers.

        Returns:
            True on success.
        """
        try:
            ws = self.spreadsheet.worksheet("Audit_Table")
            headers = self._get_headers(ws)
            # Phase 8: backup Audit_Table too
            self._backup_worksheet_json(ws, "Audit_Table")
            row = [_serialize_value(audit_data.get(h, '')) for h in headers]
            self._retry(ws.insert_row, row, index=2, value_input_option='USER_ENTERED')
            logger.info(f"Inserted audit at top: {audit_data.get('Audit ID', 'Unknown')}")
            return True
        except Exception as e:
            logger.error(f"Error inserting audit: {e}")
            raise

    # ------------------------------------------------------------------
    # Atomic transaction
    # ------------------------------------------------------------------
    def atomic_transaction(self, operations: list) -> bool:
        """Execute multiple operations sequentially.

        Unlike ExcelHandler there is no local file to backup/restore, so
        we take a JSON snapshot before the batch and execute each op in
        order.  If any op fails the remaining ops are skipped and the
        error is raised.

        Args:
            operations: list of dicts with 'type' and 'data'.
                Types: 'append_trade', 'update_trade', 'delete_trades',
                       'append_audit'

        Returns:
            True if all operations succeed.
        """
        try:
            for op in operations:
                op_type = op['type']
                op_data = op['data']

                if op_type == 'append_trade':
                    self.append_trade(op_data)
                elif op_type == 'update_trade':
                    self.update_trade(op_data['trade_id'], op_data['updates'])
                elif op_type == 'delete_trades':
                    self.delete_trades(op_data if isinstance(op_data, list)
                                       else op_data['trade_ids'])
                elif op_type == 'append_audit':
                    self.append_audit(op_data)
                else:
                    raise ValueError(f"Unknown operation type: {op_type}")

            logger.info(f"Atomic transaction completed: {len(operations)} operations")
            return True
        except Exception as e:
            logger.error(f"Atomic transaction failed: {e}")
            raise

    # ------------------------------------------------------------------
    # Phase 5.3: Settings tab — persist user_settings to Google Sheet
    # ------------------------------------------------------------------
    def read_settings(self) -> dict:
        """Read key-value settings from a 'Settings' worksheet.
        Returns dict of {key: value}. Creates the tab if it doesn't exist.
        """
        try:
            try:
                ws = self.spreadsheet.worksheet("Settings")
            except gspread.exceptions.WorksheetNotFound:
                ws = self.spreadsheet.add_worksheet(title="Settings", rows=100, cols=2)
                ws.update('A1:B1', [['Key', 'Value']])
                return {}

            records = ws.get_all_records()
            settings = {}
            for r in records:
                key = r.get('Key', '')
                val = r.get('Value', '')
                if key:
                    # Try to parse JSON values (for dicts/lists)
                    try:
                        settings[key] = json.loads(val) if isinstance(val, str) and val.startswith('{') else val
                    except (json.JSONDecodeError, TypeError):
                        settings[key] = val
            return settings
        except Exception as e:
            logger.warning(f"Error reading Settings: {e}")
            return {}

    def write_settings(self, settings: dict) -> bool:
        """Write key-value settings to the 'Settings' worksheet.
        Overwrites all existing settings.
        """
        try:
            try:
                ws = self.spreadsheet.worksheet("Settings")
            except gspread.exceptions.WorksheetNotFound:
                ws = self.spreadsheet.add_worksheet(title="Settings", rows=100, cols=2)

            rows = [['Key', 'Value']]
            for key, val in settings.items():
                # Serialize dicts/lists as JSON
                if isinstance(val, (dict, list)):
                    rows.append([key, json.dumps(val)])
                else:
                    rows.append([key, _serialize_value(val)])

            ws.clear()
            ws.update(f'A1:B{len(rows)}', rows, value_input_option='USER_ENTERED')
            logger.info(f"Settings saved: {len(settings)} keys")
            return True
        except Exception as e:
            logger.error(f"Error writing Settings: {e}")
            return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _find_trade_row(self, ws, trade_id: str) -> Optional[int]:
        """Return the 1-based row index for *trade_id* in worksheet *ws*,
        or None if not found.

        Searches the TradeID column (found via header row).
        """
        headers = ws.row_values(1)
        if 'TradeID' not in headers:
            raise ValueError("TradeID column not found in headers")
        col_idx = headers.index('TradeID') + 1  # 1-based
        col_values = ws.col_values(col_idx)

        trade_id_str = str(trade_id).strip()
        for i, val in enumerate(col_values):
            if i == 0:
                continue  # skip header
            if str(val).strip() == trade_id_str:
                return i + 1  # 1-based row number
        return None

    def _backup_worksheet_json(self, ws, label: str):
        """Write a JSON snapshot of the worksheet to data/backups/.

        This provides a recovery point before destructive operations
        (update_trade, delete_trades).
        """
        try:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_label = label.replace(' ', '_')
            backup_path = self.backup_dir / f"gsheet_{safe_label}_{timestamp}.json"

            all_data = ws.get_all_values()  # list of lists, includes header
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=2)

            logger.info(f"JSON backup created: {backup_path}")
            self._cleanup_old_backups()
        except Exception as e:
            # Non-fatal: log and continue — the actual operation is more
            # important than the backup succeeding.
            logger.warning(f"Failed to create JSON backup for {label}: {e}")

    def _cleanup_old_backups(self):
        """Remove gsheet JSON backups older than BACKUP_RETENTION_DAYS."""
        cutoff = datetime.now().timestamp() - (BACKUP_RETENTION_DAYS * 24 * 60 * 60)
        for f in self.backup_dir.glob("gsheet_*.json"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    logger.info(f"Removed old backup: {f}")
            except OSError:
                pass


# ===================================================================
# Standalone validation (copied from excel_handler.py — pure DataFrame
# logic, no Excel dependency)
# ===================================================================
def validate_data_integrity(df_trades: pd.DataFrame, df_audit: pd.DataFrame) -> list:
    """
    Run integrity checks on loaded data

    Returns:
        List of error messages (empty if all checks pass)
    """
    errors = []

    # BUG-09 fix: guard for empty DataFrames
    if df_trades.empty or 'TradeID' not in df_trades.columns:
        return errors

    # Check 1: No duplicate TradeIDs (only warn if exact duplicates, not intentional splits)
    if df_trades['TradeID'].duplicated().any():
        duplicates = df_trades[df_trades['TradeID'].duplicated()]['TradeID'].unique().tolist()
        # Check if duplicates are intentional splits (e.g., T-146, T-146A, T-146B)
        true_duplicates = []
        for dup_id in duplicates:
            dup_str = str(dup_id)
            exact_dups = df_trades[df_trades['TradeID'] == dup_id]
            if len(exact_dups) > 1:
                true_duplicates.append(dup_str)

        if true_duplicates:
            errors.append(f"Duplicate TradeIDs found: {true_duplicates}")

    # Check 2: All audit references exist (handle partial split trades)
    audit_refs = df_audit['TradeID_Ref'].dropna().str.split(',').explode().str.strip()
    trade_ids = set(df_trades['TradeID'].dropna().astype(str))

    def is_valid_trade_ref(ref: str) -> bool:
        """Check if audit reference is valid, including partial split trades"""
        ref = str(ref).strip()

        # Special cases
        if ref.upper() in ('ALL', 'MULTIPLE'):
            return True

        # Direct match
        if ref in trade_ids:
            return True

        # Pattern 1: T-XXX-SUFFIX (with dash)
        pattern1 = r'^(T-?\d+)-([A-Z]|\d+)$'
        match1 = re.match(pattern1, ref)

        # Pattern 2: T-XXXSUFFIX (no dash, like T-146A)
        pattern2 = r'^(T-?\d+)([A-Z]|\d+)$'
        match2 = re.match(pattern2, ref)

        if match1:
            base_trade_id = match1.group(1)
        elif match2:
            base_trade_id = match2.group(1)
        else:
            base_trade_id = None

        if base_trade_id:
            base_variants = [base_trade_id, base_trade_id.replace('-', '')]
            for variant in base_variants:
                if variant in trade_ids:
                    return True
                for trade_id in trade_ids:
                    trade_str = str(trade_id)
                    if trade_str.startswith(variant + '-') or trade_str.startswith(variant):
                        remaining = trade_str[len(variant):]
                        if remaining.startswith('-') or (remaining and remaining[0].isalpha()):
                            return True

        # Check for numeric-only patterns (132, 133, T131)
        if ref.isdigit():
            if f"T-{ref}" in trade_ids or f"T{ref}" in trade_ids:
                return True

        # Check for TXXX format (T131, T377, etc.)
        if ref.startswith('T') and ref[1:].isdigit():
            numeric_part = ref[1:]
            if f"T-{numeric_part}" in trade_ids:
                return True

        return False

    missing = []
    for ref in audit_refs:
        if not is_valid_trade_ref(ref):
            missing.append(ref)

    if missing:
        errors.append(f"Audit references missing trades: {set(missing)}")

    # Check 3: All open positions have expiry (except STOCK)
    open_no_expiry = df_trades[
        (df_trades['Status'] == 'Open') &
        (df_trades['TradeType'] != 'STOCK') &
        (df_trades['Expiry_Date'].isna())
    ]

    if not open_no_expiry.empty:
        errors.append(f"Open options with no expiry: {open_no_expiry['TradeID'].tolist()}")

    return errors
