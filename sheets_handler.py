"""
Google Sheets read/write operations — drop-in replacement for ExcelHandler.
Same public interface: read_data_table, read_audit_table, append_trade,
update_trade, delete_trades, append_audit, atomic_transaction.
"""
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from typing import Optional
import logging

from config import GSHEET_CREDENTIALS_PATH, LOGS_DIR

# Setup logging (mirrors excel_handler)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / 'income_wheel.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DATA_TABLE_SHEET = "Data Table"
AUDIT_TABLE_SHEET = "Audit_Table"


def _get_client() -> gspread.Client:
    """Authenticate and return a gspread client."""
    creds = Credentials.from_service_account_file(
        str(GSHEET_CREDENTIALS_PATH), scopes=SCOPES
    )
    return gspread.authorize(creds)


def _sheet_to_df(worksheet: gspread.Worksheet) -> pd.DataFrame:
    """Convert a gspread worksheet to a DataFrame, preserving column headers."""
    all_values = worksheet.get_all_values()
    if not all_values:
        return pd.DataFrame()
    headers = all_values[0]
    rows = all_values[1:]
    if not rows:
        return pd.DataFrame(columns=headers)
    df = pd.DataFrame(rows, columns=headers)
    # Replace empty strings with None
    df = df.replace("", None)
    # Attempt numeric coercion column-by-column (mirrors pd.read_excel behaviour)
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="ignore")
    # Attempt date coercion for columns whose name contains 'date' or 'Date'
    for col in df.columns:
        if "date" in col.lower() or "Date" in col:
            df[col] = pd.to_datetime(df[col], errors="ignore")
    return df


def _df_to_rows(df: pd.DataFrame) -> list:
    """Convert DataFrame (excluding header) to list-of-lists for gspread batch update."""
    def _serialise(v):
        if pd.isna(v) if not isinstance(v, str) else v == "":
            return ""
        if isinstance(v, (pd.Timestamp, datetime)):
            return v.strftime("%Y-%m-%d")
        return v
    return [[_serialise(v) for v in row] for row in df.itertuples(index=False)]


class SheetsHandler:
    """Handle all Google Sheets CRUD operations with transaction safety.

    Drop-in replacement for ExcelHandler.  Constructor takes a sheet_id (str)
    instead of a file path.
    """

    def __init__(self, sheet_id: str):
        self.sheet_id = sheet_id
        self._client: Optional[gspread.Client] = None
        self._spreadsheet: Optional[gspread.Spreadsheet] = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_spreadsheet(self) -> gspread.Spreadsheet:
        if self._spreadsheet is None:
            self._client = _get_client()
            self._spreadsheet = self._client.open_by_key(self.sheet_id)
        return self._spreadsheet

    def _get_ws(self, sheet_name: str) -> gspread.Worksheet:
        return self._get_spreadsheet().worksheet(sheet_name)

    def _backup(self) -> str:
        """Copy the spreadsheet on Drive as a timestamped backup.

        Returns the new spreadsheet's id (for logging).
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        title = f"ARGUS_backup_{timestamp}"
        try:
            ss = self._get_spreadsheet()
            copied = self._client.copy(ss.id, title=title, copy_permissions=False)
            logger.info(f"Backup created on Drive: '{title}' (id={copied.id})")
            return copied.id
        except Exception as e:
            logger.warning(f"Drive backup failed (non-fatal): {e}")
            return ""

    # ------------------------------------------------------------------
    # Public read interface
    # ------------------------------------------------------------------

    def read_data_table(self) -> pd.DataFrame:
        """Read the Data Table worksheet and return a DataFrame."""
        try:
            ws = self._get_ws(DATA_TABLE_SHEET)
            df = _sheet_to_df(ws)
            logger.info(f"Loaded {len(df)} trades from Data Table")
            return df
        except Exception as e:
            logger.error(f"Error reading Data Table: {e}")
            raise

    def read_audit_table(self) -> pd.DataFrame:
        """Read the Audit_Table worksheet and return a DataFrame."""
        try:
            ws = self._get_ws(AUDIT_TABLE_SHEET)
            df = _sheet_to_df(ws)
            logger.info(f"Loaded {len(df)} audit entries")
            return df
        except Exception as e:
            logger.error(f"Error reading Audit Table: {e}")
            raise

    # ------------------------------------------------------------------
    # Public write interface
    # ------------------------------------------------------------------

    def append_trade(self, trade_data: dict) -> bool:
        """Append a new trade row to the Data Table worksheet."""
        try:
            self._backup()
            ws = self._get_ws(DATA_TABLE_SHEET)
            headers = ws.row_values(1)
            row = [_serialise_cell(trade_data.get(h)) for h in headers]
            ws.append_row(row, value_input_option="USER_ENTERED")
            logger.info(f"Appended trade: {trade_data.get('TradeID', 'Unknown')}")
            return True
        except Exception as e:
            logger.error(f"Error appending trade: {e}")
            raise

    def update_trade(self, trade_id: str, updates: dict) -> bool:
        """Update fields of an existing trade identified by TradeID."""
        try:
            self._backup()
            ws = self._get_ws(DATA_TABLE_SHEET)
            headers = ws.row_values(1)

            if "TradeID" not in headers:
                raise ValueError("TradeID column not found in Data Table headers")

            trade_id_col = headers.index("TradeID") + 1  # 1-indexed
            # Find the row that matches trade_id
            trade_id_values = ws.col_values(trade_id_col)
            try:
                row_idx = trade_id_values.index(str(trade_id)) + 1  # 1-indexed
            except ValueError:
                raise ValueError(f"TradeID {trade_id} not found in Data Table")

            # Build batch update cells
            cells = []
            for field, value in updates.items():
                if field not in headers:
                    logger.warning(f"Field '{field}' not in sheet headers — skipping")
                    continue
                col_idx = headers.index(field) + 1
                cells.append(
                    gspread.Cell(row_idx, col_idx, _serialise_cell(value))
                )

            if cells:
                ws.update_cells(cells, value_input_option="USER_ENTERED")

            logger.info(f"Updated trade: {trade_id} with {updates}")
            return True
        except Exception as e:
            logger.error(f"Error updating trade: {e}")
            raise

    def delete_trades(self, trade_ids: list) -> bool:
        """Delete one or more trades from the Data Table by TradeID."""
        if not trade_ids:
            return True
        try:
            self._backup()
            ws = self._get_ws(DATA_TABLE_SHEET)
            headers = ws.row_values(1)

            if "TradeID" not in headers:
                raise ValueError("TradeID column not found")

            trade_id_col = headers.index("TradeID") + 1
            all_ids = ws.col_values(trade_id_col)  # includes header at index 0

            # Collect row indices to delete (1-indexed), skip header row
            target_ids = {str(t).strip() for t in trade_ids}
            rows_to_delete = [
                i + 1  # 1-indexed sheet row
                for i, val in enumerate(all_ids)
                if i > 0 and str(val).strip() in target_ids
            ]

            if not rows_to_delete:
                logger.warning(f"No rows matched TradeIDs: {trade_ids}")
                return True

            # Delete rows in reverse order to avoid index shifting
            for row_idx in sorted(rows_to_delete, reverse=True):
                ws.delete_rows(row_idx)

            logger.info(f"Deleted {len(rows_to_delete)} trade(s): {trade_ids}")
            return True
        except Exception as e:
            logger.error(f"Error deleting trades: {e}")
            raise

    def append_audit(self, audit_data: dict) -> bool:
        """Append a new audit entry to the Audit_Table worksheet."""
        try:
            ws = self._get_ws(AUDIT_TABLE_SHEET)
            headers = ws.row_values(1)
            row = [_serialise_cell(audit_data.get(h)) for h in headers]
            ws.append_row(row, value_input_option="USER_ENTERED")
            logger.info(f"Appended audit: {audit_data.get('Audit ID', 'Unknown')}")
            return True
        except Exception as e:
            logger.error(f"Error appending audit: {e}")
            raise

    def atomic_transaction(self, operations: list) -> bool:
        """Execute multiple operations with a single backup upfront.

        operations: list of dicts with 'type' and 'data'.
        Types: 'append_trade', 'update_trade', 'append_audit'.
        """
        try:
            self._backup()
            for op in operations:
                op_type = op["type"]
                op_data = op["data"]
                if op_type == "append_trade":
                    self.append_trade(op_data)
                elif op_type == "update_trade":
                    self.update_trade(op_data["trade_id"], op_data["updates"])
                elif op_type == "append_audit":
                    self.append_audit(op_data)
                else:
                    raise ValueError(f"Unknown operation type: {op_type}")
            logger.info(f"Atomic transaction completed: {len(operations)} operations")
            return True
        except Exception as e:
            logger.error(f"Atomic transaction failed: {e}")
            raise


# ------------------------------------------------------------------
# Module-level helper (used internally and in tests)
# ------------------------------------------------------------------

def _serialise_cell(v):
    """Serialise a Python value to a Google Sheets-safe string."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.strftime("%Y-%m-%d")
    return v
