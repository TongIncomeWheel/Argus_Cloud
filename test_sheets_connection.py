"""
Quick connection test for Google Sheets integration.
Run this after placing gsheet_credentials.json in the ARGUS folder.

Usage:  python test_sheets_connection.py
"""
import os
from dotenv import load_dotenv
load_dotenv()

from config import GSHEET_CREDENTIALS_PATH, INCOME_WHEEL_SHEET_ID, ACTIVE_CORE_SHEET_ID

def test():
    print("=== ARGUS Google Sheets Connection Test ===\n")

    # 1. Credentials file
    if not GSHEET_CREDENTIALS_PATH.exists():
        print(f"FAIL  Credentials file not found: {GSHEET_CREDENTIALS_PATH}")
        print("      → Place your service account JSON at that path and re-run.")
        return
    print(f"OK    Credentials file found: {GSHEET_CREDENTIALS_PATH.name}")

    # 2. Sheet IDs configured
    for name, sid in [("INCOME_WHEEL", INCOME_WHEEL_SHEET_ID), ("ACTIVE_CORE", ACTIVE_CORE_SHEET_ID)]:
        if not sid:
            print(f"FAIL  {name}_SHEET_ID is empty — check your .env")
            return
        print(f"OK    {name}_SHEET_ID = {sid}")

    # 3. Authenticate
    from sheets_handler import _get_client
    try:
        client = _get_client()
        print("OK    Service account authenticated")
    except Exception as e:
        print(f"FAIL  Authentication error: {e}")
        return

    # 4. Open + read each sheet
    from sheets_handler import SheetsHandler
    for label, sid in [("Income Wheel", INCOME_WHEEL_SHEET_ID), ("Active Core", ACTIVE_CORE_SHEET_ID)]:
        try:
            handler = SheetsHandler(sid)
            df = handler.read_data_table()
            print(f"OK    [{label}] Data Table — {len(df)} rows, {len(df.columns)} columns")
            df_a = handler.read_audit_table()
            print(f"OK    [{label}] Audit Table — {len(df_a)} rows")
        except Exception as e:
            print(f"FAIL  [{label}] {e}")
            return

    print("\n=== All checks passed. ARGUS is ready to use Google Sheets. ===")

if __name__ == "__main__":
    test()
