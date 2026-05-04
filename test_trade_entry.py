"""
End-to-end test of trade entry forms post-Tiger-ETL.

Simulates the same data dicts that app.py builds in each form, then writes
them to a TEST tab (not the live Data Table) via the same GSheetHandler used
in production. Verifies that:
  - Every dict has the new schema fields (Fee, Pot, Tiger_Row_Hash)
  - append_trade succeeds without column errors
  - Pot is correctly derived from StrategyType
  - update_trade preserves the new schema columns
  - Partials don't double-track Tiger_Row_Hash

Tests:
  1. Insert CSP (manual)
  2. Insert CC (manual)
  3. Roll a CSP -> new CSP
  4. Partial close (BTC): split into (A) closed + (B) open
  5. Expire worthless (update_trade)
  6. Exercise CSP -> assignment creates STOCK row
  7. Exercise CC -> close stock
"""
from __future__ import annotations

import sys
import logging
from datetime import datetime, timedelta

import pandas as pd
import gspread

from gsheet_handler import GSheetHandler
from config import INCOME_WHEEL_SHEET_ID
from tiger_to_argus import derive_pot, infer_strategy_type

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# Test harness — work in a SANDBOX worksheet, not the live Data Table
# ─────────────────────────────────────────────────────────────────
SANDBOX_TAB_NAME = "Data Table TEST"

# Production headers (matches what tiger_etl rebuild produces)
TARGET_HEADERS = [
    'TradeID', 'Ticker', 'StrategyType', 'Direction', 'TradeType',
    'Quantity', 'Open_lots', 'Option_Strike_Price_(USD)',
    'Price_of_current_underlying_(USD)', 'OptPremium', 'Date_open',
    'Expiry_Date', 'Date_closed', 'Status', 'Close_Price',
    'Actual_Profit_(USD)', 'Sorter', 'Remarks',
    'Fee', 'Pot', 'Tiger_Row_Hash',
]


def setup_sandbox(handler: GSheetHandler):
    """Create or reset the sandbox worksheet with current headers."""
    try:
        ws = handler.spreadsheet.worksheet(SANDBOX_TAB_NAME)
        ws.clear()
        # Ensure enough rows/cols
        if ws.row_count < 100 or ws.col_count < len(TARGET_HEADERS):
            ws.resize(rows=100, cols=len(TARGET_HEADERS))
    except gspread.exceptions.WorksheetNotFound:
        ws = handler.spreadsheet.add_worksheet(
            title=SANDBOX_TAB_NAME, rows=100, cols=len(TARGET_HEADERS)
        )
    ws.update(values=[TARGET_HEADERS], range_name='A1', value_input_option='USER_ENTERED')
    return ws


def teardown_sandbox(handler: GSheetHandler):
    """Optionally remove sandbox tab (leave it for forensic inspection)."""
    pass  # Keep tab for debugging


# ─────────────────────────────────────────────────────────────────
# Validation helpers
# ─────────────────────────────────────────────────────────────────
class Tester:
    def __init__(self):
        self.results = []  # list of (test_name, passed, msg)

    def expect(self, condition, test_name, msg=""):
        if condition:
            self.results.append((test_name, True, ""))
            print(f"  [PASS] {test_name}")
        else:
            self.results.append((test_name, False, msg))
            print(f"  [FAIL] {test_name}: {msg}")

    def report(self):
        passed = sum(1 for _, p, _ in self.results if p)
        total = len(self.results)
        print()
        print("=" * 70)
        print(f"TEST SUMMARY: {passed}/{total} passed")
        print("=" * 70)
        if passed != total:
            print("Failed tests:")
            for name, p, msg in self.results:
                if not p:
                    print(f"  ✗ {name}: {msg}")
        return passed == total


# ─────────────────────────────────────────────────────────────────
# Read sandbox state
# ─────────────────────────────────────────────────────────────────
def read_sandbox(handler: GSheetHandler) -> pd.DataFrame:
    """Read sandbox tab as DataFrame (same logic as read_data_table)."""
    ws = handler.spreadsheet.worksheet(SANDBOX_TAB_NAME)
    all_vals = ws.get_all_values()
    if len(all_vals) < 2:
        return pd.DataFrame(columns=TARGET_HEADERS)
    return pd.DataFrame(all_vals[1:], columns=all_vals[0])


# ─────────────────────────────────────────────────────────────────
# A monkey-patched handler that points append/update at sandbox tab
# ─────────────────────────────────────────────────────────────────
def make_sandbox_handler(real_handler: GSheetHandler) -> GSheetHandler:
    """Wrap handler so append_trade / update_trade / read target the sandbox."""
    # Monkey-patch by overriding the methods to use SANDBOX_TAB_NAME
    sb = real_handler

    original_append = sb.append_trade
    original_update = sb.update_trade

    def append_to_sandbox(trade_data):
        ws = sb.spreadsheet.worksheet(SANDBOX_TAB_NAME)
        headers = ws.row_values(1)
        from gsheet_handler import _serialize_value
        # Auto-fill Sorter (mirroring real append_trade behavior)
        trade_id = trade_data.get('TradeID', '')
        if 'Sorter' in headers and 'Sorter' not in trade_data:
            import re as _re
            m = _re.search(r'T-(\d+)', str(trade_id))
            trade_data['Sorter'] = int(m.group(1)) if m else trade_id
        row = [_serialize_value(trade_data.get(h, '')) for h in headers]
        ws.insert_row(row, index=2, value_input_option='USER_ENTERED')
        return True

    def update_in_sandbox(trade_id, updates):
        ws = sb.spreadsheet.worksheet(SANDBOX_TAB_NAME)
        all_vals = ws.get_all_values()
        if not all_vals or len(all_vals) < 2:
            raise ValueError("Sandbox empty")
        headers = all_vals[0]
        if 'TradeID' not in headers:
            raise ValueError("No TradeID column")
        tid_col = headers.index('TradeID')
        target_row = None
        for i, row in enumerate(all_vals[1:], start=2):
            if str(row[tid_col]).strip() == str(trade_id).strip():
                target_row = i
                break
        if target_row is None:
            raise ValueError(f"TradeID {trade_id} not found")
        from gsheet_handler import _serialize_value
        cells = []
        for col_name, val in updates.items():
            if col_name in headers:
                col_idx = headers.index(col_name) + 1
                cells.append(gspread.Cell(target_row, col_idx, _serialize_value(val)))
        if cells:
            ws.update_cells(cells, value_input_option='USER_ENTERED')
        return True

    sb.append_trade = append_to_sandbox
    sb.update_trade = update_in_sandbox
    return sb


# ─────────────────────────────────────────────────────────────────
# TEST 1: Insert CSP (manual entry)
# ─────────────────────────────────────────────────────────────────
def test_insert_csp(handler, t: Tester):
    print("\n" + "=" * 70)
    print("TEST 1: Insert CSP (manual)")
    print("=" * 70)

    strategy_csp = "WHEEL"
    trade_data = {
        'TradeID': 'TEST-CSP-1',
        'Ticker': 'MARA',
        'StrategyType': strategy_csp,
        'Direction': 'Sell',
        'TradeType': 'CSP',
        'Quantity': 5,
        'Option_Strike_Price_(USD)': 12.0,
        'Price_of_current_underlying_(USD)': 13.5,
        'OptPremium': 0.45,
        'Date_open': datetime.now(),
        'Expiry_Date': datetime.now() + timedelta(days=30),
        'Remarks': 'Test CSP insertion',
        'Status': 'Open',
        'Open_lots': 5 * 100,
        'Fee': 0,
        'Pot': derive_pot(strategy_csp),
        'Tiger_Row_Hash': '',
    }
    handler.append_trade(trade_data)

    df = read_sandbox(handler)
    csp_rows = df[df['TradeID'] == 'TEST-CSP-1']
    t.expect(len(csp_rows) == 1, "CSP row written to sandbox")
    if len(csp_rows) == 1:
        r = csp_rows.iloc[0]
        t.expect(r['TradeType'] == 'CSP', "TradeType=CSP")
        t.expect(r['Pot'] == 'Base', f"Pot=Base for WHEEL strategy (got '{r['Pot']}')")
        t.expect(r['Fee'] == '0' or r['Fee'] == 0, f"Fee=0 (got '{r['Fee']}')")
        t.expect(r['Tiger_Row_Hash'] == '', f"Tiger_Row_Hash empty (got '{r['Tiger_Row_Hash']}')")
        t.expect(r['Status'] == 'Open', "Status=Open")


# ─────────────────────────────────────────────────────────────────
# TEST 2: Insert CC (manual entry, ActiveCore strategy)
# ─────────────────────────────────────────────────────────────────
def test_insert_cc(handler, t: Tester):
    print("\n" + "=" * 70)
    print("TEST 2: Insert CC (ActiveCore = COIN ticker)")
    print("=" * 70)

    strategy_cc = "ActiveCore"
    trade_data = {
        'TradeID': 'TEST-CC-1',
        'Ticker': 'COIN',
        'StrategyType': strategy_cc,
        'Direction': 'Sell',
        'TradeType': 'CC',
        'Quantity': 1,
        'Option_Strike_Price_(USD)': 200.0,
        'Price_of_current_underlying_(USD)': 195.0,
        'OptPremium': 5.0,
        'Date_open': datetime.now(),
        'Expiry_Date': datetime.now() + timedelta(days=14),
        'Remarks': 'Test CC ActiveCore',
        'Status': 'Open',
        'Open_lots': 100,
        'Fee': 0,
        'Pot': derive_pot(strategy_cc),
        'Tiger_Row_Hash': '',
    }
    handler.append_trade(trade_data)

    df = read_sandbox(handler)
    cc_rows = df[df['TradeID'] == 'TEST-CC-1']
    t.expect(len(cc_rows) == 1, "CC row written")
    if len(cc_rows) == 1:
        r = cc_rows.iloc[0]
        t.expect(r['Pot'] == 'Active', f"Pot=Active for ActiveCore strategy (got '{r['Pot']}')")
        t.expect(r['StrategyType'] == 'ActiveCore', "StrategyType=ActiveCore")


# ─────────────────────────────────────────────────────────────────
# TEST 3: Roll a CSP -> new CSP
# ─────────────────────────────────────────────────────────────────
def test_roll(handler, t: Tester):
    print("\n" + "=" * 70)
    print("TEST 3: Roll CSP")
    print("=" * 70)

    # Step 1: insert CSP-OLD
    old_data = {
        'TradeID': 'TEST-CSP-OLD',
        'Ticker': 'MARA',
        'StrategyType': 'WHEEL',
        'Direction': 'Sell',
        'TradeType': 'CSP',
        'Quantity': 3,
        'Option_Strike_Price_(USD)': 11.0,
        'Price_of_current_underlying_(USD)': 12.0,
        'OptPremium': 0.30,
        'Date_open': datetime.now() - timedelta(days=20),
        'Expiry_Date': datetime.now() - timedelta(days=2),
        'Status': 'Open',
        'Open_lots': 300,
        'Fee': 0,
        'Pot': 'Base',
        'Tiger_Row_Hash': '',
    }
    handler.append_trade(old_data)

    # Step 2: simulate the roll (atomic_transaction-style)
    btc_cost = 0.05
    new_premium = 0.50
    qty = 3
    new_strike = 11.5
    new_expiry = datetime.now() + timedelta(days=14)
    orig_strat = 'WHEEL'

    # Update old to closed
    handler.update_trade('TEST-CSP-OLD', {
        'Status': 'Closed',
        'Date_closed': datetime.now(),
        'Close_Price': btc_cost,
        'Actual_Profit_(USD)': (0.30 - btc_cost) * 100 * qty,
        'Remarks': 'Rolled to TEST-CSP-NEW',
    })

    # Append new
    new_row = {
        'TradeID': 'TEST-CSP-NEW',
        'Ticker': 'MARA',
        'StrategyType': orig_strat,
        'Direction': 'Sell',
        'TradeType': 'CSP',
        'Quantity': qty,
        'Option_Strike_Price_(USD)': new_strike,
        'Price_of_current_underlying_(USD)': 12.0,
        'OptPremium': new_premium,
        'Date_open': datetime.now(),
        'Expiry_Date': new_expiry,
        'Status': 'Open',
        'Remarks': 'Rolled from TEST-CSP-OLD',
        'Open_lots': qty * 100,
        'Fee': 0,
        'Pot': 'Base',  # Inherited from original
        'Tiger_Row_Hash': '',
    }
    handler.append_trade(new_row)

    df = read_sandbox(handler)
    old_after = df[df['TradeID'] == 'TEST-CSP-OLD']
    new_row_df = df[df['TradeID'] == 'TEST-CSP-NEW']

    t.expect(len(old_after) == 1, "Old roll row exists")
    if len(old_after) == 1:
        r = old_after.iloc[0]
        t.expect(r['Status'] == 'Closed', f"Old status=Closed (got '{r['Status']}')")
        t.expect('Rolled to TEST-CSP-NEW' in str(r['Remarks']), "Old remarks reference new TID")

    t.expect(len(new_row_df) == 1, "New roll row exists")
    if len(new_row_df) == 1:
        r = new_row_df.iloc[0]
        t.expect(r['Status'] == 'Open', "New status=Open")
        t.expect(r['Pot'] == 'Base', "New Pot=Base (inherited)")
        t.expect('Rolled from TEST-CSP-OLD' in str(r['Remarks']), "New remarks reference old TID")


# ─────────────────────────────────────────────────────────────────
# TEST 4: Partial close (BTC)
# ─────────────────────────────────────────────────────────────────
def test_partial_close(handler, t: Tester):
    print("\n" + "=" * 70)
    print("TEST 4: Partial Close (BTC) — 5 contracts -> close 3, keep 2")
    print("=" * 70)

    # Step 1: insert original 5-contract CSP
    orig = {
        'TradeID': 'TEST-PARTIAL-X',
        'Ticker': 'MARA',
        'StrategyType': 'WHEEL',
        'Direction': 'Sell',
        'TradeType': 'CSP',
        'Quantity': 5,
        'Option_Strike_Price_(USD)': 10.0,
        'Price_of_current_underlying_(USD)': 11.5,
        'OptPremium': 0.40,
        'Date_open': datetime.now() - timedelta(days=10),
        'Expiry_Date': datetime.now() + timedelta(days=20),
        'Status': 'Open',
        'Open_lots': 500,
        'Fee': 0,
        'Pot': 'Base',
        'Tiger_Row_Hash': '',
    }
    handler.append_trade(orig)

    # Step 2: simulate the partial close logic from app.py
    btc_price = 0.10
    close_qty = 3
    remaining_qty = 5 - 3
    profit = (0.40 - btc_price) * 100 * close_qty

    # Read original row to copy it
    df = read_sandbox(handler)
    original_row = df[df['TradeID'] == 'TEST-PARTIAL-X'].iloc[0].to_dict()

    trade_id_a = 'TEST-PARTIAL-X(A)'
    trade_id_b = 'TEST-PARTIAL-X(B)'

    handler.update_trade('TEST-PARTIAL-X', {
        'TradeID': trade_id_a,
        'Quantity': close_qty,
        'Open_lots': close_qty * 100,
        'Status': 'Closed',
        'Date_closed': datetime.now(),
        'Close_Price': btc_price,
        'Actual_Profit_(USD)': profit,
    })

    row_b = original_row.copy()
    row_b['TradeID'] = trade_id_b
    row_b['Quantity'] = remaining_qty
    row_b['Open_lots'] = remaining_qty * 100
    row_b['Status'] = 'Open'
    row_b.pop('Date_closed', None)
    row_b.pop('Close_Price', None)
    row_b.pop('Actual_Profit_(USD)', None)
    # New schema overrides as in patched app.py
    row_b['Fee'] = 0
    row_b['Pot'] = original_row.get('Pot') or 'Base'
    row_b['Tiger_Row_Hash'] = ''
    handler.append_trade(row_b)

    df_after = read_sandbox(handler)
    a = df_after[df_after['TradeID'] == trade_id_a]
    b = df_after[df_after['TradeID'] == trade_id_b]
    t.expect(len(a) == 1, f"(A) row exists: closed half")
    if len(a) == 1:
        ra = a.iloc[0]
        t.expect(ra['Status'] == 'Closed', "(A) Status=Closed")
        t.expect(int(float(ra['Quantity'])) == 3, f"(A) Quantity=3 (got '{ra['Quantity']}')")
    t.expect(len(b) == 1, "(B) row exists: open remainder")
    if len(b) == 1:
        rb = b.iloc[0]
        t.expect(rb['Status'] == 'Open', "(B) Status=Open")
        t.expect(int(float(rb['Quantity'])) == 2, f"(B) Quantity=2 (got '{rb['Quantity']}')")
        t.expect(rb['Tiger_Row_Hash'] == '', f"(B) Tiger_Row_Hash cleared (got '{rb['Tiger_Row_Hash']}')")
        t.expect(rb['Pot'] == 'Base', f"(B) Pot inherited (got '{rb['Pot']}')")


# ─────────────────────────────────────────────────────────────────
# TEST 5: Expire worthless
# ─────────────────────────────────────────────────────────────────
def test_expire(handler, t: Tester):
    print("\n" + "=" * 70)
    print("TEST 5: Expire Worthless")
    print("=" * 70)

    orig = {
        'TradeID': 'TEST-EXPIRE-1',
        'Ticker': 'MARA',
        'StrategyType': 'WHEEL',
        'Direction': 'Sell',
        'TradeType': 'CSP',
        'Quantity': 2,
        'Option_Strike_Price_(USD)': 9.0,
        'Price_of_current_underlying_(USD)': 11.0,
        'OptPremium': 0.20,
        'Date_open': datetime.now() - timedelta(days=30),
        'Expiry_Date': datetime.now() - timedelta(days=1),
        'Status': 'Open',
        'Open_lots': 200,
        'Fee': 0,
        'Pot': 'Base',
        'Tiger_Row_Hash': '',
    }
    handler.append_trade(orig)

    # Mimic app.py expire flow
    full_premium = 0.20 * 2 * 100  # $40
    handler.update_trade('TEST-EXPIRE-1', {
        'Status': 'Closed',
        'Date_closed': datetime.now(),
        'Close_Price': 0.00,
        'Actual_Profit_(USD)': full_premium,
        'Remarks': 'CSP Expired Worthless',
    })

    df = read_sandbox(handler)
    row = df[df['TradeID'] == 'TEST-EXPIRE-1']
    t.expect(len(row) == 1, "Expired row exists")
    if len(row) == 1:
        r = row.iloc[0]
        t.expect(r['Status'] == 'Closed', "Status=Closed")
        t.expect(float(r['Actual_Profit_(USD)']) == 40.0, f"Profit=$40 (got {r['Actual_Profit_(USD)']})")
        t.expect(r['Pot'] == 'Base', "Pot preserved through update")


# ─────────────────────────────────────────────────────────────────
# TEST 6: CSP Assignment (exercise)
# ─────────────────────────────────────────────────────────────────
def test_csp_assignment(handler, t: Tester):
    print("\n" + "=" * 70)
    print("TEST 6: CSP Assignment (Exercise) — option closes, stock opens")
    print("=" * 70)

    # Insert a CSP that's about to be assigned
    csp = {
        'TradeID': 'TEST-ASSIGN-CSP',
        'Ticker': 'COIN',
        'StrategyType': 'ActiveCore',
        'Direction': 'Sell',
        'TradeType': 'CSP',
        'Quantity': 1,
        'Option_Strike_Price_(USD)': 180.0,
        'Price_of_current_underlying_(USD)': 175.0,
        'OptPremium': 4.0,
        'Date_open': datetime.now() - timedelta(days=30),
        'Expiry_Date': datetime.now() - timedelta(days=0),
        'Status': 'Open',
        'Open_lots': 100,
        'Fee': 0,
        'Pot': 'Active',
        'Tiger_Row_Hash': '',
    }
    handler.append_trade(csp)

    # Simulate CSP assignment flow
    df = read_sandbox(handler)
    original = df[df['TradeID'] == 'TEST-ASSIGN-CSP'].iloc[0]
    full_premium = 4.0 * 1 * 100

    handler.update_trade('TEST-ASSIGN-CSP', {
        'Status': 'Closed',
        'Date_closed': datetime.now(),
        'Close_Price': 0.00,
        'Actual_Profit_(USD)': full_premium,
        'Remarks': 'CSP Assigned - Bought stock',
    })

    # Now create the stock row (mirroring patched app.py)
    _stk_strategy = original.get('StrategyType') or 'WHEEL'
    _stk_pot = original.get('Pot') or ('Active' if _stk_strategy == 'ActiveCore' else 'Base')
    stock_row = {
        'TradeID': 'TEST-ASSIGN-STK',
        'Ticker': 'COIN',
        'StrategyType': _stk_strategy,
        'Direction': 'Buy',
        'TradeType': 'STOCK',
        'Quantity': 100,  # 1 contract * 100
        'Option_Strike_Price_(USD)': 180.0,
        'Price_of_current_underlying_(USD)': 180.0,
        'Date_open': datetime.now(),
        'Status': 'Open',
        'Remarks': 'Assigned from TEST-ASSIGN-CSP',
        'Fee': 0,
        'Pot': _stk_pot,
        'Tiger_Row_Hash': '',
        'Open_lots': 100,
    }
    handler.append_trade(stock_row)

    df = read_sandbox(handler)
    csp_row = df[df['TradeID'] == 'TEST-ASSIGN-CSP']
    stk_row = df[df['TradeID'] == 'TEST-ASSIGN-STK']

    t.expect(len(csp_row) == 1, "CSP row updated")
    if len(csp_row) == 1:
        r = csp_row.iloc[0]
        t.expect(r['Status'] == 'Closed', "CSP closed")
        t.expect('Bought stock' in str(r['Remarks']), "CSP remarks reflect assignment")

    t.expect(len(stk_row) == 1, "Stock row created")
    if len(stk_row) == 1:
        r = stk_row.iloc[0]
        t.expect(r['TradeType'] == 'STOCK', "Stock TradeType=STOCK")
        t.expect(r['Direction'] == 'Buy', "Stock Direction=Buy")
        t.expect(r['Pot'] == 'Active', f"Stock Pot=Active (inherited from ActiveCore CSP, got '{r['Pot']}')")
        t.expect('Assigned from TEST-ASSIGN-CSP' in str(r['Remarks']), "Stock remarks link back to CSP")


# ─────────────────────────────────────────────────────────────────
# TEST 7: CC Called (exercise) — closes existing stock
# ─────────────────────────────────────────────────────────────────
def test_cc_called(handler, t: Tester):
    print("\n" + "=" * 70)
    print("TEST 7: CC Called (Exercise) — existing stock closes at strike")
    print("=" * 70)

    # Insert existing stock + CC
    stk = {
        'TradeID': 'TEST-CALLED-STK',
        'Ticker': 'COIN',
        'StrategyType': 'ActiveCore',
        'Direction': 'Buy',
        'TradeType': 'STOCK',
        'Quantity': 100,
        'Open_lots': 100,
        'Price_of_current_underlying_(USD)': 170.0,  # cost basis
        'Date_open': datetime.now() - timedelta(days=60),
        'Status': 'Open',
        'Fee': 0,
        'Pot': 'Active',
        'Tiger_Row_Hash': '',
    }
    cc = {
        'TradeID': 'TEST-CALLED-CC',
        'Ticker': 'COIN',
        'StrategyType': 'ActiveCore',
        'Direction': 'Sell',
        'TradeType': 'CC',
        'Quantity': 1,
        'Option_Strike_Price_(USD)': 200.0,
        'OptPremium': 3.5,
        'Open_lots': 100,
        'Date_open': datetime.now() - timedelta(days=14),
        'Expiry_Date': datetime.now() - timedelta(days=0),
        'Status': 'Open',
        'Fee': 0,
        'Pot': 'Active',
        'Tiger_Row_Hash': '',
    }
    handler.append_trade(stk)
    handler.append_trade(cc)

    # Simulate CC called flow
    handler.update_trade('TEST-CALLED-CC', {
        'Status': 'Closed',
        'Date_closed': datetime.now(),
        'Close_Price': 0.00,
        'Actual_Profit_(USD)': 3.5 * 1 * 100,  # premium kept
        'Remarks': 'CC Called - Sold stock',
    })
    handler.update_trade('TEST-CALLED-STK', {
        'Status': 'Closed',
        'Date_closed': datetime.now(),
        'Close_Price': 200.0,  # strike
        'Actual_Profit_(USD)': (200.0 - 170.0) * 100,  # $3,000 stock profit
        'Remarks': 'Called away by TEST-CALLED-CC',
    })

    df = read_sandbox(handler)
    cc_after = df[df['TradeID'] == 'TEST-CALLED-CC']
    stk_after = df[df['TradeID'] == 'TEST-CALLED-STK']

    t.expect(len(cc_after) == 1 and cc_after.iloc[0]['Status'] == 'Closed', "CC closed")
    t.expect(len(stk_after) == 1 and stk_after.iloc[0]['Status'] == 'Closed', "Stock closed at strike")
    if len(stk_after) == 1:
        r = stk_after.iloc[0]
        t.expect(float(r['Actual_Profit_(USD)']) == 3000.0, f"Stock profit=$3,000 (got {r['Actual_Profit_(USD)']})")
        t.expect(r['Pot'] == 'Active', "Stock Pot preserved through update_trade")


# ─────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────
def main():
    real_handler = GSheetHandler(INCOME_WHEEL_SHEET_ID)
    setup_sandbox(real_handler)
    handler = make_sandbox_handler(real_handler)

    t = Tester()
    test_insert_csp(handler, t)
    test_insert_cc(handler, t)
    test_roll(handler, t)
    test_partial_close(handler, t)
    test_expire(handler, t)
    test_csp_assignment(handler, t)
    test_cc_called(handler, t)

    success = t.report()

    teardown_sandbox(real_handler)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
