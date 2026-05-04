"""
Tiger → ARGUS migration orchestrator.

Coordinates: parser → transform → gSheet writes.
Adds new tabs (Tiger Statement, Tiger Cash, Tiger Imports, Reconciliation Log).
Backs up existing Data Table before destructive migration.

Usage:
    from tiger_etl import run_migration
    summary = run_migration(['file1.csv', 'file2.csv'])

CLI:
    python tiger_etl.py file1.csv file2.csv [--dry-run]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import gspread

from gsheet_handler import GSheetHandler
from config import INCOME_WHEEL_SHEET_ID
from tiger_parser import parse_files, TigerStatement, statement_to_dict
from tiger_to_argus import (
    transform_to_argus, save_audit, infer_strategy_type, derive_pot,
    tiger_trade_to_argus_row, tiger_exercise_to_argus_rows, detect_rolls,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# New Data Table schema (existing 18 + 3 new = 21 columns)
# ─────────────────────────────────────────────────────────────────
TARGET_DATA_TABLE_COLUMNS = [
    'TradeID', 'Ticker', 'StrategyType', 'Direction', 'TradeType',
    'Quantity', 'Open_lots', 'Option_Strike_Price_(USD)',
    'Price_of_current_underlying_(USD)', 'OptPremium', 'Date_open',
    'Expiry_Date', 'Date_closed', 'Status', 'Close_Price',
    'Actual_Profit_(USD)', 'Sorter', 'Remarks',
    # New columns appended at end
    'Fee', 'Pot', 'Tiger_Row_Hash',
]


# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────
def _file_hash(filepath: str) -> str:
    """SHA256 hash of file contents -> first 16 hex chars."""
    h = hashlib.sha256()
    with open(filepath, 'rb') as f:
        h.update(f.read())
    return h.hexdigest()[:16]


def _col_letter(n: int) -> str:
    """Convert 1-based column index to A1-style letter(s). 1->A, 26->Z, 27->AA, ..."""
    if n < 1:
        raise ValueError(f"Column index must be >= 1, got {n}")
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _get_or_create_worksheet(handler: GSheetHandler, name: str, rows: int = 1000, cols: int = 30):
    """Get worksheet by name, or create if it doesn't exist. Grows existing sheets if too small."""
    try:
        ws = handler.spreadsheet.worksheet(name)
        # Grow if existing sheet is smaller than requested
        if ws.row_count < rows or ws.col_count < cols:
            new_rows = max(ws.row_count, rows)
            new_cols = max(ws.col_count, cols)
            ws.resize(rows=new_rows, cols=new_cols)
            logger.info(f"Resized worksheet {name} to {new_rows}x{new_cols}")
        return ws
    except gspread.exceptions.WorksheetNotFound:
        ws = handler.spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
        logger.info(f"Created new worksheet: {name}")
        return ws


# ─────────────────────────────────────────────────────────────────
# Backup
# ─────────────────────────────────────────────────────────────────
def backup_data_table(handler: GSheetHandler, suffix: Optional[str] = None) -> str:
    """Duplicate the Data Table tab as 'Data Table (Pre-Tiger {suffix})'.
    Returns the new tab name."""
    if suffix is None:
        suffix = datetime.now().strftime('%Y-%m-%d')
    backup_name = f"Data Table (Pre-Tiger {suffix})"

    # Read current Data Table
    try:
        original_ws = handler.spreadsheet.worksheet('Data Table')
    except gspread.exceptions.WorksheetNotFound:
        logger.warning("Data Table doesn't exist - nothing to back up")
        return backup_name

    all_data = original_ws.get_all_values()

    # Try to delete existing backup with same suffix
    try:
        existing = handler.spreadsheet.worksheet(backup_name)
        handler.spreadsheet.del_worksheet(existing)
    except gspread.exceptions.WorksheetNotFound:
        pass

    # Create backup tab
    rows = max(len(all_data) + 10, 100)
    cols = max(len(all_data[0]) if all_data else 30, 30)
    backup_ws = handler.spreadsheet.add_worksheet(title=backup_name, rows=rows, cols=cols)

    if all_data:
        backup_ws.update(values=all_data, range_name=f'A1:{_col_letter(cols)}{len(all_data)}',
                          value_input_option='RAW')

    logger.info(f"Backed up Data Table -> {backup_name} ({len(all_data)} rows)")
    return backup_name


# ─────────────────────────────────────────────────────────────────
# Wipe + rebuild Data Table
# ─────────────────────────────────────────────────────────────────
def rebuild_data_table(handler: GSheetHandler, argus_rows: list) -> int:
    """Wipe existing Data Table and write fresh rows.
    Returns row count written."""
    ws = handler.spreadsheet.worksheet('Data Table')

    # Build values array — header + rows
    header = TARGET_DATA_TABLE_COLUMNS
    data_rows = []
    for row in argus_rows:
        data_rows.append([
            str(row.get(col, '')) if row.get(col) is not None else ''
            for col in header
        ])

    all_values = [header] + data_rows

    # Clear and write
    ws.clear()
    if all_values:
        # Use chunks if too large (gspread max ~10MB per request)
        chunk_size = 500
        # Write header first
        ws.update(values=[header], range_name='A1', value_input_option='USER_ENTERED')

        for i in range(0, len(data_rows), chunk_size):
            chunk = data_rows[i:i+chunk_size]
            start_row = i + 2  # row 1 is header
            end_row = start_row + len(chunk) - 1
            end_col_letter = _col_letter(len(header))
            range_str = f'A{start_row}:{end_col_letter}{end_row}'
            ws.update(values=chunk, range_name=range_str, value_input_option='USER_ENTERED')

    handler._invalidate_header_cache('Data Table')
    logger.info(f"Rebuilt Data Table: {len(data_rows)} rows written")
    return len(data_rows)


# ─────────────────────────────────────────────────────────────────
# Apply roll remarks (after rows are inserted)
# ─────────────────────────────────────────────────────────────────
def apply_roll_remarks(handler: GSheetHandler, roll_pairs: list, argus_rows: list) -> int:
    """For each (old_tid, new_tid) pair, set Remarks to 'Rolled to/from'.
    argus_rows is the list of dicts already written (for index lookup)."""
    if not roll_pairs:
        return 0

    # Build TradeID → row index mapping (1-based, row 1 = header)
    tid_to_row = {}
    for i, row in enumerate(argus_rows):
        tid = row.get('TradeID')
        if tid:
            tid_to_row[tid] = i + 2  # 1-based, header at row 1

    ws = handler.spreadsheet.worksheet('Data Table')
    headers = ws.row_values(1)
    if 'Remarks' not in headers:
        return 0
    remarks_col_idx = headers.index('Remarks') + 1
    col_letter = _col_letter(remarks_col_idx)

    # Accumulate remarks PER ROW (chain rolls A→B→C cause B to be both "rolled from A"
    # AND "rolled to C" — these must be merged, not written in separate batch_updates that
    # overwrite each other since they target the same cell).
    cell_remarks: dict = {}  # row_idx -> list of remark strings
    for old_tid, new_tid in roll_pairs:
        if old_tid in tid_to_row:
            cell_remarks.setdefault(tid_to_row[old_tid], []).append(f'Rolled to {new_tid} (Tiger)')
        if new_tid in tid_to_row:
            cell_remarks.setdefault(tid_to_row[new_tid], []).append(f'Rolled from {old_tid} (Tiger)')

    cells_to_update = []
    for row_idx, remarks_list in cell_remarks.items():
        # Preserve existing row remarks (e.g. 'Imported from Tiger ...') by reading
        # them is heavy here; instead, replace fully — paired rows already had useful
        # remarks but the roll annotation supersedes for forensic clarity.
        merged = '; '.join(remarks_list)
        cells_to_update.append({
            'range': f'{col_letter}{row_idx}',
            'values': [[merged]],
        })

    if cells_to_update:
        # Batch in chunks of 100 to avoid API limits
        for chunk_start in range(0, len(cells_to_update), 100):
            chunk = cells_to_update[chunk_start:chunk_start+100]
            ws.batch_update(chunk, value_input_option='USER_ENTERED')

    logger.info(f"Applied roll remarks: {len(cells_to_update)} cells updated for {len(roll_pairs)} pairs")
    return len(roll_pairs)


# ─────────────────────────────────────────────────────────────────
# Tiger Statement / Cash / Imports tabs
# ─────────────────────────────────────────────────────────────────
def write_tiger_statement(handler: GSheetHandler, tiger_stmt: TigerStatement, run_id: str) -> int:
    """Write all parsed Tiger trades + exercises to Tiger Statement tab (always full rewrite)."""
    needed_rows = len(tiger_stmt.trades) + len(tiger_stmt.exercises) + 50  # +50 buffer
    ws = _get_or_create_worksheet(handler, 'Tiger Statement', rows=max(needed_rows, 2000), cols=20)

    headers = [
        'Run_ID', 'Source_File', 'Source_Row', 'Trade_Date', 'Settle_Date',
        'Asset_Class', 'Symbol_Raw', 'Ticker', 'Expiry', 'Right', 'Strike',
        'Activity_Type', 'Quantity', 'Trade_Price', 'Amount', 'Fee_Total',
        'Realized_PL', 'Notes', 'Currency', 'Tiger_Row_Hash',
    ]

    data_rows = []
    for t in tiger_stmt.trades:
        data_rows.append([
            run_id, t.source_file, str(t.source_row),
            str(t.trade_date) if t.trade_date else '',
            str(t.settle_date) if t.settle_date else '',
            t.asset_class, t.symbol_raw, t.ticker,
            str(t.expiry) if t.expiry else '',
            t.right or '', str(t.strike) if t.strike else '',
            t.activity_type, str(t.quantity), str(t.trade_price),
            str(t.amount), str(t.fee_total),
            str(t.realized_pl) if t.realized_pl is not None else '',
            t.notes, t.currency, t.row_hash,
        ])

    for e in tiger_stmt.exercises:
        data_rows.append([
            run_id, e.source_file, str(e.source_row),
            str(e.event_date) if e.event_date else '',
            '',
            e.asset_class, e.symbol_raw, e.ticker,
            str(e.expiry) if e.expiry else '',
            e.right or '', str(e.strike) if e.strike else '',
            e.transaction_type, str(e.quantity), '', '',
            '0', str(e.realized_pl), '', e.currency, e.row_hash,
        ])

    # Full rewrite (idempotent — Tiger Statement is the canonical fact log)
    ws.clear()
    end_col = _col_letter(len(headers))
    ws.update(values=[headers], range_name='A1', value_input_option='USER_ENTERED')
    chunk_size = 500
    for i in range(0, len(data_rows), chunk_size):
        chunk = data_rows[i:i+chunk_size]
        start_row = i + 2
        end_row = start_row + len(chunk) - 1
        ws.update(values=chunk, range_name=f'A{start_row}:{end_col}{end_row}',
                   value_input_option='USER_ENTERED')

    logger.info(f"Wrote {len(data_rows)} rows to Tiger Statement tab")
    return len(data_rows)


def write_tiger_cash(handler: GSheetHandler, tiger_stmt: TigerStatement, run_id: str) -> int:
    """Write all cash events to Tiger Cash tab (always full rewrite)."""
    needed_rows = len(tiger_stmt.cash_events) + 50
    ws = _get_or_create_worksheet(handler, 'Tiger Cash', rows=max(needed_rows, 1000), cols=10)

    headers = ['Run_ID', 'Source_File', 'Date', 'Type', 'Description', 'Amount', 'Currency']

    data_rows = []
    for c in tiger_stmt.cash_events:
        data_rows.append([
            run_id, c.source_file,
            str(c.event_date) if c.event_date else '',
            c.event_type, c.description, str(c.amount), c.currency,
        ])

    ws.clear()
    end_col = _col_letter(len(headers))
    ws.update(values=[headers], range_name='A1', value_input_option='USER_ENTERED')
    chunk_size = 500
    for i in range(0, len(data_rows), chunk_size):
        chunk = data_rows[i:i+chunk_size]
        start_row = i + 2
        end_row = start_row + len(chunk) - 1
        ws.update(values=chunk, range_name=f'A{start_row}:{end_col}{end_row}',
                   value_input_option='USER_ENTERED')

    logger.info(f"Wrote {len(data_rows)} cash events to Tiger Cash tab")
    return len(data_rows)


def write_tiger_imports(handler: GSheetHandler, source_files: list, run_id: str,
                        trades_count: int, cash_count: int) -> None:
    """Append a row to the Tiger Imports ledger tab for each source file."""
    ws = _get_or_create_worksheet(handler, 'Tiger Imports', rows=200, cols=10)

    headers = ['Run_ID', 'Filename', 'File_Hash', 'Imported_At',
                'Trades_Imported', 'Cash_Events_Imported', 'Status']

    existing = ws.get_all_values()
    write_header = (not existing or existing[0] != headers)

    imported_at = datetime.now().isoformat()
    rows_to_write = []
    for fname in source_files:
        # Compute file hash
        fpath = Path('tiger_samples') / fname
        if not fpath.exists():
            fpath = Path(fname)
        fh = _file_hash(str(fpath)) if fpath.exists() else 'unknown'
        rows_to_write.append([
            run_id, fname, fh, imported_at,
            str(trades_count), str(cash_count), 'Success',
        ])

    if write_header:
        ws.clear()
        ws.update(values=[headers] + rows_to_write,
                   range_name=f'A1:{_col_letter(len(headers))}{len(rows_to_write)+1}',
                   value_input_option='USER_ENTERED')
    else:
        next_row = len(existing) + 1
        end_col = _col_letter(len(headers))
        if rows_to_write:
            ws.update(values=rows_to_write,
                       range_name=f'A{next_row}:{end_col}{next_row+len(rows_to_write)-1}',
                       value_input_option='USER_ENTERED')

    logger.info(f"Wrote {len(rows_to_write)} import records to Tiger Imports tab")


def write_reconciliation_log(handler: GSheetHandler, audit: dict) -> None:
    """Append a summary row to the Reconciliation Log tab."""
    ws = _get_or_create_worksheet(handler, 'Reconciliation Log', rows=200, cols=20)

    headers = ['Run_ID', 'Run_Timestamp', 'Source_Files',
                'Tiger_Trades', 'ARGUS_Enriched', 'ARGUS_Inserted',
                'ARGUS_Orphaned', 'Rolls_Auto_Paired',
                'Exercises', 'Expirations',
                'Fees_Backfilled_USD', 'Net_Cash_Adjustment_USD',
                'Period_Start', 'Period_End']

    existing = ws.get_all_values()
    write_header = (not existing or existing[0] != headers)

    s = audit['summary']
    row = [
        audit['run_id'], audit['run_timestamp'],
        ', '.join(audit['source_files']),
        s['tiger_trades_parsed'], s['argus_enriched'], s['argus_inserted'],
        s['argus_orphaned'], s['rolls_auto_paired'],
        s['exercises_processed'], s['expirations_processed'],
        s['fees_backfilled_usd'], s['net_cash_adjustment_usd'],
        s.get('period_start', ''), s.get('period_end', ''),
    ]

    if write_header:
        ws.clear()
        ws.update(values=[headers, row],
                   range_name=f'A1:{_col_letter(len(headers))}2',
                   value_input_option='USER_ENTERED')
    else:
        next_row = len(existing) + 1
        end_col = _col_letter(len(headers))
        ws.update(values=[row], range_name=f'A{next_row}:{end_col}{next_row}',
                   value_input_option='USER_ENTERED')


# ─────────────────────────────────────────────────────────────────
# Build chronologically-ordered ARGUS rows — DESTRUCTIVE REBUILD MODE
# ─────────────────────────────────────────────────────────────────
def _apply_partial_qty(row: dict, open_t, qty: int) -> None:
    """Override Quantity / Open_lots for a partial-fill row in-place."""
    asset_class = getattr(open_t, 'asset_class', '')
    if asset_class == 'Stock':
        # For stocks: Quantity = lots (100-share units), Open_lots = actual shares
        row['Quantity'] = max(qty // 100, 1) if qty >= 100 else 1
        row['Open_lots'] = qty
    elif asset_class == 'Fund':
        row['Quantity'] = qty
        row['Open_lots'] = qty
    else:
        # Options: Quantity = contracts, Open_lots = 0
        row['Quantity'] = qty
        row['Open_lots'] = 0


def _partial_hash(open_hash: str, partial_seq: int) -> str:
    """Generate the Tiger_Row_Hash for a partial row.
    First partial keeps the original hash; subsequent get a `:p<N>` suffix
    so each ARGUS row has a unique value while remaining traceable to source.
    """
    if partial_seq <= 1:
        return open_hash
    return f"{open_hash}:p{partial_seq}"


def _argus_row_from_open_only(t, tid: str, qty: Optional[int] = None) -> dict:
    """Build an ARGUS row for an open with no closing event yet (still open).
    If qty is provided (partial remainder), override Quantity/Open_lots."""
    row = tiger_trade_to_argus_row(t, tid)
    if qty is not None and qty != abs(int(t.quantity)):
        _apply_partial_qty(row, t, qty)
        # Annotate that this is a partial remainder
        row['Remarks'] = f"Partial remainder ({qty} of {abs(int(t.quantity))}) — {row.get('Remarks', '')}"
    return row


def _argus_row_from_pair_close(open_t, close_t, qty: int, pl_share: float,
                                fee_share: float, partial_seq: int, tid: str) -> dict:
    """Build a single ARGUS row from a partial-fill pair: open_t consumed by close_t for `qty` units.
    PL allocation is `pl_share` (Tiger's reported PL × qty/total)."""
    row = tiger_trade_to_argus_row(open_t, tid)
    _apply_partial_qty(row, open_t, qty)
    row['Status'] = 'Closed'
    row['Date_closed'] = close_t.trade_date.isoformat() if close_t.trade_date else ''
    row['Close_Price'] = close_t.trade_price
    row['Actual_Profit_(USD)'] = round(pl_share, 4)
    # Fee = open's fee proportional to qty + close's fee_share
    open_qty = abs(int(open_t.quantity)) or 1
    open_fee_share = (open_t.fee_total or 0) * qty / open_qty
    row['Fee'] = round(open_fee_share + fee_share, 4)
    row['Tiger_Row_Hash'] = _partial_hash(open_t.row_hash, partial_seq)
    if partial_seq > 1 or qty != abs(int(open_t.quantity)):
        row['Remarks'] = f"Partial close {partial_seq} ({qty} of {abs(int(open_t.quantity))}) — {row.get('Remarks', '')}"
    return row


def _argus_row_from_pair_exercise(open_t, ex, qty: int, pl_share: float,
                                    fee_share: float, partial_seq: int, tid: str) -> dict:
    """Build a single ARGUS row from a partial-fill pair where open_t is closed by an
    Exercise/Expire event consuming `qty` units."""
    row = tiger_trade_to_argus_row(open_t, tid)
    _apply_partial_qty(row, open_t, qty)
    row['Status'] = 'Closed'
    row['Date_closed'] = ex.event_date.isoformat() if ex.event_date else ''
    row['Close_Price'] = 0
    row['Actual_Profit_(USD)'] = round(pl_share, 4)
    open_qty = abs(int(open_t.quantity)) or 1
    open_fee_share = (open_t.fee_total or 0) * qty / open_qty
    row['Fee'] = round(open_fee_share + fee_share, 4)
    row['Tiger_Row_Hash'] = _partial_hash(open_t.row_hash, partial_seq)

    is_call = ex.right == 'CALL'
    is_expire = ex.transaction_type in ('Option Expired Worthless', 'Option Expire', 'Option Expiration')
    if is_expire:
        label = f'{"CC" if is_call else "CSP"} Expired Worthless (Tiger)'
    else:
        label = 'CC Called away by exercise (Tiger)' if is_call else 'CSP Assigned by exercise (Tiger)'

    if partial_seq > 1 or qty != abs(int(open_t.quantity)):
        row['Remarks'] = f"{label} — partial {partial_seq} ({qty} of {abs(int(open_t.quantity))})"
    else:
        row['Remarks'] = label
    return row


# Legacy compat — these are now thin wrappers (kept so existing callers don't break)
def _argus_row_from_open_and_close(open_t, close_t, tid: str) -> dict:
    """Legacy: build a single row paired with one full close (no partial)."""
    qty = abs(int(open_t.quantity))
    pl = close_t.realized_pl or 0
    fee = close_t.fee_total or 0
    return _argus_row_from_pair_close(open_t, close_t, qty, pl, fee, 1, tid)


def _argus_row_from_open_and_exercise(open_t, ex, tid: str) -> dict:
    """Legacy: build a single row paired with one full exercise (no partial)."""
    qty = abs(int(open_t.quantity))
    pl = ex.realized_pl or 0
    return _argus_row_from_pair_exercise(open_t, ex, qty, pl, 0, 1, tid)


def _argus_row_from_assignment_stock(ex, tid: str, paired_close_tid: str) -> dict:
    """Build the STOCK row that results from a CSP assignment or CC called-away."""
    is_call = ex.right == 'CALL'
    strategy = infer_strategy_type(ex.ticker)
    pot = derive_pot(strategy)
    qty = abs(int(ex.quantity))
    shares = qty * 100

    if is_call:
        # CC called away: closes the existing stock at strike
        return {
            'TradeID': tid,
            'Ticker': ex.ticker,
            'StrategyType': strategy,
            'Direction': 'Sell',
            'TradeType': 'STOCK',
            'Quantity': shares,
            'Open_lots': shares,
            'Option_Strike_Price_(USD)': '',
            'Price_of_current_underlying_(USD)': ex.strike,
            'OptPremium': '',
            'Date_open': '',
            'Expiry_Date': '',
            'Date_closed': ex.event_date.isoformat() if ex.event_date else '',
            'Status': 'Closed',
            'Close_Price': ex.strike,
            'Actual_Profit_(USD)': '',
            'Sorter': 0,
            'Remarks': f'Called away by {paired_close_tid} (Tiger)',
            'Fee': 0,
            'Pot': pot,
            'Tiger_Row_Hash': ex.row_hash + '_stk',
        }
    else:
        # CSP assigned: opens new stock at strike
        return {
            'TradeID': tid,
            'Ticker': ex.ticker,
            'StrategyType': strategy,
            'Direction': 'Buy',
            'TradeType': 'STOCK',
            'Quantity': shares,
            'Open_lots': shares,
            'Option_Strike_Price_(USD)': '',
            'Price_of_current_underlying_(USD)': ex.strike,
            'OptPremium': '',
            'Date_open': ex.event_date.isoformat() if ex.event_date else '',
            'Expiry_Date': '',
            'Date_closed': '',
            'Status': 'Open',
            'Close_Price': '',
            'Actual_Profit_(USD)': '',
            'Sorter': 0,
            'Remarks': f'Assigned from {paired_close_tid} (Tiger)',
            'Fee': 0,
            'Pot': pot,
            'Tiger_Row_Hash': ex.row_hash + '_stk',
        }


def _build_partial_pairs(opens: list, closes_or_exs: list, key_fn,
                          open_remaining: Optional[list] = None) -> tuple:
    """FIFO greedy pairing with PARTIAL-FILL aggregation support.

    Solves the multi-fill double-count bug: when a user opens contracts
    in multiple fills (5+3+2 contracts on different days, same key) Tiger
    consolidates them into ONE Expire/Exercise event with qty=10.
    Exact-quantity matching cannot pair them; this algorithm consumes
    multiple opens with one closing event by accumulating their quantities.

    Conversely, a single open fill can be CLOSED IN PARTS by multiple
    closing events (e.g., open qty=10, close qty=6, then close qty=4).

    Tiger's reported PL on the closing event is allocated proportionally
    by qty across consumed opens — Tiger remains source of truth for the
    total; we never recompute PL.

    Args:
        opens: list of TigerTrade open fills
        closes_or_exs: list of close fills or exercise/expiration events
        key_fn: callable that returns the matching key for an entity
                (typically (ticker, right, strike, expiry) for options,
                ticker for stock)
        open_remaining: optional carry-over state from a prior pass (so
                pass 2 (exercises) sees what's left after pass 1 (closes)).
                If None, initialized fresh from opens' quantities.

    Returns:
        - pairs: list of dicts. Each dict represents ONE ARGUS row to emit:
            {
              'open': TigerTrade or None (None = orphan close),
              'open_idx': int or None,
              'ce': closing event (Close trade or Exercise),
              'qty': int (qty consumed from open by this close),
              'pl_share': float (proportional share of ce.realized_pl),
              'fee_share': float (proportional share of ce.fee_total),
              'orphan': bool (True if close had no matching open),
              'partial_seq': int (1 if first partial of this open, 2 next, etc.),
            }
        - open_remaining: list of remaining qty per open (for caller to chain
                          another pass, or to emit as still-open / implicit-expire rows)
    """
    if open_remaining is None:
        open_remaining = [abs(int(getattr(o, 'quantity', 0))) for o in opens]

    pairs = []
    # Track per-open partial sequence (1, 2, 3...) for Tiger_Row_Hash suffix
    open_partial_seq = [0] * len(opens)

    for ce in closes_or_exs:
        ce_key = key_fn(ce)
        ce_total_qty = abs(int(getattr(ce, 'quantity', 0)))
        ce_date = getattr(ce, 'trade_date', None) or getattr(ce, 'event_date', None)
        ce_pl = getattr(ce, 'realized_pl', None) or 0
        ce_fee = getattr(ce, 'fee_total', None) or 0

        if ce_total_qty == 0:
            continue

        ce_remaining = ce_total_qty

        # FIFO consume from opens with matching key, opened on or before ce_date
        for i, o in enumerate(opens):
            if open_remaining[i] == 0:
                continue
            if ce_remaining == 0:
                break
            if key_fn(o) != ce_key:
                continue
            if ce_date and getattr(o, 'trade_date', None) and o.trade_date > ce_date:
                continue

            take = min(open_remaining[i], ce_remaining)
            # Allocate Tiger's PL proportionally by qty consumed
            pl_share = (ce_pl * take / ce_total_qty) if ce_total_qty > 0 else 0
            fee_share = (ce_fee * take / ce_total_qty) if ce_total_qty > 0 else 0

            open_partial_seq[i] += 1
            pairs.append({
                'open': o,
                'open_idx': i,
                'ce': ce,
                'qty': take,
                'pl_share': pl_share,
                'fee_share': fee_share,
                'orphan': False,
                'partial_seq': open_partial_seq[i],
            })
            open_remaining[i] -= take
            ce_remaining -= take

        if ce_remaining > 0:
            # Closing event has unmatched qty -> orphan (rare: typically a Tiger
            # close with no corresponding open in our window)
            orphan_pl = (ce_pl * ce_remaining / ce_total_qty) if ce_total_qty > 0 else 0
            orphan_fee = (ce_fee * ce_remaining / ce_total_qty) if ce_total_qty > 0 else 0
            pairs.append({
                'open': None,
                'open_idx': None,
                'ce': ce,
                'qty': ce_remaining,
                'pl_share': orphan_pl,
                'fee_share': orphan_fee,
                'orphan': True,
                'partial_seq': 1,
            })

    return pairs, open_remaining


def _pair_open_with_closing_event(opens: list, closes_or_exs: list, key_fn) -> tuple:
    """DEPRECATED: kept for backward compat with any external callers.
    Prefer _build_partial_pairs which supports partial-fill aggregation.
    Returns (pairs_list_of_tuples, remaining_opens_list)."""
    pairs_dicts, open_remaining = _build_partial_pairs(opens, closes_or_exs, key_fn)
    # Convert dict pairs back to (open, ce) tuples for legacy callers
    pairs_tuples = [(p['open'], p['ce']) for p in pairs_dicts if not p['orphan']]
    remaining = [o for i, o in enumerate(opens) if open_remaining[i] > 0]
    return pairs_tuples, remaining


def build_full_rebuild_rows(tiger_stmt: TigerStatement) -> tuple:
    """
    Destructive rebuild that produces ONE ARGUS row per option lifecycle:
      - Pair OpenShort/OpenLong fills with their corresponding Close fills or Exercises
      - Unpaired opens stay as Open positions
      - Unpaired closes (orphan) become standalone Closed rows (data quality flag)
      - Stock buys + sells get FIFO-paired similarly
      - CSP assignments / CC called-aways generate the resulting STOCK row

    Returns: (argus_rows, roll_pairs_with_final_tids)
    """
    rows: list = []
    counter = [0]
    def next_tid() -> str:
        counter[0] += 1
        return f'TMP-{counter[0]}'

    # ── Step 0: Bucket Tiger fills into option opens/closes, stock buys/sells, fund buys/sells ──
    # Activity types observed in Tiger CSVs:
    #   Options: OpenShort (sell-to-open), OpenLong (buy-to-open, e.g. LEAPs), Close (close-out)
    #   Stocks:  Open (buy from CSP assignment OR explicit buy), OpenShort (short sell), Close (sell)
    #   Funds:   Buy, Sell (SGD money market funds — treated as stock-like for tracking)
    OPTION_OPEN_ACTS = ('OpenShort', 'OpenLong', 'Buy', 'Open')  # 'Open' is Tiger's label for long-call buys (LEAPs)
    OPTION_CLOSE_ACTS = ('Close', 'Sell')                        # Sell on Option = closing a long position
    STOCK_BUY_ACTS = ('Buy', 'Open', 'OpenLong')         # 'Open' is Tiger's label for assignment-buys
    STOCK_SELL_ACTS = ('Sell', 'Close', 'OpenShort')     # OpenShort on stock = short sell
    FUND_BUY_ACTS = ('Buy', 'Open', 'OpenLong')
    FUND_SELL_ACTS = ('Sell', 'Close')

    option_opens = [t for t in tiger_stmt.trades if t.asset_class == 'Option' and t.activity_type in OPTION_OPEN_ACTS]
    option_closes = [t for t in tiger_stmt.trades if t.asset_class == 'Option' and t.activity_type in OPTION_CLOSE_ACTS]
    # All Stock trades go into the buckets — the Trades section is the SOURCE OF TRUTH
    # for stock movement. The Exercise/Expiration section's "Option Exercise" events do
    # NOT generate stock rows (we changed tiger_exercise_to_argus_rows to skip stock
    # rows; see _argus_row_from_open_and_exercise in this file). This avoids the
    # double-count issue where assignment-acquired stock appeared in both pathways.
    stock_buys = [t for t in tiger_stmt.trades
                  if t.asset_class == 'Stock' and t.activity_type in STOCK_BUY_ACTS]
    stock_sells = [t for t in tiger_stmt.trades
                   if t.asset_class == 'Stock' and t.activity_type in STOCK_SELL_ACTS]
    fund_buys = [t for t in tiger_stmt.trades if t.asset_class == 'Fund' and t.activity_type in FUND_BUY_ACTS]
    fund_sells = [t for t in tiger_stmt.trades if t.asset_class == 'Fund' and t.activity_type in FUND_SELL_ACTS]

    # Sort by trade_date for FIFO matching
    sort_d = lambda x: getattr(x, 'trade_date', None) or getattr(x, 'event_date', None) or pd.Timestamp.max.date()
    option_opens.sort(key=sort_d)
    option_closes.sort(key=sort_d)
    stock_buys.sort(key=sort_d)
    stock_sells.sort(key=sort_d)
    fund_buys.sort(key=sort_d)
    fund_sells.sort(key=sort_d)
    exercises_sorted = sorted(tiger_stmt.exercises, key=sort_d)

    opt_key = lambda t: (t.ticker, t.right, t.strike, t.expiry)
    stk_key = lambda t: t.ticker

    # ── Step 1+2: PARTIAL-FILL pairing for OPTIONS (closes pass, then exercises pass) ──
    # This uses FIFO greedy aggregation so that multi-fill positions (e.g., 5+3+2 contracts
    # that Tiger consolidates into a single Expire event with qty=10) reconcile correctly.
    # Tiger's reported realized_pl is allocated proportionally — never recomputed.
    opt_close_pairs, opt_open_remaining = _build_partial_pairs(
        option_opens, option_closes, opt_key
    )
    opt_ex_pairs, opt_open_remaining = _build_partial_pairs(
        option_opens, exercises_sorted, opt_key, open_remaining=opt_open_remaining
    )

    # Track which exercises were consumed by pairing
    paired_ex_hashes = {p['ce'].row_hash for p in opt_ex_pairs if not p.get('orphan')}

    # ── Step 3: PARTIAL-FILL pairing for STOCK (buys × sells) ──
    stk_pairs, stk_open_remaining = _build_partial_pairs(
        stock_buys, stock_sells, stk_key
    )

    # ── Step 4: PARTIAL-FILL pairing for FUND (e.g. SGD MMF buys × sells) ──
    fund_pair_results, fund_open_remaining = _build_partial_pairs(
        fund_buys, fund_sells, stk_key  # fund key = ticker, same as stock
    )

    # ── Step 5: Emit rows ──
    # Track row_hash -> [tid] for roll pair resolution.
    row_hash_to_tids: dict = {}
    def track(hashes, tid):
        for h in hashes:
            row_hash_to_tids.setdefault(h, []).append(tid)

    # 5a. Paired options (open + close) — supports multi-fill aggregation
    for p in opt_close_pairs:
        tid = next_tid()
        if p.get('orphan'):
            # Orphan close — close fill with no matching open
            ce = p['ce']
            row = tiger_trade_to_argus_row(ce, tid)
            row['Remarks'] = f'Orphan Close (no matching Tiger open) - {row.get("Remarks", "")}'
            row['Actual_Profit_(USD)'] = round(p['pl_share'], 4)
            rows.append(row)
            track([ce.row_hash], tid)
        else:
            row = _argus_row_from_pair_close(
                p['open'], p['ce'], p['qty'], p['pl_share'],
                p['fee_share'], p['partial_seq'], tid
            )
            rows.append(row)
            # Track BOTH the open's hash and close's hash (the close is consumed)
            track([p['open'].row_hash, p['ce'].row_hash], tid)

    # 5b. Paired options (open + exercise) — supports multi-fill aggregation.
    # Stock movement comes from the Trades section, NOT this pathway, to avoid double-counting.
    for p in opt_ex_pairs:
        tid = next_tid()
        if p.get('orphan'):
            ex = p['ce']
            ex_rows = tiger_exercise_to_argus_rows(ex, tid, None)
            for r in ex_rows:
                rows.append(r)
                track([ex.row_hash], r['TradeID'])
        else:
            row = _argus_row_from_pair_exercise(
                p['open'], p['ce'], p['qty'], p['pl_share'],
                p['fee_share'], p['partial_seq'], tid
            )
            rows.append(row)
            track([p['open'].row_hash, p['ce'].row_hash], tid)

    # 5c. Unpaired/partially-remaining options
    # If past period_end and no Tiger event, treat as implicit expiration (silent expire).
    # Otherwise emit as still-open.
    cutoff_date = tiger_stmt.period_end if tiger_stmt.period_end else pd.Timestamp.today().date()
    implicit_expires = 0
    for i, open_t in enumerate(option_opens):
        rem_qty = opt_open_remaining[i]
        if rem_qty == 0:
            continue
        tid = next_tid()
        if open_t.expiry and open_t.expiry < cutoff_date:
            # Implicit expiration — synthesize closed row for the remaining qty
            row = tiger_trade_to_argus_row(open_t, tid)
            _apply_partial_qty(row, open_t, rem_qty)
            is_call = open_t.right == 'CALL'
            row['Status'] = 'Closed'
            row['Date_closed'] = open_t.expiry.isoformat()
            row['Close_Price'] = 0
            premium_kept = round(open_t.trade_price * 100 * rem_qty, 2)
            row['Actual_Profit_(USD)'] = premium_kept
            row['Remarks'] = f'{"CC" if is_call else "CSP"} Expired Worthless (Tiger - implicit, no event row)'
            # Use partial seq = highest-already-used + 1 so hash stays unique
            existing_count = len([t for t in row_hash_to_tids.get(open_t.row_hash, []) if t])
            row['Tiger_Row_Hash'] = _partial_hash(open_t.row_hash, existing_count + 1)
            implicit_expires += 1
            rows.append(row)
            track([open_t.row_hash], tid)
        else:
            # Still open — emit open row with the remaining qty
            row = _argus_row_from_open_only(open_t, tid, qty=rem_qty)
            existing_count = len([t for t in row_hash_to_tids.get(open_t.row_hash, []) if t])
            if existing_count > 0:
                row['Tiger_Row_Hash'] = _partial_hash(open_t.row_hash, existing_count + 1)
            rows.append(row)
            track([open_t.row_hash, ], tid)
    if implicit_expires:
        logger.info(f"Inferred {implicit_expires} implicit option expirations (past expiry, no Tiger event)")

    # 5d. Unpaired exercises — Tiger Exercise/Expire events with no matching open
    # in our window (the open was pre-period). Emit closed-only rows.
    for ex in exercises_sorted:
        if ex.row_hash in paired_ex_hashes:
            continue
        tid = next_tid()
        ex_rows = tiger_exercise_to_argus_rows(ex, tid, None)
        for r in ex_rows:
            rows.append(r)
            track([ex.row_hash], r['TradeID'])

    # 5e. Paired stock (buy + sell) — supports partial fills
    for p in stk_pairs:
        tid = next_tid()
        if p.get('orphan'):
            ce = p['ce']
            row = tiger_trade_to_argus_row(ce, tid)
            row['Remarks'] = f'Orphan Stock Sell (no matching Tiger buy) - {row.get("Remarks", "")}'
            row['Actual_Profit_(USD)'] = round(p['pl_share'], 4)
            rows.append(row)
            track([ce.row_hash], tid)
        else:
            row = _argus_row_from_pair_close(
                p['open'], p['ce'], p['qty'], p['pl_share'],
                p['fee_share'], p['partial_seq'], tid
            )
            rows.append(row)
            track([p['open'].row_hash, p['ce'].row_hash], tid)

    # 5f. Remaining stock buys — emit as Open with partial remainder qty if applicable
    for i, buy_t in enumerate(stock_buys):
        rem_qty = stk_open_remaining[i]
        if rem_qty == 0:
            continue
        tid = next_tid()
        row = _argus_row_from_open_only(buy_t, tid, qty=rem_qty)
        existing_count = len([t for t in row_hash_to_tids.get(buy_t.row_hash, []) if t])
        if existing_count > 0:
            row['Tiger_Row_Hash'] = _partial_hash(buy_t.row_hash, existing_count + 1)
        rows.append(row)
        track([buy_t.row_hash], tid)

    # 5g. Paired fund (buy + sell)
    for p in fund_pair_results:
        tid = next_tid()
        if p.get('orphan'):
            ce = p['ce']
            row = tiger_trade_to_argus_row(ce, tid)
            row['Remarks'] = f'Orphan Fund Sell (no matching Tiger buy) - {row.get("Remarks", "")}'
            row['Actual_Profit_(USD)'] = round(p['pl_share'], 4)
            rows.append(row)
            track([ce.row_hash], tid)
        else:
            row = _argus_row_from_pair_close(
                p['open'], p['ce'], p['qty'], p['pl_share'],
                p['fee_share'], p['partial_seq'], tid
            )
            rows.append(row)
            track([p['open'].row_hash, p['ce'].row_hash], tid)

    # 5h. Remaining fund buys — emit as Open with partial remainder
    for i, buy_t in enumerate(fund_buys):
        rem_qty = fund_open_remaining[i]
        if rem_qty == 0:
            continue
        tid = next_tid()
        row = _argus_row_from_open_only(buy_t, tid, qty=rem_qty)
        existing_count = len([t for t in row_hash_to_tids.get(buy_t.row_hash, []) if t])
        if existing_count > 0:
            row['Tiger_Row_Hash'] = _partial_hash(buy_t.row_hash, existing_count + 1)
        rows.append(row)
        track([buy_t.row_hash], tid)

    # 5i. SAFETY NET — any Tiger trade whose row_hash isn't represented gets a fallback row.
    # With partial-fill aggregation in place, this should only catch genuinely-orphan
    # edge cases (unusual activity types, parser quirks, etc.).
    all_tiger_hashes = {t.row_hash for t in tiger_stmt.trades}
    represented = set(row_hash_to_tids.keys())
    missing = all_tiger_hashes - represented
    safety_net_count = 0
    if missing:
        for t in tiger_stmt.trades:
            if t.row_hash in missing:
                tid = next_tid()
                row = tiger_trade_to_argus_row(t, tid)
                row['Remarks'] = f'SAFETY NET: unmatched Tiger {t.activity_type} ({t.asset_class}) - {row.get("Remarks", "")}'
                rows.append(row)
                track([t.row_hash], tid)
                safety_net_count += 1
        logger.warning(f"Safety net: {safety_net_count} Tiger trades not bucketed by primary logic, emitted as fallback rows")

    # ── Step 5: Sort chronologically ──
    def sort_key(r):
        d = r.get('Date_open') or r.get('Date_closed') or '9999-99-99'
        return d
    rows.sort(key=sort_key)

    # ── Step 6: Renumber TradeIDs T-1, T-2, ... ──
    temp_to_final = {}
    for i, r in enumerate(rows, start=1):
        old = r.get('TradeID', '')
        new = f'T-{i}'
        temp_to_final[old] = new
        r['TradeID'] = new
        r['Sorter'] = i

    # ── Step 7: Update Remarks that reference TMP- TIDs (e.g. 'Assigned from TMP-N') ──
    # Use regex to capture the FULL TMP-<digits> token (avoids substring corruption
    # where 'TMP-1' would otherwise match the prefix of 'TMP-1142').
    import re as _re
    _tmp_pattern = _re.compile(r'TMP-\d+')
    def _remap_tmp(match):
        token = match.group(0)
        return temp_to_final.get(token, token)
    for r in rows:
        remarks = str(r.get('Remarks', ''))
        if 'TMP-' in remarks:
            r['Remarks'] = _tmp_pattern.sub(_remap_tmp, remarks)

    # Update row_hash_to_tids to use final TIDs
    row_hash_to_final_tids: dict = {}
    for h, tids in row_hash_to_tids.items():
        row_hash_to_final_tids[h] = [temp_to_final.get(t, t) for t in tids]

    # ── Step 8: Detect rolls and map to final TIDs ──
    # In paired-rebuild mode, the close fill is folded into the same ARGUS row as its open.
    # So a roll's "close" Tiger trade and "new open" Tiger trade map to TWO DIFFERENT ARGUS rows:
    #   - close Tiger trade -> final TID of the row that owns it (the original open + close)
    #   - new open Tiger trade -> final TID of its row (a new open or open+close pair)
    new_roll_pairs = []
    rolls = detect_rolls(tiger_stmt.trades)
    for close_t, open_t in rolls:
        close_finals = row_hash_to_final_tids.get(close_t.row_hash, [])
        open_finals = row_hash_to_final_tids.get(open_t.row_hash, [])
        if close_finals and open_finals:
            # Use the first TID in each (paired rows have just one)
            new_roll_pairs.append((close_finals[0], open_finals[0]))

    return rows, new_roll_pairs


def build_argus_rows_from_plan(plan: dict, tiger_stmt: TigerStatement) -> tuple:
    """Backward-compat wrapper — destructive mode now uses build_full_rebuild_rows."""
    return build_full_rebuild_rows(tiger_stmt)


# ─────────────────────────────────────────────────────────────────
# Main migration orchestrator
# ─────────────────────────────────────────────────────────────────
def run_migration(filepaths: list, dry_run: bool = False) -> dict:
    """Full migration pipeline. Returns summary dict."""
    run_start = datetime.now()
    run_id = run_start.strftime('%Y-%m-%d_%H-%M-%S')

    logger.info(f"=== Tiger Migration Starting: run_id={run_id}, dry_run={dry_run} ===")

    # Step 1: Parse Tiger CSVs
    logger.info("Step 1: Parsing Tiger CSVs...")
    tiger_stmt = parse_files(filepaths)
    logger.info(f"  Parsed: {len(tiger_stmt.trades)} trades, "
                f"{len(tiger_stmt.exercises)} exercises, "
                f"{len(tiger_stmt.cash_events)} cash events")

    # Step 2: Connect to gSheet, load current Data Table (just for ETL plan diff stats)
    logger.info("Step 2: Loading current ARGUS Data Table...")
    handler = GSheetHandler(INCOME_WHEEL_SHEET_ID)
    df_argus = handler.read_data_table()
    logger.info(f"  Current Data Table: {len(df_argus)} rows")

    # Step 3: Run transform to generate ETL plan (used for audit/diff stats only;
    # destructive rebuild bypasses enrich/insert and rebuilds from Tiger directly)
    logger.info("Step 3: Running transform layer (Tiger -> ARGUS plan/audit)...")
    plan = transform_to_argus(tiger_stmt, df_argus)
    plan['audit']['run_id'] = run_id

    # Save audit JSON
    audit_path = save_audit(plan['audit'])
    logger.info(f"  Audit saved: {audit_path}")
    logger.info(f"  Plan: {plan['summary']['argus_inserted']} inserts, "
                f"{plan['summary']['argus_enriched']} enriches, "
                f"{plan['summary']['rolls_auto_paired']} rolls")

    if dry_run:
        logger.info("DRY RUN - exiting without writes")
        return plan['summary']

    # Step 4: Backup current Data Table — use full timestamp so re-runs preserve prior backups
    logger.info("Step 4: Backing up current Data Table...")
    backup_name = backup_data_table(handler, suffix=run_start.strftime('%Y-%m-%d_%H%M'))
    logger.info(f"  Backup created: {backup_name}")

    # Step 5: Build final ARGUS rows (chronological, re-numbered)
    logger.info("Step 5: Building final ARGUS rows...")
    argus_rows, roll_pairs = build_argus_rows_from_plan(plan, tiger_stmt)
    logger.info(f"  Final row count: {len(argus_rows)}")

    # Step 6: Wipe + rebuild Data Table
    logger.info("Step 6: Wiping and rebuilding Data Table...")
    rows_written = rebuild_data_table(handler, argus_rows)

    # Step 7: Apply roll remarks
    logger.info("Step 7: Applying roll remarks...")
    rolls_applied = apply_roll_remarks(handler, roll_pairs, argus_rows)

    # Step 8: Write Tiger tabs
    logger.info("Step 8: Writing Tiger Statement / Cash / Imports tabs...")
    statement_count = write_tiger_statement(handler, tiger_stmt, run_id)
    cash_count = write_tiger_cash(handler, tiger_stmt, run_id)
    write_tiger_imports(handler, tiger_stmt.source_files, run_id, statement_count, cash_count)
    write_reconciliation_log(handler, plan['audit'])

    duration = (datetime.now() - run_start).total_seconds()
    logger.info(f"=== Migration COMPLETE in {duration:.1f}s ===")

    return {
        'run_id': run_id,
        'duration_seconds': duration,
        'backup_tab': backup_name,
        'data_table_rows_written': rows_written,
        'rolls_applied': rolls_applied,
        'tiger_statement_rows': statement_count,
        'tiger_cash_rows': cash_count,
        'audit_file': str(audit_path),
        **plan['summary'],
    }


def main():
    ap = argparse.ArgumentParser(description="Tiger → ARGUS migration")
    ap.add_argument('files', nargs='+', help='Tiger CSV files')
    ap.add_argument('--dry-run', action='store_true', help='Preview without writing')
    ap.add_argument('--verbose', '-v', action='store_true')
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    summary = run_migration(args.files, dry_run=args.dry_run)

    print()
    print("=" * 60)
    print("MIGRATION SUMMARY")
    print("=" * 60)
    for k, v in summary.items():
        if not isinstance(v, dict):
            print(f"  {k}: {v}")


if __name__ == '__main__':
    main()
