"""
Tiger ETL — Rollback CLI.

Restores the Data Table from a backup tab. Use this if a Tiger Update went
wrong and you need to revert to a known-good state.

Workflow:
  1. List all available backup tabs (sorted by date)
  2. Confirm which one to restore from
  3. Snapshot current Data Table to a "Pre-Rollback" backup (safety net)
  4. Wipe Data Table + write the backup's contents back

Usage:
    python tiger_etl_rollback.py --list                  # show available backups
    python tiger_etl_rollback.py --restore "<tab name>"  # restore from named tab
    python tiger_etl_rollback.py --restore latest        # restore from most recent backup
    python tiger_etl_rollback.py --restore "Data Table (Pre-Tiger Update_2026-05-04_11-54-13)" --auto-approve

Safety:
  - Always creates a "Pre-Rollback" snapshot of current state before restoring
  - Asks for explicit y/N confirmation unless --auto-approve passed
  - Never deletes any backup tabs (rollback is non-destructive to other backups)
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import gspread

from gsheet_handler import GSheetHandler
from config import INCOME_WHEEL_SHEET_ID
from tiger_etl import _col_letter, TARGET_DATA_TABLE_COLUMNS

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def list_backups(handler: GSheetHandler) -> list:
    """Return sorted list of backup tabs (most recent first)."""
    candidates = []
    for ws in handler.spreadsheet.worksheets():
        title = ws.title
        if title.startswith('Data Table (Pre-Tiger ') or title.startswith('Data Table (Pre-Rollback'):
            # Try to extract a sortable timestamp from the tab name
            m = re.search(r'(\d{4}-\d{2}-\d{2}(?:[_ ]\d{2,4}(?:-\d{2}-\d{2})?)?)', title)
            sort_key = m.group(1) if m else title
            candidates.append((sort_key, title, ws.row_count))
    candidates.sort(reverse=True)  # newest first
    return candidates


def print_backup_list(handler: GSheetHandler):
    candidates = list_backups(handler)
    if not candidates:
        print("No backup tabs found.")
        return
    print()
    print("=" * 70)
    print("Available backup tabs (newest first):")
    print("=" * 70)
    for i, (sort_key, title, rows) in enumerate(candidates):
        marker = " <- latest" if i == 0 else ""
        print(f"  {i+1:>2}. {title} ({rows} rows){marker}")
    print()


def snapshot_current_data_table(handler: GSheetHandler) -> str:
    """Save current Data Table to a 'Pre-Rollback <ts>' tab. Returns the new tab name."""
    ts = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    backup_name = f"Data Table (Pre-Rollback {ts})"
    try:
        original = handler.spreadsheet.worksheet('Data Table')
    except gspread.exceptions.WorksheetNotFound:
        logger.warning("Data Table doesn't exist — nothing to snapshot")
        return backup_name

    all_data = original.get_all_values()
    rows = max(len(all_data) + 10, 100)
    cols = max(len(all_data[0]) if all_data else 30, 30)

    # If a tab with this exact name exists (very fast successive rollbacks), drop it
    try:
        existing = handler.spreadsheet.worksheet(backup_name)
        handler.spreadsheet.del_worksheet(existing)
    except gspread.exceptions.WorksheetNotFound:
        pass

    backup_ws = handler.spreadsheet.add_worksheet(title=backup_name, rows=rows, cols=cols)
    if all_data:
        end_col = _col_letter(cols)
        backup_ws.update(values=all_data,
                          range_name=f'A1:{end_col}{len(all_data)}',
                          value_input_option='RAW')
    logger.info(f"Snapshot saved: {backup_name} ({len(all_data)} rows)")
    return backup_name


def restore_data_table(handler: GSheetHandler, source_tab: str) -> int:
    """Wipe Data Table and write the contents of `source_tab` back into it.
    Returns number of rows restored."""
    # Read source
    try:
        src_ws = handler.spreadsheet.worksheet(source_tab)
    except gspread.exceptions.WorksheetNotFound:
        raise ValueError(f"Backup tab '{source_tab}' not found in gSheet")
    src_data = src_ws.get_all_values()
    if not src_data:
        raise ValueError(f"Backup tab '{source_tab}' is empty")

    # Wipe target Data Table and rewrite
    target_ws = handler.spreadsheet.worksheet('Data Table')
    target_ws.clear()

    # Chunk write
    end_col = _col_letter(len(src_data[0]))
    chunk_size = 500
    # Header
    target_ws.update(values=[src_data[0]], range_name='A1', value_input_option='USER_ENTERED')
    # Body
    body = src_data[1:]
    for i in range(0, len(body), chunk_size):
        chunk = body[i:i + chunk_size]
        start_row = i + 2
        end_row = start_row + len(chunk) - 1
        target_ws.update(values=chunk,
                          range_name=f'A{start_row}:{end_col}{end_row}',
                          value_input_option='USER_ENTERED')

    handler._invalidate_header_cache('Data Table')
    return len(body)


def main():
    ap = argparse.ArgumentParser(description="Tiger ETL Rollback — restore Data Table from a backup tab")
    ap.add_argument('--list', action='store_true', help='List available backup tabs')
    ap.add_argument('--restore', help='Restore from named backup tab (or "latest")')
    ap.add_argument('--auto-approve', action='store_true', help='Skip y/N prompt')
    args = ap.parse_args()

    if not args.list and not args.restore:
        ap.print_help()
        sys.exit(1)

    handler = GSheetHandler(INCOME_WHEEL_SHEET_ID)

    if args.list:
        print_backup_list(handler)
        return

    # Resolve "latest"
    target = args.restore
    if target == 'latest':
        candidates = list_backups(handler)
        if not candidates:
            print("ERROR: No backup tabs found.")
            sys.exit(1)
        target = candidates[0][1]
        print(f"'latest' resolves to: {target}")

    # Verify the target exists
    try:
        ws = handler.spreadsheet.worksheet(target)
        n_rows = len(ws.get_all_values()) - 1
    except gspread.exceptions.WorksheetNotFound:
        print(f"ERROR: Backup tab '{target}' not found.")
        print()
        print_backup_list(handler)
        sys.exit(1)

    print()
    print("=" * 70)
    print("ROLLBACK PLAN")
    print("=" * 70)
    print(f"  Source:       {target}")
    print(f"  Source rows:  {n_rows}")
    print(f"  Target:       Data Table (live)")
    print(f"  Action:       (1) snapshot current Data Table to 'Pre-Rollback <ts>'")
    print(f"                (2) wipe live Data Table")
    print(f"                (3) write {n_rows} rows from source")
    print()

    if not args.auto_approve:
        resp = input("Proceed? [y/N] ").strip().lower()
        if resp != 'y':
            print("Aborted.")
            sys.exit(0)

    # Step 1: snapshot
    snapshot = snapshot_current_data_table(handler)
    print(f"  [OK] Pre-rollback snapshot: {snapshot}")

    # Step 2+3: restore
    n = restore_data_table(handler, target)
    print(f"  [OK] Restored {n} rows from '{target}'")

    print()
    print("=" * 70)
    print("ROLLBACK COMPLETE")
    print("=" * 70)
    print(f"Refresh the ARGUS app to see the restored data.")
    print(f"If you need to undo this rollback, the previous state is in: {snapshot}")


if __name__ == '__main__':
    main()
