"""
One-shot cleanup of stale gSheet tabs.

Production policy: only these tabs should exist:
  - Data Table              (live trade data — single source of truth)
  - Audit_Table             (action audit trail)
  - Settings                (FX rate, pot deposits, capital allocation)
  - Tiger Imports           (file-hash ledger for re-upload idempotency)
  - 1 rollback backup       (Data Table (Pre-Tiger Update_2026-05-04_11-54-13))

Everything else is deleted.

Safety:
  - Lists what will be kept and what will be deleted
  - Asks for explicit y/N confirmation
  - Saves a JSON snapshot of EVERY tab being deleted (to data/gsheet_archive_<ts>/)
    so we can resurrect any tab from disk if we change our mind
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import gspread

from gsheet_handler import GSheetHandler
from config import INCOME_WHEEL_SHEET_ID

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


KEEP = {
    'Data Table',
    'Audit_Table',
    'Settings',
    'Tiger Imports',
    'Data Table (Pre-Tiger Update_2026-05-04_11-54-13)',  # rollback point
}


def main():
    handler = GSheetHandler(INCOME_WHEEL_SHEET_ID)
    all_ws = handler.spreadsheet.worksheets()

    keep, delete = [], []
    for ws in all_ws:
        if ws.title in KEEP:
            keep.append(ws)
        else:
            delete.append(ws)

    print()
    print("=" * 70)
    print("gSheet CLEANUP PLAN")
    print("=" * 70)
    print(f"Total tabs: {len(all_ws)}  ->  After cleanup: {len(keep)}")
    print()
    print("KEEPING:")
    for ws in keep:
        print(f"  + {ws.title}")
    print()
    print(f"DELETING ({len(delete)} tabs):")
    for ws in delete:
        print(f"  - {ws.title}")

    # Identify any KEEP tabs that don't exist (typos, etc.)
    existing_titles = {ws.title for ws in all_ws}
    missing_keep = KEEP - existing_titles
    if missing_keep:
        print()
        print(f"WARNING: {len(missing_keep)} tabs in KEEP list don't exist in gSheet:")
        for t in missing_keep:
            print(f"  ? {t}")

    print()
    if not delete:
        print("Nothing to delete. gSheet already clean.")
        return

    resp = input(f"Proceed with deleting {len(delete)} tabs? Each one will be JSON-archived first. [y/N] ").strip().lower()
    if resp != 'y':
        print("Aborted.")
        sys.exit(0)

    # Archive all tabs about to be deleted
    archive_dir = Path('data') / f'gsheet_archive_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    archive_dir.mkdir(parents=True, exist_ok=True)
    print()
    print(f"Archiving {len(delete)} tabs to: {archive_dir}/")
    for ws in delete:
        try:
            data = ws.get_all_values()
            safe_name = ws.title.replace('/', '_').replace('\\', '_')
            out = archive_dir / f"{safe_name}.json"
            with open(out, 'w', encoding='utf-8') as f:
                json.dump({'tab': ws.title, 'rows': data, 'row_count': ws.row_count, 'col_count': ws.col_count}, f)
            print(f"  archived: {ws.title} ({len(data)} rows) -> {out.name}")
        except Exception as e:
            print(f"  WARN archiving {ws.title}: {e}")

    print()
    print("Deleting tabs...")
    deleted = 0
    for ws in delete:
        try:
            handler.spreadsheet.del_worksheet(ws)
            print(f"  [OK] deleted: {ws.title}")
            deleted += 1
        except Exception as e:
            print(f"  [FAIL] {ws.title}: {e}")

    print()
    print("=" * 70)
    print(f"CLEANUP COMPLETE — {deleted}/{len(delete)} tabs deleted")
    print("=" * 70)
    print(f"Archive saved at: {archive_dir}/")
    print("If you need to resurrect any tab, the JSON files contain row data.")


if __name__ == '__main__':
    main()
