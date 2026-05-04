"""
Tiger ETL — Update & Reconcile (additive, idempotent).

Diffs a freshly-uploaded Tiger CSV against the existing ARGUS Data Table and
applies only the deltas. Unlike `tiger_etl.run_migration()` which is destructive
(wipe + rebuild), this module preserves all existing rows and only adds /
updates rows for genuinely-new Tiger activity.

Public API
----------
- compute_update_plan(tiger_stmt, df_argus, file_meta) -> UpdatePlan
- apply_update_plan(plan, handler) -> dict (audit summary)
- run_update(filepath_or_filelike, handler=None, *, dry_run=False, auto_approve=False) -> dict

CLI
---
    python tiger_etl_update.py path/to/new.csv [--dry-run] [--auto-approve]

Idempotency
-----------
Cross-file safe: relies on content-based row_hash from tiger_parser. Re-uploading
the same CSV (or an overlapping CSV) produces a no-op diff.

Conservative split
------------------
When an incoming Close has qty < the matching ARGUS open's qty, the open is
split: existing row updated to closed-qty + Status=Closed; new sibling row
appended for the remainder with Tiger_Row_Hash = <orig>:p2.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Union, Iterable

import pandas as pd

from gsheet_handler import GSheetHandler
from config import INCOME_WHEEL_SHEET_ID
from tiger_parser import parse_file, parse_files, TigerStatement, TigerTrade, TigerExercise
from tiger_to_argus import (
    infer_strategy_type, derive_pot, detect_rolls,
    tiger_trade_to_argus_row, tiger_exercise_to_argus_rows,
)
from tiger_etl import (
    _build_partial_pairs, _apply_partial_qty, _partial_hash,
    _argus_row_from_pair_close, _argus_row_from_pair_exercise,
    _file_hash, _col_letter, TARGET_DATA_TABLE_COLUMNS,
    write_tiger_statement, write_tiger_cash, write_tiger_imports,
    write_reconciliation_log,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# Plan dataclasses
# ─────────────────────────────────────────────────────────────────
@dataclass
class RowInsert:
    """A new ARGUS row to append (Status=Open or Closed-only orphan)."""
    trade_id: Optional[str]   # filled at apply-time (next available T-N)
    fields: dict
    tiger_row_hash: str
    source: str               # 'new_open' / 'orphan_close' / 'orphan_exercise' / 'partial_remainder' / 'roll_open'
    note: str = ''


@dataclass
class RowUpdate:
    """An update to an existing ARGUS row — close-out or partial-close."""
    trade_id: str
    updates: dict             # field -> new value
    before: dict              # field -> previous value (for audit)
    matched_close_hash: Optional[str] = None
    matched_exercise_hash: Optional[str] = None
    source: str = 'update'


@dataclass
class UpdatePlan:
    """Summary of changes that Update & Reconcile will apply."""
    run_id: str
    file_name: str
    file_hash: str
    inserts: list = field(default_factory=list)
    updates: list = field(default_factory=list)
    roll_pairs: list = field(default_factory=list)  # list of (close_tid, new_tid) — final TIDs filled at apply time
    orphans: list = field(default_factory=list)
    cash_events_new: list = field(default_factory=list)
    fees_backfilled: float = 0.0
    nav_drift: dict = field(default_factory=dict)
    holdings_drift: list = field(default_factory=list)
    summary: dict = field(default_factory=dict)
    already_imported: bool = False
    notes: list = field(default_factory=list)

    def has_blocking_issues(self) -> bool:
        """Should the Apply button be disabled?"""
        return False  # Orphans are warnings only — user can still apply

    def to_dict(self) -> dict:
        return {
            'run_id': self.run_id,
            'file_name': self.file_name,
            'file_hash': self.file_hash,
            'already_imported': self.already_imported,
            'summary': self.summary,
            'inserts': [asdict(i) for i in self.inserts],
            'updates': [asdict(u) for u in self.updates],
            'roll_pairs': self.roll_pairs,
            'orphans': self.orphans,
            'cash_events_new': self.cash_events_new,
            'fees_backfilled': self.fees_backfilled,
            'nav_drift': self.nav_drift,
            'holdings_drift': self.holdings_drift,
            'notes': self.notes,
        }


# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────
def _hash_filelike_or_path(filepath_or_filelike) -> str:
    """SHA256 first 16 chars of file content (works for path or file-like)."""
    h = hashlib.sha256()
    if hasattr(filepath_or_filelike, 'read'):
        pos = None
        try:
            pos = filepath_or_filelike.tell()
        except Exception:
            pass
        try:
            filepath_or_filelike.seek(0)
        except Exception:
            pass
        data = filepath_or_filelike.read()
        if isinstance(data, str):
            data = data.encode('utf-8')
        h.update(data)
        try:
            if pos is not None:
                filepath_or_filelike.seek(pos)
            else:
                filepath_or_filelike.seek(0)
        except Exception:
            pass
    else:
        with open(filepath_or_filelike, 'rb') as f:
            h.update(f.read())
    return h.hexdigest()[:16]


def _check_already_imported(file_hash: str, handler: GSheetHandler) -> Optional[dict]:
    """Look up the Tiger Imports tab — return prior import metadata if same hash exists."""
    try:
        ws = handler.spreadsheet.worksheet('Tiger Imports')
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            return None
        headers = rows[0]
        hash_col = headers.index('File_Hash') if 'File_Hash' in headers else None
        run_col = headers.index('Run_ID') if 'Run_ID' in headers else None
        date_col = headers.index('Imported_At') if 'Imported_At' in headers else None
        if hash_col is None:
            return None
        for r in rows[1:]:
            if len(r) > hash_col and r[hash_col].strip() == file_hash:
                return {
                    'run_id': r[run_col] if run_col is not None and len(r) > run_col else '',
                    'imported_at': r[date_col] if date_col is not None and len(r) > date_col else '',
                    'filename': r[1] if len(r) > 1 else '',
                }
    except Exception as e:
        logger.warning(f"Could not check Tiger Imports tab: {e}")
    return None


def _gather_existing_row_hashes(df_argus: pd.DataFrame, handler: Optional[GSheetHandler] = None) -> set:
    """Collect all Tiger_Row_Hash values that have been ingested into ARGUS.

    Sources:
    1. Data Table 'Tiger_Row_Hash' column — the open trades' hashes
    2. Tiger Statement tab — the canonical fact log including close fills
       and exercise events that don't appear directly in Data Table
       (they're consumed by paired open rows)

    Strips :pN, :dup<N>, and _stk suffixes to get the bare content hash.
    """
    hashes = set()

    def _strip_suffixes(h: str) -> str:
        h = h.strip()
        if not h or h == 'nan':
            return ''
        for suffix_marker in (':p', ':dup', '_stk'):
            if suffix_marker in h:
                h = h.split(suffix_marker)[0]
                break
        return h

    # Source 1: Data Table
    if 'Tiger_Row_Hash' in df_argus.columns:
        for h in df_argus['Tiger_Row_Hash'].astype(str):
            stripped = _strip_suffixes(h)
            if stripped:
                hashes.add(stripped)

    # Source 2: Tiger Statement tab (covers close fills + exercise events that
    # don't appear in Data Table directly because they're folded into paired rows)
    if handler is not None:
        try:
            ws = handler.spreadsheet.worksheet('Tiger Statement')
            rows = ws.get_all_values()
            if rows and len(rows) > 1:
                headers = rows[0]
                if 'Tiger_Row_Hash' in headers:
                    hash_col = headers.index('Tiger_Row_Hash')
                    for r in rows[1:]:
                        if len(r) > hash_col:
                            stripped = _strip_suffixes(r[hash_col])
                            if stripped:
                                hashes.add(stripped)
        except Exception as e:
            logger.warning(f"Could not read Tiger Statement for hash dedup: {e}")

    return hashes


def _next_trade_id(df_argus: pd.DataFrame) -> int:
    """Return the next available T-N number."""
    if df_argus.empty or 'TradeID' not in df_argus.columns:
        return 1
    max_n = 0
    import re as _re
    for tid in df_argus['TradeID'].astype(str):
        m = _re.match(r'T-(\d+)', tid)
        if m:
            max_n = max(max_n, int(m.group(1)))
    return max_n + 1


def _argus_row_to_match_key(row: pd.Series) -> tuple:
    """Build a (ticker, right_inferred, strike, expiry) key from an ARGUS row."""
    tt = str(row.get('TradeType', '')).strip()
    right = None
    if tt in ('CC', 'LEAP', 'LEAP_CALL'):
        right = 'CALL'
    elif tt in ('CSP', 'LEAP_PUT'):
        right = 'PUT'

    strike_val = pd.to_numeric(row.get('Option_Strike_Price_(USD)'), errors='coerce')
    strike = float(strike_val) if pd.notna(strike_val) else None

    expiry_val = pd.to_datetime(row.get('Expiry_Date'), errors='coerce')
    expiry = expiry_val.date() if pd.notna(expiry_val) else None

    return (str(row.get('Ticker', '')).strip(), right, strike, expiry)


# ─────────────────────────────────────────────────────────────────
# Build the diff plan
# ─────────────────────────────────────────────────────────────────
def compute_update_plan(
    tiger_stmt: TigerStatement,
    df_argus: pd.DataFrame,
    file_name: str,
    file_hash: str,
    handler: Optional[GSheetHandler] = None,
) -> UpdatePlan:
    """Compute the additive update plan."""
    run_id = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    plan = UpdatePlan(run_id=run_id, file_name=file_name, file_hash=file_hash)

    # ── Idempotency: file-level check ──
    if handler is not None:
        prior = _check_already_imported(file_hash, handler)
        if prior:
            plan.already_imported = True
            plan.notes.append(f"File already imported on {prior.get('imported_at')} (run_id={prior.get('run_id')})")
            plan.summary = {'new_trades': 0, 'updates': 0, 'rolls': 0, 'orphans': 0, 'cash_events': 0}
            return plan

    # ── Idempotency: row-level (cross-file) ──
    existing_hashes = _gather_existing_row_hashes(df_argus, handler=handler)
    new_trades = [t for t in tiger_stmt.trades if t.row_hash not in existing_hashes]
    new_exercises = [e for e in tiger_stmt.exercises if e.row_hash not in existing_hashes]
    plan.notes.append(f"Cross-file dedup: {len(tiger_stmt.trades) - len(new_trades)} trades + "
                      f"{len(tiger_stmt.exercises) - len(new_exercises)} exercises already in ARGUS")

    # ── Build buckets from new events ──
    OPTION_OPEN_ACTS = ('OpenShort', 'OpenLong', 'Buy', 'Open')
    OPTION_CLOSE_ACTS = ('Close', 'Sell')
    STOCK_BUY_ACTS = ('Buy', 'Open', 'OpenLong')
    STOCK_SELL_ACTS = ('Sell', 'Close', 'OpenShort')
    FUND_BUY_ACTS = ('Buy', 'Open', 'OpenLong')
    FUND_SELL_ACTS = ('Sell', 'Close')

    option_opens = [t for t in new_trades if t.asset_class == 'Option' and t.activity_type in OPTION_OPEN_ACTS]
    option_closes = [t for t in new_trades if t.asset_class == 'Option' and t.activity_type in OPTION_CLOSE_ACTS]
    stock_buys = [t for t in new_trades if t.asset_class == 'Stock' and t.activity_type in STOCK_BUY_ACTS]
    stock_sells = [t for t in new_trades if t.asset_class == 'Stock' and t.activity_type in STOCK_SELL_ACTS]
    fund_buys = [t for t in new_trades if t.asset_class == 'Fund' and t.activity_type in FUND_BUY_ACTS]
    fund_sells = [t for t in new_trades if t.asset_class == 'Fund' and t.activity_type in FUND_SELL_ACTS]

    # ── Step 1: Each option-close (new) needs to find a match in EXISTING ARGUS opens ──
    # We treat EXISTING open ARGUS rows + new option_opens as the candidate "open pool"
    # for matching. This catches cases like: Tiger CSV today has a Close that closes
    # an open from a previous CSV (already in ARGUS).
    df_open = df_argus[df_argus['Status'].astype(str).str.lower() == 'open'].copy()
    df_open_opt = df_open[df_open['TradeType'].isin(['CC', 'CSP', 'LEAP', 'LEAP_PUT', 'LEAP_CALL'])].copy()

    # For each option close, attempt to match against EXISTING open ARGUS rows first (FIFO partial-fill)
    # Build a mutable open-qty dict keyed by index in df_open_opt
    existing_open_remaining: dict = {}
    for idx, row in df_open_opt.iterrows():
        q = pd.to_numeric(row.get('Quantity'), errors='coerce')
        if pd.notna(q) and q > 0:
            existing_open_remaining[idx] = int(q)

    plan.fees_backfilled = 0.0

    for ce in option_closes + new_exercises:
        ce_total_qty = abs(int(getattr(ce, 'quantity', 0)))
        ce_pl = getattr(ce, 'realized_pl', None) or 0
        ce_fee = getattr(ce, 'fee_total', None) or 0
        ce_date = getattr(ce, 'trade_date', None) or getattr(ce, 'event_date', None)
        ce_close_price = getattr(ce, 'trade_price', 0) if hasattr(ce, 'trade_price') else 0

        if ce_total_qty == 0:
            continue

        # Build search key
        ce_key = (ce.ticker, ce.right, ce.strike, getattr(ce, 'expiry', None))
        ce_remaining = ce_total_qty

        # FIFO match against existing ARGUS open rows
        for idx in list(existing_open_remaining.keys()):
            if ce_remaining == 0:
                break
            if existing_open_remaining[idx] == 0:
                continue
            row = df_open_opt.loc[idx]
            row_key = _argus_row_to_match_key(row)
            if row_key != ce_key:
                continue
            row_date = pd.to_datetime(row.get('Date_open'), errors='coerce')
            if pd.notna(row_date) and ce_date and row_date.date() > ce_date:
                continue

            avail = existing_open_remaining[idx]
            take = min(avail, ce_remaining)
            pl_share = (ce_pl * take / ce_total_qty) if ce_total_qty > 0 else 0
            fee_share = (ce_fee * take / ce_total_qty) if ce_total_qty > 0 else 0

            existing_qty = avail
            new_qty = existing_qty - take
            tid = str(row['TradeID'])

            existing_fee = pd.to_numeric(row.get('Fee', 0), errors='coerce') or 0

            if take == existing_qty:
                # FULL close — update existing row in place
                is_expire = isinstance(ce, TigerExercise) and ce.transaction_type in (
                    'Option Expired Worthless', 'Option Expire', 'Option Expiration'
                )
                is_exercise = isinstance(ce, TigerExercise) and ce.transaction_type == 'Option Exercise'
                is_call = ce.right == 'CALL'
                if is_expire:
                    remarks = f'{"CC" if is_call else "CSP"} Expired Worthless (Tiger)'
                elif is_exercise:
                    remarks = 'CC Called away by exercise (Tiger)' if is_call else 'CSP Assigned by exercise (Tiger)'
                else:
                    remarks = f'Closed via Tiger update {run_id}'

                close_price = 0 if isinstance(ce, TigerExercise) else ce_close_price
                close_date = ce.event_date if isinstance(ce, TigerExercise) else ce.trade_date

                plan.updates.append(RowUpdate(
                    trade_id=tid,
                    updates={
                        'Status': 'Closed',
                        'Date_closed': close_date.isoformat() if close_date else '',
                        'Close_Price': close_price,
                        'Actual_Profit_(USD)': round(pl_share, 4),
                        'Fee': round(float(existing_fee) + fee_share, 4),
                        'Remarks': remarks,
                    },
                    before={
                        'Status': str(row.get('Status', '')),
                        'Date_closed': str(row.get('Date_closed', '')),
                        'Close_Price': str(row.get('Close_Price', '')),
                        'Actual_Profit_(USD)': str(row.get('Actual_Profit_(USD)', '')),
                        'Fee': str(row.get('Fee', '')),
                        'Remarks': str(row.get('Remarks', '')),
                    },
                    matched_close_hash=ce.row_hash if isinstance(ce, TigerTrade) else None,
                    matched_exercise_hash=ce.row_hash if isinstance(ce, TigerExercise) else None,
                    source='full_close',
                ))
                plan.fees_backfilled += fee_share
            else:
                # PARTIAL close — conservative split (decision 3b=A):
                #   - update existing row to closed-qty + Status=Closed
                #   - append new sibling row for remainder with :p2 hash suffix
                is_expire = isinstance(ce, TigerExercise) and ce.transaction_type in (
                    'Option Expired Worthless', 'Option Expire', 'Option Expiration'
                )
                is_exercise = isinstance(ce, TigerExercise) and ce.transaction_type == 'Option Exercise'
                is_call = ce.right == 'CALL'
                if is_expire:
                    remarks = f'{"CC" if is_call else "CSP"} Expired Worthless (partial {take} of {existing_qty}) (Tiger)'
                elif is_exercise:
                    remarks = ('CC Called away by exercise' if is_call else 'CSP Assigned by exercise') + f' (partial {take} of {existing_qty}) (Tiger)'
                else:
                    remarks = f'Partial close ({take} of {existing_qty}) via Tiger update {run_id}'

                close_price = 0 if isinstance(ce, TigerExercise) else ce_close_price
                close_date = ce.event_date if isinstance(ce, TigerExercise) else ce.trade_date

                plan.updates.append(RowUpdate(
                    trade_id=tid,
                    updates={
                        'Quantity': take,
                        'Status': 'Closed',
                        'Date_closed': close_date.isoformat() if close_date else '',
                        'Close_Price': close_price,
                        'Actual_Profit_(USD)': round(pl_share, 4),
                        'Fee': round(float(existing_fee) * take / existing_qty + fee_share, 4),
                        'Remarks': remarks,
                    },
                    before={
                        'Quantity': str(row.get('Quantity', '')),
                        'Status': str(row.get('Status', '')),
                        'Fee': str(row.get('Fee', '')),
                    },
                    matched_close_hash=ce.row_hash if isinstance(ce, TigerTrade) else None,
                    matched_exercise_hash=ce.row_hash if isinstance(ce, TigerExercise) else None,
                    source='partial_close',
                ))

                # New sibling row for the remainder (Open status)
                remainder_fields = {col: row.get(col, '') for col in TARGET_DATA_TABLE_COLUMNS if col in df_argus.columns}
                remainder_fields['Quantity'] = new_qty
                remainder_fields['Status'] = 'Open'
                remainder_fields['Date_closed'] = ''
                remainder_fields['Close_Price'] = ''
                remainder_fields['Actual_Profit_(USD)'] = ''
                remainder_fields['Fee'] = round(float(existing_fee) * new_qty / existing_qty, 4)
                remainder_fields['Remarks'] = f'Partial remainder ({new_qty} of {existing_qty}) — split from {tid}'
                # Keep same Tiger source but tag as :p2 for uniqueness
                orig_hash = str(row.get('Tiger_Row_Hash', '')).strip()
                remainder_fields['Tiger_Row_Hash'] = _partial_hash(orig_hash, 2) if orig_hash else ''

                plan.inserts.append(RowInsert(
                    trade_id=None,
                    fields=remainder_fields,
                    tiger_row_hash=remainder_fields['Tiger_Row_Hash'],
                    source='partial_remainder',
                    note=f'Sibling of {tid} after partial close',
                ))
                plan.fees_backfilled += fee_share

            existing_open_remaining[idx] -= take
            ce_remaining -= take

        # If close still has remaining qty after exhausting existing opens, treat as orphan (rare)
        if ce_remaining > 0:
            plan.orphans.append({
                'type': 'unmatched_close',
                'tiger_row_hash': ce.row_hash,
                'ticker': ce.ticker,
                'right': ce.right,
                'strike': ce.strike,
                'expiry': str(getattr(ce, 'expiry', None)) if getattr(ce, 'expiry', None) else '',
                'qty': ce_remaining,
                'realized_pl_share': round((ce_pl * ce_remaining / ce_total_qty), 4) if ce_total_qty else 0,
                'note': 'Close/Exercise with no matching open in ARGUS (orphan)',
            })

    # ── Step 2: New OpenShort / OpenLong → append as new ARGUS open rows ──
    # (option_opens that are genuinely new — they may pair with closes ABOVE
    # if the close also came in this batch, but the existing-pool match
    # should have caught them. Anything that didn't match becomes a new open.)
    for t in option_opens:
        # Was this open hash consumed by a close-pairing above? Check if any
        # update or partial-remainder references this hash.
        hash_already_inserted_via_pair = any(
            u.matched_close_hash == t.row_hash or u.matched_exercise_hash == t.row_hash
            for u in plan.updates
        )
        if hash_already_inserted_via_pair:
            continue
        row = tiger_trade_to_argus_row(t, 'PENDING')
        plan.inserts.append(RowInsert(
            trade_id=None,
            fields=row,
            tiger_row_hash=t.row_hash,
            source='new_open',
            note='',
        ))

    # ── Step 3: Stock buys / sells (paired or standalone) ──
    stk_pairs, stk_open_remaining = _build_partial_pairs(
        stock_buys, stock_sells, lambda x: x.ticker
    )
    for p in stk_pairs:
        if p.get('orphan'):
            ce = p['ce']
            row = tiger_trade_to_argus_row(ce, 'PENDING')
            row['Remarks'] = 'Orphan Stock Sell — no matching new buy'
            row['Actual_Profit_(USD)'] = round(p['pl_share'], 4)
            plan.inserts.append(RowInsert(
                trade_id=None, fields=row, tiger_row_hash=ce.row_hash,
                source='orphan_stock_sell',
            ))
        else:
            qty = p['qty']
            row = tiger_trade_to_argus_row(p['open'], 'PENDING')
            _apply_partial_qty(row, p['open'], qty)
            row['Status'] = 'Closed'
            row['Date_closed'] = p['ce'].trade_date.isoformat() if p['ce'].trade_date else ''
            row['Close_Price'] = p['ce'].trade_price
            row['Actual_Profit_(USD)'] = round(p['pl_share'], 4)
            row['Fee'] = round((p['open'].fee_total or 0) * qty / max(abs(int(p['open'].quantity)), 1) + p['fee_share'], 4)
            row['Tiger_Row_Hash'] = _partial_hash(p['open'].row_hash, p['partial_seq'])
            plan.inserts.append(RowInsert(
                trade_id=None, fields=row, tiger_row_hash=row['Tiger_Row_Hash'],
                source='stock_pair',
            ))

    # Remaining (unpaired) stock buys → emit as Open
    for i, buy in enumerate(stock_buys):
        if stk_open_remaining[i] == 0:
            continue
        row = tiger_trade_to_argus_row(buy, 'PENDING')
        if stk_open_remaining[i] != abs(int(buy.quantity)):
            _apply_partial_qty(row, buy, stk_open_remaining[i])
        plan.inserts.append(RowInsert(
            trade_id=None, fields=row, tiger_row_hash=buy.row_hash, source='new_stock_buy',
        ))

    # Stock sells with no matching buy → orphan
    used_sell_hashes = {p['ce'].row_hash for p in stk_pairs if not p.get('orphan')}
    for s in stock_sells:
        if s.row_hash in used_sell_hashes:
            continue
        row = tiger_trade_to_argus_row(s, 'PENDING')
        row['Remarks'] = f'Orphan Stock Sell — {row.get("Remarks", "")}'
        plan.inserts.append(RowInsert(
            trade_id=None, fields=row, tiger_row_hash=s.row_hash,
            source='orphan_stock_sell',
        ))

    # ── Step 4: Fund (SGD MMF) — same partial-fill logic ──
    fund_pairs, fund_open_remaining = _build_partial_pairs(
        fund_buys, fund_sells, lambda x: x.ticker
    )
    for p in fund_pairs:
        if p.get('orphan'):
            ce = p['ce']
            row = tiger_trade_to_argus_row(ce, 'PENDING')
            row['Remarks'] = 'Orphan Fund Sell'
            row['Actual_Profit_(USD)'] = round(p['pl_share'], 4)
            plan.inserts.append(RowInsert(
                trade_id=None, fields=row, tiger_row_hash=ce.row_hash, source='orphan_fund',
            ))
        else:
            qty = p['qty']
            row = tiger_trade_to_argus_row(p['open'], 'PENDING')
            _apply_partial_qty(row, p['open'], qty)
            row['Status'] = 'Closed'
            row['Date_closed'] = p['ce'].trade_date.isoformat() if p['ce'].trade_date else ''
            row['Close_Price'] = p['ce'].trade_price
            row['Actual_Profit_(USD)'] = round(p['pl_share'], 4)
            row['Fee'] = round((p['open'].fee_total or 0) * qty / max(abs(int(p['open'].quantity)), 1) + p['fee_share'], 4)
            row['Tiger_Row_Hash'] = _partial_hash(p['open'].row_hash, p['partial_seq'])
            plan.inserts.append(RowInsert(
                trade_id=None, fields=row, tiger_row_hash=row['Tiger_Row_Hash'], source='fund_pair',
            ))
    for i, buy in enumerate(fund_buys):
        if fund_open_remaining[i] == 0:
            continue
        row = tiger_trade_to_argus_row(buy, 'PENDING')
        if fund_open_remaining[i] != abs(int(buy.quantity)):
            _apply_partial_qty(row, buy, fund_open_remaining[i])
        plan.inserts.append(RowInsert(
            trade_id=None, fields=row, tiger_row_hash=buy.row_hash, source='new_fund_buy',
        ))

    # ── Step 5: Roll detection within the new+existing pool ──
    # detect_rolls operates on Tiger trades only; we run it on the new trades.
    # Each roll pair is a (close_t, new_open_t) where they share day + ticker but differ in strike/expiry.
    rolls = detect_rolls(new_trades)
    for close_t, open_t in rolls:
        # The close already created an update (matched to existing open).
        # The new_open_t is a new insert. Both are in plan; record the linkage.
        plan.roll_pairs.append({
            'close_tiger_hash': close_t.row_hash,
            'new_open_tiger_hash': open_t.row_hash,
            'close_old_argus_tid': next(
                (u.trade_id for u in plan.updates if u.matched_close_hash == close_t.row_hash),
                None
            ),
        })

    # ── Step 6: New cash events ──
    # Cash events have weak idempotency keys — we use (date, type, amount, description).
    existing_cash_keys = set()
    if handler is not None:
        try:
            ws = handler.spreadsheet.worksheet('Tiger Cash')
            cash_rows = ws.get_all_values()
            if cash_rows and len(cash_rows) > 1:
                cheaders = cash_rows[0]
                date_i = cheaders.index('Date') if 'Date' in cheaders else 2
                type_i = cheaders.index('Type') if 'Type' in cheaders else 3
                amt_i = cheaders.index('Amount') if 'Amount' in cheaders else 5
                desc_i = cheaders.index('Description') if 'Description' in cheaders else 4
                for r in cash_rows[1:]:
                    if len(r) > max(date_i, type_i, amt_i, desc_i):
                        existing_cash_keys.add((r[date_i], r[type_i], r[amt_i], r[desc_i]))
        except Exception:
            pass

    for c in tiger_stmt.cash_events:
        key = (str(c.event_date) if c.event_date else '', c.event_type, str(c.amount), c.description)
        if key not in existing_cash_keys:
            plan.cash_events_new.append({
                'source_file': c.source_file,
                'date': str(c.event_date) if c.event_date else '',
                'type': c.event_type,
                'description': c.description,
                'amount': c.amount,
                'currency': c.currency,
            })

    # ── Step 7: NAV reconcile ──
    if 'end' in tiger_stmt.account_overview:
        tiger_nav = tiger_stmt.account_overview['end'].get('total', 0)
        # ARGUS NAV is best-computed after apply, but we provide a snapshot here
        # showing the current ARGUS Data Table totals + post-apply estimate.
        plan.nav_drift = {
            'tiger_end_nav': tiger_nav,
            'tiger_period_end': str(tiger_stmt.period_end) if tiger_stmt.period_end else '',
            'tiger_account_overview': tiger_stmt.account_overview['end'],
            'note': 'ARGUS-side NAV computation uses live prices and is rendered on Dashboard. Drift = Tiger - ARGUS post-apply.',
        }

    # ── Step 8: Holdings reconcile ──
    # Compare Tiger end-of-period holdings to projected ARGUS open positions after apply
    plan.holdings_drift = []
    for h in tiger_stmt.holdings:
        if h.asset_class != 'Option':
            continue
        # Find matching ARGUS rows
        if df_open_opt.empty:
            continue
        same_contract = df_open_opt[
            (df_open_opt['Ticker'] == h.ticker)
            & (df_open_opt['Option_Strike_Price_(USD)'].astype(str).str.startswith(str(h.strike)))
        ]
        argus_qty = pd.to_numeric(same_contract['Quantity'], errors='coerce').sum() if not same_contract.empty else 0
        if abs(argus_qty - abs(h.quantity)) > 0.5:
            plan.holdings_drift.append({
                'ticker': h.ticker, 'right': h.right, 'strike': h.strike,
                'expiry': str(h.expiry) if h.expiry else '',
                'tiger_qty': abs(h.quantity), 'argus_qty_pre_apply': float(argus_qty),
            })

    # ── Summary ──
    plan.summary = {
        'new_trades_in_csv': len(new_trades),
        'new_exercises_in_csv': len(new_exercises),
        'inserts': len(plan.inserts),
        'updates': len(plan.updates),
        'roll_pairs': len(plan.roll_pairs),
        'orphans': len(plan.orphans),
        'cash_events_new': len(plan.cash_events_new),
        'fees_backfilled_usd': round(plan.fees_backfilled, 2),
        'tiger_end_nav': plan.nav_drift.get('tiger_end_nav'),
        'holdings_with_drift': len(plan.holdings_drift),
    }
    return plan


# ─────────────────────────────────────────────────────────────────
# Apply the plan to gSheet
# ─────────────────────────────────────────────────────────────────
def apply_update_plan(
    plan: UpdatePlan,
    tiger_stmt: TigerStatement,
    handler: GSheetHandler,
) -> dict:
    """Mutate the live gSheet according to the plan. Idempotent (re-applying
    a no-op plan is a no-op). Saves audit JSON. Creates a Data Table backup
    BEFORE applying any change."""
    summary = {
        'run_id': plan.run_id,
        'started_at': datetime.now().isoformat(),
        'inserts_applied': 0,
        'updates_applied': 0,
        'cash_events_appended': 0,
        'errors': [],
    }

    if plan.already_imported:
        summary['note'] = 'File already imported — no changes applied'
        summary['ended_at'] = datetime.now().isoformat()
        return summary

    # ── Backup Data Table ──
    from tiger_etl import backup_data_table
    backup_name = backup_data_table(handler, suffix=f"Update_{plan.run_id}")
    summary['backup_tab'] = backup_name

    # ── Read current Data Table to determine next TradeID ──
    df_argus = handler.read_data_table()
    next_n = _next_trade_id(df_argus)

    # ── Apply updates first (before inserts so inserts can reference the updated state) ──
    for u in plan.updates:
        try:
            handler.update_trade(u.trade_id, u.updates)
            summary['updates_applied'] += 1
        except Exception as e:
            summary['errors'].append(f"update {u.trade_id}: {e}")
            logger.error(f"Failed to update {u.trade_id}: {e}")

    # ── Append inserts ──
    # Sort inserts so deterministic Tx-N assignment by chronological Date_open
    inserts_sorted = sorted(
        plan.inserts,
        key=lambda i: (i.fields.get('Date_open') or i.fields.get('Date_closed') or '9999-99-99', i.source)
    )

    # Build mapping: Tiger_Row_Hash -> assigned T-N (for roll-pair Remarks)
    hash_to_new_tid = {}

    for ins in inserts_sorted:
        new_tid = f'T-{next_n}'
        next_n += 1
        ins.fields['TradeID'] = new_tid
        ins.fields['Sorter'] = next_n - 1
        # Apply roll remark if this insert is the new-open side of a roll
        for rp in plan.roll_pairs:
            if rp['new_open_tiger_hash'] == ins.tiger_row_hash:
                old_tid = rp.get('close_old_argus_tid')
                if old_tid:
                    existing_remark = ins.fields.get('Remarks', '')
                    ins.fields['Remarks'] = f'Rolled from {old_tid} (Tiger update); {existing_remark}'.rstrip('; ')
                    rp['new_argus_tid'] = new_tid
        try:
            handler.append_trade(ins.fields)
            hash_to_new_tid[ins.tiger_row_hash] = new_tid
            summary['inserts_applied'] += 1
        except Exception as e:
            summary['errors'].append(f"insert ({ins.source}): {e}")
            logger.error(f"Failed to insert {ins.source} row: {e}")

    # ── Update old-side roll Remarks for the existing rows that were closed by rolls ──
    for rp in plan.roll_pairs:
        old_tid = rp.get('close_old_argus_tid')
        new_tid = rp.get('new_argus_tid')
        if old_tid and new_tid:
            try:
                handler.update_trade(old_tid, {
                    'Remarks': f'Rolled to {new_tid} (Tiger update)',
                })
            except Exception as e:
                summary['errors'].append(f"roll remark for {old_tid}: {e}")

    # ── Append cash events ──
    if plan.cash_events_new:
        # Reuse write_tiger_cash but keep additive (don't wipe). Easier: append directly.
        try:
            ws = handler.spreadsheet.worksheet('Tiger Cash')
            existing_rows = ws.get_all_values()
            next_row = len(existing_rows) + 1
            new_rows = [
                [plan.run_id, c['source_file'], c['date'], c['type'], c['description'],
                 str(c['amount']), c['currency']]
                for c in plan.cash_events_new
            ]
            if new_rows:
                end_col = _col_letter(7)
                ws.update(values=new_rows,
                          range_name=f'A{next_row}:{end_col}{next_row + len(new_rows) - 1}',
                          value_input_option='USER_ENTERED')
                summary['cash_events_appended'] = len(new_rows)
        except Exception as e:
            summary['errors'].append(f"cash events: {e}")

    # ── Append to Tiger Statement (NEW trades only — not the whole CSV) ──
    new_trade_hashes = {ins.tiger_row_hash.split(':')[0] for ins in plan.inserts if ins.source != 'partial_remainder'}
    new_trade_hashes |= {u.matched_close_hash for u in plan.updates if u.matched_close_hash}
    new_trade_hashes |= {u.matched_exercise_hash for u in plan.updates if u.matched_exercise_hash}
    new_trade_hashes.discard(None)

    new_trades_for_log = [t for t in tiger_stmt.trades if t.row_hash in new_trade_hashes]
    new_exs_for_log = [e for e in tiger_stmt.exercises if e.row_hash in new_trade_hashes]
    if new_trades_for_log or new_exs_for_log:
        try:
            from tiger_etl import _get_or_create_worksheet
            ws = _get_or_create_worksheet(handler, 'Tiger Statement', rows=2000, cols=20)
            existing = ws.get_all_values()
            next_row = len(existing) + 1
            headers = [
                'Run_ID', 'Source_File', 'Source_Row', 'Trade_Date', 'Settle_Date',
                'Asset_Class', 'Symbol_Raw', 'Ticker', 'Expiry', 'Right', 'Strike',
                'Activity_Type', 'Quantity', 'Trade_Price', 'Amount', 'Fee_Total',
                'Realized_PL', 'Notes', 'Currency', 'Tiger_Row_Hash',
            ]
            # Ensure header exists
            if not existing or existing[0] != headers:
                ws.update(values=[headers], range_name='A1', value_input_option='USER_ENTERED')
                next_row = 2
            rows_to_write = []
            for t in new_trades_for_log:
                rows_to_write.append([
                    plan.run_id, t.source_file, str(t.source_row),
                    str(t.trade_date) if t.trade_date else '', str(t.settle_date) if t.settle_date else '',
                    t.asset_class, t.symbol_raw, t.ticker,
                    str(t.expiry) if t.expiry else '', t.right or '', str(t.strike) if t.strike else '',
                    t.activity_type, str(t.quantity), str(t.trade_price), str(t.amount),
                    str(t.fee_total), str(t.realized_pl) if t.realized_pl is not None else '',
                    t.notes, t.currency, t.row_hash,
                ])
            for e in new_exs_for_log:
                rows_to_write.append([
                    plan.run_id, e.source_file, str(e.source_row),
                    str(e.event_date) if e.event_date else '', '',
                    e.asset_class, e.symbol_raw, e.ticker,
                    str(e.expiry) if e.expiry else '', e.right or '', str(e.strike) if e.strike else '',
                    e.transaction_type, str(e.quantity), '', '',
                    '0', str(e.realized_pl), '', e.currency, e.row_hash,
                ])
            if rows_to_write:
                end_col = _col_letter(20)
                ws.update(values=rows_to_write,
                          range_name=f'A{next_row}:{end_col}{next_row + len(rows_to_write) - 1}',
                          value_input_option='USER_ENTERED')
        except Exception as e:
            summary['errors'].append(f"tiger statement append: {e}")

    # ── Tiger Imports ledger ──
    try:
        from tiger_etl import _get_or_create_worksheet
        ws = _get_or_create_worksheet(handler, 'Tiger Imports', rows=200, cols=10)
        existing = ws.get_all_values()
        next_row = len(existing) + 1
        headers = ['Run_ID', 'Filename', 'File_Hash', 'Imported_At',
                   'Trades_Imported', 'Cash_Events_Imported', 'Status']
        if not existing or existing[0] != headers:
            ws.update(values=[headers], range_name='A1', value_input_option='USER_ENTERED')
            next_row = 2
        ws.update(values=[[
            plan.run_id, plan.file_name, plan.file_hash, datetime.now().isoformat(),
            str(summary['inserts_applied']), str(summary['cash_events_appended']),
            'Update' if not summary['errors'] else 'PartialError',
        ]], range_name=f'A{next_row}:{_col_letter(7)}{next_row}',
            value_input_option='USER_ENTERED')
    except Exception as e:
        summary['errors'].append(f"tiger imports: {e}")

    # ── Reconciliation Log ──
    try:
        from tiger_etl import _get_or_create_worksheet
        ws = _get_or_create_worksheet(handler, 'Reconciliation Log', rows=200, cols=20)
        existing = ws.get_all_values()
        next_row = len(existing) + 1
        headers = ['Run_ID', 'Run_Type', 'Run_Timestamp', 'Source_Files',
                   'Inserts', 'Updates', 'Rolls', 'Orphans', 'Cash_Events_New',
                   'Fees_Backfilled_USD', 'Tiger_End_NAV', 'Holdings_Drift_Count']
        if not existing or existing[0] != headers:
            ws.update(values=[headers], range_name='A1', value_input_option='USER_ENTERED')
            next_row = 2
        nav = plan.nav_drift.get('tiger_end_nav', '')
        ws.update(values=[[
            plan.run_id, 'Update', datetime.now().isoformat(),
            plan.file_name,
            str(summary['inserts_applied']), str(summary['updates_applied']),
            str(len(plan.roll_pairs)), str(len(plan.orphans)),
            str(summary['cash_events_appended']), str(round(plan.fees_backfilled, 2)),
            str(nav), str(len(plan.holdings_drift)),
        ]], range_name=f'A{next_row}:{_col_letter(12)}{next_row}',
            value_input_option='USER_ENTERED')
    except Exception as e:
        summary['errors'].append(f"reconciliation log: {e}")

    # ── Save audit JSON ──
    audit_path = Path('data/etl_audit') / f'etl_update_{plan.run_id}.json'
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with open(audit_path, 'w', encoding='utf-8') as f:
        json.dump({'plan': plan.to_dict(), 'apply_summary': summary}, f, indent=2, default=str)
    summary['audit_file'] = str(audit_path)

    summary['ended_at'] = datetime.now().isoformat()
    return summary


# ─────────────────────────────────────────────────────────────────
# High-level orchestrator
# ─────────────────────────────────────────────────────────────────
def run_update(
    filepath_or_filelike,
    handler: Optional[GSheetHandler] = None,
    *,
    dry_run: bool = False,
    auto_approve: bool = False,
    file_name: Optional[str] = None,
) -> dict:
    """Parse the file, build the plan, optionally apply.

    For Streamlit usage: pass an UploadedFile object. Set auto_approve=True
    once the user clicks the Apply button. For dry-run preview, set dry_run=True.
    """
    if handler is None:
        handler = GSheetHandler(INCOME_WHEEL_SHEET_ID)

    # Determine file name
    if file_name is None:
        if hasattr(filepath_or_filelike, 'name'):
            file_name = filepath_or_filelike.name
        else:
            file_name = Path(str(filepath_or_filelike)).name

    file_hash = _hash_filelike_or_path(filepath_or_filelike)

    # Parse
    tiger_stmt = parse_file(filepath_or_filelike, source_name=file_name)
    df_argus = handler.read_data_table()

    # Build plan
    plan = compute_update_plan(tiger_stmt, df_argus, file_name, file_hash, handler=handler)

    if dry_run:
        return {'plan': plan.to_dict(), 'applied': False}

    if not auto_approve and not plan.already_imported:
        # In CLI mode this is the y/N prompt. Streamlit handles approval via UI.
        print(f"Plan summary: {json.dumps(plan.summary, indent=2)}")
        resp = input("Apply this plan? [y/N] ").strip().lower()
        if resp != 'y':
            return {'plan': plan.to_dict(), 'applied': False, 'note': 'User declined'}

    apply_summary = apply_update_plan(plan, tiger_stmt, handler)
    return {'plan': plan.to_dict(), 'applied': True, 'apply_summary': apply_summary}


# ─────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Tiger Update & Reconcile (additive, idempotent)")
    ap.add_argument('file', help='Path to Tiger CSV')
    ap.add_argument('--dry-run', action='store_true', help='Preview without applying')
    ap.add_argument('--auto-approve', action='store_true', help='Skip y/N prompt')
    ap.add_argument('--verbose', '-v', action='store_true')
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    result = run_update(args.file, dry_run=args.dry_run, auto_approve=args.auto_approve)
    print()
    print('=' * 60)
    print('UPDATE PLAN SUMMARY')
    print('=' * 60)
    summary = result['plan']['summary']
    for k, v in summary.items():
        print(f"  {k}: {v}")
    if result.get('applied'):
        print()
        print('APPLY RESULT:')
        for k, v in result['apply_summary'].items():
            if k != 'errors' or v:
                print(f"  {k}: {v}")


if __name__ == '__main__':
    main()
