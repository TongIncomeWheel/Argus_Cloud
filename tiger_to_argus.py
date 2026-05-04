"""
Tiger → ARGUS transform layer.

Pure-Python module. Takes parsed Tiger statement + current ARGUS Data Table
DataFrame, produces:
  - List of ARGUS-format row proposals (enrich, insert, orphan)
  - Audit trail dict for traceability

No gSheet writes. No Streamlit. Pure logic.

Usage:
    from tiger_parser import parse_files
    from tiger_to_argus import transform_to_argus

    stmt = parse_files(['file1.csv', 'file2.csv'])
    plan = transform_to_argus(stmt, df_argus_current)
    # plan = {'changes': [...], 'audit': {...}, 'summary': {...}}
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# StrategyType inference (per user spec)
# ─────────────────────────────────────────────────────────────────
def infer_strategy_type(ticker: str) -> str:
    """
    Per user rule:
      COIN → ActiveCore (Active Income Pot)
      SPY  → PMCC (Base Pot)
      else → WHEEL (Base Pot)
    """
    t = (ticker or '').upper().strip()
    if t == 'COIN':
        return 'ActiveCore'
    if t == 'SPY':
        return 'PMCC'
    return 'WHEEL'


def derive_pot(strategy_type: str) -> str:
    """Pot is derived from StrategyType."""
    return 'Active' if strategy_type == 'ActiveCore' else 'Base'


# ─────────────────────────────────────────────────────────────────
# Match logic
# ─────────────────────────────────────────────────────────────────
DATE_TOLERANCE_DAYS = 1  # ±1 day to handle US/SGT timezone


def _activity_to_argus_side(activity: str, qty: float, asset_class: str, right: Optional[str]) -> str:
    """
    Map Tiger Activity Type + qty + right → ARGUS side identifier
    Used for match key + insert classification.
    """
    if asset_class == 'Stock':
        return 'STOCK_BUY' if qty > 0 else 'STOCK_SELL'
    # Options
    if activity == 'OpenShort':
        return 'OPEN_SHORT_PUT' if right == 'PUT' else 'OPEN_SHORT_CALL'
    if activity == 'OpenLong':
        return 'OPEN_LONG_PUT' if right == 'PUT' else 'OPEN_LONG_CALL'
    if activity == 'Close':
        # Closing a short position has positive qty; closing long has negative
        if qty > 0:
            return 'CLOSE_SHORT_PUT' if right == 'PUT' else 'CLOSE_SHORT_CALL'
        else:
            return 'CLOSE_LONG_PUT' if right == 'PUT' else 'CLOSE_LONG_CALL'
    return f'UNKNOWN_{activity}'


def _argus_row_side(row) -> str:
    """Compute the side identifier for an existing ARGUS row."""
    tt = str(row.get('TradeType', '')).strip()
    direction = str(row.get('Direction', '')).strip()
    status = str(row.get('Status', '')).strip()

    if tt == 'STOCK':
        return 'STOCK_BUY' if direction == 'Buy' else 'STOCK_SELL'

    # Options
    is_open = (status == 'Open')
    if tt == 'CC':  # short call
        return 'OPEN_SHORT_CALL' if is_open else 'CLOSE_SHORT_CALL'
    if tt == 'CSP':  # short put
        return 'OPEN_SHORT_PUT' if is_open else 'CLOSE_SHORT_PUT'
    if tt == 'LEAP':  # long call (long put rare, ignored)
        return 'OPEN_LONG_CALL' if is_open else 'CLOSE_LONG_CALL'
    return 'UNKNOWN'


def _tiger_activity_to_argus_trade_type(tiger_trade) -> str:
    """Map Tiger trade → ARGUS TradeType ('CC' / 'CSP' / 'LEAP' / 'LEAP_PUT' / 'STOCK')."""
    if tiger_trade.asset_class == 'Stock':
        return 'STOCK'
    if tiger_trade.asset_class == 'Fund':
        return 'STOCK'  # Treat fund holdings as stock-like
    # Options — 'Open' (positive qty) and 'OpenLong' both mean buying long
    is_long_open = (
        tiger_trade.activity_type in ('OpenLong', 'Buy')
        or (tiger_trade.activity_type == 'Open' and tiger_trade.quantity > 0)
    )
    if tiger_trade.right == 'PUT':
        if is_long_open:
            return 'LEAP_PUT'  # Long put = LEAP_PUT (used for protection)
        return 'CSP'           # Short put (default)
    if tiger_trade.right == 'CALL':
        if is_long_open:
            # Long call: LEAP if DTE > 180, else just LEAP (we use 'LEAP' generically for long calls)
            if tiger_trade.expiry and tiger_trade.trade_date:
                dte = (tiger_trade.expiry - tiger_trade.trade_date).days
                if dte > 180:
                    return 'LEAP'
            return 'LEAP'  # Short-dated long call (still long)
        return 'CC'  # Short call (default)
    return 'UNKNOWN'


def _tiger_direction(tiger_trade) -> str:
    """Map Tiger qty sign → 'Buy' / 'Sell'."""
    if tiger_trade.activity_type in ('OpenShort',):
        return 'Sell'
    if tiger_trade.activity_type in ('OpenLong',):
        return 'Buy'
    if tiger_trade.activity_type == 'Close':
        # Close of a short = Buy (BTC); close of a long = Sell
        return 'Buy' if tiger_trade.quantity > 0 else 'Sell'
    if tiger_trade.asset_class == 'Stock':
        return 'Buy' if tiger_trade.quantity > 0 else 'Sell'
    return 'Buy'


def _match_score(tiger_trade, argus_row) -> int:
    """
    Score how well a tiger trade matches an ARGUS row.
    Returns -1 if no match (different side/strike/expiry/qty), else date_diff in days.
    Lower score = better match.
    """
    # Compute Tiger side
    tiger_side = _activity_to_argus_side(
        tiger_trade.activity_type, tiger_trade.quantity,
        tiger_trade.asset_class, tiger_trade.right
    )
    argus_side = _argus_row_side(argus_row)

    if tiger_side != argus_side:
        return -1

    # Match strike (options only)
    if tiger_trade.asset_class == 'Option':
        argus_strike = pd.to_numeric(argus_row.get('Option_Strike_Price_(USD)', 0), errors='coerce')
        tiger_strike = tiger_trade.strike or 0
        if abs((argus_strike or 0) - tiger_strike) > 0.01:
            return -1

        # Match expiry
        argus_expiry = pd.to_datetime(argus_row.get('Expiry_Date'), errors='coerce')
        if pd.isna(argus_expiry):
            return -1
        if argus_expiry.date() != tiger_trade.expiry:
            return -1

    # Match quantity (absolute)
    argus_qty = pd.to_numeric(argus_row.get('Quantity', 0), errors='coerce') or 0
    if abs(abs(argus_qty) - abs(tiger_trade.quantity)) > 0.001:
        return -1

    # Match date with tolerance
    is_open_side = tiger_side.startswith('OPEN_') or tiger_side == 'STOCK_BUY'
    if is_open_side:
        argus_date = pd.to_datetime(argus_row.get('Date_open'), errors='coerce')
    else:
        argus_date = pd.to_datetime(argus_row.get('Date_closed'), errors='coerce')

    if pd.isna(argus_date):
        return -1
    if not tiger_trade.trade_date:
        return -1

    date_diff = abs((argus_date.date() - tiger_trade.trade_date).days)
    if date_diff > DATE_TOLERANCE_DAYS:
        return -1

    return date_diff  # 0 = exact, 1 = ±1 day


def _find_match(tiger_trade, df_argus, used_indices: set) -> Optional[int]:
    """Return the index of the best-matching ARGUS row, or None."""
    # Pre-filter by ticker for performance
    candidates = df_argus[df_argus['Ticker'].astype(str).str.upper() == (tiger_trade.ticker or '').upper()]
    candidates = candidates[~candidates.index.isin(used_indices)]

    best_idx = None
    best_score = float('inf')
    for idx, row in candidates.iterrows():
        score = _match_score(tiger_trade, row)
        if score >= 0 and score < best_score:
            best_score = score
            best_idx = idx
    return best_idx


# ─────────────────────────────────────────────────────────────────
# Roll detection (same-day Close + OpenShort same ticker different strike/expiry)
# ─────────────────────────────────────────────────────────────────
def detect_rolls(tiger_trades: list) -> list:
    """
    Return list of (close_trade, new_trade) pairs that look like rolls.
    A roll: same trade_date, same ticker, one Close + one OpenShort,
    different strike OR different expiry.
    """
    by_day_ticker: dict = {}
    for t in tiger_trades:
        if t.asset_class != 'Option':
            continue
        key = (t.trade_date, t.ticker)
        by_day_ticker.setdefault(key, []).append(t)

    pairs = []
    for (day, ticker), trades in by_day_ticker.items():
        closes = [t for t in trades if t.activity_type == 'Close']
        opens = [t for t in trades if t.activity_type == 'OpenShort']

        # Greedy pair: each close finds the open with same right (PUT-PUT, CALL-CALL)
        # but DIFFERENT strike or expiry
        used_open_idx: set = set()
        for c in closes:
            for i, o in enumerate(opens):
                if i in used_open_idx:
                    continue
                if c.right != o.right:
                    continue
                # Same strike AND same expiry = same contract being closed and reopened (not a roll)
                if c.strike == o.strike and c.expiry == o.expiry:
                    continue
                pairs.append((c, o))
                used_open_idx.add(i)
                break

    return pairs


# ─────────────────────────────────────────────────────────────────
# Tiger Trade → ARGUS Row Construction (for inserts)
# ─────────────────────────────────────────────────────────────────
def tiger_trade_to_argus_row(
    tiger_trade, trade_id: str, pmcc_tickers: Optional[set] = None
) -> dict:
    """Build a new ARGUS Data Table row from a Tiger fill."""
    ticker = tiger_trade.ticker
    asset_class = tiger_trade.asset_class
    activity = tiger_trade.activity_type

    strategy = infer_strategy_type(ticker)
    pot = derive_pot(strategy)
    direction = _tiger_direction(tiger_trade)
    trade_type = _tiger_activity_to_argus_trade_type(tiger_trade)

    qty = abs(int(tiger_trade.quantity))

    row = {
        'TradeID': trade_id,
        'Ticker': ticker,
        'StrategyType': strategy,
        'Direction': direction,
        'TradeType': trade_type,
        'Quantity': qty,
        'Open_lots': qty * 100 if asset_class == 'Stock' else 0,
        'Option_Strike_Price_(USD)': tiger_trade.strike if asset_class == 'Option' else '',
        'Price_of_current_underlying_(USD)': tiger_trade.trade_price if asset_class == 'Stock' else '',
        'OptPremium': tiger_trade.trade_price if asset_class == 'Option' else '',
        'Date_open': tiger_trade.trade_date.isoformat() if tiger_trade.trade_date else '',
        'Expiry_Date': tiger_trade.expiry.isoformat() if tiger_trade.expiry else '',
        'Date_closed': '',
        'Status': 'Open',
        'Close_Price': '',
        'Actual_Profit_(USD)': '',
        'Sorter': int(trade_id.replace('T-', '').split('-')[0]) if trade_id.startswith('T-') else 0,
        'Remarks': f'Imported from Tiger ({tiger_trade.source_file})',
        # New columns
        'Fee': tiger_trade.fee_total,
        'Pot': pot,
        'Tiger_Row_Hash': tiger_trade.row_hash,
    }

    # Adjust for stock buys
    if asset_class == 'Stock':
        if direction == 'Buy':
            row['TradeType'] = 'STOCK'
            row['Status'] = 'Open'
            # qty here from Tiger = shares (not contracts)
            row['Quantity'] = abs(int(tiger_trade.quantity)) // 100 or 1
            row['Open_lots'] = abs(int(tiger_trade.quantity))
        else:
            # Stock sell — closes existing position
            row['TradeType'] = 'STOCK'
            row['Status'] = 'Closed'
            row['Date_closed'] = tiger_trade.trade_date.isoformat() if tiger_trade.trade_date else ''
            row['Close_Price'] = tiger_trade.trade_price

    # Closing trades: status=Closed, Date_closed=trade_date, Close_Price=trade_price
    if activity == 'Close':
        row['Status'] = 'Closed'
        row['Date_open'] = ''  # unknown; matched via existing ARGUS row
        row['Date_closed'] = tiger_trade.trade_date.isoformat() if tiger_trade.trade_date else ''
        row['Close_Price'] = tiger_trade.trade_price
        if tiger_trade.realized_pl is not None:
            row['Actual_Profit_(USD)'] = tiger_trade.realized_pl

    return row


def tiger_exercise_to_argus_rows(
    tiger_exercise, close_trade_id: str, stock_trade_id: Optional[str]
) -> list:
    """
    Build the ARGUS row(s) for an Exercise/Expiration event.
    Returns 1 row for expiration, 2 rows for exercise (CSP assigned or CC called).
    """
    rows = []
    ticker = tiger_exercise.ticker
    strategy = infer_strategy_type(ticker)
    pot = derive_pot(strategy)
    qty = abs(int(tiger_exercise.quantity))

    # Row 1: close the option
    # Tiger uses 'Option Expire' / 'Option Expired Worthless' / 'Option Expiration' interchangeably
    if tiger_exercise.transaction_type in ('Option Expired Worthless', 'Option Expire', 'Option Expiration'):
        # Close at $0, full premium kept (Tiger's realized_pl is the kept premium)
        is_call = tiger_exercise.right == 'CALL'
        row1 = {
            'TradeID': close_trade_id,
            'Ticker': ticker,
            'StrategyType': strategy,
            'Direction': 'Sell',
            'TradeType': 'CC' if is_call else 'CSP',
            'Quantity': qty,
            'Open_lots': 0,
            'Option_Strike_Price_(USD)': tiger_exercise.strike,
            'Price_of_current_underlying_(USD)': '',
            'OptPremium': '',  # backfilled if we have the open trade
            'Date_open': '',
            'Expiry_Date': tiger_exercise.expiry.isoformat() if tiger_exercise.expiry else '',
            'Date_closed': tiger_exercise.event_date.isoformat() if tiger_exercise.event_date else '',
            'Status': 'Closed',
            'Close_Price': 0,
            'Actual_Profit_(USD)': tiger_exercise.realized_pl,
            'Sorter': int(close_trade_id.replace('T-', '').split('-')[0]) if close_trade_id.startswith('T-') else 0,
            'Remarks': f'{"CC" if is_call else "CSP"} Expired Worthless (Tiger)',
            'Fee': 0,
            'Pot': pot,
            'Tiger_Row_Hash': tiger_exercise.row_hash,
        }
        rows.append(row1)

    elif tiger_exercise.transaction_type == 'Option Exercise':
        is_call = tiger_exercise.right == 'CALL'
        # Close the option
        row1 = {
            'TradeID': close_trade_id,
            'Ticker': ticker,
            'StrategyType': strategy,
            'Direction': 'Sell',
            'TradeType': 'CC' if is_call else 'CSP',
            'Quantity': qty,
            'Open_lots': 0,
            'Option_Strike_Price_(USD)': tiger_exercise.strike,
            'Price_of_current_underlying_(USD)': '',
            'OptPremium': '',
            'Date_open': '',
            'Expiry_Date': tiger_exercise.expiry.isoformat() if tiger_exercise.expiry else '',
            'Date_closed': tiger_exercise.event_date.isoformat() if tiger_exercise.event_date else '',
            'Status': 'Closed',
            'Close_Price': 0,
            'Actual_Profit_(USD)': tiger_exercise.realized_pl,
            'Sorter': int(close_trade_id.replace('T-', '').split('-')[0]) if close_trade_id.startswith('T-') else 0,
            'Remarks': f'CC Called away by exercise (Tiger)' if is_call else f'CSP Assigned by exercise (Tiger)',
            'Fee': 0,
            'Pot': pot,
            'Tiger_Row_Hash': tiger_exercise.row_hash,
        }
        rows.append(row1)

        # Row 2: stock movement
        if stock_trade_id:
            shares = qty * 100
            if is_call:
                # CC called: close existing STOCK at strike (we don't know existing cost basis here;
                # downstream ETL apply step will look up the existing open STOCK row and update it.
                # Generate this as a "stock close" instruction.)
                row2 = {
                    'TradeID': stock_trade_id,
                    'Ticker': ticker,
                    'StrategyType': strategy,
                    'Direction': 'Sell',
                    'TradeType': 'STOCK',
                    'Quantity': shares,
                    'Open_lots': shares,
                    'Option_Strike_Price_(USD)': '',
                    'Price_of_current_underlying_(USD)': tiger_exercise.strike,
                    'OptPremium': '',
                    'Date_open': '',
                    'Expiry_Date': '',
                    'Date_closed': tiger_exercise.event_date.isoformat() if tiger_exercise.event_date else '',
                    'Status': 'Closed',
                    'Close_Price': tiger_exercise.strike,
                    'Actual_Profit_(USD)': '',  # downstream computes from cost basis
                    'Sorter': int(stock_trade_id.replace('T-', '').split('-')[0]) if stock_trade_id.startswith('T-') else 0,
                    'Remarks': f'Called away by {close_trade_id} (Tiger)',
                    'Fee': 0,
                    'Pot': pot,
                    'Tiger_Row_Hash': tiger_exercise.row_hash + '_stk',
                }
            else:
                # CSP assigned: open new STOCK at strike
                row2 = {
                    'TradeID': stock_trade_id,
                    'Ticker': ticker,
                    'StrategyType': strategy,
                    'Direction': 'Buy',
                    'TradeType': 'STOCK',
                    'Quantity': shares,
                    'Open_lots': shares,
                    'Option_Strike_Price_(USD)': '',
                    'Price_of_current_underlying_(USD)': tiger_exercise.strike,
                    'OptPremium': '',
                    'Date_open': tiger_exercise.event_date.isoformat() if tiger_exercise.event_date else '',
                    'Expiry_Date': '',
                    'Date_closed': '',
                    'Status': 'Open',
                    'Close_Price': '',
                    'Actual_Profit_(USD)': '',
                    'Sorter': int(stock_trade_id.replace('T-', '').split('-')[0]) if stock_trade_id.startswith('T-') else 0,
                    'Remarks': f'Assigned from {close_trade_id} (Tiger)',
                    'Fee': 0,
                    'Pot': pot,
                    'Tiger_Row_Hash': tiger_exercise.row_hash + '_stk',
                }
            rows.append(row2)

    return rows


# ─────────────────────────────────────────────────────────────────
# Main transform — produce the full change plan
# ─────────────────────────────────────────────────────────────────
def transform_to_argus(
    tiger_stmt, df_argus_current: pd.DataFrame,
    pmcc_tickers: Optional[set] = None,
    next_trade_id_start: Optional[int] = None,
) -> dict:
    """
    Build the ETL change plan.

    Returns:
      {
        'changes': list of change dicts (enrich/insert/orphan/roll_paired),
        'summary': dict of counts,
        'audit': dict with full traceability,
        'argus_proposals': list of dicts ready for atomic_transaction,
      }
    """
    if df_argus_current is None:
        df_argus_current = pd.DataFrame()

    # Determine next available TradeID number
    if next_trade_id_start is None:
        existing_ids = df_argus_current['TradeID'].astype(str).str.extract(r'T-(\d+)')[0]
        existing_ids = existing_ids.dropna().astype(int)
        next_trade_id_start = (existing_ids.max() + 1) if not existing_ids.empty else 1

    next_id_counter = [next_trade_id_start]

    def gen_trade_id() -> str:
        tid = f"T-{next_id_counter[0]}"
        next_id_counter[0] += 1
        return tid

    used_argus_indices: set = set()
    changes = []

    # ── Step 1: Match each Tiger trade to ARGUS rows ──
    enrich_count = 0
    insert_count = 0
    fees_backfilled = 0.0

    for tiger_trade in tiger_stmt.trades:
        match_idx = _find_match(tiger_trade, df_argus_current, used_argus_indices)

        if match_idx is not None:
            # ENRICH
            argus_row = df_argus_current.loc[match_idx]
            used_argus_indices.add(match_idx)
            fields_changed = {}

            # Determine which fields to update (tiger is golden for these)
            is_close = tiger_trade.activity_type == 'Close'
            if is_close:
                # Close trade: update Close_Price + Actual_Profit_(USD) + Date_closed + Fee
                old_cp = pd.to_numeric(argus_row.get('Close_Price', 0), errors='coerce') or 0
                if abs(old_cp - tiger_trade.trade_price) > 0.001:
                    fields_changed['Close_Price'] = {'before': float(old_cp), 'after': tiger_trade.trade_price}
                if tiger_trade.realized_pl is not None:
                    old_pl = pd.to_numeric(argus_row.get('Actual_Profit_(USD)', 0), errors='coerce') or 0
                    if abs(old_pl - tiger_trade.realized_pl) > 0.01:
                        fields_changed['Actual_Profit_(USD)'] = {'before': float(old_pl), 'after': tiger_trade.realized_pl}
                old_dc = pd.to_datetime(argus_row.get('Date_closed'), errors='coerce')
                tiger_d = tiger_trade.trade_date
                if pd.notna(old_dc) and tiger_d and old_dc.date() != tiger_d:
                    fields_changed['Date_closed'] = {'before': str(old_dc.date()), 'after': str(tiger_d)}
            else:
                # Open trade: update OptPremium (or Price for stock) + Date_open + Fee
                if tiger_trade.asset_class == 'Option':
                    old_op = pd.to_numeric(argus_row.get('OptPremium', 0), errors='coerce') or 0
                    if abs(old_op - tiger_trade.trade_price) > 0.001:
                        fields_changed['OptPremium'] = {'before': float(old_op), 'after': tiger_trade.trade_price}
                else:
                    old_pr = pd.to_numeric(argus_row.get('Price_of_current_underlying_(USD)', 0), errors='coerce') or 0
                    if abs(old_pr - tiger_trade.trade_price) > 0.001:
                        fields_changed['Price_of_current_underlying_(USD)'] = {'before': float(old_pr), 'after': tiger_trade.trade_price}
                old_do = pd.to_datetime(argus_row.get('Date_open'), errors='coerce')
                tiger_d = tiger_trade.trade_date
                if pd.notna(old_do) and tiger_d and old_do.date() != tiger_d:
                    fields_changed['Date_open'] = {'before': str(old_do.date()), 'after': str(tiger_d)}

            # Always backfill Fee + Tiger_Row_Hash (additive, won't break anything)
            old_fee = pd.to_numeric(argus_row.get('Fee', 0), errors='coerce') or 0
            if abs(old_fee - tiger_trade.fee_total) > 0.001:
                fields_changed['Fee'] = {'before': float(old_fee), 'after': tiger_trade.fee_total}
                fees_backfilled += tiger_trade.fee_total

            old_hash = str(argus_row.get('Tiger_Row_Hash', '')).strip()
            if old_hash != tiger_trade.row_hash:
                fields_changed['Tiger_Row_Hash'] = {'before': old_hash, 'after': tiger_trade.row_hash}

            if fields_changed:
                changes.append({
                    'type': 'enrich',
                    'trade_id': str(argus_row['TradeID']),
                    'tiger_row_hash': tiger_trade.row_hash,
                    'tiger_source_file': tiger_trade.source_file,
                    'tiger_source_row': tiger_trade.source_row,
                    'fields_changed': fields_changed,
                })
                enrich_count += 1
        else:
            # INSERT — but only if it's an OPEN trade (closes need an existing open to match)
            # If a Close has no match, log as orphan_tiger (Tiger has it, ARGUS doesn't)
            new_tid = gen_trade_id()
            new_row = tiger_trade_to_argus_row(tiger_trade, new_tid)
            changes.append({
                'type': 'insert',
                'trade_id': new_tid,
                'tiger_row_hash': tiger_trade.row_hash,
                'tiger_source_file': tiger_trade.source_file,
                'tiger_source_row': tiger_trade.source_row,
                'details': new_row,
            })
            insert_count += 1
            fees_backfilled += tiger_trade.fee_total

    # ── Step 2: Process Exercises and Expirations ──
    exercise_count = 0
    expiration_count = 0
    for tiger_ex in tiger_stmt.exercises:
        # Try to match the closing leg to an existing open ARGUS option
        # Build a synthetic match-able trade
        from types import SimpleNamespace
        synthetic = SimpleNamespace(
            asset_class='Option',
            ticker=tiger_ex.ticker,
            expiry=tiger_ex.expiry,
            right=tiger_ex.right,
            strike=tiger_ex.strike,
            quantity=tiger_ex.quantity,
            activity_type='Close',
            trade_date=tiger_ex.event_date,
            trade_price=0,
            row_hash=tiger_ex.row_hash,
            source_file=tiger_ex.source_file,
            source_row=tiger_ex.source_row,
        )
        match_idx = _find_match(synthetic, df_argus_current, used_argus_indices)

        if match_idx is not None:
            # ENRICH the existing CC/CSP to be Closed with proper signature
            argus_row = df_argus_current.loc[match_idx]
            used_argus_indices.add(match_idx)
            fields_changed = {}
            is_call = tiger_ex.right == 'CALL'
            is_expire = tiger_ex.transaction_type in ('Option Expired Worthless', 'Option Expire', 'Option Expiration')
            label = 'Expired Worthless' if is_expire else ('CC Called' if is_call else 'CSP Assigned')

            old_status = str(argus_row.get('Status', '')).strip()
            if old_status != 'Closed':
                fields_changed['Status'] = {'before': old_status, 'after': 'Closed'}

            old_cp = pd.to_numeric(argus_row.get('Close_Price', 0), errors='coerce') or 0
            if abs(old_cp - 0) > 0.001:
                fields_changed['Close_Price'] = {'before': float(old_cp), 'after': 0}

            if tiger_ex.realized_pl is not None:
                old_pl = pd.to_numeric(argus_row.get('Actual_Profit_(USD)', 0), errors='coerce') or 0
                if abs(old_pl - tiger_ex.realized_pl) > 0.01:
                    fields_changed['Actual_Profit_(USD)'] = {'before': float(old_pl), 'after': tiger_ex.realized_pl}

            old_dc = pd.to_datetime(argus_row.get('Date_closed'), errors='coerce')
            if tiger_ex.event_date and (pd.isna(old_dc) or old_dc.date() != tiger_ex.event_date):
                fields_changed['Date_closed'] = {
                    'before': str(old_dc.date()) if pd.notna(old_dc) else '',
                    'after': str(tiger_ex.event_date),
                }

            old_remarks = str(argus_row.get('Remarks', '')).strip()
            new_remarks = f'{label} (Tiger)'
            if label.lower() not in old_remarks.lower():
                fields_changed['Remarks'] = {'before': old_remarks, 'after': new_remarks}

            if fields_changed:
                changes.append({
                    'type': 'enrich',
                    'subtype': tiger_ex.transaction_type,
                    'trade_id': str(argus_row['TradeID']),
                    'tiger_row_hash': tiger_ex.row_hash,
                    'tiger_source_file': tiger_ex.source_file,
                    'tiger_source_row': tiger_ex.source_row,
                    'fields_changed': fields_changed,
                })
        else:
            # INSERT — exercise/expiration without ARGUS counterpart
            close_tid = gen_trade_id()
            stock_tid = gen_trade_id() if tiger_ex.transaction_type == 'Option Exercise' else None
            new_rows = tiger_exercise_to_argus_rows(tiger_ex, close_tid, stock_tid)
            for r in new_rows:
                changes.append({
                    'type': 'insert',
                    'subtype': tiger_ex.transaction_type,
                    'trade_id': r['TradeID'],
                    'tiger_row_hash': tiger_ex.row_hash + ('_stk' if r.get('TradeType') == 'STOCK' else ''),
                    'tiger_source_file': tiger_ex.source_file,
                    'tiger_source_row': tiger_ex.source_row,
                    'details': r,
                })
                insert_count += 1

        if tiger_ex.transaction_type in ('Option Expired Worthless', 'Option Expire', 'Option Expiration'):
            expiration_count += 1
        elif tiger_ex.transaction_type == 'Option Exercise':
            exercise_count += 1

    # ── Step 3: Orphan detection — ARGUS rows with no Tiger match ──
    orphan_count = 0
    if not df_argus_current.empty:
        for idx, row in df_argus_current.iterrows():
            if idx in used_argus_indices:
                continue
            # Skip rows that pre-date our Tiger coverage — they're not orphans
            argus_date = pd.to_datetime(row.get('Date_open'), errors='coerce')
            if pd.notna(argus_date) and tiger_stmt.period_start:
                if argus_date.date() < tiger_stmt.period_start:
                    continue
            # Skip rows that post-date our Tiger coverage
            if pd.notna(argus_date) and tiger_stmt.period_end:
                if argus_date.date() > tiger_stmt.period_end:
                    continue

            changes.append({
                'type': 'orphan',
                'trade_id': str(row['TradeID']),
                'reason': f"No Tiger match within ±{DATE_TOLERANCE_DAYS}d for {row.get('Ticker')} {row.get('TradeType')} strike=${row.get('Option_Strike_Price_(USD)')} qty={row.get('Quantity')} date={row.get('Date_open')}",
                'argus_record': {k: (v.isoformat() if hasattr(v, 'isoformat') else str(v)) for k, v in row.to_dict().items()},
            })
            orphan_count += 1

    # ── Step 4: Roll detection ──
    rolls = detect_rolls(tiger_stmt.trades)
    rolls_paired_count = len(rolls)
    for close_t, open_t in rolls:
        # Find the change records for both
        close_change = next((c for c in changes if c.get('tiger_row_hash') == close_t.row_hash), None)
        open_change = next((c for c in changes if c.get('tiger_row_hash') == open_t.row_hash), None)
        if close_change and open_change:
            close_tid = close_change.get('trade_id')
            open_tid = open_change.get('trade_id')
            changes.append({
                'type': 'roll_paired',
                'old_trade_id': close_tid,
                'new_trade_id': open_tid,
                'trade_date': str(close_t.trade_date) if close_t.trade_date else '',
                'ticker': close_t.ticker,
                'old_strike': close_t.strike,
                'new_strike': open_t.strike,
                'old_expiry': str(close_t.expiry) if close_t.expiry else '',
                'new_expiry': str(open_t.expiry) if open_t.expiry else '',
            })

    # ── Step 5: Cash adjustments summary ──
    cash_breakdown: dict = {}
    for c in tiger_stmt.cash_events:
        cash_breakdown[c.event_type] = cash_breakdown.get(c.event_type, 0) + c.amount

    # Net cash adjustment (excluding deposits/withdrawals which affect deposit base)
    net_cash_adj = sum(
        v for k, v in cash_breakdown.items()
        if k not in ('Deposit', 'Withdrawal', 'Stock_Transfer', 'Segment_Transfer')
    )

    # ── Final summary ──
    summary = {
        'tiger_trades_parsed': len(tiger_stmt.trades),
        'tiger_exercises_parsed': len(tiger_stmt.exercises),
        'argus_enriched': enrich_count,
        'argus_inserted': insert_count,
        'argus_orphaned': orphan_count,
        'rolls_auto_paired': rolls_paired_count,
        'exercises_processed': exercise_count,
        'expirations_processed': expiration_count,
        'cash_events_parsed': len(tiger_stmt.cash_events),
        'fees_backfilled_usd': round(fees_backfilled, 2),
        'cash_adjustment_breakdown': {k: round(v, 2) for k, v in cash_breakdown.items()},
        'net_cash_adjustment_usd': round(net_cash_adj, 2),
        'period_start': str(tiger_stmt.period_start) if tiger_stmt.period_start else None,
        'period_end': str(tiger_stmt.period_end) if tiger_stmt.period_end else None,
    }

    audit = {
        'run_id': datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
        'run_timestamp': datetime.now().isoformat(),
        'source_files': tiger_stmt.source_files,
        'summary': summary,
        'changes': changes,
        'tiger_account_overview': tiger_stmt.account_overview,
    }

    return {
        'summary': summary,
        'changes': changes,
        'audit': audit,
    }


# ─────────────────────────────────────────────────────────────────
# Audit file persistence
# ─────────────────────────────────────────────────────────────────
def save_audit(audit: dict, audit_dir: str = 'data/etl_audit') -> Path:
    """Save audit trail to a timestamped JSON file. Returns path."""
    p = Path(audit_dir)
    p.mkdir(parents=True, exist_ok=True)
    fname = f"etl_run_{audit['run_id']}.json"
    fpath = p / fname
    with open(fpath, 'w', encoding='utf-8') as f:
        json.dump(audit, f, indent=2, default=str)
    logger.info(f"Audit saved: {fpath}")
    return fpath


# ─────────────────────────────────────────────────────────────────
# CLI — for testing
# ─────────────────────────────────────────────────────────────────
def main():
    import argparse
    ap = argparse.ArgumentParser(description="Transform parsed Tiger statement → ARGUS rows.")
    ap.add_argument('parsed_json', help='Output of tiger_parser.py (JSON)')
    ap.add_argument('--argus-csv', help='Optional: current ARGUS Data Table CSV for matching')
    ap.add_argument('--output', '-o', default='data/etl_audit', help='Audit output directory')
    args = ap.parse_args()

    # Load parsed Tiger
    with open(args.parsed_json, 'r') as f:
        parsed = json.load(f)

    # Reconstruct lightweight TigerStatement-like object
    from types import SimpleNamespace
    def to_ns(d):
        return SimpleNamespace(**{
            **d,
            'trade_date': datetime.fromisoformat(d['trade_date']).date() if d.get('trade_date') else None,
            'expiry': datetime.fromisoformat(d['expiry']).date() if d.get('expiry') else None,
            'event_date': datetime.fromisoformat(d['event_date']).date() if d.get('event_date') else None,
        }) if isinstance(d, dict) else d

    stmt = SimpleNamespace(
        trades=[to_ns(t) for t in parsed.get('trades', [])],
        exercises=[to_ns(e) for e in parsed.get('exercises', [])],
        cash_events=[to_ns(c) for c in parsed.get('cash_events', [])],
        period_start=datetime.fromisoformat(parsed['period_start']).date() if parsed.get('period_start') else None,
        period_end=datetime.fromisoformat(parsed['period_end']).date() if parsed.get('period_end') else None,
        source_files=parsed.get('source_files', []),
        account_overview=parsed.get('account_overview', {}),
    )

    df_argus = pd.DataFrame()
    if args.argus_csv:
        df_argus = pd.read_csv(args.argus_csv)

    plan = transform_to_argus(stmt, df_argus)
    audit_path = save_audit(plan['audit'], args.output)

    print()
    print("ETL Plan Summary")
    print("=" * 50)
    s = plan['summary']
    print(f"  Period:                  {s['period_start']} -> {s['period_end']}")
    print(f"  Tiger trades parsed:     {s['tiger_trades_parsed']}")
    print(f"  Tiger exercises parsed:  {s['tiger_exercises_parsed']}")
    print(f"  ARGUS rows enriched:     {s['argus_enriched']}")
    print(f"  ARGUS rows inserted:     {s['argus_inserted']}")
    print(f"  ARGUS rows orphaned:     {s['argus_orphaned']}")
    print(f"  Rolls auto-paired:       {s['rolls_auto_paired']}")
    print(f"  Fees backfilled (USD):   ${s['fees_backfilled_usd']:,.2f}")
    print(f"  Net cash adjustment:     ${s['net_cash_adjustment_usd']:,.2f}")
    print(f"  Audit saved to:          {audit_path}")


if __name__ == '__main__':
    main()
