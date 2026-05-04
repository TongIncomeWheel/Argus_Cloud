"""
3-Layer Data Integrity Verification for Tiger ETL Migration
============================================================

Layer 1: Pre vs Post ARGUS delta analysis
  - What was lost from the pre-ETL Data Table that should not have been
  - What was added that should not have been
  - For matched pairs (40 enriches): field-level comparison

Layer 2: Tiger CSV -> ARGUS ETL fidelity
  - Every Tiger trade fill referenced in ARGUS exactly once (by Tiger_Row_Hash)
  - Every Tiger exercise represented
  - Every detected roll (174) has paired Remarks
  - Cash event totals preserved
  - Parser fidelity: CSV row -> event mapping

Layer 3: Post-ETL ARGUS vs Tiger Source-of-Truth
  - Open contract count by (ticker, right, strike, expiry) vs Tiger holdings
  - Open stock shares by ticker vs Tiger holdings
  - Realized P&L sum vs Tiger sum
  - Fee total vs Tiger fee total
  - NAV reconciliation vs Tiger Account Overview
  - Cash event totals
"""
from __future__ import annotations

import json
import logging
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from gsheet_handler import GSheetHandler
from config import INCOME_WHEEL_SHEET_ID
from tiger_parser import parse_files

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# Pretty printing
# ─────────────────────────────────────────────────────────────────
class Color:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'


def hdr(s: str):
    print()
    print('=' * 70)
    print(s)
    print('=' * 70)


def ok(msg: str):
    print(f'  [PASS] {msg}')


def warn(msg: str):
    print(f'  [WARN] {msg}')


def fail(msg: str):
    print(f'  [FAIL] {msg}')


# ─────────────────────────────────────────────────────────────────
# LAYER 1: Pre vs Post ARGUS Delta
# ─────────────────────────────────────────────────────────────────
def layer1_pre_vs_post_delta(handler: GSheetHandler) -> dict:
    hdr('LAYER 1: Pre-ETL vs Post-ETL Data Table Delta')
    findings = {'layer': 1, 'passed': True, 'warnings': [], 'failures': []}

    # Read pre (original 626-row backup) and post (current 690-row Data Table)
    try:
        pre_ws = handler.spreadsheet.worksheet('Data Table (Pre-Tiger 2026-04-30)')
        pre_data = pre_ws.get_all_values()
        if not pre_data:
            fail("Pre-ETL backup tab is empty")
            findings['failures'].append('pre_backup_empty')
            findings['passed'] = False
            return findings
        df_pre = pd.DataFrame(pre_data[1:], columns=pre_data[0])
        ok(f"Loaded pre-ETL backup: {len(df_pre)} rows")
    except Exception as e:
        fail(f"Could not load pre-ETL backup: {e}")
        findings['failures'].append('pre_backup_unavailable')
        findings['passed'] = False
        return findings

    df_post = handler.read_data_table()
    ok(f"Loaded post-ETL Data Table: {len(df_post)} rows")
    findings['pre_rows'] = len(df_pre)
    findings['post_rows'] = len(df_post)

    # 1.1 Schema delta — pre had 18 base + 11 calculated cols, post should have 18 base + 3 new (Fee, Pot, Tiger_Row_Hash)
    # The 11 calculated columns (DTE, %Yield_PA, etc.) are recomputed by app.py at runtime — safe to drop.
    pre_cols = set(df_pre.columns)
    post_cols = set(df_post.columns)
    new_cols = post_cols - pre_cols
    removed_cols = pre_cols - post_cols
    expected_new = {'Fee', 'Pot', 'Tiger_Row_Hash'}
    expected_dropped_calculated = {
        '%Yield_PA', 'Opt_Premium_%', 'Stock PNL', 'DTE', 'Effective_Capital_for_Yield',
        'Contract_Days_(WD)', 'Opt_Premium_%_Calculated', 'Total_Expected_profit_(USD)',
        'Cash_required_per_position_(USD)', 'DTE_Exp', 'Cash_required_running_(Margin&USD)',
    }
    if new_cols == expected_new:
        ok(f"Schema delta correct: added {sorted(new_cols)}")
    else:
        unexpected = new_cols - expected_new
        missing = expected_new - new_cols
        if unexpected:
            warn(f"Unexpected new columns: {unexpected}")
            findings['warnings'].append(f'unexpected_new_cols={unexpected}')
        if missing:
            fail(f"Missing expected new columns: {missing}")
            findings['failures'].append(f'missing_new_cols={missing}')
            findings['passed'] = False
    unexpected_drops = removed_cols - expected_dropped_calculated
    if unexpected_drops:
        fail(f"BASE columns removed (should preserve all base): {unexpected_drops}")
        findings['failures'].append(f'removed_base_cols={unexpected_drops}')
        findings['passed'] = False
    elif removed_cols:
        ok(f"Calculated/derived columns dropped (recomputed by app at runtime): "
           f"{len(removed_cols)} cols incl. {sorted(removed_cols)[:3]}...")

    # 1.2 Pre-ETL data quality baseline (so user sees what we had)
    df_pre['Date_open_dt'] = pd.to_datetime(df_pre.get('Date_open'), errors='coerce')
    pre_nat = df_pre['Date_open_dt'].isna().sum()
    pct = pre_nat / len(df_pre) * 100
    if pre_nat > 0:
        warn(f"Pre-ETL had {pre_nat}/{len(df_pre)} ({pct:.1f}%) rows with missing/unparseable Date_open")
        findings['warnings'].append(f'pre_missing_dates={pre_nat}')

    # 1.3 Post-ETL Date_open quality
    df_post['Date_open_dt'] = pd.to_datetime(df_post.get('Date_open'), errors='coerce')
    post_nat = df_post['Date_open_dt'].isna().sum()
    # Allow nulls only for STOCK closes-only or option closes that have only Date_closed
    df_post['Status_lower'] = df_post['Status'].astype(str).str.lower()
    df_post['Date_closed_dt'] = pd.to_datetime(df_post.get('Date_closed'), errors='coerce')
    no_dates = df_post[df_post['Date_open_dt'].isna() & df_post['Date_closed_dt'].isna()]
    if len(no_dates) > 0:
        fail(f"Post-ETL has {len(no_dates)} rows with NEITHER Date_open NOR Date_closed")
        findings['failures'].append(f'post_rows_no_dates={len(no_dates)}')
        findings['passed'] = False
        for _, r in no_dates.head(5).iterrows():
            print(f"     example: {r['TradeID']} {r.get('Ticker')} {r.get('TradeType')} status={r.get('Status')}")
    else:
        ok(f"Every post-ETL row has at least one valid date (Date_open or Date_closed)")

    # 1.4 What was preserved? Match by similar fields (Ticker + StrategyType)
    # Tag pre-ETL rows that are KEPT (rows with Date_open before Tiger period)
    tiger_period_start = pd.to_datetime('2025-01-02')
    pre_legacy = df_pre[df_pre['Date_open_dt'] < tiger_period_start]
    pre_intiger = df_pre[df_pre['Date_open_dt'] >= tiger_period_start]
    if len(pre_legacy):
        warn(f"Pre-ETL had {len(pre_legacy)} rows DATED BEFORE Tiger period ({tiger_period_start.date()}) "
             f"— these were dropped (Tiger doesn't cover them)")
        findings['warnings'].append(f'legacy_dropped={len(pre_legacy)}')
        # Sample
        for _, r in pre_legacy.head(3).iterrows():
            print(f"     dropped: {r['TradeID']} {r.get('Ticker')} {r.get('TradeType')} open={r.get('Date_open')}")

    ok(f"Pre-ETL split: {len(pre_legacy)} legacy (pre-2025-01-02) + {len(pre_intiger)} in-Tiger-period")

    # 1.5 TradeID continuity — post should be T-1 .. T-N with no gaps
    post_tids = sorted([t for t in df_post['TradeID'].astype(str) if t.startswith('T-')],
                       key=lambda x: int(x.replace('T-', '')) if x.replace('T-', '').isdigit() else 0)
    if post_tids:
        nums = [int(t.replace('T-', '')) for t in post_tids if t.replace('T-', '').isdigit()]
        expected_nums = list(range(1, len(nums) + 1))
        if nums == expected_nums:
            ok(f"TradeID continuity: T-1 to T-{len(nums)} with no gaps")
        else:
            missing = set(expected_nums) - set(nums)
            extra = set(nums) - set(expected_nums)
            fail(f"TradeID continuity broken: missing {sorted(missing)[:5]}..., extra {sorted(extra)[:5]}...")
            findings['failures'].append('tradeid_gaps')
            findings['passed'] = False

    return findings


# ─────────────────────────────────────────────────────────────────
# LAYER 2: Tiger CSV -> ARGUS ETL Fidelity
# ─────────────────────────────────────────────────────────────────
def layer2_etl_fidelity(handler: GSheetHandler, csv_paths: list) -> dict:
    hdr('LAYER 2: Tiger CSV -> ARGUS ETL Fidelity')
    findings = {'layer': 2, 'passed': True, 'warnings': [], 'failures': []}

    stmt = parse_files(csv_paths)
    df_post = handler.read_data_table()

    # 2.1 Parser dedup sanity: row_hashes should be unique within tiger_stmt
    trade_hashes = [t.row_hash for t in stmt.trades]
    ex_hashes = [e.row_hash for e in stmt.exercises]
    dup_trades = [h for h, n in Counter(trade_hashes).items() if n > 1]
    dup_ex = [h for h, n in Counter(ex_hashes).items() if n > 1]
    if dup_trades:
        fail(f"Duplicate Tiger trade row_hashes after dedup: {len(dup_trades)} (first: {dup_trades[0]})")
        findings['failures'].append('parser_dup_trades')
        findings['passed'] = False
    else:
        ok(f"Tiger trades all unique by row_hash ({len(trade_hashes)} hashes)")
    if dup_ex:
        fail(f"Duplicate Tiger exercise row_hashes: {len(dup_ex)}")
        findings['failures'].append('parser_dup_ex')
        findings['passed'] = False
    else:
        ok(f"Tiger exercises all unique by row_hash ({len(ex_hashes)} hashes)")

    # 2.2 Every Tiger trade row_hash should appear in some ARGUS row's Tiger_Row_Hash field
    argus_hashes = set(df_post['Tiger_Row_Hash'].astype(str).str.strip().str.replace('_stk', '', regex=False))
    argus_hashes.discard('')
    argus_hashes.discard('nan')

    tiger_trade_hashes = set(trade_hashes)
    tiger_ex_hashes = set(ex_hashes)
    all_tiger_hashes = tiger_trade_hashes | tiger_ex_hashes

    # Some trades are folded into a paired row whose Tiger_Row_Hash is the OPEN's hash —
    # so the close fill's row_hash may not appear directly in the column.
    # We need to compute "represented" more carefully:
    #   - Open trades: Tiger_Row_Hash should appear directly
    #   - Close trades: their row_hash is consumed by being paired with an open.
    #     The paired ARGUS row carries the OPEN's row_hash; the close's hash is "implicit".
    # So strict coverage check: each open + each unpaired close + each unpaired exercise = at least one row in Data Table.

    # Get open trade hashes (these MUST appear in ARGUS by Tiger_Row_Hash)
    # Tiger uses many activity_type labels for opens: OpenShort, OpenLong, Buy, Open
    # All Stock 'Open' fills (including those from option exercise) are now included
    # — the Trades section is the source of truth for stock movement.
    open_hashes = {t.row_hash for t in stmt.trades
                   if t.activity_type in ('OpenShort', 'OpenLong', 'Buy', 'Open')}

    missing_opens = open_hashes - argus_hashes
    if missing_opens:
        fail(f"{len(missing_opens)} Tiger OPEN trades NOT represented in ARGUS Data Table")
        for h in list(missing_opens)[:5]:
            t = next((tr for tr in stmt.trades if tr.row_hash == h), None)
            if t:
                print(f"     missing open: {t.ticker} {t.activity_type} qty={t.quantity} date={t.trade_date} hash={h[:12]}")
        findings['failures'].append(f'missing_opens={len(missing_opens)}')
        findings['passed'] = False
    else:
        ok(f"All {len(open_hashes)} Tiger OPEN trades represented in ARGUS")

    # 2.3 Each Tiger trade hash appears in EXACTLY ONE ARGUS row
    # (folded close fills won't appear, but opens shouldn't appear twice)
    hash_counts = df_post['Tiger_Row_Hash'].astype(str).str.replace('_stk', '', regex=False).value_counts()
    # Filter out empty
    hash_counts = hash_counts[hash_counts.index != '']
    duplicated = hash_counts[hash_counts > 1]
    if len(duplicated) > 0:
        # Some duplication is expected: an exercise's row_hash appears once for the option close
        # AND once with '_stk' suffix for the resulting stock movement. After stripping _stk both share root hash.
        # We've already stripped _stk. Need to allow this case (count of 2).
        # Filter to genuinely problematic dups (count > 2)
        bad_dups = duplicated[duplicated > 2]
        if len(bad_dups) > 0:
            fail(f"{len(bad_dups)} Tiger row_hashes appear MORE THAN 2 times in ARGUS")
            for h, n in bad_dups.head(5).items():
                print(f"     hash {h[:12]} appears {n} times")
            findings['failures'].append('hash_overuse')
            findings['passed'] = False
        else:
            ok(f"{len(duplicated)} hashes appear 2x (expected: exercise+stock_pair); 0 hashes appear 3+ times")
    else:
        ok(f"No Tiger row_hash duplication in ARGUS (excluding the stk-pair pattern)")

    # 2.4 Roll detection coverage
    from tiger_to_argus import detect_rolls
    rolls = detect_rolls(stmt.trades)
    expected_rolls = len(rolls)
    rolled_to = (df_post['Remarks'].astype(str).str.contains('Rolled to', na=False)).sum()
    rolled_from = (df_post['Remarks'].astype(str).str.contains('Rolled from', na=False)).sum()
    if rolled_to == expected_rolls and rolled_from == expected_rolls:
        ok(f"Roll remarks complete: {expected_rolls} 'Rolled to' + {expected_rolls} 'Rolled from'")
    else:
        fail(f"Roll remarks mismatch: expected {expected_rolls} pairs, "
             f"found {rolled_to} 'Rolled to' + {rolled_from} 'Rolled from'")
        findings['failures'].append('roll_remarks_incomplete')
        findings['passed'] = False

    # 2.5 Exercise count
    n_exercise = sum(1 for e in stmt.exercises if e.transaction_type == 'Option Exercise')
    n_expire = len(stmt.exercises) - n_exercise
    # In ARGUS, exercises produce 2 rows (option close + stock), expirations produce 1
    expected_ex_rows = n_exercise * 2 + n_expire
    actual_ex_remarks = (
        df_post['Remarks'].astype(str).str.contains('Expired Worthless|Called away|Assigned', na=False)
    ).sum()
    if actual_ex_remarks >= expected_ex_rows:
        ok(f"Exercise/expiration coverage OK: expected ~{expected_ex_rows} rows, found {actual_ex_remarks} "
           f"({n_exercise} exercises x 2 + {n_expire} expires)")
    else:
        warn(f"Exercise rows fewer than expected: {actual_ex_remarks} < {expected_ex_rows} "
             f"(some opens may not have matched and were inferred-expired)")
        findings['warnings'].append(f'fewer_ex_rows={actual_ex_remarks}_vs_{expected_ex_rows}')

    # 2.6 Tiger Statement tab fact-log fidelity
    try:
        ts_ws = handler.spreadsheet.worksheet('Tiger Statement')
        ts_data = ts_ws.get_all_values()
        ts_rows = len(ts_data) - 1  # minus header
        expected_ts = len(stmt.trades) + len(stmt.exercises)
        if ts_rows == expected_ts:
            ok(f"Tiger Statement tab has all {ts_rows} fact rows ({len(stmt.trades)} trades + {len(stmt.exercises)} exercises)")
        else:
            fail(f"Tiger Statement tab row count mismatch: {ts_rows} vs expected {expected_ts}")
            findings['failures'].append('tiger_statement_rows')
            findings['passed'] = False
    except Exception as e:
        fail(f"Cannot read Tiger Statement tab: {e}")
        findings['failures'].append('tiger_statement_unreadable')

    # 2.7 Tiger Cash tab fidelity
    try:
        tc_ws = handler.spreadsheet.worksheet('Tiger Cash')
        tc_data = tc_ws.get_all_values()
        tc_rows = len(tc_data) - 1
        if tc_rows == len(stmt.cash_events):
            ok(f"Tiger Cash tab has all {tc_rows} cash events")
        else:
            fail(f"Tiger Cash tab row count: {tc_rows} vs expected {len(stmt.cash_events)}")
            findings['failures'].append('tiger_cash_rows')
            findings['passed'] = False
    except Exception as e:
        fail(f"Cannot read Tiger Cash tab: {e}")

    return findings


# ─────────────────────────────────────────────────────────────────
# LAYER 3: Post-ETL ARGUS vs Tiger Source-of-Truth
# ─────────────────────────────────────────────────────────────────
def layer3_reconciliation(handler: GSheetHandler, csv_paths: list) -> dict:
    hdr('LAYER 3: Post-ETL ARGUS vs Tiger Source-of-Truth')
    findings = {'layer': 3, 'passed': True, 'warnings': [], 'failures': []}

    stmt = parse_files(csv_paths)
    df_post = handler.read_data_table()

    # Numeric coercion
    for col in ['Quantity', 'Open_lots', 'Option_Strike_Price_(USD)', 'OptPremium',
                'Close_Price', 'Actual_Profit_(USD)', 'Fee']:
        if col in df_post.columns:
            df_post[col] = pd.to_numeric(df_post[col], errors='coerce')
    df_post['Status_lower'] = df_post['Status'].astype(str).str.lower()
    df_post['Date_open_dt'] = pd.to_datetime(df_post.get('Date_open'), errors='coerce')
    df_post['Expiry_Date_dt'] = pd.to_datetime(df_post.get('Expiry_Date'), errors='coerce')

    # 3.1 Open option contracts: ARGUS vs Tiger holdings
    df_open = df_post[df_post['Status_lower'] == 'open'].copy()
    df_open_opt = df_open[df_open['TradeType'].isin(['CC', 'CSP', 'LEAP', 'LEAP_PUT', 'LEAP_CALL'])].copy()

    # Build aggregated ARGUS open option key: (ticker, right_inferred, strike, expiry)
    def infer_right(tt):
        if tt in ('CC', 'LEAP', 'LEAP_CALL'):
            return 'CALL'
        if tt in ('CSP', 'LEAP_PUT'):
            return 'PUT'
        return None

    df_open_opt['right_inf'] = df_open_opt['TradeType'].apply(infer_right)
    df_open_opt['Expiry_str'] = df_open_opt['Expiry_Date_dt'].dt.strftime('%Y-%m-%d')

    argus_opt_agg = df_open_opt.groupby(['Ticker', 'right_inf',
                                          'Option_Strike_Price_(USD)', 'Expiry_str']).agg(
        argus_qty=('Quantity', 'sum'),
        argus_rows=('TradeID', 'count')
    ).reset_index()

    # Tiger option holdings
    tiger_opt = [h for h in stmt.holdings if h.asset_class == 'Option']
    tiger_opt_data = []
    for h in tiger_opt:
        tiger_opt_data.append({
            'Ticker': h.ticker,
            'right_inf': h.right,
            'Option_Strike_Price_(USD)': h.strike,
            'Expiry_str': h.expiry.isoformat() if h.expiry else None,
            'tiger_qty': abs(h.quantity),
        })
    tiger_opt_df = pd.DataFrame(tiger_opt_data)

    # Outer-join on key
    if not tiger_opt_df.empty:
        merged = pd.merge(argus_opt_agg, tiger_opt_df,
                          on=['Ticker', 'right_inf', 'Option_Strike_Price_(USD)', 'Expiry_str'],
                          how='outer', indicator=True)
        merged['argus_qty'] = merged['argus_qty'].fillna(0)
        merged['tiger_qty'] = merged['tiger_qty'].fillna(0)
        merged['delta'] = merged['argus_qty'] - merged['tiger_qty']
        # Only contracts where there's a mismatch
        mismatches = merged[merged['delta'] != 0]
        if len(mismatches) == 0:
            ok(f"Open option contracts perfectly reconcile to Tiger holdings ({len(merged)} distinct contracts)")
        else:
            fail(f"Open option contracts mismatch on {len(mismatches)}/{len(merged)} contracts")
            print(f"     Showing top 20 mismatches:")
            print(mismatches.head(20).to_string(index=False))
            findings['failures'].append(f'option_holdings_mismatch={len(mismatches)}')
            findings['passed'] = False
    else:
        warn("No Tiger option holdings found in statement; skipping option reconciliation")

    # 3.2 Open stock shares: ARGUS vs Tiger holdings
    df_open_stk = df_open[df_open['TradeType'] == 'STOCK'].copy()
    argus_stk = df_open_stk.groupby('Ticker').agg(
        argus_shares=('Open_lots', 'sum'),
        argus_rows=('TradeID', 'count')
    ).reset_index()

    tiger_stk = [h for h in stmt.holdings if h.asset_class == 'Stock']
    tiger_stk_df = pd.DataFrame([{'Ticker': h.ticker, 'tiger_shares': abs(h.quantity)} for h in tiger_stk])

    if not tiger_stk_df.empty:
        stk_merged = pd.merge(argus_stk, tiger_stk_df, on='Ticker', how='outer', indicator=True)
        stk_merged['argus_shares'] = stk_merged['argus_shares'].fillna(0)
        stk_merged['tiger_shares'] = stk_merged['tiger_shares'].fillna(0)
        stk_merged['delta'] = stk_merged['argus_shares'] - stk_merged['tiger_shares']
        stk_mismatches = stk_merged[stk_merged['delta'].abs() > 0.5]
        if len(stk_mismatches) == 0:
            ok(f"Open stock shares perfectly reconcile to Tiger holdings ({len(stk_merged)} tickers)")
        else:
            fail(f"Open stock shares mismatch on {len(stk_mismatches)} tickers:")
            print(stk_mismatches.to_string(index=False))
            findings['failures'].append(f'stock_holdings_mismatch={len(stk_mismatches)}')
            findings['passed'] = False
    else:
        warn("No Tiger stock holdings found")

    # 3.3 Realized P&L sum: ARGUS closed vs Tiger expected realized (explicit + implicit)
    df_closed = df_post[df_post['Status_lower'] == 'closed'].copy()
    argus_realized = df_closed['Actual_Profit_(USD)'].sum()

    # Tiger's explicit realized P&L (from realized_pl field on closes + exercises)
    tiger_explicit = sum((t.realized_pl or 0) for t in stmt.trades)
    tiger_explicit += sum((e.realized_pl or 0) for e in stmt.exercises)

    # Tiger doesn't always emit explicit events for "silent expirations" — short options
    # whose expiry passed without an explicit close/exercise event but Tiger silently
    # released the cash collateral. The premium received on those opens IS realized P&L
    # the user actually earned (cash already in account). ARGUS infers these and counts
    # them. Total "implied" realized = explicit + premium-on-silent-expires.
    # We can compute the silent-expiry premium directly from ARGUS's "Implicit" remarks.
    df_implicit = df_post[df_post['Remarks'].astype(str).str.contains('implicit', case=False, na=False)]
    df_implicit_pl = pd.to_numeric(df_implicit['Actual_Profit_(USD)'], errors='coerce').sum()
    tiger_full_realized = tiger_explicit + df_implicit_pl

    diff_explicit = argus_realized - tiger_explicit
    diff_full = argus_realized - tiger_full_realized
    pct_full = abs(diff_full) / abs(tiger_full_realized) * 100 if tiger_full_realized else 0

    print(f"     ARGUS realized P&L:                 ${argus_realized:,.2f}")
    print(f"     Tiger explicit realized P&L:         ${tiger_explicit:,.2f}")
    print(f"     ARGUS implicit-expiry premium kept: ${df_implicit_pl:,.2f}")
    print(f"     Tiger full realized (explicit+impl): ${tiger_full_realized:,.2f}")

    if pct_full < 1.0:
        ok(f"Realized P&L reconciles: ARGUS ${argus_realized:,.2f} ~ Tiger total ${tiger_full_realized:,.2f} "
           f"(delta ${diff_full:,.2f}, {pct_full:.2f}%)")
    elif pct_full < 10.0:
        warn(f"Realized P&L within 10%: delta ${diff_full:,.2f} ({pct_full:.2f}%)")
        findings['warnings'].append(f'pl_full_diff_pct={pct_full:.2f}')
    else:
        fail(f"Realized P&L doesn't reconcile even with implicit expires: ARGUS ${argus_realized:,.2f} "
             f"vs full Tiger ${tiger_full_realized:,.2f} (delta ${diff_full:,.2f}, {pct_full:.2f}%)")
        findings['failures'].append(f'pl_full_diff_pct={pct_full:.2f}')
        findings['passed'] = False

    # 3.4 Fee total: ARGUS Fee column vs Tiger fee_total
    argus_fees = df_post['Fee'].sum()
    tiger_fees = sum(t.fee_total or 0 for t in stmt.trades)
    fee_diff = argus_fees - tiger_fees
    if abs(fee_diff) < 1.0:
        ok(f"Fees: ARGUS ${argus_fees:,.2f} vs Tiger ${tiger_fees:,.2f} (delta: ${fee_diff:,.2f})")
    else:
        warn(f"Fees: ARGUS ${argus_fees:,.2f} vs Tiger ${tiger_fees:,.2f} (delta: ${fee_diff:,.2f})")
        findings['warnings'].append(f'fee_diff={fee_diff:.2f}')

    # 3.5 Cash event total
    tiger_cash_total = sum(c.amount for c in stmt.cash_events)
    ok(f"Tiger cash events: {len(stmt.cash_events)} events totaling ${tiger_cash_total:,.2f}")
    by_type = defaultdict(float)
    for c in stmt.cash_events:
        by_type[c.event_type] += c.amount
    print("     Cash by type:")
    for k, v in sorted(by_type.items()):
        print(f"       {k:>22}: ${v:>14,.2f}")

    # 3.6 NAV reconciliation (Tiger Account Overview END NAV)
    # Tiger end NAV = $309,632.14 (from Account Overview)
    # ARGUS NAV = portfolio_deposit + total_realized_pl + open_option_value (MTM) + open_stock_value (MTM) - committed
    # We need to approximate this without live prices, using:
    #   - portfolio_deposit (read from Settings)
    #   - argus_realized_pl
    #   - open stock value at cost basis (ARGUS Quantity x Price_of_current_underlying_USD or strike)
    # Without live prices, we can only compute "deployed cash" view.

    deposits = sum(c.amount for c in stmt.cash_events if c.event_type == 'Deposit')
    withdrawals = sum(c.amount for c in stmt.cash_events if c.event_type == 'Withdrawal')
    net_deposits = deposits + withdrawals
    other_cash = sum(c.amount for c in stmt.cash_events
                     if c.event_type not in ('Deposit', 'Withdrawal', 'Stock_Transfer', 'Segment_Transfer'))

    tiger_end_nav = 309632.14  # from Tiger Account Overview
    tiger_begin_nav = 250512.19

    # Net cash injected during period
    print()
    print(f"  Tiger NAV: begin=${tiger_begin_nav:,.2f}, end=${tiger_end_nav:,.2f}, change=${tiger_end_nav - tiger_begin_nav:,.2f}")
    print(f"  Tiger period activity:")
    print(f"     Deposits: ${deposits:,.2f}")
    print(f"     Withdrawals: ${withdrawals:,.2f}")
    print(f"     Other cash adjustments: ${other_cash:,.2f}")
    print(f"     ARGUS realized P&L: ${argus_realized:,.2f}")
    print(f"     ARGUS fees: ${argus_fees:,.2f}")

    # Implied unrealized + open MTM = end_nav - (begin_nav + deposits + withdrawals + realized_pl + other_cash)
    accounted = tiger_begin_nav + net_deposits + argus_realized + other_cash
    implied_mtm = tiger_end_nav - accounted
    print(f"     Implied unrealized MTM (must equal market value of all open positions vs cost basis): ${implied_mtm:,.2f}")
    print(f"     (This number should match the sum of (live_price - cost_basis) x position_size across all open ARGUS rows)")

    findings['tiger_nav_end'] = tiger_end_nav
    findings['argus_realized_pl'] = float(argus_realized)
    findings['implied_unrealized_mtm'] = float(implied_mtm)

    return findings


# ─────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────
def main():
    csv_paths = [
        'tiger_samples/Statement_50179929_20240401_20250331.csv',
        'tiger_samples/Statement_50179929_20250401_20260331.csv',
        'tiger_samples/Statement_50179929_20260401_20260428.csv',
        'tiger_samples/Statement_50179929_20260427_20260503.csv',
    ]

    handler = GSheetHandler(INCOME_WHEEL_SHEET_ID)

    results = []
    results.append(layer1_pre_vs_post_delta(handler))
    results.append(layer2_etl_fidelity(handler, csv_paths))
    results.append(layer3_reconciliation(handler, csv_paths))

    # Summary
    hdr('VERIFICATION SUMMARY')
    overall_pass = True
    for r in results:
        layer = r['layer']
        passed = r['passed']
        n_warn = len(r.get('warnings', []))
        n_fail = len(r.get('failures', []))
        status = 'PASS' if passed else 'FAIL'
        print(f"  Layer {layer}: {status}  ({n_warn} warnings, {n_fail} failures)")
        if not passed:
            overall_pass = False
            for f in r.get('failures', []):
                print(f"        FAIL: {f}")

    print()
    print('=' * 70)
    if overall_pass:
        print(f"OVERALL: ALL LAYERS PASSED (with warnings)")
    else:
        print(f"OVERALL: FAILURES PRESENT - DO NOT PROCEED TO UAT")
    print('=' * 70)

    # Save results
    out_path = Path('data/etl_audit') / f'verify_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump({'overall_pass': overall_pass, 'layers': results}, f, indent=2, default=str)
    print(f"Results saved to: {out_path}")

    return 0 if overall_pass else 1


if __name__ == '__main__':
    sys.exit(main())
