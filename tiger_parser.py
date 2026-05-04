"""
Tiger Brokers Activity Statement parser.

Pure-Python module: parses Tiger's multi-section CSV exports into structured
Python objects. No Streamlit, no gspread, no file writes (other than --output JSON).

Usage:
    from tiger_parser import parse_file, parse_files
    stmt = parse_files(['Statement_2024.csv', 'Statement_2025.csv'])

CLI:
    python tiger_parser.py Statement_2024.csv Statement_2025.csv --output parsed.json
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import re
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# Fee column names — we sum these into a single fee_total per trade
# ─────────────────────────────────────────────────────────────────
FEE_COLUMN_NAMES = [
    'Transaction Fee', 'Other Tripartite fees', 'Settlement Fee', 'SEC Fee',
    'Option Regulatory Fee', 'Stamp Duty', 'Transaction Levy', 'Clearing Fee',
    'Trading Activity Fee', 'Exchange Fee', 'Future Regulatory Fee',
    'Commission', 'Platform Fee', 'Option Settlement Fee', 'Subscription Fee',
    'Redemption Fee', 'Switching Fee', 'PH Stock Transaction Tax',
    'Tax Service Fee', 'AFRC Transaction Levy', 'Trading Tariff',
    'Brokerage fee', 'Handing Fee', 'Securities Management Fee',
    'Transfer Fees (CSDC)', 'Transfer Fees (HKSCC)',
    'Stamp Duty On Stock Borrowing', 'Consolidated Audit Trail Fee',
    'Processing Fee', 'CM DA SI Fee', 'DVP SI Fee', 'IPO Transaction Fee',
    'IPO Process Fee', 'Ipo Settle Fee', 'IPO Channel Fee', 'GST',
]

# ─────────────────────────────────────────────────────────────────
# Regex for OCC option symbol parsing
#   "Coinbase Global, Inc. (COIN 20260424 PUT 170.0)"
# ─────────────────────────────────────────────────────────────────
OPTION_SYMBOL_RE = re.compile(
    r'\(([A-Z][A-Z0-9.]*)\s+(\d{8})\s+(PUT|CALL)\s+([\d.]+)\)'
)
STOCK_SYMBOL_RE = re.compile(r'\(([^)]+)\)\s*$')


# ─────────────────────────────────────────────────────────────────
# Dataclasses — output schema
# ─────────────────────────────────────────────────────────────────
@dataclass
class TigerTrade:
    source_file: str
    source_row: int
    trade_date: Optional[date]
    settle_date: Optional[date]
    asset_class: str                  # "Option" | "Stock" | "Fund"
    symbol_raw: str
    ticker: str
    expiry: Optional[date]
    right: Optional[str]              # "PUT" | "CALL" | None for stock
    strike: Optional[float]
    activity_type: str                # "OpenShort" | "Close" | "OpenLong" | etc.
    quantity: float                   # Signed (-1 = sold 1 contract)
    trade_price: float
    amount: float                     # Signed cash flow
    fee_total: float                  # Sum of all fee columns, abs (positive)
    realized_pl: Optional[float]      # Tiger's pre-computed realized P&L (closes only)
    notes: str
    currency: str
    market: str                       # "US" | "SG" | "HK"
    row_hash: str                     # 12-char SHA256 truncate, deterministic ID


@dataclass
class TigerExercise:
    source_file: str
    source_row: int
    event_date: Optional[date]
    asset_class: str
    symbol_raw: str
    ticker: str
    expiry: Optional[date]
    right: Optional[str]
    strike: Optional[float]
    quantity: float
    transaction_type: str             # "Option Exercise" | "Option Expired Worthless"
    realized_pl: float
    cash_settlement: float
    currency: str
    row_hash: str


@dataclass
class TigerHolding:
    source_file: str
    asset_class: str
    symbol_raw: str
    ticker: str
    expiry: Optional[date]
    right: Optional[str]
    strike: Optional[float]
    quantity: float
    multiplier: float
    cost_price: float
    close_price: float
    value: float
    unrealized_pl: float
    currency: str
    as_of_date: Optional[date]


@dataclass
class TigerCashEvent:
    source_file: str
    event_date: Optional[date]
    event_type: str                   # "Dividend" | "Interest" | "Allowance" | "Deposit" | "Withdrawal" | "Securities_Lending"
    description: str
    amount: float                     # Signed
    currency: str


@dataclass
class TigerStatement:
    source_files: list = field(default_factory=list)
    period_start: Optional[date] = None
    period_end: Optional[date] = None
    trades: list = field(default_factory=list)
    exercises: list = field(default_factory=list)
    holdings: list = field(default_factory=list)
    cash_events: list = field(default_factory=list)
    account_overview: dict = field(default_factory=dict)
    fx_rates: dict = field(default_factory=dict)

    def summary(self) -> dict:
        return {
            'source_files': self.source_files,
            'period_start': str(self.period_start) if self.period_start else None,
            'period_end': str(self.period_end) if self.period_end else None,
            'trades_count': len(self.trades),
            'exercises_count': len(self.exercises),
            'holdings_count': len(self.holdings),
            'cash_events_count': len(self.cash_events),
            'cash_events_by_type': self._cash_event_breakdown(),
        }

    def _cash_event_breakdown(self) -> dict:
        out = {}
        for e in self.cash_events:
            out[e.event_type] = out.get(e.event_type, 0) + 1
        return out


# ─────────────────────────────────────────────────────────────────
# Helper functions
# ─────────────────────────────────────────────────────────────────
def parse_amount(raw) -> float:
    """Parse '1,234.56' or '-580.00' or '' → float (0.0 if empty)."""
    if raw is None:
        return 0.0
    s = str(raw).strip().replace(',', '').replace('"', '')
    if not s or s in ('-', '--'):
        return 0.0
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def parse_int_qty(raw) -> float:
    """Parse quantity: '5.0' or '-1' or '500000' → float."""
    return parse_amount(raw)


def parse_trade_time(raw: str) -> Optional[date]:
    """Parse '2026-03-23\\n12:32:58, US/Eastern' or '2026-03-23, 12:32, GMT+8' → date."""
    if not raw:
        return None
    s = str(raw).strip()
    # Take first date-like token
    m = re.search(r'(\d{4}-\d{2}-\d{2})', s)
    if m:
        try:
            return datetime.strptime(m.group(1), '%Y-%m-%d').date()
        except ValueError:
            return None
    return None


def parse_simple_date(raw: str) -> Optional[date]:
    """Parse '2025-07-07' or '2025-04-01~2026-03-31' → date (first one)."""
    if not raw:
        return None
    m = re.search(r'(\d{4}-\d{2}-\d{2})', str(raw))
    if m:
        try:
            return datetime.strptime(m.group(1), '%Y-%m-%d').date()
        except ValueError:
            return None
    return None


def parse_option_symbol(symbol_raw: str) -> Optional[tuple]:
    """Parse 'Coinbase Global, Inc. (COIN 20260424 PUT 170.0)' → (ticker, expiry, right, strike)."""
    if not symbol_raw:
        return None
    m = OPTION_SYMBOL_RE.search(symbol_raw)
    if not m:
        return None
    ticker = m.group(1)
    try:
        expiry = datetime.strptime(m.group(2), '%Y%m%d').date()
    except ValueError:
        return None
    right = m.group(3)
    try:
        strike = float(m.group(4))
    except ValueError:
        return None
    return (ticker, expiry, right, strike)


def parse_stock_symbol(symbol_raw: str) -> Optional[str]:
    """Parse 'NVIDIA (NVDA)' or 'MM2 Asia (1B0.SI)' → 'NVDA' / '1B0.SI'."""
    if not symbol_raw:
        return None
    m = STOCK_SYMBOL_RE.search(symbol_raw)
    return m.group(1).strip() if m else None


def compute_row_hash(*parts) -> str:
    """SHA256 of concatenated parts → first 12 hex chars."""
    s = '|'.join(str(p) if p is not None else '' for p in parts)
    return hashlib.sha256(s.encode('utf-8')).hexdigest()[:12]


def col_idx(headers: list, name: str) -> Optional[int]:
    """Return column index for a header name, or None if not present."""
    try:
        return headers.index(name)
    except ValueError:
        return None


def safe_get(row: list, idx: Optional[int], default='') -> str:
    """Safely fetch row[idx] with bounds and None guard."""
    if idx is None or idx >= len(row):
        return default
    return row[idx]


# ─────────────────────────────────────────────────────────────────
# Section row parsers (private)
# ─────────────────────────────────────────────────────────────────
def _parse_trade_row(
    row: list, headers: list, source_file: str, source_row: int
) -> Optional[TigerTrade]:
    """Parse one DATA row from the Trades section.
    Returns None for duplicate rows (blank symbol)."""

    sym_idx = col_idx(headers, 'Symbol')
    if sym_idx is None:
        return None
    symbol_raw = safe_get(row, sym_idx).strip()

    # Dedup rule: blank symbol = duplicate of previous DATA row
    if not symbol_raw:
        return None

    asset_class = safe_get(row, 1).strip()  # column 1 holds "Option" / "Stock" / "Fund"
    activity = safe_get(row, col_idx(headers, 'Activity Type')).strip()

    # Parse symbol
    ticker = ''
    expiry = None
    right = None
    strike = None

    if asset_class == 'Option':
        opt = parse_option_symbol(symbol_raw)
        if opt:
            ticker, expiry, right, strike = opt
        else:
            # Could not parse as option — log and skip
            logger.warning(f"Row {source_row}: could not parse option symbol: {symbol_raw[:60]}")
            ticker = parse_stock_symbol(symbol_raw) or ''
    else:
        # Stock or Fund
        ticker = parse_stock_symbol(symbol_raw) or ''

    qty = parse_amount(safe_get(row, col_idx(headers, 'Quantity')))
    price = parse_amount(safe_get(row, col_idx(headers, 'Trade Price')))
    amount = parse_amount(safe_get(row, col_idx(headers, 'Amount')))

    # Sum all fees
    fee_total = 0.0
    for fee_col in FEE_COLUMN_NAMES:
        idx = col_idx(headers, fee_col)
        if idx is not None:
            fee_total += parse_amount(safe_get(row, idx))
    fee_total = abs(fee_total)

    realized_raw = safe_get(row, col_idx(headers, 'Realized P/L'))
    realized_pl = parse_amount(realized_raw) if realized_raw and realized_raw.strip() else None

    notes = safe_get(row, col_idx(headers, 'Notes')).strip()

    trade_date = parse_trade_time(safe_get(row, col_idx(headers, 'Trade Time')))
    settle_date = parse_simple_date(safe_get(row, col_idx(headers, 'Settle Date')))

    currency = safe_get(row, col_idx(headers, 'Currency')).strip() or 'USD'
    market = safe_get(row, col_idx(headers, 'Market')).strip()

    # CONTENT hash — based on the trade's economic identity only (no source_file).
    # An occurrence-counter suffix is added later in parse_file() to disambiguate
    # legitimately-distinct fills with identical content (multi-fill orders).
    # This way:
    #   - Same content in two different CSVs (overlapping date range) → same row_hash → dedup ✓
    #   - 5+3+2 multi-fill order in one CSV → distinct row_hashes via occurrence counter ✓
    row_hash = compute_row_hash(
        trade_date, ticker, expiry, right, strike, activity,
        round(qty, 4), round(price, 5), round(fee_total, 4)
    )

    return TigerTrade(
        source_file=source_file,
        source_row=source_row,
        trade_date=trade_date,
        settle_date=settle_date,
        asset_class=asset_class,
        symbol_raw=symbol_raw,
        ticker=ticker,
        expiry=expiry,
        right=right,
        strike=strike,
        activity_type=activity,
        quantity=qty,
        trade_price=price,
        amount=amount,
        fee_total=fee_total,
        realized_pl=realized_pl,
        notes=notes,
        currency=currency,
        market=market,
        row_hash=row_hash,
    )


def _parse_exercise_row(
    row: list, headers: list, source_file: str, source_row: int
) -> Optional[TigerExercise]:
    """Parse one DATA row from Exercise and Expiration."""
    sym_idx = col_idx(headers, 'Symbol')
    if sym_idx is None:
        return None
    symbol_raw = safe_get(row, sym_idx).strip()
    if not symbol_raw:
        return None

    asset_class = safe_get(row, 1).strip()
    ticker = ''
    expiry = None
    right = None
    strike = None
    if asset_class == 'Option':
        opt = parse_option_symbol(symbol_raw)
        if opt:
            ticker, expiry, right, strike = opt

    qty = parse_amount(safe_get(row, col_idx(headers, 'Quantity')))
    transaction_type = safe_get(row, col_idx(headers, 'Transaction Type')).strip()
    realized_pl = parse_amount(safe_get(row, col_idx(headers, 'Realized P/L')))
    cash_settlement = parse_amount(safe_get(row, col_idx(headers, 'Cash Settlement')))
    event_date = parse_trade_time(safe_get(row, col_idx(headers, 'Date/Time')))

    # Currency might be in column 2 ("Currency: USD" prefix) for this section
    currency = 'USD'
    for i in range(min(5, len(row))):
        cell = safe_get(row, i)
        if cell.startswith('Currency:'):
            currency = cell.replace('Currency:', '').strip()
            break

    # CONTENT hash — see _parse_trade_row for rationale. Occurrence-counter applied later.
    row_hash = compute_row_hash(
        event_date, ticker, expiry, right, strike, transaction_type,
        round(qty, 4), round(realized_pl, 4)
    )

    return TigerExercise(
        source_file=source_file,
        source_row=source_row,
        event_date=event_date,
        asset_class=asset_class,
        symbol_raw=symbol_raw,
        ticker=ticker,
        expiry=expiry,
        right=right,
        strike=strike,
        quantity=qty,
        transaction_type=transaction_type,
        realized_pl=realized_pl,
        cash_settlement=cash_settlement,
        currency=currency,
        row_hash=row_hash,
    )


def _parse_holding_row(
    row: list, headers: list, source_file: str, as_of_date: Optional[date]
) -> Optional[TigerHolding]:
    """Parse one DATA row from Holdings section."""
    sym_idx = col_idx(headers, 'Symbol')
    if sym_idx is None:
        return None
    symbol_raw = safe_get(row, sym_idx).strip()
    if not symbol_raw:
        return None

    asset_class = safe_get(row, 1).strip()
    ticker = ''
    expiry = None
    right = None
    strike = None
    if asset_class == 'Option':
        opt = parse_option_symbol(symbol_raw)
        if opt:
            ticker, expiry, right, strike = opt
    else:
        ticker = parse_stock_symbol(symbol_raw) or ''

    return TigerHolding(
        source_file=source_file,
        asset_class=asset_class,
        symbol_raw=symbol_raw,
        ticker=ticker,
        expiry=expiry,
        right=right,
        strike=strike,
        quantity=parse_amount(safe_get(row, col_idx(headers, 'Quantity'))),
        multiplier=parse_amount(safe_get(row, col_idx(headers, 'Multiplier'))),
        cost_price=parse_amount(safe_get(row, col_idx(headers, 'Cost Price'))),
        close_price=parse_amount(safe_get(row, col_idx(headers, 'Close Price'))),
        value=parse_amount(safe_get(row, col_idx(headers, 'Value'))),
        unrealized_pl=parse_amount(safe_get(row, col_idx(headers, 'Unrealized P/L'))),
        currency=safe_get(row, col_idx(headers, 'Currency')).strip() or 'USD',
        as_of_date=as_of_date,
    )


def _parse_cash_row(
    row: list, headers: list, source_file: str, section: str
) -> Optional[TigerCashEvent]:
    """Parse a cash event from Dividends, Interest, Allowance, Deposits & Withdrawals,
    Securities Lent, Transfer, Segment Transfer."""
    # Map section to event_type
    type_map = {
        'Dividends': 'Dividend',
        'Interest': 'Interest',
        'Allowance': 'Allowance',
        'Deposits & Withdrawals': 'Deposit',  # Refined below
        'Securities Lent': 'Securities_Lending',
        'Transfer': 'Stock_Transfer',
        'Segment Transfer': 'Segment_Transfer',
    }
    event_type = type_map.get(section, section)

    # Date column varies by section
    date_col_candidates = ['Date', 'Lending Date', 'Date/Time', 'Transfer Method']
    event_date = None
    for c in date_col_candidates:
        idx = col_idx(headers, c)
        if idx is not None:
            event_date = parse_simple_date(safe_get(row, idx))
            if event_date:
                break

    # Description / amount / currency
    desc = safe_get(row, col_idx(headers, 'Description')).strip()
    if not desc:
        # Some sections use "Symbol" or other columns
        for c in ['Symbol', 'Net Cash Value', 'Phase']:
            idx = col_idx(headers, c)
            if idx is not None:
                desc = safe_get(row, idx).strip()
                if desc:
                    break

    # Refine Deposit vs Withdrawal
    if section == 'Deposits & Withdrawals':
        if 'withdraw' in desc.lower():
            event_type = 'Withdrawal'
        else:
            event_type = 'Deposit'

    # Amount column varies
    amount = 0.0
    for c in ['Amount', 'Net Cash Value', 'Interest Paid', 'Cash Dividends', 'Market Value']:
        idx = col_idx(headers, c)
        if idx is not None:
            v = parse_amount(safe_get(row, idx))
            if v != 0:
                amount = v
                break

    currency = safe_get(row, col_idx(headers, 'Currency')).strip() or 'USD'

    return TigerCashEvent(
        source_file=source_file,
        event_date=event_date,
        event_type=event_type,
        description=desc,
        amount=amount,
        currency=currency,
    )


# ─────────────────────────────────────────────────────────────────
# Main parse function — single file
# ─────────────────────────────────────────────────────────────────
# Sections we care about (everything else ignored)
PARSED_SECTIONS = {
    'Trades', 'Exercise and Expiration', 'Holdings',
    'Dividends', 'Interest', 'Allowance', 'Deposits & Withdrawals',
    'Securities Lent', 'Transfer', 'Segment Transfer',
    'Account Overview', 'Base Currency Exchange Rate',
}


def parse_file(filepath, *, source_name: Optional[str] = None) -> TigerStatement:
    """Parse one Tiger Activity Statement CSV.

    Accepts EITHER:
      - a string/Path to a CSV on disk
      - a file-like object (e.g., Streamlit's UploadedFile, BytesIO, StringIO)
        — useful for the in-app Tiger Import page where the CSV never touches disk

    Args:
        filepath: path or file-like object
        source_name: override for source_file name (required when passing a
                     file-like without a `.name` attribute)

    Idempotency: row_hash is content-based with within-file occurrence counter
    so re-uploading a file produces identical hashes (no double-counting).
    """
    # Resolve source name + open a text reader for the CSV
    file_handle = None
    close_after = False
    if hasattr(filepath, 'read'):
        # File-like object (Streamlit UploadedFile, BytesIO, StringIO, etc.)
        if source_name is None:
            source_name = getattr(filepath, 'name', 'uploaded.csv')
        # If it returns bytes, decode; if str, use as-is
        raw = filepath.read()
        if isinstance(raw, bytes):
            # Strip UTF-8 BOM if present
            if raw.startswith(b'\xef\xbb\xbf'):
                raw = raw[3:]
            text = raw.decode('utf-8')
        else:
            text = raw
        # Reset cursor for any future readers
        try:
            filepath.seek(0)
        except Exception:
            pass
        import io as _io
        file_handle = _io.StringIO(text)
        close_after = True
    else:
        fp = Path(filepath)
        if not fp.exists():
            raise FileNotFoundError(f"Tiger CSV not found: {filepath}")
        source_name = source_name or fp.name
        file_handle = open(fp, 'r', encoding='utf-8-sig')
        close_after = True

    stmt = TigerStatement(source_files=[source_name])
    current_section = None
    current_headers: list = []
    holdings_as_of: Optional[date] = None

    try:
        reader = csv.reader(file_handle)
        for source_row, row in enumerate(reader, start=1):
            if not row or not row[0].strip():
                continue

            sec = row[0].strip()

            # Detect section change + capture headers
            # A header row has section_name in col[0] and column names from col[4] onward
            # We detect headers by checking if column 4 looks like a header word (e.g., "Symbol", "Date")
            # vs a "DATA" / "TOTAL" / "HEADER_DATA" marker
            row_marker = safe_get(row, 3).strip()

            if sec in PARSED_SECTIONS:
                if row_marker not in ('DATA', 'TOTAL', 'HEADER_DATA'):
                    # This is a header row — cache the column headers
                    if sec != current_section:
                        current_section = sec
                    # Headers start from col 4 in most sections
                    # For Account Overview / Cash Report, columns can differ — we capture all
                    current_headers = [c.strip() for c in row]
                    continue

                # Skip TOTAL rows
                if row_marker == 'TOTAL':
                    continue

                # Process DATA rows
                if row_marker == 'DATA':
                    if sec == 'Trades':
                        t = _parse_trade_row(row, current_headers, source_name, source_row)
                        if t:
                            stmt.trades.append(t)
                    elif sec == 'Exercise and Expiration':
                        e = _parse_exercise_row(row, current_headers, source_name, source_row)
                        if e:
                            stmt.exercises.append(e)
                    elif sec == 'Holdings':
                        h = _parse_holding_row(row, current_headers, source_name, holdings_as_of)
                        if h:
                            stmt.holdings.append(h)
                    elif sec in ('Dividends', 'Interest', 'Allowance',
                                 'Deposits & Withdrawals', 'Securities Lent',
                                 'Transfer', 'Segment Transfer'):
                        c = _parse_cash_row(row, current_headers, source_name, sec)
                        if c:
                            stmt.cash_events.append(c)
                    elif sec == 'Account Overview':
                        # Capture beginning/end period totals for sanity check
                        label = safe_get(row, 4).strip()
                        if label in ('Beginning Of The Period', 'End Of The Period'):
                            key = 'begin' if 'Beginning' in label else 'end'
                            stmt.account_overview[key] = {
                                'cash': parse_amount(safe_get(row, 5)),
                                'stock': parse_amount(safe_get(row, 6)),
                                'option': parse_amount(safe_get(row, 7)),
                                'fund': parse_amount(safe_get(row, 8)),
                                'future': parse_amount(safe_get(row, 9)),
                                'card_balance': parse_amount(safe_get(row, 10)),
                                'funds_in_transit': parse_amount(safe_get(row, 11)),
                                'interest_accruals': parse_amount(safe_get(row, 12)),
                                'dividend_accruals': parse_amount(safe_get(row, 13)),
                                'total': parse_amount(safe_get(row, 14)),
                            }
    finally:
        if close_after and file_handle is not None:
            try:
                file_handle.close()
            except Exception:
                pass

    # ── Occurrence-counter dedup suffix ──
    # Two LEGITIMATELY-distinct fills with identical content (e.g., a 16-contract
    # MARA $12.5 PUT order split into 2× 8-contract executions on the same day at
    # the same price) get distinguished by appending ":dup2", ":dup3", etc.
    # First occurrence keeps the bare content hash. Re-uploading the same CSV
    # produces the same suffixes (deterministic by parse order).
    _trade_seen: dict = {}
    for t in stmt.trades:
        n = _trade_seen.get(t.row_hash, 0) + 1
        _trade_seen[t.row_hash] = n
        if n > 1:
            t.row_hash = f"{t.row_hash}:dup{n}"
    _ex_seen: dict = {}
    for e in stmt.exercises:
        n = _ex_seen.get(e.row_hash, 0) + 1
        _ex_seen[e.row_hash] = n
        if n > 1:
            e.row_hash = f"{e.row_hash}:dup{n}"

    # Set period boundaries from first trade and last trade dates
    if stmt.trades:
        dates = [t.trade_date for t in stmt.trades if t.trade_date]
        if dates:
            stmt.period_start = min(dates)
            stmt.period_end = max(dates)

    # Apply holdings as_of_date as period_end (best guess)
    if stmt.period_end:
        for h in stmt.holdings:
            h.as_of_date = stmt.period_end

    logger.info(
        f"Parsed {fp.name}: {len(stmt.trades)} trades, {len(stmt.exercises)} exercises, "
        f"{len(stmt.holdings)} holdings, {len(stmt.cash_events)} cash events"
    )
    return stmt


# ─────────────────────────────────────────────────────────────────
# Multi-file parsing — chronological merge with row hash dedup
# ─────────────────────────────────────────────────────────────────
def parse_files(filepaths: list) -> TigerStatement:
    """Parse multiple Tiger CSVs, merge chronologically, dedup by row_hash."""
    combined = TigerStatement()
    seen_trade_hashes: set = set()
    seen_exercise_hashes: set = set()

    for fp in filepaths:
        stmt = parse_file(fp)
        combined.source_files.append(Path(fp).name)

        # Dedup trades
        for t in stmt.trades:
            if t.row_hash not in seen_trade_hashes:
                seen_trade_hashes.add(t.row_hash)
                combined.trades.append(t)

        # Dedup exercises
        for e in stmt.exercises:
            if e.row_hash not in seen_exercise_hashes:
                seen_exercise_hashes.add(e.row_hash)
                combined.exercises.append(e)

        # Cash events: dedup by (source_file, date, type, description, amount)
        # to avoid duplicating the same dividend across overlapping uploads
        existing_keys = {
            (c.source_file, c.event_date, c.event_type, c.description, c.amount)
            for c in combined.cash_events
        }
        for c in stmt.cash_events:
            key = (c.source_file, c.event_date, c.event_type, c.description, c.amount)
            if key not in existing_keys:
                existing_keys.add(key)
                combined.cash_events.append(c)

        # Holdings: only keep latest snapshot (by source file processing order)
        if stmt.holdings:
            combined.holdings = stmt.holdings  # Latest file wins
            combined.account_overview = stmt.account_overview  # Latest file wins

    # Sort everything chronologically
    combined.trades.sort(key=lambda x: (x.trade_date or date.min, x.source_row))
    combined.exercises.sort(key=lambda x: (x.event_date or date.min, x.source_row))
    combined.cash_events.sort(key=lambda x: (x.event_date or date.min, x.event_type))

    # Set combined period
    all_dates = [t.trade_date for t in combined.trades if t.trade_date] + \
                [e.event_date for e in combined.exercises if e.event_date]
    if all_dates:
        combined.period_start = min(all_dates)
        combined.period_end = max(all_dates)

    # Source files dedup
    combined.source_files = list(dict.fromkeys(combined.source_files))

    return combined


# ─────────────────────────────────────────────────────────────────
# JSON serialization
# ─────────────────────────────────────────────────────────────────
def _to_jsonable(obj):
    """Recursively convert dataclass/date objects to JSON-safe dicts/strings."""
    if hasattr(obj, '__dataclass_fields__'):
        return {k: _to_jsonable(v) for k, v in asdict(obj).items()}
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(v) for v in obj]
    return obj


def statement_to_dict(stmt: TigerStatement) -> dict:
    """Convert TigerStatement to JSON-serializable dict."""
    return {
        'source_files': stmt.source_files,
        'period_start': stmt.period_start.isoformat() if stmt.period_start else None,
        'period_end': stmt.period_end.isoformat() if stmt.period_end else None,
        'summary': stmt.summary(),
        'account_overview': stmt.account_overview,
        'trades': [_to_jsonable(t) for t in stmt.trades],
        'exercises': [_to_jsonable(e) for e in stmt.exercises],
        'holdings': [_to_jsonable(h) for h in stmt.holdings],
        'cash_events': [_to_jsonable(c) for c in stmt.cash_events],
    }


# ─────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Parse Tiger Brokers Activity Statement CSV(s).")
    ap.add_argument('files', nargs='+', help='One or more Tiger CSV files')
    ap.add_argument('--output', '-o', default='parsed.json', help='Output JSON path')
    ap.add_argument('--verbose', '-v', action='store_true')
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format='%(asctime)s %(levelname)s %(message)s',
    )

    stmt = parse_files(args.files)

    out_path = Path(args.output)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(statement_to_dict(stmt), f, indent=2, default=str)

    print(f"\nParsed {len(args.files)} file(s):")
    print(f"  Period:        {stmt.period_start} -> {stmt.period_end}")
    print(f"  Trades:        {len(stmt.trades)}")
    print(f"  Exercises:     {len(stmt.exercises)}")
    print(f"  Holdings:      {len(stmt.holdings)}")
    print(f"  Cash events:   {len(stmt.cash_events)}")
    if stmt.cash_events:
        breakdown = stmt.summary()['cash_events_by_type']
        for t, n in sorted(breakdown.items(), key=lambda x: -x[1]):
            print(f"    {t}: {n}")
    print(f"\nOutput written to: {out_path.resolve()}")
    print(f"  Size: {out_path.stat().st_size / 1024:.1f} KB")


if __name__ == '__main__':
    main()
