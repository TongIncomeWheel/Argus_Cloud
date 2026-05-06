"""Adapters: Tiger API objects → ARGUS-shaped dicts.

ARGUS Data Table schema (existing — keep stable):
    TradeID, Ticker, TradeType, Direction, Status, Quantity,
    OptPremium, Option_Strike_Price_(USD), Expiry_Date,
    Date_open, Date_closed, StrategyType, Pot,
    Tiger_Row_Hash, Source, Notes

These adapters produce dicts in that schema from Tiger Position objects.
TradeID is left blank (assigned by sync engine on insert).

Position-vs-Trade distinction:
- A Tiger Position is a *current state snapshot* (open lot).
- An ARGUS row is a *trade event* (open or close).
- We can derive an Open ARGUS row from a Position. We cannot recover the
  exact open date without an order history join — that's what get_filled_orders
  is for in Phase 2.
"""
from __future__ import annotations

from datetime import datetime, date
from typing import Dict, Optional, Set


# Tickers that ARGUS treats as PMCC (LEAP-backed CCs). Long calls on these
# tickers map to TradeType=LEAP; short calls to CC (covered by LEAP).
DEFAULT_PMCC_TICKERS: Set[str] = {"SPY"}


def _parse_expiry(s: str) -> Optional[date]:
    """Tiger gives expiry as 'YYYYMMDD' string. Returns date object or None."""
    if not s:
        return None
    try:
        return datetime.strptime(str(s), "%Y%m%d").date()
    except (ValueError, TypeError):
        return None


def _classify_option(symbol: str, right: str, qty_signed: float, pmcc_tickers: Set[str]) -> str:
    """Map (ticker, right, qty sign) → ARGUS TradeType.

    Rules:
      Long call  on PMCC ticker  → LEAP
      Long put                   → LEAP_PUT (rare)
      Short call on any ticker   → CC (covered by stock OR LEAP — ARGUS handles both)
      Short put                  → CSP
    """
    is_long = qty_signed > 0
    is_call = str(right).upper() in ("CALL", "C")

    if is_long and is_call:
        return "LEAP" if symbol.upper() in pmcc_tickers else "LEAP"  # treat long calls as LEAP regardless
    if is_long and not is_call:
        return "LEAP_PUT"
    if not is_long and is_call:
        return "CC"
    return "CSP"  # short put


def position_to_argus_row(
    pos,
    pmcc_tickers: Optional[Set[str]] = None,
) -> Dict:
    """Convert a Tiger Position object to an ARGUS Data Table row dict.

    Args:
        pos: tigeropen Position object (from TradeClient.get_positions)
        pmcc_tickers: set of tickers ARGUS treats as PMCC

    Returns:
        dict with ARGUS columns. Status is always 'Open' (Tiger only returns
        currently-open positions). TradeID is empty — sync engine assigns it.
    """
    pmcc_tickers = pmcc_tickers or DEFAULT_PMCC_TICKERS
    contract = pos.contract
    qty_signed = float(pos.quantity)
    abs_qty = abs(qty_signed)
    direction = "Buy" if qty_signed > 0 else "Sell"
    sec_type = str(getattr(contract, "sec_type", "")).upper()

    if sec_type == "STK":
        return {
            "TradeID": "",
            "Ticker": getattr(contract, "symbol", ""),
            "TradeType": "STOCK",
            "Direction": direction,
            "Status": "Open",
            "Quantity": int(abs_qty) if abs_qty == int(abs_qty) else abs_qty,
            "OptPremium": "",
            "Option_Strike_Price_(USD)": "",
            "Expiry_Date": "",
            "Date_open": "",  # Tiger Position doesn't carry open date — fill from order history
            "Date_closed": "",
            "StrategyType": "",  # Pot derives from this in ARGUS — leave blank for sync to preserve existing
            "Pot": "",
            "Tiger_Row_Hash": _row_hash_stock(contract.symbol, qty_signed, pos.average_cost),
            "Source": "TigerAPI",
            "Notes": "",
            # Bookkeeping fields not in Data Table but useful for sync:
            "_account": getattr(pos, "account", ""),
            "_avg_cost": float(pos.average_cost or 0),
            "_market_price": float(pos.market_price or 0),
            "_market_value": float(pos.market_value or 0),
            "_unrealized_pnl": float(getattr(pos, "unrealized_pnl", 0) or 0),
            "_contract_id": getattr(contract, "contract_id", None),
        }

    # Options
    expiry = _parse_expiry(getattr(contract, "expiry", ""))
    strike = float(getattr(contract, "strike", 0) or 0)
    right = str(getattr(contract, "put_call", getattr(contract, "right", "")))
    ticker = getattr(contract, "symbol", "")

    return {
        "TradeID": "",
        "Ticker": ticker,
        "TradeType": _classify_option(ticker, right, qty_signed, pmcc_tickers),
        "Direction": direction,
        "Status": "Open",
        "Quantity": int(abs_qty) if abs_qty == int(abs_qty) else abs_qty,
        "OptPremium": float(pos.average_cost or 0),  # avg cost per share (premium)
        "Option_Strike_Price_(USD)": strike,
        "Expiry_Date": expiry.strftime("%Y-%m-%d") if expiry else "",
        "Date_open": "",
        "Date_closed": "",
        "StrategyType": "",
        "Pot": "",
        "Tiger_Row_Hash": _row_hash_option(ticker, right, expiry, strike, qty_signed, pos.average_cost),
        "Source": "TigerAPI",
        "Notes": "",
        # Bookkeeping
        "_account": getattr(pos, "account", ""),
        "_avg_cost": float(pos.average_cost or 0),
        "_market_price": float(pos.market_price or 0),
        "_market_value": float(pos.market_value or 0),
        "_unrealized_pnl": float(getattr(pos, "unrealized_pnl", 0) or 0),
        "_contract_id": getattr(contract, "contract_id", None),
    }


def positions_to_argus_rows(positions, pmcc_tickers: Optional[Set[str]] = None):
    """Bulk convert Tiger positions → ARGUS rows."""
    return [position_to_argus_row(p, pmcc_tickers) for p in positions]


# ─────────────────────────────────────────────────────────────────
# Order → ARGUS row (open or closed trade event)
# ─────────────────────────────────────────────────────────────────

def _classify_order_trade_type(symbol: str, sec_type: str, right: str, action: str,
                                is_opening: bool, pmcc_tickers: Set[str]) -> str:
    """Determine ARGUS TradeType from a Tiger order, applying user's strategy:

      • PUT  on any ticker        → CSP   (user never goes long puts)
      • CALL on non-PMCC ticker   → CC    (no LEAPs outside PMCC)
      • CALL on PMCC ticker       → LEAP if the underlying position is LONG,
                                    else CC. Determined by combining the
                                    transaction's `action` with `is_opening`:
                                      LONG  if (BUY+open)  or (SELL+close)
                                      SHORT if (SELL+open) or (BUY+close)
      • STK  → STOCK
      • MLEG → MULTILEG (expanded into legs upstream in tiger_data)
    """
    sec_type_u = str(sec_type).upper()
    if sec_type_u == "STK":
        return "STOCK"
    if sec_type_u == "MLEG":
        return "MULTILEG"
    if sec_type_u != "OPT":
        return "OTHER"

    right_u = str(right).upper()
    is_call = right_u in ("CALL", "C")
    is_put = right_u in ("PUT", "P")
    if not (is_call or is_put):
        return "OTHER"

    # User's strategy: ALL puts are short cash-secured. No LEAP_PUTs.
    if is_put:
        return "CSP"

    # Calls on non-PMCC tickers are always short.
    if symbol not in pmcc_tickers:
        return "CC"

    # PMCC ticker calls — could be LEAP (long) or CC (short).
    is_sell = str(action).upper() == "SELL"
    is_short = (is_opening and is_sell) or (not is_opening and not is_sell)
    return "CC" if is_short else "LEAP"


def order_to_argus_row(order, pmcc_tickers: Optional[Set[str]] = None) -> Dict:
    """Convert a Tiger Order → ARGUS-shape dict for trade history.

    Tiger orders map to ARGUS rows as follows:
      - is_open = True  → opening transaction (STO / BTO)
      - is_open = False → closing transaction (BTC / STC), with realized_pnl populated

    For ARGUS analytics:
      - Every order becomes one row (no pairing needed for new orders)
      - 'Status' = 'Closed' if is_open=False, 'Open' otherwise (at order level)
      - 'Actual_Profit_(USD)' = order.realized_pnl (Tiger's broker-side P&L)
    """
    pmcc_tickers = pmcc_tickers or DEFAULT_PMCC_TICKERS
    contract = order.contract
    sec_type = str(getattr(contract, "sec_type", "")).upper()
    ticker = getattr(contract, "symbol", "")
    action = str(getattr(order, "action", "")).upper()
    tiger_is_open = bool(getattr(order, "is_open", True))

    # Determine `is_open` (does this transaction OPEN the position?) per user's strategy:
    #  - All puts: short positions → SELL=open, BUY=close (overrides Tiger flag)
    #  - Calls on non-PMCC ticker: short → SELL=open, BUY=close
    #  - Calls on PMCC ticker: trust Tiger's flag (LEAPs are real long positions)
    #  - STK / MLEG: use Tiger's flag
    right_raw = ""
    if sec_type == "OPT":
        right_raw = str(getattr(contract, "put_call", getattr(contract, "right", ""))).upper()
    is_put_opt = sec_type == "OPT" and right_raw in ("PUT", "P")
    is_call_opt = sec_type == "OPT" and right_raw in ("CALL", "C")
    if is_put_opt or (is_call_opt and ticker not in pmcc_tickers):
        is_open = (action == "SELL")
    else:
        is_open = tiger_is_open

    qty_filled = abs(int(getattr(order, "filled", 0) or 0))
    avg_fill = float(getattr(order, "avg_fill_price", 0) or 0)
    commission = float(getattr(order, "commission", 0) or 0)
    gst = float(getattr(order, "gst", 0) or 0)
    realized_pnl = float(getattr(order, "realized_pnl", 0) or 0)
    filled_cash = float(getattr(order, "filled_cash_amount", 0) or 0)

    # Detect EXPIRED (Tiger emits synthetic close orders at $0 fill, $0 commission
    # when an option position expires worthless — no actual trade, just bookkeeping).
    is_expiry = (
        sec_type == "OPT"
        and not is_open
        and avg_fill <= 0.01
        and commission <= 0.01
    )

    # Timestamps (millisecond → ISO date)
    trade_time_ms = getattr(order, "trade_time", None)
    order_time_ms = getattr(order, "order_time", None)
    trade_dt = None
    if trade_time_ms:
        try:
            trade_dt = datetime.fromtimestamp(trade_time_ms / 1000)
        except Exception:
            pass
    order_dt = None
    if order_time_ms:
        try:
            order_dt = datetime.fromtimestamp(order_time_ms / 1000)
        except Exception:
            pass

    # Direction in ARGUS schema:
    # CSP-open  = SELL PUT  → Direction = "Sell"
    # CSP-close = BUY  PUT  → Direction = "Buy"
    direction = "Sell" if action == "SELL" else "Buy"

    if sec_type == "STK":
        trade_type = "STOCK"
        strike = ""
        expiry = ""
        right = ""
    else:
        right = str(getattr(contract, "put_call", getattr(contract, "right", "")))
        strike = float(getattr(contract, "strike", 0) or 0)
        expiry_raw = getattr(contract, "expiry", "")
        expiry = _parse_expiry(expiry_raw).strftime("%Y-%m-%d") if _parse_expiry(expiry_raw) else ""
        trade_type = _classify_order_trade_type(ticker, sec_type, right, action, is_open, pmcc_tickers)

    # Derive Event label — what actually happened
    if is_expiry:
        event = "EXPIRED"
    elif sec_type == "MLEG":
        event = "COMBO"  # multi-leg parent (legs are expanded separately upstream)
    elif sec_type == "STK":
        event = "BTO" if action == "BUY" else "STC"  # stock buy/sell
    elif sec_type == "OPT":
        if is_open and action == "SELL":
            event = "STO"  # Sell-to-Open (CSP/CC opening)
        elif is_open and action == "BUY":
            event = "BTO"  # Buy-to-Open (LEAP opening)
        elif not is_open and action == "BUY":
            event = "BTC"  # Buy-to-Close (CSP/CC closing via active buyback)
        elif not is_open and action == "SELL":
            event = "STC"  # Sell-to-Close (LEAP closing)
        else:
            event = action
    else:
        event = action  # MLEG / unknown — use raw action

    return {
        "TradeID": str(getattr(order, "id", "") or ""),
        "OrderID_Tiger": str(getattr(order, "id", "") or ""),
        "ExternalID": str(getattr(order, "external_id", "") or ""),
        "Ticker": ticker,
        "TradeType": trade_type,
        "Direction": direction,
        "Action": action,            # raw Tiger action
        "Event": event,              # EXPIRED / STO / BTC / STC / BTO / etc.
        "Status": "Closed" if not is_open else "Open",
        "is_opening": is_open,        # True for STO/BTO, False for BTC/STC
        "Quantity": qty_filled,
        "OptPremium": avg_fill if sec_type != "STK" else "",
        "FillPrice": avg_fill,
        "FilledCashAmount": filled_cash,
        "Option_Strike_Price_(USD)": strike,
        "Expiry_Date": expiry,
        "Right": right,
        "Date_open": order_dt.strftime("%Y-%m-%d") if (is_open and order_dt) else "",
        "Date_closed": trade_dt.strftime("%Y-%m-%d") if (not is_open and trade_dt) else "",
        "TradeDate": trade_dt.strftime("%Y-%m-%d") if trade_dt else "",
        "TradeDateTime": trade_dt.isoformat(timespec="seconds") if trade_dt else "",
        "Actual_Profit_(USD)": realized_pnl,
        "Commission": commission,
        "GST": gst,
        "Source": "TigerAPI",
        "_status": str(getattr(order, "status", "")).replace("OrderStatus.", ""),
    }


def orders_to_argus_rows(orders, pmcc_tickers: Optional[Set[str]] = None):
    return [order_to_argus_row(o, pmcc_tickers) for o in orders]


def classify_combo_type(txns, ticker: str, pmcc_tickers: Optional[Set[str]] = None) -> str:
    """Classify a multi-leg combo's overall type by inspecting all legs.

    Returns one of: 'LEAP' | 'CC' | 'CSP'.

    Logic:
      - Any leg is a PUT  → 'CSP' (puts are almost always short on PMCC tickers too)
      - Non-PMCC ticker calls → 'CC' (we hold no long calls on those)
      - PMCC ticker calls   → 'LEAP' if max(leg_premium) > $30/share
                              AND min(leg_DTE) > 30 days; else 'CC'

    The combo-level (vs per-leg) check correctly handles edge cases:
      • A LEAP roll where the closing leg has < 60 DTE (caught via the OTHER
        leg's longer DTE & high premium).
      • A deep-ITM CC roll where premiums look LEAP-like but both legs are
        short-dated (caught via min_DTE ≤ 30).
    """
    pmcc_tickers = pmcc_tickers or DEFAULT_PMCC_TICKERS
    if not txns:
        return "CC"

    has_put = False
    for t in txns:
        c = getattr(t, "contract", None)
        if c and str(getattr(c, "put_call", "")).upper() in ("PUT", "P"):
            has_put = True
            break
    if has_put:
        return "CSP"

    if ticker not in pmcc_tickers:
        return "CC"

    max_prem = 0.0
    min_dte: Optional[int] = None
    today_d = date.today()
    for t in txns:
        c = getattr(t, "contract", None)
        if not c:
            continue
        try:
            prem = abs(float(getattr(t, "filled_price", 0) or 0))
            max_prem = max(max_prem, prem)
        except (TypeError, ValueError):
            pass
        exp_str = getattr(c, "expiry", "")
        parsed = _parse_expiry(exp_str)
        if parsed is not None:
            dte = (parsed - today_d).days
            min_dte = dte if min_dte is None else min(min_dte, dte)

    if max_prem > 30 and (min_dte is not None and min_dte > 30):
        return "LEAP"
    return "CC"


def txn_to_argus_row(t, parent_order=None, pmcc_tickers: Optional[Set[str]] = None,
                     combo_type: Optional[str] = None) -> Optional[Dict]:
    """Convert a Tiger Transaction (single leg of a multi-leg combo) → ARGUS row.

    For MLEG (combo / roll) orders, get_filled_orders gives one parent order
    with no per-leg detail. Calling get_transactions(order_id) returns one
    Transaction per leg, each with full contract info (strike, expiry, put_call).

    Open/close inference per leg (Tiger doesn't expose `is_open` per leg):
      • For PMCC tickers (e.g. SPY) — discriminate by DTE:
          - Long-dated calls (DTE > 180 d): LEAP (long).  BTO=open, STC=close
          - Short-dated calls: CC (short).               STO=open, BTC=close
      • For non-PMCC tickers — assume short positions (CC/CSP):
                                                         STO=open, BTC=close
    Realized P&L from the parent is attributed to the CLOSING leg.
    """
    pmcc_tickers = pmcc_tickers or DEFAULT_PMCC_TICKERS
    contract = getattr(t, "contract", None)
    if not contract:
        return None

    sec_type = str(getattr(contract, "sec_type", "")).upper()
    ticker = getattr(contract, "symbol", "")
    action = str(getattr(t, "action", "")).upper()
    qty = abs(int(getattr(t, "filled_quantity", 0) or 0))
    fill_price = float(getattr(t, "filled_price", 0) or 0)
    filled_cash = float(getattr(t, "filled_amount", 0) or 0)

    txn_time_ms = getattr(t, "transaction_time", None)
    txn_dt = None
    if txn_time_ms:
        try:
            txn_dt = datetime.fromtimestamp(int(txn_time_ms) / 1000)
        except Exception:
            pass

    LEAP_DTE_THRESHOLD = 180  # days

    direction = "Sell" if action == "SELL" else "Buy"

    if sec_type == "STK":
        trade_type = "STOCK"
        strike = ""
        expiry = ""
        right = ""
        is_opening = (action == "BUY")  # BTO=open
        parsed = None
    else:
        right = str(getattr(contract, "put_call", getattr(contract, "right", "")))
        strike = float(getattr(contract, "strike", 0) or 0)
        expiry_raw = getattr(contract, "expiry", "")
        parsed = _parse_expiry(expiry_raw)
        expiry = parsed.strftime("%Y-%m-%d") if parsed else ""

        is_call = right.upper() in ("CALL", "C")
        dte = (parsed - date.today()).days if parsed else 0

        # If combo_type was supplied (caller already classified the whole combo),
        # use that — it's more reliable than per-leg DTE alone.
        if combo_type == "LEAP":
            is_long_pos = True
        elif combo_type in ("CC", "CSP"):
            is_long_pos = False
        else:
            # Fallback: per-leg DTE heuristic
            is_long_pos = bool(ticker in pmcc_tickers and is_call and dte > LEAP_DTE_THRESHOLD)

        if is_long_pos:
            is_opening = (action == "BUY")     # LEAP: BTO=open, STC=close
        else:
            is_opening = (action == "SELL")    # Short: STO=open, BTC=close

        trade_type = _classify_order_trade_type(ticker, sec_type, right, action, is_opening, pmcc_tickers)

    # Allocate parent's realized P&L to the closing (SELL) leg only
    parent_pnl = 0.0
    if parent_order is not None and not is_opening:
        try:
            parent_pnl = float(getattr(parent_order, "realized_pnl", 0) or 0)
        except (TypeError, ValueError):
            parent_pnl = 0.0

    parent_id = getattr(parent_order, "id", "") if parent_order else ""

    # Event label for the leg — combo legs are always active rolls (not expiries)
    if sec_type == "STK":
        event = "BTO" if action == "BUY" else "STC"
    elif is_opening and action == "SELL":
        event = "STO"
    elif is_opening and action == "BUY":
        event = "BTO"
    elif not is_opening and action == "BUY":
        event = "BTC"
    elif not is_opening and action == "SELL":
        event = "STC"
    else:
        event = action

    return {
        "TradeID": str(getattr(t, "id", "") or ""),
        "OrderID_Tiger": str(getattr(t, "order_id", "") or parent_id or ""),
        "ExternalID": "",
        "Ticker": ticker,
        "TradeType": trade_type,
        "Direction": direction,
        "Action": action,
        "Event": event,
        "Status": "Closed" if not is_opening else "Open",
        "is_opening": is_opening,
        "Quantity": qty,
        "OptPremium": fill_price if sec_type != "STK" else "",
        "FillPrice": fill_price,
        "FilledCashAmount": filled_cash,
        "Option_Strike_Price_(USD)": strike,
        "Expiry_Date": expiry,
        "Right": right,
        "Date_open": txn_dt.strftime("%Y-%m-%d") if (is_opening and txn_dt) else "",
        "Date_closed": txn_dt.strftime("%Y-%m-%d") if (not is_opening and txn_dt) else "",
        "TradeDate": txn_dt.strftime("%Y-%m-%d") if txn_dt else "",
        "TradeDateTime": txn_dt.isoformat(timespec="seconds") if txn_dt else "",
        "Actual_Profit_(USD)": parent_pnl,
        "Commission": 0.0,  # Tiger reports fees on parent order, not per-leg
        "GST": 0.0,
        "Source": f"TigerAPI-LEG (combo {parent_id})",
        "_status": "FILLED",
    }


# ── Hash helpers ─────────────────────────────────────────────────
def _row_hash_stock(ticker: str, qty_signed: float, avg_cost: float) -> str:
    """Stable hash for a stock lot (ticker + signed qty + avg cost)."""
    import hashlib
    payload = f"STK|{ticker}|{qty_signed}|{avg_cost:.4f}"
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _row_hash_option(
    ticker: str, right: str, expiry, strike: float, qty_signed: float, avg_cost: float
) -> str:
    """Stable hash for an option lot."""
    import hashlib
    exp_str = expiry.strftime("%Y%m%d") if expiry else ""
    payload = f"OPT|{ticker}|{exp_str}|{right.upper()}|{strike:.2f}|{qty_signed}|{avg_cost:.4f}"
    return hashlib.sha256(payload.encode()).hexdigest()[:16]
