"""Google Sheets Data Table reader — slim, Cloud-Run-native.

Used by get_position_roc to read the full unbounded fill history from the
'Data Table' tab of the Income Wheel sheet. We do NOT import the
top-level gsheet_handler.py module because it pulls in streamlit,
local-filesystem backup paths, and a logging setup that fights with
Cloud Run's stdout. Instead this module talks to gspread directly and
uses Application Default Credentials (the Cloud Run runtime service
account), avoiding the need to mount a credentials JSON.

Setup outside the code (manual, one-time per project):
  1. Enable the Google Sheets API + Drive API on the GCP project.
  2. Open the Income Wheel sheet in Google Drive and share it with the
     Cloud Run runtime SA — typically
     <PROJECT_NUMBER>-compute@developer.gserviceaccount.com — as Viewer.
  3. Set the env var MCP_INCOME_WHEEL_SHEET_ID (or INCOME_WHEEL_SHEET_ID)
     on the Cloud Run service to the spreadsheet id.

If the sheet is not configured or unreachable, callers fall through to
the Tiger MCP fallback path (get_filled_orders, 90-day window).
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Any, Optional

logger = logging.getLogger("tiger-mcp.data_table")


# ── Pot routing ──────────────────────────────────────────────────────────────
# Locked by the PM (see ARGUS E1 build instruction). Tickers not in any pot
# get pot='unknown' and DO appear in the per-position list but NOT in by_pot
# aggregates. EXCLUDE_TICKERS are dropped entirely.
CORE_TICKERS = {"MARA", "CRCL"}
ACTIVE_TICKERS = {"BE", "COIN", "DELL", "MSFT", "MP", "SLB"}
SIDECAR_TICKERS = {"ECHO", "INTC"}
EXCLUDE_TICKERS = {"KO", "MCD", "NVDA", "SPY"}

JUICED_THRESHOLD = 0.65


def get_pot(symbol: str) -> str:
    s = (symbol or "").upper()
    if s in CORE_TICKERS:
        return "core"
    if s in ACTIVE_TICKERS:
        return "active"
    if s in SIDECAR_TICKERS:
        return "sidecar"
    return "unknown"


# ── Sheets access ────────────────────────────────────────────────────────────


def _sheet_id() -> Optional[str]:
    """Pick up the Income Wheel sheet id from env. Returns None when unset
    so callers can fall through to the Tiger MCP fallback path.

    Sentinel "NOT_SET" is also treated as unset — the deploy workflow
    seeds the Secret Manager entry with this string on first run so the
    Cloud Run revision can boot before the operator pastes the real id.
    """
    raw = (
        os.environ.get("MCP_INCOME_WHEEL_SHEET_ID")
        or os.environ.get("INCOME_WHEEL_SHEET_ID")
        or ""
    ).strip()
    if not raw or raw.upper() == "NOT_SET":
        return None
    return raw


def _open_sheet(sheet_id: str):
    """Open the spreadsheet via Application Default Credentials.

    Raises a regular exception on failure — caller decides whether to fall
    back to Tiger MCP. We do NOT swallow errors silently; the failure mode
    is reported in the tool's `entry_source` and `notes` so the operator
    sees it.
    """
    import gspread
    import google.auth

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds, project = google.auth.default(scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)


def read_data_table() -> list[dict]:
    """Return the Data Table tab as a list of dicts (one per row).

    Empty list on misconfig / API error — caller checks and falls back.
    """
    sheet_id = _sheet_id()
    if not sheet_id:
        logger.info(
            "MCP_INCOME_WHEEL_SHEET_ID not set — Sheets reader disabled. "
            "Tools that depend on it will use the Tiger MCP fallback."
        )
        return []
    try:
        sh = _open_sheet(sheet_id)
        ws = sh.worksheet("Data Table")
        rows = ws.get_all_records()
        logger.info("Read %d rows from Data Table (sheet=%s)", len(rows), sheet_id[:8])
        return rows
    except Exception as e:
        logger.warning("Data Table read failed: %s", e)
        return []


# ── Date / number coercion ───────────────────────────────────────────────────


def _parse_iso_date(value: Any) -> Optional[date]:
    """Parse Date_open / Expiry_Date entries to date.

    Sheets returns strings; people enter various formats. Try the common ones
    in order, give up to None."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(s[:10] if "-" in s or "/" in s else s[:8], fmt).date()
        except ValueError:
            continue
    return None


def _to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _norm_right(value: Any) -> str:
    """Normalize PUT/CALL from various Data Table conventions."""
    s = str(value or "").strip().upper()
    if s in ("PUT", "P"):
        return "PUT"
    if s in ("CALL", "C"):
        return "CALL"
    return s


# ── Position → Sheet row matcher ─────────────────────────────────────────────


def _row_is_short_option(row: dict) -> bool:
    """Filter Data Table rows to short option STO rows."""
    direction = str(row.get("Direction", "")).strip()
    trade_type = str(row.get("TradeType", "")).strip().upper()
    return direction in ("Sell", "OpenShort", "SELL", "STO") and trade_type == "OPT"


def _row_right(row: dict) -> str:
    """Infer right (PUT/CALL) from a Data Table row.

    The schema lacks a dedicated right column — the convention is to read it
    out of StrategyType or Remarks. We accept either."""
    for key in ("Right", "StrategyType", "Remarks"):
        val = str(row.get(key, "")).upper()
        if "PUT" in val or " P " in val or val.endswith("P"):
            return "PUT"
        if "CALL" in val or " C " in val or val.endswith("C"):
            return "CALL"
    return ""


def _match_open_row(rows: list[dict], symbol: str, strike: float, expiry: date,
                    right: str) -> Optional[dict]:
    """Find the most recent matching STO row in Data Table for one open position.

    Match key: ticker + strike + expiry + right + short-option STO.
    Tie-break: take the most recent Date_open (handles rolled positions
    where multiple STO rows exist for the same strike/expiry — newest wins).
    """
    sym = (symbol or "").upper()
    candidates = []
    for r in rows:
        if not _row_is_short_option(r):
            continue
        if str(r.get("Ticker", "")).upper() != sym:
            continue
        if _norm_right(_row_right(r)) != right:
            continue
        try:
            row_strike = float(r.get("Option_Strike_Price_(USD)", 0))
        except (TypeError, ValueError):
            continue
        if abs(row_strike - strike) > 0.01:
            continue
        row_expiry = _parse_iso_date(r.get("Expiry_Date"))
        if row_expiry != expiry:
            continue
        row_open = _parse_iso_date(r.get("Date_open"))
        if row_open is None:
            continue
        candidates.append((row_open, r))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


# ── Tiger MCP fallback: match positions to filled-order STO ──────────────────


def _match_fill_for_position(filled_orders: list[dict], symbol: str, strike: float,
                             expiry: date, right: str) -> Optional[dict]:
    """When Sheets is unavailable, find the entry STO fill from Tiger's 90-day
    window. Most recent matching SELL_TO_OPEN normal fill wins."""
    sym = (symbol or "").upper()
    candidates = []
    for o in filled_orders:
        if (o.get("fill_type") or "").lower() != "normal":
            continue
        if (o.get("sec_type") or "").upper() != "OPT":
            continue
        if str(o.get("symbol", "")).upper() != sym:
            continue
        if _norm_right(o.get("right")) != right:
            continue
        try:
            if abs(float(o.get("strike", 0)) - strike) > 0.01:
                continue
        except (TypeError, ValueError):
            continue
        order_expiry = _parse_iso_date(o.get("expiry"))
        if order_expiry != expiry:
            continue
        # Direction: SELL family
        action = (o.get("action") or "").upper().replace(" ", "_")
        if "SELL" not in action:
            continue
        # Use trade_time for "most recent"
        ts_raw = o.get("trade_time") or o.get("order_time")
        ts = _parse_iso_date(ts_raw)
        if ts is None:
            continue
        candidates.append((ts, o))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


# ── RoC compute ──────────────────────────────────────────────────────────────


def compute_position_roc(symbol: str, strike: float, expiry: date, right: str,
                         qty: int, avg_cost: float, market_price: float,
                         entry_date: date, today: date) -> dict:
    """All math lives here. No I/O, no Sheets, no Tiger client — pure compute
    so unit tests are trivial.

    Sign convention: `qty` is unsigned. Caller passes abs() of the position's
    quantity. avg_cost = premium received per share at STO (positive number).
    market_price = cost-to-BTC per share now.
    """
    notional = strike * 100.0 * qty
    premium_received = avg_cost * 100.0 * qty
    current_value = market_price * 100.0 * qty
    pnl_to_date = premium_received - current_value

    yield_on_notional = (premium_received / notional) if notional > 0 else 0.0
    pct_harvested = (pnl_to_date / premium_received) if premium_received > 0 else 0.0

    days_held = (today - entry_date).days
    dte_at_entry = (expiry - entry_date).days
    dte_remaining = (expiry - today).days

    annualised_roc: Optional[float] = None
    if days_held > 0:
        annualised_roc = yield_on_notional * 365.0 / days_held

    juiced = pct_harvested >= JUICED_THRESHOLD

    return {
        "entry_date": entry_date.isoformat(),
        "days_held": days_held,
        "dte_at_entry": dte_at_entry,
        "dte_remaining": dte_remaining,
        "notional": round(notional, 2),
        "premium_received": round(premium_received, 2),
        "current_value": round(current_value, 2),
        "pnl_to_date": round(pnl_to_date, 2),
        "yield_on_notional": round(yield_on_notional, 4),
        "pct_harvested": round(pct_harvested, 4),
        "annualised_roc": round(annualised_roc, 4) if annualised_roc is not None else None,
        "juiced": juiced,
        "juiced_threshold": JUICED_THRESHOLD,
    }


# ── Aggregation ──────────────────────────────────────────────────────────────


def _empty_pot_agg() -> dict:
    return {
        "total_notional": 0.0,
        "total_premium_received": 0.0,
        "total_pnl_to_date": 0.0,
        "portfolio_yield_on_notional": 0.0,
        "portfolio_pct_harvested": 0.0,
        "position_count": 0,
    }


def aggregate_positions(positions: list[dict]) -> dict:
    """Build the aggregates block from per-position rows."""
    by_pot = {"core": _empty_pot_agg(), "active": _empty_pot_agg(), "sidecar": _empty_pot_agg()}

    total_notional = 0.0
    total_premium = 0.0
    total_pnl = 0.0
    juiced_count = 0
    missing_entry = 0

    for p in positions:
        pot = p.get("pot")
        notional = p.get("notional") or 0.0
        premium = p.get("premium_received") or 0.0
        pnl = p.get("pnl_to_date") or 0.0

        total_notional += notional
        total_premium += premium
        total_pnl += pnl

        if p.get("juiced"):
            juiced_count += 1
        if not p.get("entry_fill_found"):
            missing_entry += 1

        if pot in by_pot:
            bucket = by_pot[pot]
            bucket["total_notional"] += notional
            bucket["total_premium_received"] += premium
            bucket["total_pnl_to_date"] += pnl
            bucket["position_count"] += 1

    # Finalize per-pot percentages
    for bucket in by_pot.values():
        n = bucket["total_notional"]
        prem = bucket["total_premium_received"]
        bucket["portfolio_yield_on_notional"] = round(prem / n, 4) if n > 0 else 0.0
        bucket["portfolio_pct_harvested"] = (
            round(bucket["total_pnl_to_date"] / prem, 4) if prem > 0 else 0.0
        )
        bucket["total_notional"] = round(bucket["total_notional"], 2)
        bucket["total_premium_received"] = round(bucket["total_premium_received"], 2)
        bucket["total_pnl_to_date"] = round(bucket["total_pnl_to_date"], 2)

    return {
        "by_pot": by_pot,
        "total_notional": round(total_notional, 2),
        "total_premium_received": round(total_premium, 2),
        "total_pnl_to_date": round(total_pnl, 2),
        "portfolio_yield_on_notional": (
            round(total_premium / total_notional, 4) if total_notional > 0 else 0.0
        ),
        "portfolio_pct_harvested": (
            round(total_pnl / total_premium, 4) if total_premium > 0 else 0.0
        ),
        "juiced_count": juiced_count,
        "total_positions": len(positions),
        "positions_missing_entry": missing_entry,
    }
