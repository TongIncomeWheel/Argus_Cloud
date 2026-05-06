"""Two-tier persistent archive for Tiger order history.

  • Tier 1 (CANONICAL):  gSheet `Orders_Archive` tab — survives Streamlit Cloud
                         restarts indefinitely, the source of truth.
  • Tier 2 (CACHE):      Local parquet file — fast read/write within a session,
                         rebuilt from gSheet on Cloud where filesystem is ephemeral.

Read flow:
    parquet (if exists) → else gSheet → save parquet for next time
Write flow ("Refresh archive"):
    Tiger API → DataFrame → write to BOTH gSheet AND parquet
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

ARCHIVE_DIR = Path(__file__).parent.parent / "data" / "archive"
ORDERS_PARQUET = ARCHIVE_DIR / "orders.parquet"
ARCHIVE_META = ARCHIVE_DIR / "orders_meta.json"

# gSheet tab name for the canonical archive
ARCHIVE_SHEET_TITLE = "Orders_Archive"


def _ensure_dir() -> None:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────
# gSheet (canonical) — Tier 1
# ─────────────────────────────────────────────────────────────────
def _get_gsheet_handler():
    """Return GSheetHandler instance for the Income Wheel sheet, or None on failure."""
    try:
        from gsheet_handler import GSheetHandler
        from config import INCOME_WHEEL_SHEET_ID
        if not INCOME_WHEEL_SHEET_ID:
            return None
        return GSheetHandler(INCOME_WHEEL_SHEET_ID)
    except Exception as e:
        logger.warning("gSheet handler init failed: %s", e)
        return None


def _get_or_create_archive_ws(handler):
    """Return the Orders_Archive worksheet, creating it if missing."""
    try:
        return handler.spreadsheet.worksheet(ARCHIVE_SHEET_TITLE)
    except Exception:
        # Doesn't exist — create with generous initial size
        try:
            return handler.spreadsheet.add_worksheet(
                title=ARCHIVE_SHEET_TITLE, rows=2000, cols=30,
            )
        except Exception as e:
            logger.error("Could not create Orders_Archive tab: %s", e)
            return None


def read_archive_from_gsheet() -> pd.DataFrame:
    """Read the Orders_Archive tab → DataFrame. Empty DataFrame if missing/error."""
    handler = _get_gsheet_handler()
    if handler is None:
        return pd.DataFrame()
    try:
        ws = handler.spreadsheet.worksheet(ARCHIVE_SHEET_TITLE)
    except Exception:
        # Tab doesn't exist yet
        return pd.DataFrame()
    try:
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            return pd.DataFrame()
        header, body = rows[0], rows[1:]
        df = pd.DataFrame(body, columns=header)
        # Restore types
        if "TradeDateTime" in df.columns:
            df["TradeDateTime"] = pd.to_datetime(df["TradeDateTime"], errors="coerce")
        if "is_opening" in df.columns:
            df["is_opening"] = df["is_opening"].apply(lambda v: str(v).strip().lower() == "true")
        for c in ("Quantity", "FillPrice", "FilledCashAmount", "Option_Strike_Price_(USD)",
                  "Actual_Profit_(USD)", "Commission", "GST", "OptPremium"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
    except Exception as e:
        logger.warning("Could not read Orders_Archive tab: %s", e)
        return pd.DataFrame()


def write_archive_to_gsheet(df: pd.DataFrame) -> bool:
    """Replace the entire Orders_Archive tab with this DataFrame's contents.
    Returns True on success."""
    if df is None or df.empty:
        logger.warning("Refusing to write empty archive to gSheet")
        return False
    handler = _get_gsheet_handler()
    if handler is None:
        logger.warning("No gSheet handler — skipping canonical write")
        return False
    ws = _get_or_create_archive_ws(handler)
    if ws is None:
        return False
    try:
        # Coerce all values to strings (gSheet stores as strings)
        df_str = df.copy()
        df_str = df_str.fillna("")
        for c in df_str.columns:
            df_str[c] = df_str[c].astype(str)
        values = [list(df_str.columns)] + df_str.values.tolist()
        # Resize the tab to fit
        try:
            ws.resize(rows=max(len(values), 100), cols=max(len(df_str.columns), 10))
        except Exception:
            pass
        ws.clear()
        ws.update(values=values, range_name="A1")
        logger.info("Wrote %d rows to Orders_Archive tab", len(df))
        return True
    except Exception as e:
        logger.error("Could not write Orders_Archive tab: %s", e)
        return False


def gsheet_archive_summary() -> dict:
    """Quick summary of the gSheet archive — for the Config status display."""
    df = read_archive_from_gsheet()
    if df.empty:
        return {"exists": False, "rows": 0}
    out = {"exists": True, "rows": len(df)}
    if "TradeDateTime" in df.columns:
        out["earliest"] = df["TradeDateTime"].min()
        out["latest"] = df["TradeDateTime"].max()
    return out


def _read_parquet_cache() -> pd.DataFrame:
    """Tier-2 (cache) — parquet read. Empty DataFrame if file doesn't exist."""
    if not ORDERS_PARQUET.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(ORDERS_PARQUET)
        if "TradeDateTime" in df.columns:
            df["TradeDateTime"] = pd.to_datetime(df["TradeDateTime"], errors="coerce")
        return df
    except Exception as e:
        logger.warning("Could not read parquet cache: %s", e)
        return pd.DataFrame()


def _write_parquet_cache(df: pd.DataFrame) -> bool:
    _ensure_dir()
    if df is None or df.empty:
        return False
    try:
        df.to_parquet(ORDERS_PARQUET, index=False)
        import json
        meta = {
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "rows": len(df),
            "earliest": str(df["TradeDateTime"].min()) if "TradeDateTime" in df.columns and not df.empty else None,
            "latest": str(df["TradeDateTime"].max()) if "TradeDateTime" in df.columns and not df.empty else None,
        }
        with open(ARCHIVE_META, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, default=str)
        return True
    except Exception as e:
        logger.error("Failed to write parquet cache: %s", e)
        return False


def load_orders_archive() -> pd.DataFrame:
    """Read archived orders. Two-tier:
        1. Try parquet cache (fast, may be missing on Cloud after restart)
        2. Fall back to gSheet (canonical, always available)
        3. On gSheet hit, populate the parquet cache for future fast reads
    Returns empty DataFrame if no archive exists anywhere yet.
    """
    df = _read_parquet_cache()
    if not df.empty:
        return df
    # Cache miss — try gSheet
    df = read_archive_from_gsheet()
    if not df.empty:
        _write_parquet_cache(df)  # populate cache for next session
    return df


def save_orders_archive(df: pd.DataFrame) -> dict:
    """Write the orders DataFrame to BOTH layers (canonical + cache).

    Returns status dict:
      {'parquet_ok': bool, 'gsheet_ok': bool, 'rows': int}
    """
    if df is None or df.empty:
        logger.warning("Refusing to save empty DataFrame to archive")
        return {"parquet_ok": False, "gsheet_ok": False, "rows": 0}

    parquet_ok = _write_parquet_cache(df)
    gsheet_ok = write_archive_to_gsheet(df)

    return {"parquet_ok": parquet_ok, "gsheet_ok": gsheet_ok, "rows": len(df)}


def merge_with_archive(df_live: pd.DataFrame) -> pd.DataFrame:
    """Combine the live (last-90-days) DataFrame with the on-disk archive.

    Dedups by TradeID — the archive may overlap with live by a few days, in
    which case the live version wins (it has the freshest expansion data).

    Returns one DataFrame sorted by TradeDateTime descending (newest first).
    """
    df_arc = load_orders_archive()
    if df_arc.empty:
        return df_live
    if df_live is None or df_live.empty:
        return df_arc

    combined = pd.concat([df_arc, df_live], ignore_index=True, sort=False)
    if "TradeID" in combined.columns:
        # Live rows come last in concat — `keep='last'` prefers the live version
        combined = combined.drop_duplicates(subset=["TradeID"], keep="last")
    if "TradeDateTime" in combined.columns:
        combined["TradeDateTime"] = pd.to_datetime(combined["TradeDateTime"], errors="coerce")
        combined = combined.sort_values("TradeDateTime", ascending=False).reset_index(drop=True)
    return combined


def archive_summary() -> dict:
    """Combined summary across both tiers.

    Reports what's in the parquet cache (Tier 2) AND in gSheet (Tier 1).
    Used by Config UI to show user the state of each layer separately.
    """
    out = {
        "parquet": {"exists": False, "rows": 0, "saved_at": None,
                    "earliest": None, "latest": None},
        "gsheet": {"exists": False, "rows": 0, "earliest": None, "latest": None},
    }

    # Parquet (cache)
    if ORDERS_PARQUET.exists():
        df = _read_parquet_cache()
        out["parquet"]["exists"] = True
        out["parquet"]["rows"] = len(df)
        if "TradeDateTime" in df.columns and not df.empty:
            out["parquet"]["earliest"] = df["TradeDateTime"].min()
            out["parquet"]["latest"] = df["TradeDateTime"].max()
        if ARCHIVE_META.exists():
            try:
                import json
                with open(ARCHIVE_META, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                out["parquet"]["saved_at"] = meta.get("saved_at")
            except Exception:
                pass

    # gSheet (canonical) — wrapped to gracefully handle no-credentials case
    try:
        gsheet = gsheet_archive_summary()
        out["gsheet"] = gsheet
    except Exception as e:
        logger.warning("gSheet summary failed: %s", e)

    return out


# ─────────────────────────────────────────────────────────────────
# Quarter cadence — disciplines the archive workflow
# ─────────────────────────────────────────────────────────────────
def quarter_end(year: int, q: int) -> datetime:
    """End-of-day datetime of the given quarter end (Q1=Mar 31, Q2=Jun 30, etc.)."""
    if q == 1:
        return datetime(year, 3, 31, 23, 59, 59)
    if q == 2:
        return datetime(year, 6, 30, 23, 59, 59)
    if q == 3:
        return datetime(year, 9, 30, 23, 59, 59)
    return datetime(year, 12, 31, 23, 59, 59)


def current_quarter(d: Optional[datetime] = None) -> tuple:
    d = d or datetime.now()
    return d.year, (d.month - 1) // 3 + 1


def most_recent_completed_quarter(d: Optional[datetime] = None) -> tuple:
    """The latest quarter that has fully ended as of `d`."""
    d = d or datetime.now()
    if d.month <= 3:
        return d.year - 1, 4
    if d.month <= 6:
        return d.year, 1
    if d.month <= 9:
        return d.year, 2
    return d.year, 3


def archive_cadence_status(d: Optional[datetime] = None) -> dict:
    """Determine if the quarterly archive is due.

    Logic:
      • The 'most recent completed quarter' is the last one whose 3 months are done.
      • If our archive's latest TradeDateTime covers through that quarter's end → up-to-date.
      • Otherwise, archive is DUE (overdue by the gap from quarter-end to today).
      • When up-to-date, show countdown to the NEXT due date (current quarter end + 5d).

    Returns:
      {
        'status': 'DUE' | 'OK' | 'EMPTY',
        'badge': str,                 # short label for header chip
        'detail': str,                # longer explanation
        'days_until_next': int,       # days until next archive due (negative if overdue)
        'next_due_date': date,        # next archive due date
        'pending_quarter': str,       # e.g. '2026-Q1' if DUE, else next quarter due
        'archive_through': str,       # ISO date of latest archived row, or 'none'
      }
    """
    from datetime import timedelta as _td
    d = d or datetime.now()
    y_done, q_done = most_recent_completed_quarter(d)
    pending_qend = quarter_end(y_done, q_done)
    pending_due_date = (pending_qend + _td(days=5)).date()

    # Read archive's latest date (from gSheet — canonical) — falls back to parquet
    df = load_orders_archive()
    if df is None or df.empty:
        return {
            "status": "EMPTY",
            "badge": f"⚠️ No archive — archive {y_done}-Q{q_done}",
            "detail": (
                f"No archive yet. Run the first archive to backfill through "
                f"end of {y_done}-Q{q_done}."
            ),
            "days_until_next": (pending_due_date - d.date()).days,
            "next_due_date": pending_due_date,
            "pending_quarter": f"{y_done}-Q{q_done}",
            "archive_through": "none",
        }

    latest = pd.to_datetime(df["TradeDateTime"], errors="coerce").max()
    archive_through = latest.date().isoformat() if pd.notna(latest) else "unknown"

    if pd.notna(latest) and latest >= pending_qend:
        # Up-to-date — countdown to NEXT quarter's due date
        cur_y, cur_q = current_quarter(d)
        next_due = (quarter_end(cur_y, cur_q) + _td(days=5)).date()
        days_until = (next_due - d.date()).days
        return {
            "status": "OK",
            "badge": f"✅ Archive · next {cur_y}-Q{cur_q} in {days_until}d",
            "detail": (
                f"Archive complete through {archive_through}. "
                f"Next archive due {next_due} (after {cur_y}-Q{cur_q} ends)."
            ),
            "days_until_next": days_until,
            "next_due_date": next_due,
            "pending_quarter": f"{cur_y}-Q{cur_q}",
            "archive_through": archive_through,
        }

    # Archive missing data through the latest completed quarter end → DUE
    days_overdue = (d.date() - pending_due_date).days
    return {
        "status": "DUE",
        "badge": f"🔴 Archive {y_done}-Q{q_done} · overdue {max(0,days_overdue)}d",
        "detail": (
            f"Archive contains data through {archive_through}, but "
            f"{y_done}-Q{q_done} ended {pending_qend.date()}. "
            f"Click Archive Now to append the missing data."
        ),
        "days_until_next": -max(0, days_overdue),
        "next_due_date": pending_due_date,
        "pending_quarter": f"{y_done}-Q{q_done}",
        "archive_through": archive_through,
    }


def delete_archive() -> bool:
    """Wipe the archive (used by 'Reset archive' button)."""
    deleted = False
    if ORDERS_PARQUET.exists():
        try:
            ORDERS_PARQUET.unlink()
            deleted = True
        except Exception as e:
            logger.warning("Failed to delete archive parquet: %s", e)
    if ARCHIVE_META.exists():
        try:
            ARCHIVE_META.unlink()
        except Exception:
            pass
    return deleted
