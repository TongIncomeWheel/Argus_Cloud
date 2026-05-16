"""Top-level data loaders — Tiger API → pandas DataFrames.

This is the single entry point app_v2 uses for all data. Caches aggressively
at the Streamlit level to keep API quota in check while still feeling live.

Functions return DataFrames with stable column names so app pages don't have
to know they came from Tiger.

Cache TTLs:
  - Account state: 30s   (NAV / cash moves with every fill)
  - Open positions: 30s  (same)
  - Closed orders: 5min  (history doesn't move backward; new fills bust cache)
  - NAV history: 10min   (daily granularity — no need to refetch)
  - Funding history: 10min
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd
import streamlit as st

from tiger_api.client import TigerClient, TigerAssets
from tiger_api.adapters import (
    positions_to_argus_rows,
    orders_to_argus_rows,
    DEFAULT_PMCC_TICKERS,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# Singletons / state
# ─────────────────────────────────────────────────────────────────
_CLIENT: Optional[TigerClient] = None


def _client() -> TigerClient:
    """Lazy singleton — one TigerClient per session."""
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = TigerClient()
    return _CLIENT


def get_account() -> str:
    return _client().account


def get_license() -> str:
    return _client().license


def is_sandbox() -> bool:
    return _client().is_sandbox


# ─────────────────────────────────────────────────────────────────
# Account-level
# ─────────────────────────────────────────────────────────────────
@dataclass
class AccountSummary:
    """Lightweight, JSON-serializable account snapshot for caching."""
    nav: float
    cash: float
    realized_pnl_today: float
    unrealized_pnl: float
    currency: str
    bp: float                 # cash_available_for_trade in S-segment (in base currency)
    gross_position_value: float
    equity_with_loan: float
    capability: str
    fetched_at: str           # ISO timestamp
    sandbox: bool
    # Margin & headroom (S-segment, base currency = USD for TBSG)
    init_margin: float = 0.0          # initial margin used by current positions
    maintain_margin: float = 0.0      # maintenance margin requirement
    excess_liquidation: float = 0.0   # NAV − maintain_margin (cushion before forced liquidation)
    leverage: float = 0.0             # gross_position_value / equity_with_loan
    locked_funds: float = 0.0
    # Multi-currency cash breakdown — keyed by currency code.
    # Each value: {'cash_balance': float, 'cash_available': float, 'forex_rate_to_usd': float}
    # Negative cash_balance for a currency = margin loan in that currency.
    currency_cash: dict = None


@st.cache_data(ttl=30, show_spinner="📡 Fetching account state from Tiger…")
def load_account_summary() -> AccountSummary:
    """One-shot account snapshot (NAV, cash, BP, margin) — combines two
    underlying API calls (get_assets + get_prime_assets) into a flat dataclass.

    Returns dataclass (cacheable as immutable). Fields are guaranteed numeric
    or empty string — never None or 'inf'.
    """
    c = _client()
    assets: TigerAssets = c.get_assets()

    # BP & segment detail from prime — and CRITICALLY, sum NAV across all segments
    # because Tiger's get_assets() only returns the SEC ('S') segment. After an
    # MMF subscription (S→F transfer), the SEC NAV drops by the MMF amount,
    # which would make the header show a wildly wrong NAV. We fix this by
    # using prime_assets and summing net_liquidation across ALL segments.
    bp = 0.0
    gpv = 0.0
    eql = 0.0
    cap = "?"
    init_margin = 0.0
    maintain_margin = 0.0
    excess_liq = 0.0
    leverage = 0.0
    locked_funds = 0.0
    currency_cash: dict = {}
    nav_total = 0.0
    nav_from_prime = False
    try:
        prime = c.get_prime_assets()
        if prime and hasattr(prime, "segments"):
            # Sum net_liquidation across ALL segments (S=Securities, F=Funds/MMF, C=Futures)
            for seg_name, seg in prime.segments.items():
                if seg is None:
                    continue
                try:
                    nav_total += float(getattr(seg, "net_liquidation", 0) or 0)
                    nav_from_prime = True
                except (TypeError, ValueError):
                    pass
            seg_s = prime.segments.get("S")
            if seg_s:
                bp = float(getattr(seg_s, "cash_available_for_trade", 0) or 0)
                gpv = float(getattr(seg_s, "gross_position_value", 0) or 0)
                eql = float(getattr(seg_s, "equity_with_loan", 0) or 0)
                cap = str(getattr(seg_s, "capability", "?"))
                init_margin = float(getattr(seg_s, "init_margin", 0) or 0)
                maintain_margin = float(getattr(seg_s, "maintain_margin", 0) or 0)
                excess_liq = float(getattr(seg_s, "excess_liquidation", 0) or 0)
                leverage = float(getattr(seg_s, "leverage", 0) or 0)
                locked_funds = float(getattr(seg_s, "locked_funds", 0) or 0)
                # Multi-currency cash breakdown
                ca = getattr(seg_s, "currency_assets", None) or {}
                for ccy, asset in ca.items():
                    try:
                        currency_cash[str(ccy).upper()] = {
                            "cash_balance": float(getattr(asset, "cash_balance", 0) or 0),
                            "cash_available": float(getattr(asset, "cash_available_for_trade", 0) or 0),
                            "forex_rate_to_usd": float(getattr(asset, "forex_rate", 1.0) or 1.0),
                        }
                    except (TypeError, ValueError):
                        continue
    except Exception as e:
        logger.warning("prime_assets fetch failed: %s", e)

    # Sanitize 'inf' values from get_assets summary (cosmetic SDK bug)
    def _clean(v):
        try:
            f = float(v)
            return 0.0 if (f != f or f in (float("inf"), float("-inf"))) else f
        except (TypeError, ValueError):
            return 0.0

    # NAV: prefer all-segments sum (handles MMF in F segment); fall back to assets.nav
    nav_value = _clean(nav_total) if nav_from_prime else _clean(assets.nav)

    return AccountSummary(
        nav=nav_value,
        cash=_clean(assets.cash),
        realized_pnl_today=_clean(assets.realized_pnl_today),
        unrealized_pnl=_clean(assets.unrealized_pnl),
        currency=str(assets.currency or "USD"),
        bp=_clean(bp),
        gross_position_value=_clean(gpv),
        equity_with_loan=_clean(eql),
        capability=cap,
        fetched_at=datetime.now().isoformat(timespec="seconds"),
        sandbox=c.is_sandbox,
        init_margin=_clean(init_margin),
        maintain_margin=_clean(maintain_margin),
        excess_liquidation=_clean(excess_liq),
        leverage=_clean(leverage),
        locked_funds=_clean(locked_funds),
        currency_cash=currency_cash,
    )


# ─────────────────────────────────────────────────────────────────
# Open positions DataFrame
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=30, show_spinner="📡 Fetching open positions from Tiger…")
def load_open_positions(pmcc_tickers_tuple: tuple = ()) -> pd.DataFrame:
    """Open positions (stocks + options) as a unified DataFrame.

    Args:
        pmcc_tickers_tuple: tuple of ticker symbols to treat as PMCC.
            Tuple (not set) so it's hashable for cache key.

    Columns:
        TradeID, Ticker, TradeType, Direction, Status, Quantity, OptPremium,
        Option_Strike_Price_(USD), Expiry_Date, Date_open, Date_closed,
        StrategyType, Pot, Tiger_Row_Hash, Source, Notes,
        _account, _avg_cost, _market_price, _market_value, _unrealized_pnl,
        _contract_id
    """
    c = _client()
    pmcc_set = set(pmcc_tickers_tuple) | DEFAULT_PMCC_TICKERS
    rows = positions_to_argus_rows(c.get_all_positions(), pmcc_tickers=pmcc_set)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────
# Closed orders DataFrame (for P&L analytics)
# ─────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────
# Permanent MLEG-expansion cache (combos don't change after they fill)
# ─────────────────────────────────────────────────────────────────
# Cold start was dominated by re-expanding the SAME combos every session —
# 59 combos × 1.05s = 80s of pure waiting on Tiger's 60/min rate limit.
#
# Combos are immutable after fill, so we cache per-leg rows by `order_id`
# in a parquet file. Each subsequent session only expands NEW combos
# (typically 0–3 per day for an active wheel trader).
from pathlib import Path as _Path

_MLEG_CACHE_PATH = _Path(__file__).parent.parent / "data" / "mleg_cache.parquet"


def _seed_mleg_cache_from_archive() -> dict:
    """Reconstruct MLEG cache from the gSheet archive (cold-start recovery).

    On Streamlit Cloud the ephemeral filesystem wipes the parquet cache every
    time the app sleeps.  Previously this meant re-expanding ALL ~60 combos
    via Tiger API (1.05s each = 60-90s).

    The archive already contains the expanded legs with:
      - Source = "TigerAPI-LEG (combo <order_id>)"
      - OrderID_Tiger = the parent order ID

    We parse these to rebuild the cache dict[order_id → list[leg_row_dict]],
    save to parquet locally, and return it — zero Tiger API calls needed.
    """
    import re
    try:
        from tiger_api.archive import load_orders_archive
        archive = load_orders_archive()
        if archive.empty:
            return {}
        # Find MLEG-expanded rows — they have Source matching "TigerAPI-LEG (combo ...)"
        if "Source" not in archive.columns:
            return {}
        mask = archive["Source"].astype(str).str.contains("TigerAPI-LEG", na=False)
        mleg_rows = archive[mask].copy()
        if mleg_rows.empty:
            return {}
        # Extract parent order_id from Source field or OrderID_Tiger
        cache = {}
        for _, row in mleg_rows.iterrows():
            # Try OrderID_Tiger first (most reliable)
            oid = str(row.get("OrderID_Tiger", "") or "").strip()
            if not oid:
                # Fallback: parse from Source "TigerAPI-LEG (combo 12345678)"
                m = re.search(r"combo\s+(\d+)", str(row.get("Source", "")))
                if m:
                    oid = m.group(1)
            if not oid:
                continue
            leg = row.to_dict()
            # Remove archive-specific columns that aren't part of the cache
            for drop_col in ("_mleg_order_id",):
                leg.pop(drop_col, None)
            cache.setdefault(oid, []).append(leg)
        if cache:
            _save_mleg_cache(cache)
            logger.info("MLEG cache seeded from archive: %d combos (%d legs)",
                        len(cache), sum(len(v) for v in cache.values()))
        return cache
    except Exception as e:
        logger.warning("MLEG cache seed from archive failed: %s", e)
        return {}


def _load_mleg_cache() -> dict:
    """Read the parquet cache → dict[order_id_str, list[leg_row_dict]].

    On Cloud cold start (parquet wiped), automatically seeds from the gSheet
    archive — avoids 60-90s of Tiger API re-expansion.
    """
    if _MLEG_CACHE_PATH.exists():
        try:
            df = pd.read_parquet(_MLEG_CACHE_PATH)
            if not df.empty and "_mleg_order_id" in df.columns:
                cache = {}
                for oid, grp in df.groupby("_mleg_order_id"):
                    cache[str(oid)] = grp.drop(columns=["_mleg_order_id"]).to_dict("records")
                if cache:
                    return cache
        except Exception as e:
            logger.warning("MLEG cache read failed: %s", e)

    # Parquet missing or empty — seed from archive (Cloud cold-start recovery)
    logger.info("MLEG parquet cache missing — seeding from gSheet archive")
    return _seed_mleg_cache_from_archive()


def _save_mleg_cache(cache: dict) -> None:
    """Persist dict[order_id_str, list[leg_row_dict]] back to parquet."""
    if not cache:
        return
    try:
        _MLEG_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        rows = []
        for oid, legs in cache.items():
            for leg in legs:
                row = dict(leg)
                row["_mleg_order_id"] = str(oid)
                rows.append(row)
        if rows:
            df = pd.DataFrame(rows)
            df.to_parquet(_MLEG_CACHE_PATH, index=False)
    except Exception as e:
        logger.warning("MLEG cache write failed: %s", e)


def clear_mleg_cache() -> bool:
    """Wipe the persistent MLEG expansion cache (force fresh re-expansion next load)."""
    if _MLEG_CACHE_PATH.exists():
        try:
            _MLEG_CACHE_PATH.unlink()
            return True
        except Exception as e:
            logger.warning("MLEG cache delete failed: %s", e)
    return False


# ─────────────────────────────────────────────────────────────────
# Closed orders DataFrame (for P&L analytics)
# ─────────────────────────────────────────────────────────────────
@st.cache_data(
    ttl=300,
    show_spinner="📜 Loading recent transactions (90 days)…",
)
def load_orders(days: int = 90, pmcc_tickers_tuple: tuple = ()) -> pd.DataFrame:
    """All filled orders in the lookback window, with multi-leg combos expanded.

    Each single-leg order is one row. Multi-leg combo orders (Tiger's MLEG —
    diagonal rolls, vertical spreads, etc.) are EXPANDED via Tiger's
    get_transactions(order_id) into one row per leg, each with full contract
    details (strike, expiry, put_call). The combo's realized P&L is attributed
    to the closing (SELL) leg.

    Closing orders carry Tiger's realized_pnl in 'Actual_Profit_(USD)'.

    Performance: previously re-expanded EVERY combo on every cold start
    (~80s for 59 combos due to Tiger's 60/min rate limit). Now uses a
    permanent parquet cache keyed by order_id so each combo is expanded
    exactly ONCE in its lifetime. Typical daily cold start = 0–3 new
    combos to expand = ~3s instead of 80s.
    """
    from tiger_api.adapters import txn_to_argus_row, classify_combo_type

    c = _client()
    orders = c.get_filled_orders(days=days)
    pmcc_set = set(pmcc_tickers_tuple) | DEFAULT_PMCC_TICKERS
    initial_rows = orders_to_argus_rows(orders, pmcc_tickers=pmcc_set)
    if not initial_rows:
        return pd.DataFrame()

    import time as _time

    mleg_pairs = [
        (r, o) for r, o in zip(initial_rows, orders) if r.get("TradeType") == "MULTILEG"
    ]
    non_mleg = [r for r in initial_rows if r.get("TradeType") != "MULTILEG"]
    final_rows = list(non_mleg)

    if mleg_pairs:
        # Load persistent cache; identify which combos still need expansion.
        cache = _load_mleg_cache()
        cached_count = 0
        new_pairs = []
        for argus_row, original_order in mleg_pairs:
            oid = str(getattr(original_order, "id", ""))
            if oid and oid in cache:
                final_rows.extend(cache[oid])
                cached_count += 1
            else:
                new_pairs.append((argus_row, original_order))

        if cached_count and not new_pairs:
            logger.info("MLEG: %d/%d combos served from cache (no API calls)",
                        cached_count, len(mleg_pairs))
        elif new_pairs:
            eta_sec = int(len(new_pairs) * 1.05)
            cache_msg = f" ({cached_count} from cache · {len(new_pairs)} new)" if cached_count else ""
            with st.status(
                f"Expanding {len(new_pairs)} new multi-leg combo orders"
                f"{cache_msg} (~{eta_sec}s · Tiger 60/min rate limit)…",
                expanded=True,
            ) as status:
                progress_bar = st.progress(0.0)
                for i, (argus_row, original_order) in enumerate(new_pairs):
                    if i > 0:
                        _time.sleep(1.05)
                    oid = str(getattr(original_order, "id", ""))
                    try:
                        txns = c.get_order_transactions(original_order.id)
                    except Exception as e:
                        logger.warning("Could not expand MLEG %s: %s", oid, e)
                        txns = []
                    if not txns:
                        fb = dict(argus_row)
                        fb["TradeType"] = "COMBO (failed to expand)"
                        fb["Source"] = "TigerAPI (combo, click 🔄 Refresh to retry expansion)"
                        final_rows.append(fb)
                        # Don't cache failures — try again next session
                    else:
                        ticker_combo = getattr(getattr(original_order, "contract", None), "symbol", "")
                        combo_type = classify_combo_type(txns, ticker_combo, pmcc_set)
                        leg_rows_for_cache = []
                        for t in txns:
                            leg_row = txn_to_argus_row(
                                t, parent_order=original_order,
                                pmcc_tickers=pmcc_set, combo_type=combo_type,
                            )
                            if leg_row:
                                final_rows.append(leg_row)
                                leg_rows_for_cache.append(leg_row)
                        # Cache the successfully-expanded legs
                        if oid and leg_rows_for_cache:
                            cache[oid] = leg_rows_for_cache
                    progress_bar.progress(
                        (i + 1) / len(new_pairs),
                        text=f"Combo {i + 1}/{len(new_pairs)} expanded",
                    )
                # Persist any new entries to disk
                _save_mleg_cache(cache)
                status.update(
                    label=f"✅ Expanded {len(new_pairs)} new · {cached_count} from cache · "
                          f"{len(final_rows)} total rows",
                    state="complete",
                    expanded=False,
                )

    df = pd.DataFrame(final_rows)
    if "TradeDateTime" in df.columns:
        df["TradeDateTime"] = pd.to_datetime(df["TradeDateTime"], errors="coerce")
        df = df.sort_values("TradeDateTime", ascending=False).reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────────────
# Funding (deposits/withdrawals)
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner="🏦 Fetching funding history…")
def load_funding_history() -> pd.DataFrame:
    """Lifetime deposits & withdrawals with explicit currency.
    Columns from Tiger: id, ref_id, type, type_desc, currency, amount,
    business_date, completed_status, updated_at, created_at.
    """
    return _client().get_funding_history()


@st.cache_data(ttl=600, show_spinner="💰 Fetching Tiger Vault (MMF) history…")
def load_vault_history() -> pd.DataFrame:
    """Tiger Vault (SGD/USD MMF) activity — subscriptions, redemptions, transfers.

    Returns df with columns: id, currency, type, desc, contract_name, seg_type,
    amount (signed), business_date, updated_at.

    `type` values include: 'Funds Transfer In/Out', 'Fund Subscription', 'Trade'
    (buy/sell of MMF), 'Campaign Subsidy' (interest accruals).
    """
    try:
        df = _client()._trade_client.get_fund_details(
            seg_types=["FUND"],
            start_date="2024-01-01",
            end_date=datetime.now().strftime("%Y-%m-%d"),
        )
        if not isinstance(df, pd.DataFrame):
            return pd.DataFrame()
        if df.empty:
            return df
        # Normalize: parse business_date as datetime, sort descending
        df = df.copy()
        if "business_date" in df.columns:
            df["business_date"] = pd.to_datetime(df["business_date"], errors="coerce")
            df = df.sort_values("business_date", ascending=False).reset_index(drop=True)
        return df
    except Exception as e:
        logger.warning("get_fund_details failed: %s", e)
        return pd.DataFrame()


def vault_summary() -> dict:
    """Summary of current Tiger Vault state + lifetime activity.

    Returns:
      {
        'current_balance_sgd': float,  # 0 if liquidated
        'current_balance_usd': float,
        'lifetime_buys_sgd': float,    # cumulative subscriptions (positive)
        'lifetime_sells_sgd': float,   # cumulative redemptions (positive value)
        'last_activity_date': str,
        'fund_names': list[str],
      }
    """
    df = load_vault_history()
    out = {
        "current_balance_sgd": 0.0,
        "current_balance_usd": 0.0,
        "lifetime_buys_sgd": 0.0,
        "lifetime_sells_sgd": 0.0,
        "last_activity_date": None,
        "fund_names": [],
    }
    if df.empty:
        return out
    try:
        # Trade rows are buys (positive) / sells (negative) of fund units
        if "type" in df.columns and "amount" in df.columns:
            trades = df[df["type"].astype(str) == "Trade"].copy()
            if not trades.empty:
                trades["amount_num"] = pd.to_numeric(trades["amount"], errors="coerce").fillna(0)
                # Buys (negative amount in FUND seg = cash leaving FUND to buy units)
                buys = trades[trades["amount_num"] < 0]["amount_num"].abs().sum()
                # Sells (positive amount = units sold returning cash)
                sells = trades[trades["amount_num"] > 0]["amount_num"].sum()
                out["lifetime_buys_sgd"] = float(buys)
                out["lifetime_sells_sgd"] = float(sells)
                out["current_balance_sgd"] = float(buys - sells)  # net subscribed remaining
            if "contract_name" in df.columns:
                names = (
                    df[df["contract_name"].astype(str).str.strip() != ""]["contract_name"]
                    .dropna().unique().tolist()
                )
                out["fund_names"] = sorted(names)
            if "business_date" in df.columns and not df["business_date"].dropna().empty:
                last = df["business_date"].dropna().max()
                out["last_activity_date"] = last.strftime("%Y-%m-%d")
    except Exception as e:
        logger.warning("vault_summary parse failed: %s", e)
    return out


# ─────────────────────────────────────────────────────────────────
# FX trades — SGD↔USD spot conversions
# ─────────────────────────────────────────────────────────────────
# Tiger records FX trades in the SEC fund_details endpoint as PAIRED rows:
#   • type='Currency Exchange - Base Currency'      → USD leg (positive amount = received)
#   • type='Currency Exchange - Quotation Currency' → SGD leg (negative amount = spent)
# Both legs share the SAME `updated_at` timestamp + `desc`, so we group by that.
#
# This endpoint is rate-limited at 10/min. We cache for 10 min and pull a wide
# date window so a single call covers history.
@st.cache_data(ttl=600, show_spinner="💱 Fetching FX trade history…")
def load_fx_trades(start_date: str = "2024-01-01") -> pd.DataFrame:
    """Return paired FX trades — one row per conversion (not per leg).

    Columns:
      trade_date     — date of the conversion
      pair           — 'SGD→USD' or 'USD→SGD'
      from_ccy       — currency given up
      from_amount    — positive amount of from_ccy spent
      to_ccy         — currency received
      to_amount      — positive amount of to_ccy received
      rate           — implied rate (from / to)
      desc           — Tiger's description string
    """
    import time as _t
    today = datetime.now().strftime("%Y-%m-%d")
    tc = _client()._trade_client
    # Pull SEC fund_details with pagination (max limit=100/call, 10 calls/min cap).
    # Sleep 7s between calls to stay safely under the rate limit (≈8.5 calls/min).
    all_rows = []
    for page in range(20):  # safety cap (max ~2000 rows)
        if page > 0:
            _t.sleep(7)
        try:
            df = tc.get_fund_details(
                seg_types=["SEC"],
                start_date=start_date, end_date=today,
                start=page * 100, limit=100,
            )
        except Exception as e:
            err = str(e).lower()
            if "rate limit" in err or "code=4" in err:
                logger.info("FX fund_details rate limit; sleep 65s page %d", page)
                _t.sleep(65)
                try:
                    df = tc.get_fund_details(
                        seg_types=["SEC"],
                        start_date=start_date, end_date=today,
                        start=page * 100, limit=100,
                    )
                except Exception as e2:
                    logger.warning("FX fund_details retry failed page %d: %s", page, e2)
                    break
            else:
                logger.warning("FX fund_details page %d failed: %s", page, e)
                break
        if not isinstance(df, pd.DataFrame) or df.empty:
            break
        all_rows.append(df)
        logger.info("FX fund_details page %d: %d rows", page, len(df))
        if len(df) < 100:
            break
    if not all_rows:
        return pd.DataFrame()
    full = pd.concat(all_rows, ignore_index=True)

    # Filter for FX rows only
    fx_mask = full["type"].astype(str).str.contains("Currency Exchange", na=False)
    fx = full[fx_mask].copy()
    if fx.empty:
        return pd.DataFrame()

    # Pair the legs: same updated_at + same desc → one trade
    pairs = []
    for (ts, desc), grp in fx.groupby(["updated_at", "desc"]):
        if len(grp) != 2:
            continue
        base = grp[grp["type"].astype(str).str.contains("Base", na=False)]
        quot = grp[grp["type"].astype(str).str.contains("Quotation", na=False)]
        if base.empty or quot.empty:
            continue
        b = base.iloc[0]; q = quot.iloc[0]
        b_amt = float(b["amount"]); q_amt = float(q["amount"])
        # The leg with NEGATIVE amount is "from" (spent); positive is "to" (received)
        if q_amt < 0 and b_amt > 0:
            from_ccy, from_amt = q["currency"], abs(q_amt)
            to_ccy, to_amt = b["currency"], abs(b_amt)
        elif b_amt < 0 and q_amt > 0:
            from_ccy, from_amt = b["currency"], abs(b_amt)
            to_ccy, to_amt = q["currency"], abs(q_amt)
        else:
            continue  # skip malformed
        rate = from_amt / to_amt if to_amt > 0 else 0.0
        pairs.append({
            "trade_date": pd.to_datetime(b["business_date"], errors="coerce"),
            "pair": f"{from_ccy}→{to_ccy}",
            "from_ccy": from_ccy,
            "from_amount": from_amt,
            "to_ccy": to_ccy,
            "to_amount": to_amt,
            "rate": rate,
            "desc": str(desc) if desc else "",
        })
    if not pairs:
        return pd.DataFrame()
    out = pd.DataFrame(pairs).sort_values("trade_date", ascending=False).reset_index(drop=True)
    return out


def fx_position_summary(current_fx_sgd_per_usd: float) -> dict:
    """Compute lifetime FX activity stats + unrealized FX P&L on net USD bought.

    Args:
      current_fx_sgd_per_usd: spot rate today (e.g. 1.2766 means 1 USD = 1.2766 SGD)

    Returns:
      {
        'lifetime_sgd_to_usd': S$ spent buying USD (sum),
        'lifetime_usd_received': $ USD received from those buys,
        'avg_buy_rate':           weighted avg rate (SGD per USD),
        'current_rate':           today's rate,
        'unrealized_fx_pnl_sgd':  paper P&L on the cumulative net long USD,
        'unrealized_fx_pnl_usd':  same, USD-eq,
        'trade_count':            # of conversions,
        'last_trade_date':        most recent FX trade,
      }
    """
    df = load_fx_trades()
    out = {
        "lifetime_sgd_to_usd": 0.0,
        "lifetime_usd_received": 0.0,
        "avg_buy_rate": 0.0,
        "current_rate": float(current_fx_sgd_per_usd or 0),
        "unrealized_fx_pnl_sgd": 0.0,
        "unrealized_fx_pnl_usd": 0.0,
        "trade_count": 0,
        "last_trade_date": None,
    }
    if df.empty:
        return out
    # Filter SGD→USD trades (the meaningful direction for our SGD-base account)
    sgd_to_usd = df[df["pair"] == "SGD→USD"]
    if not sgd_to_usd.empty:
        sgd_total = float(sgd_to_usd["from_amount"].sum())
        usd_total = float(sgd_to_usd["to_amount"].sum())
        out["lifetime_sgd_to_usd"] = sgd_total
        out["lifetime_usd_received"] = usd_total
        out["avg_buy_rate"] = sgd_total / usd_total if usd_total > 0 else 0
        # Unrealized FX P&L: USD bought × (current rate − avg buy rate)
        # If SGD strengthened (rate down): the USD you bought is now worth fewer SGD → loss
        if current_fx_sgd_per_usd and out["avg_buy_rate"]:
            out["unrealized_fx_pnl_sgd"] = (current_fx_sgd_per_usd - out["avg_buy_rate"]) * usd_total
            out["unrealized_fx_pnl_usd"] = out["unrealized_fx_pnl_sgd"] / current_fx_sgd_per_usd
    out["trade_count"] = len(df)
    if "trade_date" in df.columns and not df["trade_date"].dropna().empty:
        out["last_trade_date"] = df["trade_date"].dropna().max().strftime("%Y-%m-%d")
    return out


# ─────────────────────────────────────────────────────────────────
# Lifetime cash flow picture — where every dollar lives
# ─────────────────────────────────────────────────────────────────
def compute_cash_flow_picture(summary, df_open: pd.DataFrame, settings: dict,
                               include_fx: bool = False) -> dict:
    """Build the lifetime "where is my cash" trace.

    Combines:
      • get_funding_history       — lifetime SGD deposits (fast, cached 10min)
      • vault_summary             — MMF subscribe/redeem/yield (fast, cached 10min)
      • df_open + cost basis      — current long position value (cost basis)
      • prime_assets currency_cash — current SGD/USD balances
      • init_margin               — Tiger's actual margin requirement
      • load_fx_trades            — ONLY if include_fx=True (slow first call:
                                     paginates SEC fund_details, ~60-70s due to
                                     10/min rate limit; cached 10min after).

    Returns a dict with keys for the 'inflows', 'current_state', and 'outflows'.
    All amounts in USD-eq for portability except where noted.
    """
    out = {
        "deposits_sgd": 0.0,
        "deposits_usd_eq": 0.0,
        "fx_conversions": {
            "sgd_spent": 0.0, "usd_received": 0.0,
            "trade_count": 0, "avg_rate": 0.0,
        },
        "mmf": {
            "lifetime_subscribed_sgd": 0.0,
            "lifetime_redeemed_sgd": 0.0,
            "lifetime_yield_sgd": 0.0,
            "current_balance_sgd": 0.0,
        },
        "current": {
            "sgd_idle_cash": 0.0,           # SGD in SEC, not in MMF
            "sgd_idle_cash_usd_eq": 0.0,
            "usd_loan": 0.0,                 # negative cash = margin loan
            "long_stock_cost": 0.0,
            "long_leap_cost": 0.0,
            "short_csp_collateral_policy": 0.0,
            "tiger_init_margin": 0.0,
            "nav": 0.0,
            "gross_position_value": 0.0,
        },
        "fx_rate_sgd_usd": 0.0,
    }

    # FX rate (USD per SGD, i.e. 1 SGD = X USD)
    fx_usd_per_sgd = float(settings.get("sgd_usd_fx_rate_inverse", 0) or 0)
    if not fx_usd_per_sgd and summary:
        # derive from currency_cash if available
        try:
            sgd_data = summary.currency_cash.get("SGD", {})
            fx_usd_per_sgd = float(sgd_data.get("forex_rate_to_usd", 0) or 0)
        except Exception:
            pass
    if not fx_usd_per_sgd:
        fx_sgd_per_usd_setting = float(settings.get("sgd_usd_fx_rate", 1.276) or 1.276)
        fx_usd_per_sgd = 1.0 / fx_sgd_per_usd_setting if fx_sgd_per_usd_setting else 0.7833
    fx_sgd_per_usd = 1.0 / fx_usd_per_sgd if fx_usd_per_sgd else 1.276
    out["fx_rate_sgd_usd"] = fx_sgd_per_usd

    # Deposits (lifetime, SGD only — Tiger TBSG funded in SGD)
    try:
        funding = load_funding_history()
        if not funding.empty and "type" in funding.columns:
            deposits = funding[funding["type"] == 1]
            if not deposits.empty and "amount" in deposits.columns:
                # Filter SGD only (USD deposits would be type=1 with currency='USD')
                if "currency" in deposits.columns:
                    sgd_dep = deposits[deposits["currency"] == "SGD"]
                    out["deposits_sgd"] = float(sgd_dep["amount"].sum())
                else:
                    out["deposits_sgd"] = float(deposits["amount"].sum())
                out["deposits_usd_eq"] = out["deposits_sgd"] * fx_usd_per_sgd
    except Exception as e:
        logger.warning("compute_cash_flow_picture: deposits failed: %s", e)

    # FX conversions (SGD→USD historical) — only if explicitly requested
    # (the SEC fund_details paginated call is slow on first hit; user opts-in
    # via "Load FX trades" button in the Cash Maximization panel).
    if include_fx:
        try:
            fx_summ = fx_position_summary(fx_sgd_per_usd)
            out["fx_conversions"]["sgd_spent"] = fx_summ["lifetime_sgd_to_usd"]
            out["fx_conversions"]["usd_received"] = fx_summ["lifetime_usd_received"]
            out["fx_conversions"]["trade_count"] = fx_summ["trade_count"]
            out["fx_conversions"]["avg_rate"] = fx_summ["avg_buy_rate"]
        except Exception as e:
            logger.warning("compute_cash_flow_picture: fx failed: %s", e)

    # MMF activity
    try:
        v = vault_summary()
        out["mmf"]["lifetime_subscribed_sgd"] = v.get("lifetime_buys_sgd", 0)
        out["mmf"]["lifetime_redeemed_sgd"] = v.get("lifetime_sells_sgd", 0)
        out["mmf"]["lifetime_yield_sgd"] = v.get("lifetime_sells_sgd", 0) - v.get("lifetime_buys_sgd", 0)
        out["mmf"]["current_balance_sgd"] = max(0, v.get("current_balance_sgd", 0))
    except Exception as e:
        logger.warning("compute_cash_flow_picture: mmf failed: %s", e)

    # Current state — from summary + open positions
    if summary:
        out["current"]["nav"] = float(summary.nav or 0)
        out["current"]["gross_position_value"] = float(summary.gross_position_value or 0)
        out["current"]["tiger_init_margin"] = float(getattr(summary, "init_margin", 0) or 0)
        try:
            sgd_d = summary.currency_cash.get("SGD", {})
            usd_d = summary.currency_cash.get("USD", {})
            out["current"]["sgd_idle_cash"] = float(sgd_d.get("cash_balance", 0) or 0)
            out["current"]["sgd_idle_cash_usd_eq"] = out["current"]["sgd_idle_cash"] * fx_usd_per_sgd
            usd_bal = float(usd_d.get("cash_balance", 0) or 0)
            out["current"]["usd_loan"] = abs(usd_bal) if usd_bal < 0 else 0
        except Exception as e:
            logger.warning("compute_cash_flow_picture: cash split failed: %s", e)

    # Position cost bases
    if df_open is not None and not df_open.empty:
        try:
            d = df_open.copy()
            d["q"] = pd.to_numeric(d["Quantity"], errors="coerce").fillna(0).abs()
            d["k"] = pd.to_numeric(d["Option_Strike_Price_(USD)"], errors="coerce").fillna(0)
            d["avg_cost"] = pd.to_numeric(d.get("_avg_cost", 0), errors="coerce").fillna(0)
            stk = d[d["TradeType"] == "STOCK"]
            leap = d[d["TradeType"] == "LEAP"]
            csp = d[d["TradeType"] == "CSP"]
            out["current"]["long_stock_cost"] = float((stk["q"] * stk["avg_cost"]).sum())
            out["current"]["long_leap_cost"] = float((leap["q"] * leap["avg_cost"] * 100).sum())
            out["current"]["short_csp_collateral_policy"] = float((csp["q"] * csp["k"] * 100).sum())
        except Exception as e:
            logger.warning("compute_cash_flow_picture: positions failed: %s", e)

    return out


# ─────────────────────────────────────────────────────────────────
# Carry analysis — true cost of margin with MMF yield offset
# ─────────────────────────────────────────────────────────────────
def compute_carry_analysis(usd_loan: float, sgd_idle: float, fx_sgd_per_usd: float,
                           margin_rate_pct: float = 7.0,
                           mmf_yield_pct: float = 3.5) -> dict:
    """True net cost of holding the current USD margin loan.

    Args:
      usd_loan         — absolute size of USD margin loan (positive number)
      sgd_idle         — SGD cash sitting idle in SEC (potential MMF candidate)
      fx_sgd_per_usd   — SGD per USD spot rate
      margin_rate_pct  — Tiger's margin lending rate (default 7%; user-configurable)
      mmf_yield_pct    — Estimated SGD MMF yield (default 3.5%; user-configurable)

    Returns:
      {
        'annual_interest_cost_usd':  absolute $ cost
        'daily_interest_cost_usd':   $/day
        'potential_mmf_offset_usd':  $/yr if SGD parked in MMF
        'net_annual_carry_usd':      cost minus offset
        'breakeven_mmf_yield_pct':   yield required to fully offset
        'offset_pct':                % of cost that MMF would cover
      }
    """
    out = {
        "annual_interest_cost_usd": 0.0,
        "daily_interest_cost_usd": 0.0,
        "potential_mmf_offset_usd": 0.0,
        "potential_mmf_offset_sgd": 0.0,
        "net_annual_carry_usd": 0.0,
        "breakeven_mmf_yield_pct": 0.0,
        "offset_pct": 0.0,
        "margin_rate_pct": float(margin_rate_pct),
        "mmf_yield_pct": float(mmf_yield_pct),
    }
    try:
        loan = max(float(usd_loan or 0), 0)
        sgd = max(float(sgd_idle or 0), 0)
        fx = float(fx_sgd_per_usd or 0)
        m = float(margin_rate_pct or 0) / 100.0
        y = float(mmf_yield_pct or 0) / 100.0

        out["annual_interest_cost_usd"] = loan * m
        out["daily_interest_cost_usd"] = out["annual_interest_cost_usd"] / 365.0

        sgd_yield = sgd * y                 # SGD yield earned annually
        out["potential_mmf_offset_sgd"] = sgd_yield
        if fx > 0:
            out["potential_mmf_offset_usd"] = sgd_yield / fx

        out["net_annual_carry_usd"] = (
            out["annual_interest_cost_usd"] - out["potential_mmf_offset_usd"]
        )
        if loan > 0 and m > 0 and fx > 0 and sgd > 0:
            # Yield needed on SGD MMF to break even = (loan × m × fx) / sgd
            out["breakeven_mmf_yield_pct"] = (loan * m * fx) / sgd * 100.0
        if out["annual_interest_cost_usd"] > 0:
            out["offset_pct"] = (
                out["potential_mmf_offset_usd"] / out["annual_interest_cost_usd"] * 100.0
            )
    except Exception as e:
        logger.warning("compute_carry_analysis failed: %s", e)
    return out


# ─────────────────────────────────────────────────────────────────
# Vault pull alert — flash if Tiger auto-redeemed MMF recently
# ─────────────────────────────────────────────────────────────────
def detect_vault_pull_alert(window_days: int = 14, min_amount_sgd: float = 5000.0) -> dict:
    """Detect recent FUND→SEC transfers (Tiger auto-pulling MMF for margin).

    Used to flash a header alert when significant MMF redemption happened in the
    recent window. The user gets no Tiger notification for these events, so the
    delta vs prior session is the only trigger.

    Returns:
      {
        'alert':    bool,             # True if at least one pull >= min_amount_sgd in window
        'events':   list of dicts,    # the qualifying transfer rows
        'total_sgd': float,           # cumulative S$ pulled in window
        'newest_date': str|None,
      }
    """
    out = {"alert": False, "events": [], "total_sgd": 0.0, "newest_date": None}
    try:
        seg = _client().get_segment_fund_history()
        if not seg:
            return out
        cutoff_ms = int((datetime.now() - pd.Timedelta(days=window_days)).timestamp() * 1000)
        events = []
        for s in seg:
            from_seg = getattr(s, "from_segment", "")
            to_seg = getattr(s, "to_segment", "")
            if from_seg != "FUND" or to_seg != "SEC":
                continue
            ts = getattr(s, "created_at", 0) or 0
            if ts < cutoff_ms:
                continue
            amount = float(getattr(s, "amount", 0) or 0)
            if amount < min_amount_sgd:
                continue
            events.append({
                "date": pd.Timestamp(ts, unit="ms").strftime("%Y-%m-%d"),
                "amount_sgd": amount,
                "currency": getattr(s, "currency", "?"),
                "status": getattr(s, "status_desc", "?"),
            })
        if events:
            out["alert"] = True
            out["events"] = events
            out["total_sgd"] = float(sum(e["amount_sgd"] for e in events))
            out["newest_date"] = max(e["date"] for e in events)
    except Exception as e:
        logger.warning("detect_vault_pull_alert failed: %s", e)
    return out


# ─────────────────────────────────────────────────────────────────
# Daily NAV / P&L history
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner="📈 Fetching NAV history…")
def load_nav_history(days: int = 365) -> pd.DataFrame:
    """Daily NAV / cash / position value time series.

    Columns: date (datetime), asset, pnl, pnl_percentage, cash_balance,
    gross_position_value, deposit (any others Tiger adds).
    """
    raw = _client().get_nav_history(days=days)
    history = (raw or {}).get("history", [])
    if not history:
        return pd.DataFrame()
    df = pd.DataFrame(history)
    if "date" in df.columns:
        # Tiger gives ms timestamps
        df["date"] = pd.to_datetime(df["date"], unit="ms", errors="coerce")
        df = df.sort_values("date").reset_index(drop=True)
    return df


@st.cache_data(ttl=600, show_spinner=False)
def load_nav_summary(days: int = 365) -> dict:
    """Summary stats from get_nav_history: pnl, pnl_percentage, annualized_return."""
    raw = _client().get_nav_history(days=days)
    return (raw or {}).get("summary", {}) or {}


def _yfinance_spot_prices(symbols: list) -> dict:
    """Primary spot fetcher via Yahoo Finance — free, real-time-ish, no auth."""
    if not symbols:
        return {}
    try:
        import yfinance as yf
        tickers = yf.Tickers(" ".join(symbols))
        out = {}
        for sym in symbols:
            try:
                t = tickers.tickers.get(sym.upper())
                if not t:
                    continue
                fast = getattr(t, "fast_info", None)
                price = None
                if fast is not None:
                    price = (
                        fast.get("last_price")
                        or fast.get("lastPrice")
                        or fast.get("regular_market_price")
                    )
                if not price:
                    info = t.info or {}
                    price = info.get("regularMarketPrice") or info.get("currentPrice")
                if price:
                    out[sym.upper()] = float(price)
            except Exception:
                continue
        return out
    except Exception as e:
        logger.warning("yfinance spot fetch failed: %s", e)
        return {}


def _alpaca_spot_prices(symbols: list) -> dict:
    """Fallback spot fetcher via Alpaca (free tier = IEX feed, sometimes stale)."""
    import os
    api_key = os.getenv("ALPACA_API_KEY")
    secret = os.getenv("ALPACA_SECRET_KEY")
    if not api_key or not secret or not symbols:
        return {}
    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockLatestQuoteRequest
        client = StockHistoricalDataClient(api_key, secret)
        req = StockLatestQuoteRequest(symbol_or_symbols=symbols)
        quotes = client.get_stock_latest_quote(req)
        out = {}
        for sym, q in quotes.items():
            bid = float(getattr(q, "bid_price", 0) or 0)
            ask = float(getattr(q, "ask_price", 0) or 0)
            if bid and ask:
                out[sym.upper()] = (bid + ask) / 2
            elif ask:
                out[sym.upper()] = ask
            elif bid:
                out[sym.upper()] = bid
        return out
    except Exception as e:
        logger.warning("Alpaca spot fetch failed: %s", e)
        return {}


@st.cache_data(ttl=3600, show_spinner=False)
def load_earnings_calendar(tickers_tuple: tuple) -> dict:
    """Earnings dates per ticker from yfinance.

    Returns: {ticker: next_earnings_date (datetime.date) | None}.
    Cached 1 hour — earnings dates rarely change intraday.
    """
    out = {}
    if not tickers_tuple:
        return out
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not installed — earnings calendar unavailable")
        return out
    for tkr in tickers_tuple:
        try:
            t = yf.Ticker(tkr)
            cal = getattr(t, "calendar", None)
            if not cal:
                continue
            ed = cal.get("Earnings Date") if isinstance(cal, dict) else None
            if isinstance(ed, list) and ed:
                # Take the nearest earnings date (could be a range — pick first)
                out[tkr.upper()] = ed[0]
            elif ed:
                out[tkr.upper()] = ed
        except Exception as e:
            logger.debug("Earnings fetch failed for %s: %s", tkr, e)
            continue
    return out


@st.cache_data(ttl=120, show_spinner="📈 Fetching option quotes (Alpaca)…")
def load_option_quotes(positions_key: tuple) -> dict:
    """Per-option bid/ask/last/mid + IV + real Greeks from Alpaca.

    Tiger denies option quote permissions for retail TBSG, so we use Alpaca's
    OptionSnapshot endpoint. One call returns latest_quote (bid/ask), latest_trade
    (last), implied_volatility, AND broker-quality Greeks — all batched in one
    HTTP request (chunks of 100 symbols).

    Args:
      positions_key: tuple of tuples (ticker, expiry_iso, strike, put_call)
        — must be hashable for cache key. expiry as 'YYYY-MM-DD'.

    Returns:
      {(ticker, expiry, strike, put_call): {
         'bid', 'ask', 'last', 'mid', 'iv',
         'delta_alpaca', 'gamma_alpaca', 'theta_alpaca', 'vega_alpaca',
      }} — entries with missing data are still present with None values.
    """
    if not positions_key:
        return {}
    import os
    api_key = os.getenv("ALPACA_API_KEY")
    secret = os.getenv("ALPACA_SECRET_KEY")
    if not api_key or not secret:
        logger.warning("Alpaca credentials missing — option quotes unavailable")
        return {}

    # Build OCC option symbols (e.g. COIN260522P00180000) — Alpaca's identifier
    occ_to_key = {}
    for entry in positions_key:
        try:
            tkr, exp, strike, pc = entry
            yymmdd = str(exp).replace("-", "")[2:]  # 'YYMMDD' from 'YYYY-MM-DD'
            strike_int = int(round(float(strike) * 1000))
            cp = "C" if str(pc).upper().startswith("C") else "P"
            occ = f"{str(tkr).upper()}{yymmdd}{cp}{strike_int:08d}"
            occ_to_key[occ] = (str(tkr).upper(), str(exp), float(strike), cp)
        except Exception:
            continue
    if not occ_to_key:
        return {}

    try:
        from alpaca.data.historical import OptionHistoricalDataClient
        from alpaca.data.requests import OptionSnapshotRequest
        client = OptionHistoricalDataClient(api_key, secret)
    except Exception as e:
        logger.warning("Alpaca client init failed: %s", e)
        return {}

    out = {}
    symbols = list(occ_to_key.keys())
    # Alpaca limit: 100 symbols per OptionSnapshot request — chunk if needed
    for i in range(0, len(symbols), 100):
        chunk = symbols[i:i + 100]
        try:
            req = OptionSnapshotRequest(symbol_or_symbols=chunk)
            snaps = client.get_option_snapshot(req)
        except Exception as e:
            logger.warning("Alpaca option snapshot chunk %d-%d failed: %s",
                           i, i + len(chunk), e)
            continue
        for occ_sym, snap in (snaps or {}).items():
            key = occ_to_key.get(occ_sym)
            if not key:
                continue
            lq = getattr(snap, "latest_quote", None)
            lt = getattr(snap, "latest_trade", None)
            greeks = getattr(snap, "greeks", None)
            bid = float(lq.bid_price) if lq and lq.bid_price else None
            ask = float(lq.ask_price) if lq and lq.ask_price else None
            last = float(lt.price) if lt and lt.price else None
            mid = ((bid + ask) / 2) if (bid and ask and bid > 0 and ask > 0) else None
            iv = getattr(snap, "implied_volatility", None)
            iv = float(iv) if iv else None
            d_a = float(greeks.delta) if greeks and greeks.delta is not None else None
            g_a = float(greeks.gamma) if greeks and greeks.gamma is not None else None
            t_a = float(greeks.theta) if greeks and greeks.theta is not None else None
            v_a = float(greeks.vega) if greeks and greeks.vega is not None else None
            out[key] = {
                "bid": bid, "ask": ask, "last": last, "mid": mid, "iv": iv,
                "delta_alpaca": d_a, "gamma_alpaca": g_a,
                "theta_alpaca": t_a, "vega_alpaca": v_a,
            }
    return out


@st.cache_data(ttl=60, show_spinner="💲 Fetching spot prices…")
def load_spot_prices(symbols_tuple: tuple) -> dict:
    """Cached spot price lookup — 1-minute TTL. Multi-source fallback:

       1. yfinance (free, low-lag, no auth) — primary
       2. Alpaca (IEX, ~15min lag) — fallback
       3. Tiger get_briefs (often denied for retail TBSG) — last resort

    Returns dict {ticker: spot_price}. Missing tickers omitted.
    """
    symbols = list(symbols_tuple)
    if not symbols:
        return {}
    out = _yfinance_spot_prices(symbols)
    missing = [s for s in symbols if s not in out]
    if missing:
        out.update(_alpaca_spot_prices(missing))
    missing = [s for s in symbols if s not in out]
    if missing:
        out.update(_client().get_spot_prices(missing))
    return out


# ─────────────────────────────────────────────────────────────────
# Full-history loader (live + archive merged)
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="📊 Combining live + archive history…")
def load_orders_full(pmcc_tickers_tuple: tuple = ()) -> pd.DataFrame:
    """Live (last 90 days) + on-disk archive, deduped by TradeID.

    Used by P&L analytics for YTD / MTD / lifetime slicing — keeps fast cold
    starts (90-day live load) while preserving deep history via the archive.
    """
    from tiger_api.archive import merge_with_archive
    live = load_orders(days=90, pmcc_tickers_tuple=pmcc_tickers_tuple)
    return merge_with_archive(live)


def append_to_archive(pmcc_tickers_tuple: tuple = (), force_days_back: int = 0) -> dict:
    """Smart-window archive append.

    Pulls only the data needed to fill the gap between the existing archive's
    latest TradeDateTime and today (with a 7-day buffer for late settlements).

      • First-ever archive: pulls 365 days (full backfill from Tiger's window)
      • Routine quarterly: pulls ~95-100 days (last quarter + buffer)
      • Skipped quarter: pulls enough to bridge the gap (auto-adapts)
      • force_days_back > 0: override (used for explicit "rebuild last N days")

    Dedupes by TradeID against existing archive, prefers the NEW data
    (fresher classification) on conflict. Saves to BOTH gSheet and parquet.
    """
    from tiger_api.adapters import txn_to_argus_row, classify_combo_type
    from tiger_api.archive import save_orders_archive, load_orders_archive
    import time as _time

    c = _client()
    pmcc_set = set(pmcc_tickers_tuple) | DEFAULT_PMCC_TICKERS

    # Read existing archive to determine pull window
    existing = load_orders_archive()
    if force_days_back and force_days_back > 0:
        days_back = int(force_days_back)
    elif existing.empty:
        days_back = 365  # first archive — full backfill
    else:
        latest = pd.to_datetime(existing.get("TradeDateTime"), errors="coerce").max()
        if pd.isna(latest):
            days_back = 365
        else:
            gap_days = (datetime.now() - latest).days
            days_back = max(90, gap_days + 7)  # 7-day buffer for late fills
    days_back = min(days_back, 600)  # Tiger's max window

    # Fetch
    orders = c.get_filled_orders(days=days_back)
    initial_rows = orders_to_argus_rows(orders, pmcc_tickers=pmcc_set)
    if not initial_rows:
        return {"rows": 0, "ok": False, "msg": "No orders returned from Tiger."}

    mleg_pairs = [
        (r, o) for r, o in zip(initial_rows, orders) if r.get("TradeType") == "MULTILEG"
    ]
    non_mleg = [r for r in initial_rows if r.get("TradeType") != "MULTILEG"]
    new_rows = list(non_mleg)

    if mleg_pairs:
        eta = int(len(mleg_pairs) * 1.05)
        with st.status(
            f"Archiving · pulling {days_back}d · expanding {len(mleg_pairs)} combos (~{eta}s)…",
            expanded=True,
        ) as status:
            progress = st.progress(0.0)
            for i, (argus_row, original_order) in enumerate(mleg_pairs):
                if i > 0:
                    _time.sleep(1.05)
                try:
                    txns = c.get_order_transactions(original_order.id)
                except Exception as e:
                    logger.warning("Archive append: MLEG %s expand failed: %s", original_order.id, e)
                    txns = []
                if not txns:
                    fb = dict(argus_row)
                    fb["TradeType"] = "COMBO (failed to expand)"
                    fb["Source"] = "TigerAPI (combo, expand failed)"
                    new_rows.append(fb)
                else:
                    ticker_combo = getattr(getattr(original_order, "contract", None), "symbol", "")
                    combo_type = classify_combo_type(txns, ticker_combo, pmcc_set)
                    for t in txns:
                        leg = txn_to_argus_row(
                            t, parent_order=original_order,
                            pmcc_tickers=pmcc_set, combo_type=combo_type,
                        )
                        if leg:
                            new_rows.append(leg)
                progress.progress(
                    (i + 1) / len(mleg_pairs),
                    text=f"Combo {i + 1}/{len(mleg_pairs)} · {len(new_rows)} new rows",
                )
            status.update(
                label=f"✅ Pulled {len(new_rows)} rows · merging with archive…",
                state="complete", expanded=False,
            )

    new_df = pd.DataFrame(new_rows)
    if "TradeDateTime" in new_df.columns:
        new_df["TradeDateTime"] = pd.to_datetime(new_df["TradeDateTime"], errors="coerce")

    # Merge with existing archive — dedupe by TradeID, NEW data wins on conflict
    if existing.empty:
        combined = new_df
        added_count = len(new_df)
    else:
        prior_ids = set(existing.get("TradeID", pd.Series(dtype=str)).astype(str).tolist())
        combined = pd.concat([existing, new_df], ignore_index=True, sort=False)
        if "TradeID" in combined.columns:
            combined = combined.drop_duplicates(subset=["TradeID"], keep="last")
        if "TradeDateTime" in combined.columns:
            combined["TradeDateTime"] = pd.to_datetime(combined["TradeDateTime"], errors="coerce")
            combined = combined.sort_values("TradeDateTime", ascending=False).reset_index(drop=True)
        new_ids = set(new_df.get("TradeID", pd.Series(dtype=str)).astype(str).tolist())
        added_count = len(new_ids - prior_ids)

    save_status = save_orders_archive(combined)
    return {
        "ok": True,
        "rows": len(combined),
        "added": added_count,
        "days_back": days_back,
        "parquet_ok": save_status.get("parquet_ok", False),
        "gsheet_ok": save_status.get("gsheet_ok", False),
    }


# Backwards-compat shim — old callers still see a function name they recognize.
def rebuild_orders_archive(days_back: int = 365, pmcc_tickers_tuple: tuple = ()) -> dict:
    return append_to_archive(pmcc_tickers_tuple=pmcc_tickers_tuple, force_days_back=days_back)


# ─────────────────────────────────────────────────────────────────
# Smart-detect auto-archive (Option C)
# ─────────────────────────────────────────────────────────────────
# Tiger keeps fills for ~90 days. If the archive hasn't been touched in 80+ days,
# we're in danger of losing fills permanently — so we proactively run an append.
# 80 = 90 (Tiger window) − 10 (safety buffer for late settlements / off days).
ARCHIVE_STALE_THRESHOLD_DAYS = 80


@st.cache_data(ttl=600, show_spinner=False)
def auto_archive_if_stale(pmcc_tickers_tuple: tuple = ()) -> dict:
    """Run an archive append IFF the archive is at risk of falling behind Tiger's
    90-day window. Cached for 10 min so it runs at most once per ten minutes per
    session. Silent — caller decides whether to surface a toast.

    Returns:
        {
          'action': 'ok' | 'archived' | 'first_run' | 'no_date' | 'error',
          'archive_latest': ISO date or None,
          'days_old': int (days between archive_latest and today),
          'result': dict | None  (the append_to_archive() result, when action='archived')
          'msg': str
        }
    """
    from tiger_api.archive import archive_summary
    try:
        summary = archive_summary()
    except Exception as e:
        return {"action": "error", "msg": f"Archive summary failed: {e}",
                "archive_latest": None, "days_old": None, "result": None}

    gs = summary.get("gsheet", {})
    if not gs.get("exists") or gs.get("rows", 0) == 0:
        # First run — archive empty. Don't auto-pull a giant 365d window without
        # asking — the user clicks the manual button in Config or via the
        # "first archive" prompt to kick off the backfill.
        return {"action": "first_run", "archive_latest": None, "days_old": None,
                "result": None,
                "msg": "Archive empty. Click 'Archive Now' in header or Config to backfill."}

    latest = pd.to_datetime(gs.get("latest"), errors="coerce")
    if pd.isna(latest):
        return {"action": "no_date", "msg": "Archive has no parseable dates",
                "archive_latest": None, "days_old": None, "result": None}

    days_old = (datetime.now() - latest).days
    if days_old < ARCHIVE_STALE_THRESHOLD_DAYS:
        # Healthy — archive's latest fill is within Tiger's window
        return {"action": "ok", "archive_latest": latest.date().isoformat(),
                "days_old": days_old, "result": None,
                "msg": f"Archive up-to-date (latest fill {days_old}d old)"}

    # Stale — auto-append. Pulls (gap + 7d buffer) days from Tiger.
    logger.info("Archive stale (%dd old) — auto-appending", days_old)
    result = append_to_archive(pmcc_tickers_tuple=pmcc_tickers_tuple)
    return {
        "action": "archived",
        "archive_latest": latest.date().isoformat(),
        "days_old": days_old,
        "result": result,
        "msg": f"Auto-archived: {result.get('added', 0)} new rows added.",
    }


@st.cache_data(ttl=300, show_spinner=False)
def get_data_coverage() -> dict:
    """Return a snapshot of data coverage across the two sources.

    Used by the header to show: archive range + live (Tiger 90d) range + gap status.

    Returns:
      {
        'archive': {'exists': bool, 'earliest': date|None, 'latest': date|None, 'rows': int},
        'live':    {'earliest': date|None, 'latest': date|None, 'days': 90},
        'gap_days': int,    # days between archive.latest and live.earliest (negative = overlap)
        'health':  'OK_OVERLAP' | 'GAP' | 'NO_ARCHIVE' | 'ERROR',
        'msg':     str,     # one-liner for caption
      }
    """
    from tiger_api.archive import archive_summary
    today = datetime.now().date()
    live_earliest = today - pd.Timedelta(days=90).to_pytimedelta()
    out = {
        "archive": {"exists": False, "earliest": None, "latest": None, "rows": 0},
        "live": {"earliest": live_earliest, "latest": today, "days": 90},
        "gap_days": None,
        "health": "ERROR",
        "msg": "",
    }
    try:
        summary = archive_summary()
        gs = summary.get("gsheet", {})
        if gs.get("exists") and gs.get("rows", 0) > 0:
            earliest = pd.to_datetime(gs.get("earliest"), errors="coerce")
            latest = pd.to_datetime(gs.get("latest"), errors="coerce")
            out["archive"] = {
                "exists": True,
                "earliest": earliest.date() if pd.notna(earliest) else None,
                "latest": latest.date() if pd.notna(latest) else None,
                "rows": int(gs.get("rows", 0)),
            }
            if pd.notna(latest):
                # Gap (positive) = days between archive.latest and live.earliest
                # Negative = overlap (good)
                gap = (live_earliest - latest.date()).days
                out["gap_days"] = gap
                if gap <= 0:
                    out["health"] = "OK_OVERLAP"
                    out["msg"] = f"Continuous · {-gap}d overlap"
                else:
                    out["health"] = "GAP"
                    out["msg"] = f"⚠️ {gap}d gap between archive end and live window"
            else:
                out["health"] = "ERROR"
                out["msg"] = "Archive has no parseable dates"
        else:
            out["health"] = "NO_ARCHIVE"
            out["msg"] = "No archive yet — only live 90d available"
    except Exception as e:
        out["health"] = "ERROR"
        out["msg"] = f"Coverage check failed: {e}"
    return out


# ─────────────────────────────────────────────────────────────────
# Cache invalidation
# ─────────────────────────────────────────────────────────────────
def refresh_all():
    """Force-bust all caches. Call after manual refresh button."""
    load_account_summary.clear()
    load_open_positions.clear()
    load_orders.clear()
    load_orders_full.clear()
    load_funding_history.clear()
    load_nav_history.clear()
    load_nav_summary.clear()
    auto_archive_if_stale.clear()
    get_data_coverage.clear()
