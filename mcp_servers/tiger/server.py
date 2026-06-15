"""Tiger MCP server — read-only Tiger Open API tools.

Wraps `tiger_api.client.TigerClient` and exposes its read-only methods as
MCP tools. Launched by Claude Code at session start via the project's
.mcp.json declaration.

Tools intentionally limited to READ operations in this phase. Write
operations (execute_roll, place_order, cancel_order) will land in a
separate follow-up with a preview + confirmation flow.
"""
from __future__ import annotations

import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

# Make the repo root importable when launched as `python -m mcp_servers.tiger.server`
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from mcp.server.fastmcp import FastMCP

from mcp_servers.tiger.auth import bootstrap_from_env

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger("tiger-mcp")

bootstrap_from_env()

mcp = FastMCP(
    "tiger",
    instructions=(
        "Read-only access to the user's Tiger Brokers account via the official "
        "tigeropen SDK. Use these tools to inspect positions, orders, funding, "
        "and NAV history. No order placement, modification, or cancellation."
    ),
)

_client = None


def _get_client():
    """Lazy TigerClient — instantiated on first tool call so the server
    starts even when credentials are not yet present."""
    global _client
    if _client is None:
        from tiger_api.client import TigerClient
        _client = TigerClient()
    return _client


def _scalar(v: Any) -> Any:
    """Convert one value to a JSON-friendly form. Handles primitives, lists,
    dicts, datetimes; falls back to str() for unknown SDK objects."""
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, (list, tuple, set)):
        return [_scalar(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _scalar(val) for k, val in v.items()}
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:
            pass
    return str(v)


def _safe_attrs(obj: Any) -> dict:
    """Best-effort dict of an SDK object — skips callables and private names."""
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return {str(k): _scalar(v) for k, v in obj.items()}
    out = {}
    for k in dir(obj):
        if k.startswith("_"):
            continue
        try:
            v = getattr(obj, k)
        except Exception:
            continue
        if callable(v):
            continue
        out[k] = _scalar(v)
    return out


def _position_to_dict(p) -> dict:
    c = getattr(p, "contract", None)
    return {
        "symbol": getattr(c, "symbol", "") if c else "",
        "sec_type": getattr(c, "sec_type", "") if c else "",
        "right": getattr(c, "right", None) if c else None,
        "strike": getattr(c, "strike", None) if c else None,
        "expiry": getattr(c, "expiry", None) if c else None,
        "currency": getattr(c, "currency", "USD") if c else "USD",
        "quantity": _scalar(getattr(p, "quantity", 0)),
        "avg_cost": _scalar(getattr(p, "average_cost", None)),
        "market_price": _scalar(getattr(p, "market_price", None)),
        "market_value": _scalar(getattr(p, "market_value", None)),
        "unrealized_pnl": _scalar(getattr(p, "unrealized_pnl", None)),
        "realized_pnl": _scalar(getattr(p, "realized_pnl", None)),
    }


def _order_to_dict(o) -> dict:
    c = getattr(o, "contract", None)
    return {
        "id": _scalar(getattr(o, "id", None)),
        "status": _scalar(getattr(o, "status", None)),
        "action": _scalar(getattr(o, "action", None)),
        "order_type": _scalar(getattr(o, "order_type", None)),
        "sec_type": getattr(c, "sec_type", "") if c else "",
        "symbol": getattr(c, "symbol", "") if c else "",
        "right": getattr(c, "right", None) if c else None,
        "strike": getattr(c, "strike", None) if c else None,
        "expiry": getattr(c, "expiry", None) if c else None,
        "quantity": _scalar(getattr(o, "quantity", None)),
        "filled": _scalar(getattr(o, "filled", None)),
        "avg_fill_price": _scalar(getattr(o, "avg_fill_price", None)),
        "limit_price": _scalar(getattr(o, "limit_price", None)),
        "stop_price": _scalar(getattr(o, "stop_price", None)),
        "commission": _scalar(getattr(o, "commission", None)),
        "gst": _scalar(getattr(o, "gst", None)),
        "realized_pnl": _scalar(getattr(o, "realized_pnl", None)),
        "trade_time": _scalar(getattr(o, "trade_time", None)),
        "order_time": _scalar(getattr(o, "order_time", None)),
    }


# ── Tools ────────────────────────────────────────────────────────────────────


@mcp.tool()
def get_account_summary() -> dict:
    """Snapshot of NAV, cash, gross position value, and today's P&L. One API round-trip."""
    client = _get_client()
    a = client.get_assets()
    return {
        "account": client.account,
        "license": client.license,
        "is_sandbox": client.is_sandbox,
        "currency": a.currency,
        "nav": a.nav,
        "cash": a.cash,
        "stock_value": a.stock_value,
        "realized_pnl_today": a.realized_pnl_today,
        "unrealized_pnl": a.unrealized_pnl,
    }


@mcp.tool()
def get_stock_positions() -> list[dict]:
    """All stock positions currently held."""
    return [_position_to_dict(p) for p in _get_client().get_stock_positions()]


@mcp.tool()
def get_option_positions() -> list[dict]:
    """All option positions currently held — includes strike, expiry, right."""
    return [_position_to_dict(p) for p in _get_client().get_option_positions()]


@mcp.tool()
def get_filled_orders(days: int = 7) -> list[dict]:
    """Filled orders within the last N days. Auto-chunked into 30-day windows
    to respect Tiger's 90-day window cap and 100-fill-per-call cap."""
    return [_order_to_dict(o) for o in _get_client().get_filled_orders(days=days)]


@mcp.tool()
def get_open_orders() -> list[dict]:
    """Currently working orders (not yet filled or cancelled)."""
    return [_order_to_dict(o) for o in _get_client().get_open_orders()]


@mcp.tool()
def get_cancelled_orders(days: int = 7) -> list[dict]:
    """Cancelled orders within the last N days."""
    return [_order_to_dict(o) for o in _get_client().get_cancelled_orders(days=days)]


@mcp.tool()
def get_transactions(symbol: str, days: int = 30, limit: int = 100) -> list[dict]:
    """Per-fill executions for one ticker, ms-precision timestamps. Tiger requires
    a symbol filter — for all-portfolio fills iterate over tickers from positions."""
    txns = _get_client().get_transactions(symbol=symbol, days=days, limit=limit)
    return [_safe_attrs(t) for t in txns]


@mcp.tool()
def get_order_transactions(order_id: str) -> list[dict]:
    """Per-leg fills for one order id. Used to expand multi-leg combo (MLEG)
    rolls and spreads into individual leg fills with strike, expiry, right."""
    txns = _get_client().get_order_transactions(order_id=order_id)
    return [_safe_attrs(t) for t in txns]


@mcp.tool()
def get_prime_assets() -> dict:
    """Detailed segment balances, margin, buying power, and multi-currency state."""
    pa = _get_client().get_prime_assets()
    return _safe_attrs(pa)


@mcp.tool()
def get_funding_history() -> list[dict]:
    """All deposits and withdrawals across the lifetime of the account.
    Each row carries explicit currency, resolving CSV SGD/USD ambiguity."""
    df = _get_client().get_funding_history()
    if df is None or not hasattr(df, "to_dict"):
        return []
    return df.to_dict(orient="records")


@mcp.tool()
def get_spot_prices(symbols: list[str]) -> dict:
    """Latest spot prices for a list of tickers. Tickers without a Tiger quote
    are silently omitted from the result."""
    return _get_client().get_spot_prices(symbols)


@mcp.tool()
def get_nav_history(days: int = 30) -> dict:
    """Daily NAV / P&L / cash time series for the last N days."""
    return _get_client().get_nav_history(days=days)


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
