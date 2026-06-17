"""Tiger MCP server — read-only Tiger Open API tools.

Wraps `tiger_api.client.TigerClient` and exposes its read-only methods as
MCP tools. Two transports supported via `--transport`:

  stdio              — launched per-session by Claude Code via .mcp.json
                        (default; no auth, credentials via env vars)
  sse                — long-lived HTTP server suitable for Fly.io / Cloud Run
                        (requires MCP_BEARER_TOKEN; clients send
                         `Authorization: Bearer <token>` on every request)

Phase 2a auth = bearer token only. Phase 2b will replace this with OAuth 2.1
so the Claude.ai consumer Custom Connectors flow can authenticate.

Tools are intentionally read-only here. Write operations (execute_roll,
place_order, cancel_order) will land in a separate follow-up with a
preview + confirmation gate.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

# Make the repo root importable when launched as `python -m mcp_servers.tiger.server`
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from mcp.server.auth.settings import (
    AuthSettings,
    ClientRegistrationOptions,
    RevocationOptions,
)
from mcp.server.fastmcp import FastMCP

from mcp_servers.tiger.auth import BearerTokenVerifier, bootstrap_from_env
from mcp_servers.tiger.oauth.consent import make_consent_routes
from mcp_servers.tiger.oauth.provider import TigerOAuthProvider
from mcp_servers.tiger.oauth.storage import InMemoryStorage

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger("tiger-mcp")

bootstrap_from_env()


def _build_server() -> FastMCP:
    """Build FastMCP with the right auth mode for the runtime config.

    Three modes, picked by env vars present:

    1. MCP_OAUTH_OWNER_PASSWORD set  →  full OAuth 2.1 + PKCE + DCR. Required
       for Claude.ai consumer Custom Connectors. The owner password gates
       the consent step inside the OAuth flow.
    2. MCP_BEARER_TOKEN set          →  static bearer-only gate. Suitable
       for Claude Code over HTTP (which lets you set a header) but not for
       the Claude consumer app.
    3. Neither set                   →  unauthenticated. Only safe for
       stdio transport (process-local, spawned by Claude Code).
    """
    name = "tiger"
    instructions = (
        "Access to the user's Tiger Brokers account via the official tigeropen "
        "SDK. Read-only tools cover positions, orders, funding, and NAV history. "
        "Write tools (place_option_order, cancel_order, execute_roll) preview by "
        "default — they ONLY submit to Tiger when called again with confirm=True. "
        "Always show the user the preview spec and get explicit approval before "
        "passing confirm=True."
    )
    host = os.environ.get("MCP_HOST", "0.0.0.0")
    # MCP_PORT first (explicit), then PORT (Cloud Run / Heroku convention),
    # then 8080 (Cloud Run default).
    port = int(os.environ.get("MCP_PORT") or os.environ.get("PORT") or "8080")
    base_url = os.environ.get("MCP_BASE_URL", f"http://{host}:{port}")

    if os.environ.get("MCP_OAUTH_OWNER_PASSWORD", "").strip():
        logger.info("Building FastMCP with OAuth 2.1 + PKCE + DCR")
        storage = InMemoryStorage()
        provider = TigerOAuthProvider(storage, base_url)
        mcp_instance = FastMCP(
            name,
            instructions=instructions,
            host=host,
            port=port,
            auth_server_provider=provider,
            auth=AuthSettings(
                issuer_url=base_url,
                resource_server_url=base_url,
                required_scopes=["tiger:read"],
                client_registration_options=ClientRegistrationOptions(
                    enabled=True,
                    valid_scopes=["tiger:read"],
                    default_scopes=["tiger:read"],
                ),
                revocation_options=RevocationOptions(enabled=True),
            ),
        )
        render, handle = make_consent_routes(provider)
        mcp_instance.custom_route("/consent", methods=["GET"])(render)
        mcp_instance.custom_route("/consent", methods=["POST"])(handle)
        return mcp_instance

    bearer = os.environ.get("MCP_BEARER_TOKEN", "").strip()
    if not bearer:
        logger.warning("MCP_BEARER_TOKEN not set — HTTP transport will be UNAUTHENTICATED")
        return FastMCP(name, instructions=instructions, host=host, port=port)

    logger.info("Building FastMCP with static bearer auth")
    return FastMCP(
        name,
        instructions=instructions,
        host=host,
        port=port,
        token_verifier=BearerTokenVerifier(bearer),
        auth=AuthSettings(
            issuer_url=base_url,
            resource_server_url=base_url,
            required_scopes=["tiger:read"],
        ),
    )


mcp = _build_server()

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


# ── Write tools (Phase 2c) — preview by default, confirm explicitly ──────────


def _preview_envelope(action_summary: str, spec: dict) -> dict:
    """Wrap a preview spec with consistent headers so the LLM and the user
    both see clearly that NO order was placed."""
    return {
        "preview": True,
        "placed": False,
        "summary": action_summary,
        "spec": spec,
        "next_step": "Call this tool again with confirm=True to actually submit.",
    }


@mcp.tool()
def place_option_order(
    symbol: str,
    expiry: str,
    strike: float,
    right: str,
    side: str,
    quantity: int,
    limit_price: float,
    time_in_force: str = "DAY",
    confirm: bool = False,
) -> dict:
    """Place a single-leg option order with a limit price.

    PREVIEW BY DEFAULT — pass confirm=True to actually submit. Always preview
    first so the user can verify symbol, strike, expiry, side, quantity, and
    price before any order goes to Tiger.

    Args:
      symbol: ticker, e.g. "MSTR"
      expiry: option expiry, ISO date "YYYY-MM-DD"
      strike: numeric strike price
      right: "PUT" or "CALL"
      side: one of SELL_TO_OPEN (CSP/CC entry), BUY_TO_CLOSE (close short),
            BUY_TO_OPEN (long entry), SELL_TO_CLOSE (close long)
      quantity: number of contracts (must be > 0)
      limit_price: limit price per share — Tiger multiplies by 100 internally
      time_in_force: "DAY" (default) or "GTC"
      confirm: False = preview only (default); True = submit
    """
    symbol_n = symbol.strip().upper()
    right_n = right.strip().upper()
    side_n = side.strip().upper().replace(" ", "_").replace("-", "_")

    spec = {
        "symbol": symbol_n,
        "expiry": expiry,
        "strike": float(strike),
        "right": right_n,
        "side": side_n,
        "quantity": int(quantity),
        "limit_price": float(limit_price),
        "time_in_force": time_in_force.upper(),
        "premium_per_contract_usd": round(float(limit_price) * 100, 2),
        "total_premium_usd": round(float(limit_price) * 100 * int(quantity), 2),
    }
    summary = (
        f"{side_n} {quantity}x {symbol_n} {strike:g}{right_n[0]} "
        f"exp {expiry} @ ${limit_price:.2f} ({time_in_force.upper()})"
    )

    if not confirm:
        return _preview_envelope(summary, spec)

    result = _get_client().place_option_order(
        symbol=symbol_n,
        expiry=expiry,
        strike=float(strike),
        right=right_n,
        side=side_n,
        quantity=int(quantity),
        limit_price=float(limit_price),
        time_in_force=time_in_force,
    )
    return {"preview": False, "placed": True, "summary": summary, **result}


@mcp.tool()
def cancel_order(order_id: str, confirm: bool = False) -> dict:
    """Cancel a working order by id.

    PREVIEW BY DEFAULT — pass confirm=True to actually cancel.
    """
    spec = {"order_id": str(order_id)}
    summary = f"Cancel order id={order_id}"
    if not confirm:
        return _preview_envelope(summary, spec)
    result = _get_client().cancel_order(order_id)
    return {"preview": False, "placed": True, "summary": summary, **result}


@mcp.tool()
def execute_roll(
    symbol: str,
    close_expiry: str,
    close_strike: float,
    close_right: str,
    new_expiry: str,
    new_strike: float,
    quantity: int,
    net_credit_limit: float,
    time_in_force: str = "DAY",
    confirm: bool = False,
) -> dict:
    """Roll a short option: atomic BUY_TO_CLOSE existing leg + SELL_TO_OPEN new
    leg as one combo (MLEG) order with a single net-credit limit.

    PREVIEW BY DEFAULT — pass confirm=True to actually submit. Always preview
    first so the user can verify both legs and the net credit.

    Args:
      symbol: ticker, same on both legs
      close_expiry / close_strike / close_right: identify the existing short
        leg you want to close (right is "PUT" or "CALL"; both legs must share)
      new_expiry / new_strike: the replacement short leg (same right)
      quantity: number of contracts (must be > 0)
      net_credit_limit: per-contract net credit required (positive = collect
        at least this much net premium; negative = accept a net debit of that
        magnitude)
      time_in_force: "DAY" (default) or "GTC"
      confirm: False = preview only (default); True = submit
    """
    symbol_n = symbol.strip().upper()
    right_n = close_right.strip().upper()

    spec = {
        "symbol": symbol_n,
        "right": right_n,
        "close_leg": {"expiry": close_expiry, "strike": float(close_strike), "side": "BUY_TO_CLOSE"},
        "open_leg": {"expiry": new_expiry, "strike": float(new_strike), "side": "SELL_TO_OPEN"},
        "quantity": int(quantity),
        "net_credit_limit_per_contract_usd": float(net_credit_limit),
        "net_credit_limit_total_usd": round(float(net_credit_limit) * 100 * int(quantity), 2),
        "time_in_force": time_in_force.upper(),
    }
    summary = (
        f"Roll {quantity}x {symbol_n} {right_n}: "
        f"close {close_strike:g} exp {close_expiry} + "
        f"open {new_strike:g} exp {new_expiry} "
        f"@ net {'credit' if net_credit_limit >= 0 else 'debit'} "
        f"${abs(net_credit_limit):.2f}/contract"
    )

    if not confirm:
        return _preview_envelope(summary, spec)

    result = _get_client().execute_combo_roll(
        symbol=symbol_n,
        close_expiry=close_expiry,
        close_strike=float(close_strike),
        close_right=right_n,
        new_expiry=new_expiry,
        new_strike=float(new_strike),
        quantity=int(quantity),
        net_credit_limit=float(net_credit_limit),
        time_in_force=time_in_force,
    )
    return {"preview": False, "placed": True, "summary": summary, **result}


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="mcp_servers.tiger.server",
        description="Tiger MCP server — read-only Tiger Brokers tools.",
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default=os.environ.get("MCP_TRANSPORT", "stdio"),
        help="MCP transport (default: stdio; sse/streamable-http for hosted use)",
    )
    args = parser.parse_args()
    logger.info("Starting tiger MCP server on transport=%s", args.transport)
    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()
