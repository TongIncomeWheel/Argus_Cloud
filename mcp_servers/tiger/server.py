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
from typing import Any, Optional

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
from mcp_servers.tiger.oauth.storage import InMemoryStorage, OAuthStorage

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger("tiger-mcp")

bootstrap_from_env()


def _build_storage() -> OAuthStorage:
    """Pick the OAuth storage backend.

    MCP_OAUTH_STORAGE=firestore  →  Firestore (persists across cold starts;
                                    required for the hosted deploy so claude.ai
                                    OAuth tokens survive container restarts)
    anything else (or unset)     →  InMemoryStorage (tests, stdio dev)

    If Firestore is requested but unavailable (import error, client init
    error), we log loudly and crash rather than silently fall back to
    in-memory — silent fallback was the original source of overnight
    disconnects because the operator couldn't tell from outside whether
    Firestore was actually live.
    """
    backend = os.environ.get("MCP_OAUTH_STORAGE", "memory").strip().lower()
    logger.info("======================================================")
    logger.info("MCP_OAUTH_STORAGE = %s", backend)
    logger.info("======================================================")
    if backend == "firestore":
        try:
            from mcp_servers.tiger.oauth.firestore_storage import FirestoreStorage
        except ImportError as e:
            logger.error(
                "MCP_OAUTH_STORAGE=firestore but google-cloud-firestore is not "
                "installed. Refusing to silently fall back to in-memory because "
                "that's the original disconnect bug. Install the package or "
                "set MCP_OAUTH_STORAGE to something else.", exc_info=e,
            )
            raise
        project = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
        database = os.environ.get("FIRESTORE_DATABASE", "(default)")
        try:
            storage = FirestoreStorage(project=project, database=database)
        except Exception as e:
            logger.error(
                "FirestoreStorage init failed (project=%s db=%s). Refusing to "
                "silently fall back to in-memory.", project, database, exc_info=e,
            )
            raise
        logger.info("OAuth storage = Firestore (project=%s, db=%s) — PERSISTENT",
                    project or "<ADC>", database)
        return storage
    logger.warning(
        "OAuth storage = in-memory (state lost on restart). "
        "This is only safe for stdio/local dev. Hosted deploys MUST set "
        "MCP_OAUTH_STORAGE=firestore — otherwise claude.ai connector "
        "will silently disconnect after every Cloud Run cold start."
    )
    return InMemoryStorage()


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
        "SDK. Read-only tools cover positions, orders, funding, NAV history. "
        "Option market data (chain, Greeks, briefs, bars, depth, ticks) is "
        "served from Tiger's US Option L1 subscription. "
        "For EQUITY (stock/ETF) spot prices use the IBKR connector's "
        "get_price_snapshot — Tiger MCP intentionally does not expose a "
        "spot-prices tool. "
        "Write tools (place_stock_order, place_option_order, cancel_order, "
        "execute_roll) preview by default — they ONLY submit to Tiger when "
        "called again with confirm=True. Always show the user the preview "
        "spec and get explicit approval before passing confirm=True."
    )
    host = os.environ.get("MCP_HOST", "0.0.0.0")
    # MCP_PORT first (explicit), then PORT (Cloud Run / Heroku convention),
    # then 8080 (Cloud Run default).
    port = int(os.environ.get("MCP_PORT") or os.environ.get("PORT") or "8080")
    base_url = os.environ.get("MCP_BASE_URL", f"http://{host}:{port}")

    if os.environ.get("MCP_OAUTH_OWNER_PASSWORD", "").strip():
        logger.info("Building FastMCP with OAuth 2.1 + PKCE + DCR")
        storage = _build_storage()
        provider = TigerOAuthProvider(storage, base_url)
        # Split scopes so the consent screen makes clear we ask for BOTH
        # read access (positions/orders/NAV) AND trade access (place,
        # cancel, roll). A single "tiger:read" was misleading users into
        # thinking trades were blocked.
        scopes = ["tiger:read", "tiger:trade"]
        mcp_instance = FastMCP(
            name,
            instructions=instructions,
            host=host,
            port=port,
            auth_server_provider=provider,
            auth=AuthSettings(
                issuer_url=base_url,
                resource_server_url=base_url,
                required_scopes=scopes,
                client_registration_options=ClientRegistrationOptions(
                    enabled=True,
                    valid_scopes=scopes,
                    default_scopes=scopes,
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
            required_scopes=["tiger:read", "tiger:trade"],
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
    qty_raw = _scalar(getattr(p, "quantity", 0))
    mkt_price = _scalar(getattr(p, "market_price", None))
    mkt_value = _scalar(getattr(p, "market_value", None))
    quantity = qty_raw
    quantity_scaling_note = None

    # Detect Tiger's fractional-share quirk: NVDA-style positions where
    # `quantity` is returned as an integer scaled by 1e5 (or similar)
    # rather than the true fractional shares. Symptom: qty * mkt_price
    # is orders of magnitude larger than mkt_value. When we detect that,
    # prefer the value/price-derived quantity.
    try:
        if (qty_raw is not None and mkt_price not in (None, 0)
                and mkt_value not in (None, 0)):
            qty_implied = float(mkt_value) / float(mkt_price)
            qty_stated = float(qty_raw) * float(mkt_price)
            if abs(qty_stated) > 0 and abs(qty_implied) > 0:
                # If the stated qty*price is 1000x or more vs market value,
                # the SDK is reporting a scaled integer. Substitute.
                if abs(qty_stated) >= 1000 * abs(float(mkt_value)):
                    quantity = qty_implied
                    quantity_scaling_note = (
                        f"Tiger reported quantity={qty_raw} but market_value "
                        f"implies {qty_implied:.6f} actual shares — fractional "
                        f"share scaling normalized."
                    )
    except (TypeError, ValueError, ZeroDivisionError):
        pass

    out = {
        "symbol": getattr(c, "symbol", "") if c else "",
        "sec_type": getattr(c, "sec_type", "") if c else "",
        "right": getattr(c, "right", None) if c else None,
        "strike": getattr(c, "strike", None) if c else None,
        "expiry": getattr(c, "expiry", None) if c else None,
        "currency": getattr(c, "currency", "USD") if c else "USD",
        "quantity": quantity,
        "avg_cost": _scalar(getattr(p, "average_cost", None)),
        "market_price": mkt_price,
        "market_value": mkt_value,
        "unrealized_pnl": _scalar(getattr(p, "unrealized_pnl", None)),
        "realized_pnl": _scalar(getattr(p, "realized_pnl", None)),
    }
    if quantity_scaling_note is not None:
        out["quantity_raw"] = qty_raw
        out["quantity_note"] = quantity_scaling_note
    return out


def _normalize_enum_str(v) -> str:
    """Strip the SDK enum class prefix from a stringified value.

    Tiger's SDK serializes enums as their fully-qualified repr — e.g.
    `OrderStatus.FILLED`, `SecurityType.OPT`, `ActionType.SELL`. The
    classifier needs the bare token. Returns uppercase for case-safe
    comparisons; returns "" for None / unparseable.
    """
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    # Take the last dotted segment so "OrderStatus.FILLED" → "FILLED",
    # "FILLED" → "FILLED", and "package.Module.OrderStatus.FILLED" still works.
    if "." in s:
        s = s.rsplit(".", 1)[-1]
    return s.upper()


def _to_float_or_none(v) -> Optional[float]:
    """Coerce to float for the zero-comparison path; None on failure."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _order_to_dict(o) -> dict:
    c = getattr(o, "contract", None)
    raw_status = getattr(o, "status", None)
    raw_sec_type = getattr(c, "sec_type", "") if c else ""
    raw_action = getattr(o, "action", None)
    raw_order_type = getattr(o, "order_type", None)

    # Normalised forms: enum prefix stripped, uppercase. Used for both
    # the user-facing output (cleaner) AND the classifier (correct).
    status_n = _normalize_enum_str(raw_status)
    sec_type_n = _normalize_enum_str(raw_sec_type)
    action_n = _normalize_enum_str(raw_action) or None
    order_type_n = _normalize_enum_str(raw_order_type) or None

    avg_fill_raw = _scalar(getattr(o, "avg_fill_price", None))
    limit_raw = _scalar(getattr(o, "limit_price", None))
    commission_raw = _scalar(getattr(o, "commission", None))
    quantity_raw = _scalar(getattr(o, "quantity", None))

    avg_fill_f = _to_float_or_none(avg_fill_raw)
    limit_f = _to_float_or_none(limit_raw)
    commission_f = _to_float_or_none(commission_raw)
    quantity_f = _to_float_or_none(quantity_raw)

    # Classify the order's fill_type. Tiger reports expiry-day events
    # (worthless expiration, auto-exercise of OTM contracts) as "filled"
    # orders with avg_fill_price=0, limit_price=0, commission=0. Without
    # tagging, downstream code can't distinguish them from real trades
    # and ends up averaging "0" fill prices into P&L calculations.
    #
    # Use the normalised status / sec_type — Tiger serialises enums as
    # "OrderStatus.FILLED" / "SecurityType.OPT", which would never match
    # bare "FILLED" / "OPT" without prefix stripping.
    is_zero_priced = (
        (avg_fill_f is None or avg_fill_f == 0.0)
        and (limit_f is None or limit_f == 0.0)
        and (commission_f is None or commission_f == 0.0)
    )
    has_size = quantity_f is not None and quantity_f != 0.0
    is_completed_status = status_n in (
        "FILLED", "EXPIRED", "EXERCISED", "AUTO_EXERCISED", "AUTO-EXERCISED",
    )
    fill_type = (
        "expiration"
        if (sec_type_n == "OPT" and is_zero_priced and has_size and is_completed_status)
        else "normal"
    )

    return {
        "id": _scalar(getattr(o, "id", None)),
        "status": status_n or None,           # cleaned (no "OrderStatus." prefix)
        "status_raw": str(raw_status) if raw_status is not None else None,
        "fill_type": fill_type,
        "action": action_n,
        "order_type": order_type_n,
        "sec_type": sec_type_n or None,       # cleaned
        "symbol": getattr(c, "symbol", "") if c else "",
        "right": getattr(c, "right", None) if c else None,
        "strike": getattr(c, "strike", None) if c else None,
        "expiry": getattr(c, "expiry", None) if c else None,
        "quantity": quantity_raw,
        "filled": _scalar(getattr(o, "filled", None)),
        "avg_fill_price": avg_fill_raw,
        "limit_price": limit_raw,
        "stop_price": _scalar(getattr(o, "stop_price", None)),
        "commission": commission_raw,
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


# get_spot_prices intentionally NOT exposed as an MCP tool.
# Equity spot quotes are served by the IBKR MCP connector
# (`get_price_snapshot`). Tiger's spot-prices endpoint requires a US
# Equity L1 subscription separate from the US Option L1 subscription;
# routing equity quotes to IBKR keeps Tiger MCP focused on its specialty
# (option market data + your Tiger trade account). The underlying
# TigerClient.get_spot_prices() method stays in place so Argus's
# Streamlit `tiger_data.load_spot_prices` keeps working with its own
# yfinance/Alpaca fallback chain.


@mcp.tool()
def get_nav_history(days: int = 30) -> dict:
    """Daily NAV / P&L / cash time series for the last N days."""
    return _get_client().get_nav_history(days=days)


# ── Option chain / Greeks / quotes (Phase 2d) ────────────────────────────────


@mcp.tool()
def get_option_expirations(symbols: list[str]) -> dict:
    """Available option expiry dates per underlying.

    Args:
      symbols: list of tickers, e.g. ["MSTR", "AAPL"]
    Returns: {symbol: [YYYY-MM-DD, ...]}
    """
    return _get_client().get_option_expirations(symbols)


@mcp.tool()
def get_option_chain(symbol: str, expiry: str, include_greeks: bool = True) -> list[dict]:
    """Full option chain for one underlying + expiry.

    Each row is a contract with strike, right (PUT/CALL), bid, ask, volume,
    open interest, and (if include_greeks=True) delta, gamma, theta, vega,
    rho, implied_vol. Use this to screen roll candidates across strikes.

    Args:
      symbol: ticker, e.g. "MSTR"
      expiry: option expiry, ISO date "YYYY-MM-DD"
      include_greeks: True (default) to include per-contract Greeks + IV
    """
    return _get_client().get_option_chain(symbol=symbol, expiry=expiry, include_greeks=include_greeks)


@mcp.tool()
def get_option_briefs(contracts: list[dict]) -> list[dict]:
    """Real-time bid / ask / open-interest / HV / last per option contract.

    Use this for accurate mark-to-market and roll execution pricing on
    specific contracts you already know.

    Args:
      contracts: list of {symbol, expiry, strike, right} dicts.
                 expiry is ISO YYYY-MM-DD, right is "PUT" or "CALL".
    """
    return _get_client().get_option_briefs(contracts)


@mcp.tool()
def get_option_greeks(contracts: list[dict]) -> list[dict]:
    """Δ / Γ / Θ / ν / ρ + implied vol per option contract.

    Highest-priority quote tool — feeds roll timing and position-level
    risk. Returns only the Greek fields + identifier so payloads stay tight.

    Args:
      contracts: list of {symbol, expiry, strike, right} dicts.
                 expiry is ISO YYYY-MM-DD, right is "PUT" or "CALL".
    """
    return _get_client().get_option_greeks(contracts)


@mcp.tool()
def get_option_bars(contracts: list[dict], period: str = "day", limit: int = 60) -> dict:
    """OHLC bars per option contract.

    Args:
      contracts: list of {symbol, expiry, strike, right} dicts
      period: "day" (default), "week", "month", or intraday "1min" ... "60min"
      limit: max bars per contract (default 60)
    Returns: {identifier: [bar_dict, ...]}
    """
    return _get_client().get_option_bars(contracts, period=period, limit=limit)


@mcp.tool()
def get_option_depth(contracts: list[dict]) -> list[dict]:
    """L2 bid/ask depth per option contract — full ladder, not just NBBO.

    Args:
      contracts: list of {symbol, expiry, strike, right} dicts
    """
    return _get_client().get_option_depth(contracts)


@mcp.tool()
def get_option_trade_ticks(contracts: list[dict], limit: int = 50) -> dict:
    """Recent trade-tick history per option contract.

    Args:
      contracts: list of {symbol, expiry, strike, right} dicts
      limit: max ticks per contract (default 50; ticks are voluminous)
    Returns: {identifier: [tick_dict, ...]}
    """
    return _get_client().get_option_trade_ticks(contracts, limit=limit)


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
def place_stock_order(
    symbol: str,
    side: str,
    quantity: float,
    order_type: str = "LMT",
    limit_price: float | None = None,
    stop_price: float | None = None,
    time_in_force: str = "DAY",
    outside_rth: bool = False,
    currency: str = "USD",
    confirm: bool = False,
) -> dict:
    """Place a stock / ETF order.

    PREVIEW BY DEFAULT — pass confirm=True to actually submit.

    Args:
      symbol: ticker, e.g. "AAPL", "00700" (HK), "SPY"
      side: "BUY" or "SELL"
      quantity: number of shares (must be > 0)
      order_type: "LMT" (default), "MKT", "STP", or "STP_LMT"
      limit_price: required when order_type in (LMT, STP_LMT)
      stop_price: required when order_type in (STP, STP_LMT) — Tiger calls
                  this `aux_price` internally
      time_in_force: "DAY" (default) or "GTC"
      outside_rth: True to allow pre-market and after-hours fills (US only)
      currency: "USD" (default), "HKD", or "SGD" — must match the symbol's
                listing market
      confirm: False = preview only (default); True = submit
    """
    sym_n = symbol.strip().upper()
    side_n = side.strip().upper()
    otype = order_type.strip().upper().replace("-", "_")

    spec = {
        "symbol": sym_n,
        "side": side_n,
        "quantity": float(quantity),
        "order_type": otype,
        "limit_price": float(limit_price) if limit_price is not None else None,
        "stop_price": float(stop_price) if stop_price is not None else None,
        "time_in_force": time_in_force.upper(),
        "outside_rth": bool(outside_rth),
        "currency": currency.upper(),
    }
    summary_price = (
        f"@ ${limit_price:.2f} limit" if otype == "LMT" and limit_price is not None
        else f"@ market" if otype == "MKT"
        else f"@ stop ${stop_price:.2f}" if otype == "STP" and stop_price is not None
        else f"@ stop ${stop_price:.2f} → ${limit_price:.2f} limit"
            if otype == "STP_LMT" and stop_price is not None and limit_price is not None
        else ""
    )
    summary = (
        f"{side_n} {quantity:g} {sym_n} {summary_price} "
        f"({time_in_force.upper()}{', outside RTH' if outside_rth else ''})"
    )

    if not confirm:
        return _preview_envelope(summary, spec)

    result = _get_client().place_stock_order(
        symbol=sym_n, side=side_n, quantity=quantity, order_type=otype,
        limit_price=limit_price, stop_price=stop_price,
        time_in_force=time_in_force, outside_rth=outside_rth, currency=currency,
    )
    return {"preview": False, "placed": True, "summary": summary, **result}


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
