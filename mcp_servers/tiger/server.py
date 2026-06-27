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
import math
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


async def _ensure_static_oauth_client(provider: TigerOAuthProvider) -> None:
    """If MCP_OAUTH_CLIENT_ID / _SECRET are set, pre-register that client
    in storage so claude.ai can use the connector form's "OAuth Client ID"
    + "OAuth Client Secret" fields instead of relying on Dynamic Client
    Registration.

    DCR puts the client_id in claude.ai's local state, which can be
    evicted (cache cleanup, internal lifecycle, etc.) — leaving the
    connector unable to refresh and silently disconnecting. Static
    credentials live in claude.ai's connector config itself, so they
    can never be lost short of the user deleting the connector.
    """
    client_id = os.environ.get("MCP_OAUTH_CLIENT_ID", "").strip()
    client_secret = os.environ.get("MCP_OAUTH_CLIENT_SECRET", "").strip()
    if not client_id:
        logger.info("MCP_OAUTH_CLIENT_ID not set — static client not registered. "
                    "claude.ai will use DCR (less reliable for our case).")
        return
    existing = await provider.get_client(client_id)
    if existing is not None:
        logger.info("Static OAuth client already in storage: %s...",
                    client_id[:16])
        return
    # First boot with this client_id — register it.
    from mcp.shared.auth import OAuthClientInformationFull
    client_info = OAuthClientInformationFull(
        client_id=client_id,
        client_secret=client_secret or None,
        client_name="Tiger MCP static client",
        redirect_uris=[
            "https://claude.ai/api/mcp/auth_callback",
            "https://claude.com/api/mcp/auth_callback",
        ],
        scope="tiger:read tiger:trade",
        token_endpoint_auth_method=("client_secret_post" if client_secret else "none"),
        grant_types=["authorization_code", "refresh_token"],
        response_types=["code"],
    )
    await provider.register_client(client_info)
    logger.info("Static OAuth client registered: %s... (with secret: %s)",
                client_id[:16], "yes" if client_secret else "no")


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
        # If static client credentials are configured, register that
        # client on the FastMCP startup hook so claude.ai's connector
        # form can pre-populate "OAuth Client ID" + "OAuth Client
        # Secret" instead of relying on Dynamic Client Registration.
        # DCR-issued credentials live in claude.ai's volatile cache and
        # appear to be the recurring root cause of overnight disconnects.
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _lifespan(_mcp):
            await _ensure_static_oauth_client(provider)
            yield

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
            lifespan=_lifespan,
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


# ── Analytics tools (Phase E1) — local compute, no Tiger Greeks dependency ───


def _yfinance_spot_batch(symbols: list[str]) -> dict[str, float]:
    """Batch fetch underlying spot prices via yfinance.

    Free, no auth, ~15-minute delayed. Used because Tiger's spot-prices
    endpoint requires a separate US Equity L1 subscription. Returns
    {SYMBOL_UPPER: float}. Tickers with no quote are dropped silently;
    callers should diff requested-vs-returned to log misses.
    """
    if not symbols:
        return {}
    try:
        import yfinance as yf
    except ImportError:
        logger.warning(
            "yfinance not installed — compute_portfolio_greeks cannot fetch "
            "underlying spots. Add yfinance to requirements-mcp.txt."
        )
        return {}
    out: dict[str, float] = {}
    uniq = sorted({s.upper() for s in symbols})
    try:
        tickers = yf.Tickers(" ".join(uniq))
        for sym in uniq:
            try:
                t = tickers.tickers.get(sym)
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
                    out[sym] = float(price)
            except Exception as e:
                logger.warning("yfinance spot fetch failed for %s: %s", sym, e)
                continue
    except Exception as e:
        logger.warning("yfinance batch fetch failed: %s", e)
    return out


def _parse_tiger_expiry(value: Any) -> Optional[date]:
    """Tiger position.contract.expiry comes back in inconsistent formats —
    sometimes ISO 'YYYY-MM-DD', sometimes compact 'YYYYMMDD', sometimes a
    datetime/date object. Coerce to a `date` or None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if not s:
        return None
    if "-" in s:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            pass
    if "/" in s:
        try:
            return datetime.strptime(s[:10], "%Y/%m/%d").date()
        except ValueError:
            pass
    try:
        return datetime.strptime(s[:8], "%Y%m%d").date()
    except ValueError:
        return None


def _round(v: Any, n: int = 4) -> Optional[float]:
    """None-safe rounding so payloads stay tight without crashing on None."""
    if v is None:
        return None
    try:
        return round(float(v), n)
    except (TypeError, ValueError):
        return None


@mcp.tool()
def compute_portfolio_greeks(
    risk_free_rate: float = 0.045,
    dividend_yield: float = 0.0,
) -> dict:
    """Compute Δ + Θ for every option position and aggregate net + gross exposure.

    Tiger denies Greeks for retail TBSG accounts, so this tool computes them
    locally via Black-Scholes:
      • Spot price: yfinance (free, ~15-min delayed)
      • IV: Newton-Raphson solved from each position's market_price
      • Delta + Theta: BS closed form, sign-flipped for short positions

    Sign convention (handled by compute_greeks — verify against your view):
      • Long calls / short puts → delta > 0
      • Short calls / long puts → delta < 0
      • Long options  → theta < 0  (decay hurts you)
      • Short options → theta > 0  (decay pays you)

    Position-level scaling:
      delta_shares      = delta * 100 * |quantity|
      theta_per_day_usd = theta_per_day * 100 * |quantity|

    Args:
      risk_free_rate: annualized risk-free rate (default 4.5%)
      dividend_yield: continuous dividend yield (default 0.0)

    Returns:
      positions:   per-position rows with computed Greeks and dollar exposure
      aggregates:  net + gross deltas and theta plus priced/total counts
      skipped:     positions we couldn't price, with the reason
      notes:       human-readable caveats (delayed spot, missing tickers, etc.)
    """
    from tiger_api.greeks import compute_greeks

    asof = date.today()
    raw_positions = _get_client().get_option_positions()
    positions = [_position_to_dict(p) for p in raw_positions]

    symbols = sorted({p["symbol"] for p in positions if p.get("symbol")})
    spots = _yfinance_spot_batch(symbols)
    missing_spots = [s for s in symbols if s not in spots]

    out_positions: list[dict] = []
    skipped: list[dict] = []
    net_delta_shares = 0.0
    net_theta_usd = 0.0
    gross_delta_shares = 0.0
    gross_theta_usd = 0.0
    priced = 0

    for p in positions:
        symbol = (p.get("symbol") or "").upper()
        strike = p.get("strike")
        right = (p.get("right") or "").upper()
        qty = p.get("quantity")
        market_price = p.get("market_price")
        expiry_raw = p.get("expiry")
        expiry_date = _parse_tiger_expiry(expiry_raw)

        def _skip(reason: str) -> None:
            skipped.append({
                "symbol": symbol,
                "expiry": expiry_raw,
                "strike": strike,
                "right": right,
                "reason": reason,
            })

        if right not in ("PUT", "CALL"):
            _skip(f"non-option right={right!r}")
            continue
        spot = spots.get(symbol)
        if spot is None:
            _skip(f"no spot price for {symbol} (yfinance miss)")
            continue
        if expiry_date is None:
            _skip(f"unparseable expiry {expiry_raw!r}")
            continue
        try:
            qty_f = float(qty)
            strike_f = float(strike)
            mkt_f = float(market_price)
        except (TypeError, ValueError):
            _skip(f"non-numeric qty/strike/market_price ({qty!r}/{strike!r}/{market_price!r})")
            continue
        if qty_f == 0 or strike_f <= 0 or mkt_f <= 0:
            _skip(f"zero/invalid qty={qty_f} strike={strike_f} mkt={mkt_f}")
            continue

        dte_days = (expiry_date - asof).days
        if dte_days < 0:
            _skip(f"already expired ({expiry_date.isoformat()})")
            continue
        if dte_days == 0:
            dte_days = 0.5

        is_call = (right == "CALL")
        is_long = (qty_f > 0)
        g = compute_greeks(
            spot=spot, strike=strike_f, dte_days=dte_days,
            market_price=mkt_f, is_call=is_call, is_long=is_long,
            r=risk_free_rate, q=dividend_yield,
        )
        if g["delta"] is None or g["theta_per_day"] is None:
            _skip("IV solve failed (price below intrinsic or non-convergent)")
            continue

        abs_qty = abs(qty_f)
        delta_shares = g["delta"] * 100.0 * abs_qty
        theta_usd = g["theta_per_day"] * 100.0 * abs_qty

        out_positions.append({
            "symbol": symbol,
            "expiry": expiry_date.isoformat(),
            "strike": strike_f,
            "right": right,
            "quantity": qty_f,
            "spot": _round(spot, 4),
            "market_price": _round(mkt_f, 4),
            "dte_days": dte_days,
            "delta": _round(g["delta"], 4),
            "theta_per_day": _round(g["theta_per_day"], 4),
            "iv": _round(g["iv"], 4),
            "delta_shares": _round(delta_shares, 2),
            "theta_per_day_usd": _round(theta_usd, 2),
        })
        net_delta_shares += delta_shares
        net_theta_usd += theta_usd
        gross_delta_shares += abs(delta_shares)
        gross_theta_usd += abs(theta_usd)
        priced += 1

    notes: list[str] = []
    if missing_spots:
        notes.append(
            f"No spot for: {', '.join(missing_spots)} — those positions skipped. "
            "yfinance may rate-limit, or the ticker isn't on Yahoo."
        )
    notes.append(
        "Spot ≈ 15-min delayed (yfinance). IV solved per-position from market_price; "
        "stale/wide bid-ask can produce noisy IV. For real-time Greeks, wire Alpaca "
        "(Phase E2 — see BACKLOG.md)."
    )

    return {
        "positions": out_positions,
        "aggregates": {
            "net_delta_shares": _round(net_delta_shares, 2),
            "net_theta_per_day_usd": _round(net_theta_usd, 2),
            "gross_delta_shares": _round(gross_delta_shares, 2),
            "gross_theta_per_day_usd": _round(gross_theta_usd, 2),
            "priced_positions": priced,
            "total_positions": len(positions),
        },
        "skipped": skipped,
        "asof_date": asof.isoformat(),
        "spot_source": "yfinance",
        "risk_free_rate": risk_free_rate,
        "dividend_yield": dividend_yield,
        "notes": notes,
    }


@mcp.tool()
def get_position_roc(juiced_only: bool = False, pot: str = "all") -> dict:
    """Per-position RoC for all open short option positions.

    Joins open short option positions to their STO entry records, computes
    days-held, yield-on-notional, % of premium harvested, and annualised RoC.
    Flags positions ≥ 65% harvested as `juiced` (= candidates to roll or BTC
    for the income-wheel playbook).

    Data sources, by priority:
      PRIMARY:  Google Sheets Data Table (unbounded history since inception).
                Read via Application Default Credentials — the Cloud Run SA
                must be a viewer on the spreadsheet. Set the spreadsheet id
                via env var MCP_INCOME_WHEEL_SHEET_ID (or INCOME_WHEEL_SHEET_ID).
      FALLBACK: Tiger MCP get_filled_orders(days=90). Used automatically when
                Sheets is unavailable. Limited to 90 days; older positions
                will report entry_fill_found=false. Marked with
                `entry_source: "tiger_mcp_fallback"` so the operator knows
                why the picture may be incomplete.

    Pot routing:
      core    = {MARA, CRCL}
      active  = {BE, COIN, DELL, MSFT, MP, SLB}
      sidecar = {ECHO, INTC}
      Tickers in {KO, MCD, NVDA, SPY} are excluded entirely (never appear
      in any output). Other tickers get pot="unknown" — present in the
      positions list but not in by_pot aggregates.

    Args:
      juiced_only: when True, return only positions with pct_harvested >= 0.65
      pot:         "all" | "core" | "active" | "sidecar"

    Returns:
      positions[]:               per-position rows with full RoC payload
      aggregates{}:              by_pot subtotals + portfolio totals + counts
      juiced_positions[]:        subset of positions with juiced=True
      missing_entry_positions[]: subset where we couldn't find an entry STO
      asof_date:                 today's ISO date used for days-held math
      entry_source:              "google_sheets" | "tiger_mcp_fallback"
      juiced_threshold:          0.65 (echoed for transparency)
      notes[]:                   human-readable caveats / data-quality notes
    """
    from mcp_servers.tiger import data_table

    asof = date.today()
    notes: list[str] = []

    # ── 1. Open option positions from Tiger ─────────────────────────────
    raw_positions = _get_client().get_option_positions()
    open_positions = [_position_to_dict(p) for p in raw_positions]
    # We only score SHORT options (qty < 0). Long options aren't part of
    # the income-wheel playbook this tool serves.
    open_shorts = [p for p in open_positions if (p.get("quantity") or 0) < 0]

    # ── 2. Try Sheets primary; fall back to Tiger 90d fills ─────────────
    sheet_rows = data_table.read_data_table()
    entry_source = "google_sheets" if sheet_rows else "tiger_mcp_fallback"
    fallback_fills: list[dict] = []
    if not sheet_rows:
        try:
            fallback_fills = [
                _order_to_dict(o) for o in _get_client().get_filled_orders(days=90)
            ]
            notes.append(
                "Google Sheets unavailable — entry dates from Tiger 90-day "
                "fill window. Positions older than 90 days will report "
                "entry_fill_found=false."
            )
        except Exception as e:
            return {
                "error": "BOTH_SOURCES_UNAVAILABLE",
                "message": (
                    "Sheets read failed and Tiger fallback also failed. "
                    f"Tiger error: {e}"
                ),
                "positions": [],
                "aggregates": {},
                "asof_date": asof.isoformat(),
                "entry_source": "none",
            }

    # ── 3. Walk each open short, find entry, compute RoC ────────────────
    out: list[dict] = []
    missing_entry: list[dict] = []

    for p in open_shorts:
        symbol = (p.get("symbol") or "").upper()
        if not symbol or symbol in data_table.EXCLUDE_TICKERS:
            continue

        right = (p.get("right") or "").strip().upper()
        if right not in ("PUT", "CALL"):
            continue

        try:
            strike = float(p.get("strike"))
            qty = abs(float(p.get("quantity")))
            avg_cost = float(p.get("avg_cost"))
            market_price = float(p.get("market_price") or 0.0)
        except (TypeError, ValueError):
            notes.append(
                f"Skipped {symbol} {right} {p.get('strike')} {p.get('expiry')}: "
                "non-numeric qty/strike/avg_cost/market_price."
            )
            continue

        if avg_cost == 0:
            notes.append(
                f"Skipped {symbol} {right} {p.get('strike')} {p.get('expiry')}: "
                "avg_cost=0 — entry premium not recorded."
            )
            continue

        expiry_d = data_table._parse_iso_date(p.get("expiry"))
        if expiry_d is None:
            notes.append(
                f"Skipped {symbol} {right} {p.get('strike')}: unparseable expiry "
                f"{p.get('expiry')!r}."
            )
            continue
        if expiry_d < asof:
            notes.append(
                f"Skipped {symbol} {right} {p.get('strike')} {expiry_d.isoformat()}: "
                "already expired."
            )
            continue

        pot_name = data_table.get_pot(symbol)
        # Pot filter (after pot is computed, before RoC math)
        if pot != "all" and pot_name != pot:
            continue

        # ── Look up entry ────────────────────────────────────────────────
        entry_date_d: Optional[date] = None
        entry_premium: Optional[float] = None
        entry_fill_found = False

        if sheet_rows:
            row = data_table._match_open_row(sheet_rows, symbol, strike, expiry_d, right)
            if row:
                entry_date_d = data_table._parse_iso_date(row.get("Date_open"))
                entry_premium = data_table._to_float(row.get("OptPremium"))
                entry_fill_found = entry_date_d is not None
        else:
            fill = data_table._match_fill_for_position(
                fallback_fills, symbol, strike, expiry_d, right
            )
            if fill:
                entry_date_d = data_table._parse_iso_date(fill.get("trade_time")) or \
                               data_table._parse_iso_date(fill.get("order_time"))
                entry_premium = data_table._to_float(fill.get("avg_fill_price"))
                entry_fill_found = entry_date_d is not None

        # Build the row even if we couldn't find an entry — caller still
        # gets yield_on_notional and pct_harvested from avg_cost.
        if entry_date_d is None:
            base = {
                "symbol": symbol,
                "right": right,
                "strike": strike,
                "expiry": expiry_d.isoformat(),
                "quantity": -int(qty),
                "pot": pot_name,
                "entry_date": None,
                "entry_source": entry_source,
                "entry_premium": entry_premium,
                "entry_fill_found": False,
                "notes": "entry STO not found in either source",
            }
            # Still compute the no-time-component fields
            notional = strike * 100.0 * qty
            premium_received = avg_cost * 100.0 * qty
            current_value = market_price * 100.0 * qty
            pnl_to_date = premium_received - current_value
            base.update({
                "days_held": None,
                "dte_at_entry": None,
                "dte_remaining": (expiry_d - asof).days,
                "notional": round(notional, 2),
                "premium_received": round(premium_received, 2),
                "current_value": round(current_value, 2),
                "pnl_to_date": round(pnl_to_date, 2),
                "yield_on_notional": round(premium_received / notional, 4) if notional > 0 else 0.0,
                "pct_harvested": round(pnl_to_date / premium_received, 4) if premium_received > 0 else 0.0,
                "annualised_roc": None,
                "juiced": (pnl_to_date / premium_received >= data_table.JUICED_THRESHOLD) if premium_received > 0 else False,
                "juiced_threshold": data_table.JUICED_THRESHOLD,
            })
            missing_entry.append(base)
            out.append(base)
            continue

        roc = data_table.compute_position_roc(
            symbol=symbol, strike=strike, expiry=expiry_d, right=right,
            qty=int(qty), avg_cost=avg_cost, market_price=market_price,
            entry_date=entry_date_d, today=asof,
        )
        out.append({
            "symbol": symbol,
            "right": right,
            "strike": strike,
            "expiry": expiry_d.isoformat(),
            "quantity": -int(qty),
            "pot": pot_name,
            "entry_source": entry_source,
            "entry_premium": entry_premium,
            "entry_fill_found": entry_fill_found,
            "notes": "",
            **roc,
        })

    # ── 4. Optional juiced_only filter (post-compute) ───────────────────
    if juiced_only:
        out = [p for p in out if p.get("juiced")]

    aggregates = data_table.aggregate_positions(out)
    juiced_positions = [p for p in out if p.get("juiced")]

    return {
        "positions": out,
        "aggregates": aggregates,
        "juiced_positions": juiced_positions,
        "missing_entry_positions": missing_entry,
        "asof_date": asof.isoformat(),
        "entry_source": entry_source,
        "juiced_threshold": data_table.JUICED_THRESHOLD,
        "notes": notes,
    }


# ── PMCC §12 scorecard + HV helpers (Phase E1, PMCC track) ───────────────────

_GAMMA_LOW = 0.005
_GAMMA_HIGH = 0.015


def _gamma_level(gamma: float) -> str:
    """Classify gamma per §12 Greeks block. Thresholds calibrated for SPY
    near-the-money — re-tune per-ticker as the doctrine matures."""
    g = abs(gamma)
    if g < _GAMMA_LOW:
        return "low"
    if g >= _GAMMA_HIGH:
        return "high"
    return "moderate"


@mcp.tool()
def compute_hv(symbol: str, lookback_days: int = 30) -> dict:
    """Annualised historical (realised) volatility from daily closes.

    PMCC §2 calls for HV30 to set the dynamic theta hurdle. This tool
    pulls daily bars via yfinance and computes:
      hv = stdev(log returns, ddof=1) × √252

    Use the returned `hv` value directly as the `hv30` arg to
    `score_pmcc_candidate`.

    Args:
      symbol: ticker, e.g. "SPY"
      lookback_days: window of LOG RETURNS (the function fetches one extra
        close so it can produce N returns from N+1 closes; default 30 = HV30)

    Returns:
      {symbol, lookback_days, hv, sample_size, source, asof_date, notes[]}
      `hv` is the annualised decimal vol (0.17 = 17%). 0.0 means the fetch
      failed or there weren't enough bars.
    """
    from tiger_api.montecarlo import realized_vol

    notes: list[str] = []
    closes: list[float] = []
    source = "yfinance"

    try:
        import yfinance as yf
    except ImportError:
        return {
            "symbol": symbol.upper(),
            "lookback_days": lookback_days,
            "hv": 0.0,
            "sample_size": 0,
            "source": "none",
            "asof_date": date.today().isoformat(),
            "notes": ["yfinance not installed — cannot fetch bars."],
        }

    fetch_days = max(lookback_days + 10, 60)  # extra buffer for weekends/holidays
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period=f"{fetch_days}d", auto_adjust=False)
        if hist is not None and "Close" in hist.columns:
            closes = [float(c) for c in hist["Close"].tolist() if c == c]  # drop NaNs
    except Exception as e:
        notes.append(f"yfinance fetch failed for {symbol}: {e}")

    if len(closes) < lookback_days + 1:
        notes.append(
            f"Only {len(closes)} closes available, need >= {lookback_days + 1}. "
            "HV may be noisy or zero."
        )

    hv = realized_vol(closes, window=lookback_days)

    return {
        "symbol": symbol.upper(),
        "lookback_days": lookback_days,
        "hv": _round(hv, 4),
        "sample_size": len(closes),
        "source": source,
        "asof_date": date.today().isoformat(),
        "notes": notes,
    }


@mcp.tool()
def score_pmcc_candidate(
    symbol: str,
    strike: float,
    expiry: str,
    side: str,
    premium: float,
    spot: float,
    hv30: float,
    risk_free_rate: float = 0.045,
    n_paths: int = 5000,
    seed: int | None = None,
) -> dict:
    """PMCC Master Doctrine v3 §12 Trade Evaluation Scorecard.

    Runs the full scorecard for a prospective short option leg (STO):
      • Greeks (Δ, Θ, vega, gamma) via Black-Scholes from the supplied
        premium-implied IV.
      • Theta hurdle test (§2): `daily_1sigma × 0.04` = hurdle/day.
      • Monte Carlo terminal-price distribution (GBM, n_paths default 5000):
        P(profit ≥ 50%), P(profit ≥ 80%), P(loss), P(assignment),
        expected P&L, P&L stdev, CVaR (worst 5%).
      • Risk-adjusted block: annualised return, annualised vol,
        Sharpe-equivalent, capital efficiency.
      • Verdict with §12 cutoffs: pass / conditional / fail.

    All math runs server-side — Claude pastes inputs, the tool returns
    the populated scorecard structure ready for the §16 output block.

    Args:
      symbol:          underlying ticker, e.g. "SPY"
      strike:          option strike (per share)
      expiry:          ISO "YYYY-MM-DD" expiry date
      side:            "STO_PUT" or "STO_CALL" (v1 — BTC/ROLL scoring tbd)
      premium:         current option mark per share — used to solve IV
      spot:            current underlying spot (from FMP quote / IBKR snapshot)
      hv30:            30-day realised vol (decimal, e.g. 0.17). Get via
                       `compute_hv("SPY")` if you don't have a hot value.
      risk_free_rate:  annual r (default 4.5%, ~1Y Treasury)
      n_paths:         MC sample count (default 5000 per §12)
      seed:            optional RNG seed for reproducible scorecards

    Returns:
      {trade, greeks, distribution, risk_adjusted, verdict,
       capital_at_risk, asof_date, ...}
    """
    from tiger_api.greeks import (
        bs_delta_theta, bs_gamma, bs_vega, implied_vol,
    )
    from tiger_api.montecarlo import (
        mc_terminal_prices, short_option_pnl_distribution, pmcc_verdict,
    )

    # ── Input validation ────────────────────────────────────────────────
    side_n = side.strip().upper().replace(" ", "_").replace("-", "_")
    if side_n not in ("STO_PUT", "STO_CALL"):
        return {
            "error": "UNSUPPORTED_SIDE",
            "message": (
                f"side={side!r} not supported. v1 scorecard handles STO_PUT and "
                "STO_CALL only. BTC/ROLL scorecards are tracked in BACKLOG."
            ),
        }
    is_call = (side_n == "STO_CALL")
    symbol_n = symbol.strip().upper()

    asof = date.today()
    expiry_d = _parse_tiger_expiry(expiry)
    if expiry_d is None:
        return {
            "error": "BAD_EXPIRY",
            "message": f"Could not parse expiry {expiry!r}. Use ISO YYYY-MM-DD.",
        }
    dte_days = (expiry_d - asof).days
    if dte_days <= 0:
        return {
            "error": "EXPIRED",
            "message": f"Expiry {expiry_d.isoformat()} is today or past; cannot score.",
        }
    t = dte_days / 365.0

    if premium <= 0 or strike <= 0 or spot <= 0 or hv30 <= 0:
        return {
            "error": "INVALID_INPUTS",
            "message": (
                f"All of premium/strike/spot/hv30 must be > 0 "
                f"(got premium={premium}, strike={strike}, spot={spot}, hv30={hv30})."
            ),
        }

    # ── Greeks block — solve IV from premium, then BS Greeks ────────────
    iv = implied_vol(
        market_price=premium, spot=spot, strike=strike, t=t,
        r=risk_free_rate, is_call=is_call, q=0.0,
    )
    if iv is None or iv <= 0:
        return {
            "error": "IV_SOLVE_FAILED",
            "message": (
                f"Could not solve IV from premium=${premium:.2f} (likely below "
                "intrinsic or arbitrage). Re-quote the contract and retry."
            ),
        }

    delta_long, theta_per_year_long = bs_delta_theta(
        spot=spot, strike=strike, t=t, r=risk_free_rate, sigma=iv,
        is_call=is_call, q=0.0,
    )
    vega_long = bs_vega(
        spot=spot, strike=strike, t=t, r=risk_free_rate, sigma=iv, q=0.0,
    )
    gamma = bs_gamma(
        spot=spot, strike=strike, t=t, r=risk_free_rate, sigma=iv, q=0.0,
    )
    # Short position: flip delta + theta + vega signs (gamma unchanged for
    # display, magnitude only — short positions still gamma-positive
    # exposure-wise on the underlying).
    delta_short = -delta_long
    theta_per_day_short = -theta_per_year_long / 365.0
    vega_short = -vega_long

    # ── §2 theta hurdle — `daily_1σ × 0.04` ─────────────────────────────
    daily_1sigma_usd = strike * hv30 / math.sqrt(252)
    theta_hurdle = daily_1sigma_usd * 0.04

    # ── §12 Monte Carlo distribution ────────────────────────────────────
    terminal_prices = mc_terminal_prices(
        spot=spot, sigma=hv30, t=t, r=risk_free_rate,
        n_paths=n_paths, seed=seed,
    )
    distribution = short_option_pnl_distribution(
        terminal_prices=terminal_prices, strike=strike, premium=premium,
        is_call=is_call,
    )

    # ── Risk-adjusted block ─────────────────────────────────────────────
    # Capital at risk for a cash-secured / covered short is strike * 100.
    # For PMCC short calls covered by a deep-ITM LEAPS this is conservative
    # (true risk bounded by LEAPS strike) — flag in notes when relevant.
    capital_at_risk = strike * 100.0
    expected_pnl = distribution["expected_pnl"]
    pnl_stdev = distribution["pnl_stdev"]

    annualised_return = 0.0
    annualised_vol = 0.0
    if capital_at_risk > 0 and dte_days > 0:
        annualised_return = (expected_pnl / capital_at_risk) * 365.0 / dte_days
        # Convert per-trade stdev to annualised stdev via √(365/dte).
        annualised_vol = (pnl_stdev / capital_at_risk) * math.sqrt(365.0 / dte_days)

    verdict_block = pmcc_verdict(
        distribution=distribution,
        theta_per_day=theta_per_day_short,
        theta_hurdle=theta_hurdle,
        annualised_return=annualised_return,
        annualised_vol=annualised_vol,
    )

    capital_efficiency = expected_pnl / capital_at_risk if capital_at_risk > 0 else 0.0

    return {
        "trade": {
            "action": side_n,
            "symbol": symbol_n,
            "strike": strike,
            "expiry": expiry_d.isoformat(),
            "dte_days": dte_days,
            "premium": _round(premium, 4),
        },
        "greeks": {
            "delta": _round(delta_short, 4),
            "theta_per_day": _round(theta_per_day_short, 4),
            "theta_hurdle": _round(theta_hurdle, 4),
            "theta_pass": verdict_block["theta_pass"],
            "vega": _round(vega_short, 4),
            "gamma": _round(gamma, 6),
            "gamma_level": _gamma_level(gamma),
            "iv_solved": _round(iv, 4),
            "daily_1sigma_usd": _round(daily_1sigma_usd, 4),
        },
        "distribution": {
            "n_paths": n_paths,
            "hv30": hv30,
            "r": risk_free_rate,
            "p_profit_50": _round(distribution["p_profit_50"], 4),
            "p_profit_80": _round(distribution["p_profit_80"], 4),
            "p_loss": _round(distribution["p_loss"], 4),
            "p_assignment": _round(distribution["p_assignment"], 4),
            "expected_pnl": _round(distribution["expected_pnl"], 2),
            "pnl_stdev": _round(distribution["pnl_stdev"], 2),
            "cvar_5": _round(distribution["cvar_5"], 2),
            "max_profit": _round(distribution["max_profit"], 2),
        },
        "risk_adjusted": {
            "annualised_return": _round(annualised_return, 4),
            "annualised_vol": _round(annualised_vol, 4),
            "sharpe_equiv": verdict_block["sharpe_equiv"],
            "capital_efficiency": _round(capital_efficiency, 6),
        },
        "verdict": verdict_block["verdict"],
        "verdict_reasons": verdict_block["verdict_reasons"],
        "capital_at_risk": _round(capital_at_risk, 2),
        "asof_date": asof.isoformat(),
        "notes": [
            "MC = geometric Brownian motion, hv30 used as σ. Doctrine §12 "
            "spec uses HV30 (or IV30 if available). Pass IV30 as hv30 when "
            "you have it for a tighter distribution.",
            "PMCC short calls covered by a deep-ITM LEAPS have true risk "
            "bounded by the LEAPS strike — capital_at_risk reported here is "
            "the strike×100 conservative ceiling.",
        ],
    }


# ── Wheel-state classifier (Phase E1, Priority 1) ────────────────────────────


_WHEEL_STATES = {
    "CSP_OPEN",     # cash, short put(s) open
    "ASSIGNED",     # holding stock, no short call yet
    "CC_OPEN",      # holding stock + short call(s)
    "LEAP_ONLY",    # long option(s) only (PMCC chassis no short cover)
    "MIXED",        # combinations that don't fit a clean wheel state
    "IDLE",         # no live position
}


def _earliest_position_entry(
    sheet_rows: list[dict],
    fallback_fills: list[dict],
    ticker: str,
    opt_positions: list[dict],
) -> tuple[Optional[date], str]:
    """Return the earliest Date_open among the ticker's currently-held option
    positions and the source tag ('google_sheets' / 'tiger_mcp_fallback' /
    'none'). Used to anchor cycle_start_date — when this current state began."""
    from mcp_servers.tiger import data_table

    candidates: list[date] = []
    matched_via_sheets = False
    matched_via_tiger = False

    for opt in opt_positions:
        right = (opt.get("right") or "").upper()
        if right not in ("PUT", "CALL"):
            continue
        try:
            strike_f = float(opt.get("strike"))
        except (TypeError, ValueError):
            continue
        expiry_d = data_table._parse_iso_date(opt.get("expiry"))
        if expiry_d is None:
            continue

        if sheet_rows:
            row = data_table._match_open_row(sheet_rows, ticker, strike_f, expiry_d, right)
            if row:
                d = data_table._parse_iso_date(row.get("Date_open"))
                if d is not None:
                    candidates.append(d)
                    matched_via_sheets = True
                    continue

        if fallback_fills:
            fill = data_table._match_fill_for_position(
                fallback_fills, ticker, strike_f, expiry_d, right,
            )
            if fill:
                d = (
                    data_table._parse_iso_date(fill.get("trade_time"))
                    or data_table._parse_iso_date(fill.get("order_time"))
                )
                if d is not None:
                    candidates.append(d)
                    matched_via_tiger = True

    if not candidates:
        return None, "none"
    if matched_via_sheets and not matched_via_tiger:
        src = "google_sheets"
    elif matched_via_tiger and not matched_via_sheets:
        src = "tiger_mcp_fallback"
    else:
        src = "mixed"
    return min(candidates), src


@mcp.tool()
def get_wheel_state(
    symbols: list[str] | None = None,
    pot: str = "all",
) -> dict:
    """Per-ticker wheel cycle state classification.

    Returns the current wheel-cycle phase for each ticker in the book:

      CSP_OPEN   — cash secured, short put(s) open (selling premium)
      ASSIGNED   — holding stock, no short call yet (uncovered share lot)
      CC_OPEN    — holding stock + short call(s) (covered call active)
      LEAP_ONLY  — long option(s) only (PMCC chassis without short cover)
      MIXED      — combinations not cleanly modeled as a wheel state
                   (e.g. long puts, stock + short puts on same ticker)
      IDLE       — no live position (only relevant if user explicitly
                   requested a ticker not in the book)

    Pot routing follows the locked CORE / ACTIVE / SIDECAR map. Tickers
    in EXCLUDE_TICKERS (KO, MCD, NVDA, SPY) are dropped entirely.

    Cycle anchor: `cycle_start_date` is the earliest Date_open among the
    currently-held option positions for the ticker — i.e. when the current
    state's first leg was opened. Sourced from Google Sheets primary,
    Tiger 90-day fallback (same pattern as get_position_roc). `null` if
    no source could date the position.

    Args:
      symbols: optional list of tickers to filter to (case-insensitive)
      pot:     "all" | "core" | "active" | "sidecar"

    Returns:
      tickers[]:  per-ticker rows with state + positions + cycle anchor
      summary:    {total_tickers, by_state, by_pot}
      asof_date:  today's ISO date
      notes[]:    caveats (data-source warnings, etc.)
    """
    from mcp_servers.tiger import data_table

    asof = date.today()
    client = _get_client()

    stock_positions = [_position_to_dict(p) for p in client.get_stock_positions()]
    option_positions = [_position_to_dict(p) for p in client.get_option_positions()]

    # Tickers present in the book
    tickers_in_book = {
        (p.get("symbol") or "").upper() for p in stock_positions + option_positions
        if p.get("symbol")
    }
    tickers_in_book -= data_table.EXCLUDE_TICKERS

    # Resolve target set
    if symbols:
        requested = {s.strip().upper() for s in symbols if s and s.strip()}
        requested -= data_table.EXCLUDE_TICKERS
        target = sorted(requested)
    else:
        target = sorted(tickers_in_book)

    # Pot filter (after include/exclude)
    if pot != "all":
        target = [t for t in target if data_table.get_pot(t) == pot]

    # Sheets primary, Tiger 90-day fallback (mirrors get_position_roc)
    sheet_rows = data_table.read_data_table()
    fallback_fills: list[dict] = []
    notes: list[str] = []
    if not sheet_rows:
        try:
            fallback_fills = [
                _order_to_dict(o) for o in client.get_filled_orders(days=90)
            ]
            notes.append(
                "Sheets unavailable — cycle_start_date anchored to Tiger "
                "90-day fill window; older positions may report null."
            )
        except Exception as e:
            notes.append(f"Sheets and Tiger fallback both failed: {e}")

    results: list[dict] = []
    by_state: dict[str, int] = {s: 0 for s in _WHEEL_STATES}
    by_pot: dict[str, int] = {"core": 0, "active": 0, "sidecar": 0, "unknown": 0}

    for ticker in target:
        stock_rows = [
            p for p in stock_positions
            if (p.get("symbol") or "").upper() == ticker
        ]
        opt_rows = [
            p for p in option_positions
            if (p.get("symbol") or "").upper() == ticker
        ]

        # Reduce to "live" booleans / counts
        stock_qty = 0.0
        stock_avg_cost: Optional[float] = None
        for p in stock_rows:
            try:
                q = float(p.get("quantity") or 0)
                stock_qty += q
                if stock_avg_cost is None:
                    ac = p.get("avg_cost")
                    if ac is not None:
                        try:
                            stock_avg_cost = float(ac)
                        except (TypeError, ValueError):
                            pass
            except (TypeError, ValueError):
                continue

        short_puts: list[dict] = []
        short_calls: list[dict] = []
        long_opts: list[dict] = []
        for p in opt_rows:
            try:
                q = float(p.get("quantity") or 0)
            except (TypeError, ValueError):
                continue
            right = (p.get("right") or "").upper()
            if q < 0 and right == "PUT":
                short_puts.append(p)
            elif q < 0 and right == "CALL":
                short_calls.append(p)
            elif q > 0:
                long_opts.append(p)

        has_stock = stock_qty > 0

        # Classify. The wheel doctrine cleanly admits the four named states;
        # anything that doesn't fit is "MIXED" so the operator can see it.
        if has_stock and short_calls and not short_puts and not long_opts:
            state = "CC_OPEN"
        elif has_stock and not short_calls and not short_puts and not long_opts:
            state = "ASSIGNED"
        elif not has_stock and short_puts and not short_calls and not long_opts:
            state = "CSP_OPEN"
        elif not has_stock and not short_puts and not short_calls and long_opts:
            state = "LEAP_ONLY"
        elif not has_stock and not short_puts and not short_calls and not long_opts:
            state = "IDLE"
        else:
            state = "MIXED"

        cycle_start_d, cycle_start_src = _earliest_position_entry(
            sheet_rows, fallback_fills, ticker, opt_rows,
        )
        days_in_cycle = (asof - cycle_start_d).days if cycle_start_d else None

        pot_name = data_table.get_pot(ticker)
        by_state[state] = by_state.get(state, 0) + 1
        by_pot[pot_name] = by_pot.get(pot_name, 0) + 1

        results.append({
            "ticker": ticker,
            "state": state,
            "pot": pot_name,
            "has_stock": has_stock,
            "stock_quantity": stock_qty if has_stock else 0,
            "stock_avg_cost": stock_avg_cost,
            "short_puts": short_puts,
            "short_calls": short_calls,
            "long_options": long_opts,
            "current_positions_count": len(opt_rows) + len(stock_rows),
            "cycle_start_date": cycle_start_d.isoformat() if cycle_start_d else None,
            "cycle_start_source": cycle_start_src,
            "days_in_cycle": days_in_cycle,
        })

    return {
        "tickers": results,
        "summary": {
            "total_tickers": len(results),
            "by_state": by_state,
            "by_pot": by_pot,
        },
        "asof_date": asof.isoformat(),
        "entry_source": "google_sheets" if sheet_rows else "tiger_mcp_fallback",
        "notes": notes,
    }


# ── §5.1 Earning Power Test (Phase E1, Priority 4) ───────────────────────────


@mcp.tool()
def earning_power_test(
    current_theta_per_day: float,
    current_delta: float,
    new_theta_per_day: float,
    new_delta: float,
    roll_debit: float,
    new_dte_days: int,
    expected_daily_drift: float = 0.0,
    payback_threshold_frac: float = 0.5,
) -> dict:
    """PMCC Master Doctrine §5.1 — Portfolio Earning Power Test.

    The primary roll decision. Never compares remaining extrinsic in the
    dying leg against the roll debit (that test optimizes for cheapest BTC
    while ignoring lost earning power). Instead:

        current_earning  = current_theta_per_day + current_delta × drift
        new_earning      = new_theta_per_day     + new_delta     × drift
        daily_improvement = new_earning − current_earning
        payback_days     = roll_debit ÷ daily_improvement
        ROLL  if payback_days < new_dte_days × threshold_frac (default 0.5)
        HOLD  otherwise

    Use `drift = 0` for the conservative flat-market assumption. If the
    market is trending (3+ consecutive directional days), pass actual
    recent drift per $1 underlying.

    All Greeks are **per-day, per-position** numbers (not annualised).
    Delta is the dollar-delta of the leg (Δ × 100 × contracts).
    `roll_debit` is the net cash cost to execute the roll
    (positive = debit, negative = net credit collected).

    Args:
      current_theta_per_day: existing leg's θ in $/day (signed — short = positive)
      current_delta:         existing leg's delta in $/$ underlying
      new_theta_per_day:     proposed replacement leg's θ in $/day
      new_delta:             proposed replacement leg's delta
      roll_debit:            net cash to execute (positive = pay; negative = collect)
      new_dte_days:          DTE of the new leg
      expected_daily_drift:  expected daily $ move of underlying (default 0)
      payback_threshold_frac: ROLL/HOLD cutoff as fraction of new_dte_days
                              (default 0.5 — doctrine §5.1 Step 5)

    Returns:
      Same shape as a §5.4 Roll Decomposition Template row — daily
      improvement, payback days, verdict, justification — ready to paste
      into the §16 output block.
    """
    drift = expected_daily_drift
    current_earning = current_theta_per_day + current_delta * drift
    new_earning = new_theta_per_day + new_delta * drift
    daily_improvement = new_earning - current_earning

    threshold_days = new_dte_days * payback_threshold_frac

    payback_days: Optional[float] = None
    verdict: str
    reason: str

    if daily_improvement <= 0:
        verdict = "HOLD"
        reason = (
            f"daily_improvement = ${daily_improvement:.4f}/day ≤ 0 — new leg "
            "does not earn more than the current leg, so any roll debit never "
            "amortises. Hold and re-evaluate on next session."
        )
    elif roll_debit <= 0:
        # Net credit roll — improves earning AND collects cash. Always pass.
        payback_days = 0.0
        verdict = "ROLL"
        reason = (
            f"Net credit roll (${-roll_debit:.2f} collected) AND new leg earns "
            f"${daily_improvement:.4f}/day more. Pass on both axes."
        )
    else:
        payback_days = roll_debit / daily_improvement
        if payback_days < threshold_days:
            verdict = "ROLL"
            reason = (
                f"payback {payback_days:.1f} days < {threshold_days:.1f} "
                f"({payback_threshold_frac:.0%} of new DTE = {new_dte_days}d). "
                "Roll pays back well within the new leg's life."
            )
        else:
            verdict = "HOLD"
            reason = (
                f"payback {payback_days:.1f} days ≥ {threshold_days:.1f} "
                f"({payback_threshold_frac:.0%} of new DTE = {new_dte_days}d). "
                "Hold — the new leg won't earn back the debit in time."
            )

    return {
        "current_earning_per_day": _round(current_earning, 4),
        "new_earning_per_day": _round(new_earning, 4),
        "daily_improvement": _round(daily_improvement, 4),
        "roll_debit": _round(roll_debit, 2),
        "new_dte_days": new_dte_days,
        "payback_days": _round(payback_days, 2) if payback_days is not None else None,
        "threshold_days": _round(threshold_days, 2),
        "payback_threshold_frac": payback_threshold_frac,
        "expected_daily_drift": expected_daily_drift,
        "verdict": verdict,
        "reason": reason,
    }


# ── Roll candidate engine (Phase E1, Priority 2) ─────────────────────────────


def _select_candidate_expiries(
    available_expiries: list[str],
    current_expiry: date,
    n: int = 2,
) -> list[date]:
    """Pick the next N expiries strictly AFTER the current short's expiry."""
    out: list[date] = []
    for raw in available_expiries:
        d = _parse_tiger_expiry(raw)
        if d is None or d <= current_expiry:
            continue
        out.append(d)
    out.sort()
    return out[:n]


def _filter_strikes_around(target: float, chain_rows: list[dict],
                            window: int = 5) -> list[dict]:
    """Keep the window of strikes nearest to target — `window` on each side."""
    rows_with_strike = []
    for r in chain_rows:
        try:
            s = float(r.get("strike"))
        except (TypeError, ValueError):
            continue
        rows_with_strike.append((s, r))
    if not rows_with_strike:
        return []
    rows_with_strike.sort(key=lambda x: x[0])
    below = [r for r in rows_with_strike if r[0] < target][-window:]
    above = [r for r in rows_with_strike if r[0] >= target][:window]
    keep = below + above
    return [r[1] for r in keep]


@mcp.tool()
def get_roll_candidates(
    symbol: str,
    right: str,
    strike: float,
    expiry: str,
    quantity: int,
    pot: str = "core",
    cost_basis: float | None = None,
) -> dict:
    """Forward-looking roll candidates for one open short option leg.

    Returns:
      • The current position's mark (from live Tiger positions)
      • A structural anchor for the underlying (FMP daily bars, last 250d)
      • A ranked list of candidate STO legs (2 forward expiries × ±5 strikes
        around the structural target) with mid, extrinsic, net credit, Δ, Θ
      • Distance fields so Claude can apply charter rules without recompute

    Architecture: ALL data + structural analysis is server-side. Claude
    reads the result and applies pot-specific charter rules (delta band,
    cost basis check, yield gate). PM executes.

    Args:
      symbol:     ticker, e.g. "MARA"
      right:      "PUT" or "CALL"
      strike:     current position strike
      expiry:     current position expiry, ISO "YYYY-MM-DD"
      quantity:   negative int = short (e.g. -18)
      pot:        "core" | "active" — informational; constraint logic in Claude
      cost_basis: required for Core Pot calls; resistance anchor will be
                  lifted above cost_basis when supplied

    Returns:
      current_position{}, structural_anchor{}, candidates[],
      candidate_expiries_pulled[], strikes_filtered, asof_date, notes[]
    """
    from dataclasses import asdict
    from tiger_api import roll_engine

    asof = date.today()
    notes: list[str] = []

    symbol_n = symbol.strip().upper()
    right_n = right.strip().upper()
    if right_n not in ("PUT", "CALL"):
        return {"error": "BAD_RIGHT", "message": f"right must be PUT or CALL, got {right!r}"}
    current_expiry = _parse_tiger_expiry(expiry)
    if current_expiry is None:
        return {"error": "BAD_EXPIRY", "message": f"Could not parse expiry {expiry!r}"}

    qty = int(quantity)
    if qty == 0:
        return {"error": "ZERO_QUANTITY", "message": "quantity must be non-zero"}

    client = _get_client()

    # 1. Pull the current position so we can show its live mark.
    open_opts = [_position_to_dict(p) for p in client.get_option_positions()]
    current_pos = None
    for p in open_opts:
        if ((p.get("symbol") or "").upper() == symbol_n
                and (p.get("right") or "").upper() == right_n):
            try:
                if (abs(float(p.get("strike")) - float(strike)) < 0.01
                        and _parse_tiger_expiry(p.get("expiry")) == current_expiry):
                    current_pos = p
                    break
            except (TypeError, ValueError):
                continue

    if current_pos is None:
        return {
            "error": "POSITION_NOT_FOUND",
            "message": (
                f"No open {symbol_n} {right_n} {strike} exp {current_expiry.isoformat()} "
                "in the book. Confirm symbol, right, strike, expiry from "
                "get_option_positions() first."
            ),
        }

    try:
        current_mark = float(current_pos.get("market_price") or 0)
    except (TypeError, ValueError):
        current_mark = 0.0
    btc_cost_at_mid = round(current_mark * 100.0 * abs(qty), 2)
    dte_remaining = (current_expiry - asof).days

    # 2. FMP bars + structural anchor + ATR.
    try:
        bars = roll_engine.fetch_fmp_bars(symbol_n, max_bars=250)
    except roll_engine.FMPError as e:
        return {
            "error": "FMP_UNAVAILABLE",
            "message": f"FMP bars unavailable: {e}. Structural anchor cannot be computed.",
            "current_position": {
                "symbol": symbol_n, "right": right_n, "strike": float(strike),
                "expiry": current_expiry.isoformat(), "quantity": qty,
                "current_mark": current_mark, "btc_cost_at_mid": btc_cost_at_mid,
                "dte_remaining": dte_remaining,
            },
            "asof_date": asof.isoformat(),
        }

    spot = bars[-1].close if bars else 0.0
    atr_14 = roll_engine.atr(bars, period=14)
    if right_n == "PUT":
        anchor = roll_engine.find_support(bars, spot, atr_14)
    else:
        anchor = roll_engine.find_resistance(bars, spot, atr_14, cost_basis=cost_basis)
    anchor_d = asdict(anchor)
    anchor_d["source"] = (
        f"FMP daily bars {bars[0].date if bars else '?'} to "
        f"{bars[-1].date if bars else '?'} — {anchor_d['source']}"
    )
    if anchor.anchor_type == "none":
        notes.append(anchor.note or "No structural anchor — strike selection degraded.")

    # 3. Candidate expiries (next two after current).
    try:
        expiries_payload = client.get_option_expirations([symbol_n]) or {}
        avail_raw = expiries_payload.get(symbol_n, [])
    except Exception as e:
        return {
            "error": "EXPIRATIONS_UNAVAILABLE",
            "message": f"get_option_expirations failed: {e}",
            "current_position": {
                "symbol": symbol_n, "right": right_n, "strike": float(strike),
                "expiry": current_expiry.isoformat(), "quantity": qty,
                "current_mark": current_mark, "btc_cost_at_mid": btc_cost_at_mid,
                "dte_remaining": dte_remaining,
            },
            "structural_anchor": anchor_d,
            "asof_date": asof.isoformat(),
        }
    candidate_expiry_dates = _select_candidate_expiries(avail_raw, current_expiry, n=2)
    if not candidate_expiry_dates:
        return {
            "error": "NO_FORWARD_EXPIRIES",
            "message": (
                f"No expiries after {current_expiry.isoformat()} found for {symbol_n}. "
                "Tiger may not have listings yet."
            ),
            "current_position": {
                "symbol": symbol_n, "right": right_n, "strike": float(strike),
                "expiry": current_expiry.isoformat(), "quantity": qty,
                "current_mark": current_mark, "btc_cost_at_mid": btc_cost_at_mid,
                "dte_remaining": dte_remaining,
            },
            "structural_anchor": anchor_d,
            "asof_date": asof.isoformat(),
        }

    target_strike = anchor.target_strike_price or float(strike)

    # 4. Pull chain per candidate expiry, filter to ±5 strikes around target.
    candidates: list[dict] = []
    for exp_d in candidate_expiry_dates:
        exp_iso = exp_d.isoformat()
        try:
            chain = client.get_option_chain(
                symbol=symbol_n, expiry=exp_iso, include_greeks=True,
            )
        except Exception as e:
            notes.append(f"Chain pull failed for {symbol_n} {exp_iso}: {e}")
            continue

        # Filter to RIGHT side only (PUTs or CALLs)
        side_rows = [
            r for r in chain
            if (r.get("right") or "").upper() == right_n
        ]
        windowed = _filter_strikes_around(target_strike, side_rows, window=5)

        for row in windowed:
            try:
                row_strike = float(row.get("strike"))
                bid = float(row.get("bid") or 0)
                ask = float(row.get("ask") or 0)
            except (TypeError, ValueError):
                continue
            if bid <= 0 and ask <= 0:
                continue
            mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else max(bid, ask)
            if mid <= 0:
                continue

            # Extrinsic = mid − intrinsic
            if right_n == "PUT":
                intrinsic = max(0.0, row_strike - spot)
            else:
                intrinsic = max(0.0, spot - row_strike)
            extrinsic = round(mid - intrinsic, 4)
            if extrinsic < 0.05:
                continue  # discard worthless candidates per spec

            delta_v = _to_float_or_none(row.get("delta"))
            theta_v = _to_float_or_none(row.get("theta"))
            iv_v = _to_float_or_none(row.get("implied_vol") or row.get("iv"))

            math_block = roll_engine.roll_math(
                btc_mid=current_mark, sto_mid=mid, qty=qty,
            )

            dte = (exp_d - asof).days
            dist_spot = round((row_strike - spot) / spot * 100.0, 2) if spot > 0 else None
            dist_anchor = (
                round((row_strike - anchor.anchor_price) / anchor.anchor_price * 100.0, 2)
                if anchor.anchor_price else None
            )

            candidates.append({
                "expiry": exp_iso,
                "dte": dte,
                "strike": row_strike,
                "right": right_n,
                "bid": bid,
                "ask": ask,
                "mid": round(mid, 4),
                "extrinsic": extrinsic,
                "delta": delta_v,
                "theta_per_day": theta_v,
                "iv": iv_v,
                "btc_cost_total": math_block["btc_cost_total"],
                "sto_recv_total": math_block["sto_recv_total"],
                "net_credit": math_block["net_credit"],
                "net_credit_per_lot": math_block["net_credit_per_lot"],
                "dist_from_spot_pct": dist_spot,
                "dist_from_anchor_pct": dist_anchor,
            })

    candidates.sort(key=lambda r: r.get("net_credit") or 0.0, reverse=True)

    return {
        "current_position": {
            "symbol": symbol_n,
            "right": right_n,
            "strike": float(strike),
            "expiry": current_expiry.isoformat(),
            "quantity": qty,
            "current_mark": current_mark,
            "btc_cost_at_mid": btc_cost_at_mid,
            "dte_remaining": dte_remaining,
            "pot": pot,
        },
        "structural_anchor": anchor_d,
        "spot": round(spot, 4),
        "candidates": candidates,
        "candidate_expiries_pulled": [d.isoformat() for d in candidate_expiry_dates],
        "strikes_filtered": (
            f"±5 strikes around target ${target_strike:.2f}"
            if anchor.target_strike_price is not None
            else "±5 strikes around current strike (anchor degraded)"
        ),
        "asof_date": asof.isoformat(),
        "notes": notes,
    }


# ── Stress test engine (Phase E1, Priority 3) ────────────────────────────────


@mcp.tool()
def run_stress_test() -> dict:
    """Live-position stress test: Scenarios A / B / D / B+D.

    Scenarios:
      A  — Core stocks −15% (MARA + CRCL). No put assignment.
      B  — Core stocks −30% + short-put assignment loss + call premium offset
           (100% — calls go OTM and decay to zero).
      D  — SPY −20% on PMCC LEAPS chassis. Short-call premium offset 50%
           (conservative — calls don't fully expire worthless in a single shock).
      BD — Combined B + D (worst credible case).

    Inputs are pulled live — no parameters:
      get_account_summary   → NAV
      get_prime_assets      → excess liquidity, margin debit, maintain margin
      get_stock_positions   → MARA + CRCL marks
      get_option_positions  → short puts, short calls, LEAPS
      compute_portfolio_greeks → LEAPS deltas (for D + BD)
      compute_hv("SPY") then yfinance close for SPY spot

    Zone classification:
      > $60K  safe   |  $40–60K watch  |  $20–40K reduce
      $0–20K critical |  ≤ $0 insolvent

    Returns: {baseline, scenarios, current_zone, reduction_schedule,
              pmcc_hard_stop, asof_date, notes[]}
    """
    from tiger_api import stress_engine

    asof = date.today()
    notes: list[str] = []
    client = _get_client()

    # ── Account state ──────────────────────────────────────────────────
    assets = client.get_assets()
    nav = float(assets.nav or 0)

    pa = client.get_prime_assets()
    pa_d = _safe_attrs(pa)
    excess_liquidity = _to_float_or_none(pa_d.get("excess_liquidity")) or 0.0
    margin_debit = _to_float_or_none(pa_d.get("initial_margin")) or 0.0
    maintain_margin = _to_float_or_none(pa_d.get("maintain_margin")) or 0.0

    # ── Positions ──────────────────────────────────────────────────────
    stock_positions = [_position_to_dict(p) for p in client.get_stock_positions()]
    option_positions = [_position_to_dict(p) for p in client.get_option_positions()]

    # ── LEAPS deltas — borrow from compute_portfolio_greeks ────────────
    leaps_greeks_by_key: dict[str, dict] = {}
    try:
        pg = compute_portfolio_greeks()
        for row in pg.get("positions", []):
            # Only long calls — LEAPS for the PMCC stress
            if row.get("right") == "CALL" and (row.get("quantity") or 0) > 0:
                key = (
                    f"{(row.get('symbol') or '').upper()}|"
                    f"{row.get('strike')}|{row.get('expiry')}|CALL"
                )
                leaps_greeks_by_key[key] = {"delta": row.get("delta")}
    except Exception as e:
        notes.append(
            f"LEAPS Greeks pull failed: {e}. Scenario D / BD pmcc_loss falls back "
            "to 0.80 delta default per doctrine."
        )

    classified = stress_engine.classify_positions(
        stock_positions, option_positions, leaps_greeks_by_key,
    )

    # ── Core spot per ticker (for put-assignment math) ─────────────────
    core_spots: dict[str, float] = {}
    for p in stock_positions:
        sym = (p.get("symbol") or "").upper()
        try:
            mp = float(p.get("market_price") or 0)
        except (TypeError, ValueError):
            continue
        if mp > 0 and sym in stress_engine.CORE_TICKERS:
            core_spots[sym] = mp

    # ── SPY spot via yfinance (no Tiger spot endpoint) ─────────────────
    spy_spot = 0.0
    try:
        import yfinance as yf
        t = yf.Ticker("SPY")
        fast = getattr(t, "fast_info", None)
        spy_spot = float(
            (fast and (fast.get("last_price") or fast.get("regular_market_price")))
            or 0
        )
        if spy_spot <= 0:
            spy_spot = float((t.info or {}).get("regularMarketPrice") or 0)
    except Exception as e:
        notes.append(f"yfinance SPY spot fetch failed: {e}. Scenario D defaults SPY=0.")

    # ── Run scenarios ──────────────────────────────────────────────────
    payload = stress_engine.run_scenarios(
        nav=nav,
        excess_liquidity=excess_liquidity,
        margin_debit=margin_debit,
        maintain_margin=maintain_margin,
        classified=classified,
        spy_spot=spy_spot,
        core_spots=core_spots,
    )

    # ── MARA reduction schedule (fires in reduce/critical/insolvent zones) ──
    current_zone = payload["current_zone"]
    mara_shares = 0
    mara_price = 0.0
    for p in stock_positions:
        if (p.get("symbol") or "").upper() == "MARA":
            try:
                mara_shares = int(float(p.get("quantity") or 0))
                mara_price = float(p.get("market_price") or 0)
            except (TypeError, ValueError):
                continue
    if current_zone in ("reduce", "critical", "insolvent"):
        schedule = stress_engine.reduction_schedule_mara(mara_shares, mara_price)
    else:
        schedule = []

    payload["reduction_schedule"] = schedule
    payload["asof_date"] = asof.isoformat()
    payload["notes"] = notes
    return payload


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
