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
