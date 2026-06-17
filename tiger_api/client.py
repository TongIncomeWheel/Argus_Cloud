"""TigerClient — wrapper around tigeropen SDK.

Single entry point for ARGUS to call Tiger Open API. Handles:
- Auth from .streamlit/tiger_openapi_config.properties (or path from $TIGER_CONFIG_PATH)
- Read fetches: account assets, stock + option positions, filled orders, funding
- Write operations: place / cancel option orders, execute combo rolls
- Lazy client init (so importing this module doesn't hit the network)
- Defensive error handling with clear messages

Write operations were added in Phase 2c for the MCP server. They are still
exposed only through the MCP server's preview-then-confirm tools — the
Streamlit Argus app continues to use read-only flows.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

# Default location — can be overridden by $TIGER_CONFIG_PATH env var
_DEFAULT_CONFIG_REL = ".streamlit/tiger_openapi_config.properties"


@dataclass
class TigerAssets:
    """Lightweight summary of get_assets() result."""
    currency: str
    nav: float           # net liquidation
    cash: float
    stock_value: float   # gross_position_value (may be inf if SDK summary bug — caller decides)
    realized_pnl_today: float
    unrealized_pnl: float
    raw: object          # raw SDK object for debugging


def _bootstrap_from_streamlit_secrets() -> Optional[Path]:
    """If running on Streamlit Cloud, materialize tiger_openapi_config.properties
    from `st.secrets["tiger"]["properties"]` and return the directory it was
    written to.

    Tries 3 writable locations in order (covers Cloud + Docker + local quirks):
      1. .streamlit/  (relative to project root) — preferred if writable
      2. /tmp/argus_tiger_config/  — Cloud always allows /tmp
      3. tempfile.gettempdir()/argus_tiger_config/  — cross-platform fallback

    Returns the Path of the dir the file was written to, or None if no secrets
    were available.
    """
    try:
        import streamlit as st
    except ImportError:
        logger.warning("streamlit not importable — cannot bootstrap secrets")
        return None
    try:
        # Probe st.secrets — `in` operator works without raising even if no secrets
        try:
            has_tiger = "tiger" in st.secrets
        except Exception as e:
            logger.warning("st.secrets unavailable: %s", e)
            return None
        if not has_tiger:
            logger.warning("st.secrets has no [tiger] section")
            return None
        tiger_secrets = st.secrets["tiger"]
        # Accept either 'properties' (whole file) or individual keys
        properties_content = ""
        try:
            properties_content = str(tiger_secrets.get("properties", "") or "")
        except Exception:
            pass
        if not properties_content:
            keys_in_order = ("private_key_pk1", "private_key_pk8", "private_key",
                             "tiger_id", "account", "license", "env")
            lines = []
            for k in keys_in_order:
                try:
                    v = tiger_secrets.get(k)
                except Exception:
                    v = None
                if v:
                    lines.append(f"{k}={v}")
            properties_content = "\n".join(lines)
        if not properties_content.strip():
            logger.warning("Tiger secrets present but no usable content")
            return None

        content = properties_content.strip() + "\n"
        # Try multiple writable locations
        import tempfile
        candidates = [
            Path(__file__).parent.parent / ".streamlit",
            Path("/tmp") / "argus_tiger_config",
            Path(tempfile.gettempdir()) / "argus_tiger_config",
        ]
        for target_dir in candidates:
            try:
                target_dir.mkdir(parents=True, exist_ok=True)
                target_file = target_dir / "tiger_openapi_config.properties"
                target_file.write_text(content, encoding="utf-8")
                logger.info("Bootstrapped Tiger config from st.secrets → %s", target_file)
                return target_dir
            except (OSError, PermissionError) as we:
                logger.warning("Cannot write Tiger config to %s: %s", target_dir, we)
                continue
        logger.error("All bootstrap target dirs failed — Tiger config not materialized")
        return None
    except Exception as e:
        logger.warning("Tiger secrets bootstrap exception: %s", e)
        return None


def _resolve_config_dir() -> Path:
    """Find directory containing tiger_openapi_config.properties.

    Resolution order:
      1. $TIGER_CONFIG_PATH env var (file or dir)
      2. .streamlit/tiger_openapi_config.properties relative to project root
      3. If neither exists, bootstrap from Streamlit secrets and use that path
    """
    cfg_rel = os.getenv("TIGER_CONFIG_PATH", _DEFAULT_CONFIG_REL)
    cfg_path = Path(cfg_rel)
    if not cfg_path.is_absolute():
        cfg_path = Path(__file__).parent.parent / cfg_rel
    if cfg_path.is_file():
        return cfg_path.parent
    if cfg_path.is_dir():
        return cfg_path

    # Not found locally — try Streamlit Cloud secrets bootstrap
    bootstrapped_dir = _bootstrap_from_streamlit_secrets()
    if bootstrapped_dir is not None:
        # Update env var so subsequent calls (and logs) point at the right place
        os.environ["TIGER_CONFIG_PATH"] = str(bootstrapped_dir)
        return bootstrapped_dir

    raise FileNotFoundError(
        f"Tiger config not found. Either:\n"
        f"  • Set $TIGER_CONFIG_PATH to a directory containing "
        f"    tiger_openapi_config.properties\n"
        f"  • Place the file at {cfg_path}\n"
        f"  • Configure Streamlit Cloud secrets with a [tiger] section "
        f"    containing `properties = \"\"\"...\"\"\"` (full .properties content)"
    )


class TigerClient:
    """Read-only Tiger Open API client.

    Usage:
        client = TigerClient()                  # auto-loads config
        assets = client.get_assets()
        stk_pos = client.get_stock_positions()
        opt_pos = client.get_option_positions()
        fills  = client.get_filled_orders(days=7)

    All methods raise on hard failures (auth/network) but return [] for empty results.
    """

    def __init__(self, account: Optional[str] = None, config_dir: Optional[Path] = None):
        self._config_dir = Path(config_dir) if config_dir else _resolve_config_dir()
        self._trade_client = None
        self._client_config = None
        self._account = account  # if None, taken from .properties file via SDK
        self._init_clients()

    # ── Init / lazy ──────────────────────────────────────────────
    def _init_clients(self) -> None:
        from tigeropen.tiger_open_config import TigerOpenClientConfig
        from tigeropen.trade.trade_client import TradeClient

        self._client_config = TigerOpenClientConfig(props_path=str(self._config_dir))
        self._trade_client = TradeClient(self._client_config)
        # Resolve account if caller didn't pass one
        if not self._account:
            self._account = self._client_config.account
        if not self._account:
            raise RuntimeError("No Tiger account configured. Set 'account=' in tiger_openapi_config.properties")

    @property
    def account(self) -> str:
        return self._account

    @property
    def license(self) -> str:
        return getattr(self._client_config, "license", "TBSG")

    @property
    def is_sandbox(self) -> bool:
        return bool(getattr(self._client_config, "_sandbox_debug", False))

    # ── Account & cash ───────────────────────────────────────────
    def get_assets(self) -> TigerAssets:
        """Snapshot of NAV / cash / position value. One round-trip."""
        result = self._trade_client.get_assets(account=self._account)
        if not result:
            raise RuntimeError("get_assets() returned empty")
        a = result[0]
        s = a.summary
        return TigerAssets(
            currency=getattr(s, "currency", "USD"),
            nav=float(getattr(s, "net_liquidation", 0) or 0),
            cash=float(getattr(s, "cash", 0) or 0),
            stock_value=float(getattr(s, "gross_position_value", 0) or 0),
            realized_pnl_today=float(getattr(s, "realized_pl", 0) or 0),
            unrealized_pnl=float(getattr(s, "unrealized_pl", 0) or 0),
            raw=a,
        )

    # ── Positions ────────────────────────────────────────────────
    def get_stock_positions(self) -> List:
        from tigeropen.common.consts import SecurityType
        return self._trade_client.get_positions(account=self._account, sec_type=SecurityType.STK) or []

    def get_option_positions(self) -> List:
        from tigeropen.common.consts import SecurityType
        return self._trade_client.get_positions(account=self._account, sec_type=SecurityType.OPT) or []

    def get_all_positions(self) -> List:
        """Stocks + options in one list. Uses two underlying API calls."""
        return self.get_stock_positions() + self.get_option_positions()

    # ── Orders ───────────────────────────────────────────────────
    def get_filled_orders(self, days: int = 7, chunk_days: int = 30) -> List:
        """Filled orders in the last N days. Auto-chunks to respect Tiger's
        90-day window cap and 100-fill-per-call cap.

        Each Order has commission, gst, avg_fill_price, trade_time, and
        realized_pnl populated.
        """
        end = datetime.now()
        start = end - timedelta(days=days)
        return self._fetch_orders_range(start, end, chunk_days)

    def get_filled_orders_range(self, start: datetime, end: datetime, chunk_days: int = 30) -> List:
        """Same as get_filled_orders but with explicit date range."""
        return self._fetch_orders_range(start, end, chunk_days)

    def _fetch_orders_range(self, start: datetime, end: datetime, chunk_days: int = 30) -> List:
        """Walk backward from `end` to `start` in chunks of `chunk_days`.

        We use 30-day default chunks because:
          - Tiger caps any window at 90 days
          - Any single window is also capped at 100 fills
          - 30-day windows for an active wheel trader stay <100 fills consistently
        """
        all_orders = []
        seen_ids = set()
        cursor = end
        while cursor > start:
            window_start = max(cursor - timedelta(days=chunk_days), start)
            try:
                orders = self._trade_client.get_filled_orders(
                    account=self._account,
                    start_time=int(window_start.timestamp() * 1000),
                    end_time=int(cursor.timestamp() * 1000),
                )
                for o in (orders or []):
                    oid = getattr(o, "id", None)
                    if oid and oid in seen_ids:
                        continue  # boundary dedup
                    if oid:
                        seen_ids.add(oid)
                    all_orders.append(o)
            except Exception as e:
                logger.warning("orders chunk %s..%s failed: %s",
                               window_start.date(), cursor.date(), e)
            cursor = window_start
        return all_orders

    def get_open_orders(self) -> List:
        """Currently working orders (not yet filled or cancelled)."""
        try:
            return self._trade_client.get_open_orders(account=self._account) or []
        except Exception as e:
            logger.warning("get_open_orders failed: %s", e)
            return []

    def get_cancelled_orders(self, days: int = 7) -> List:
        """Cancelled orders in the last N days."""
        end = datetime.now()
        start = end - timedelta(days=days)
        try:
            return self._trade_client.get_cancelled_orders(
                account=self._account,
                start_time=int(start.timestamp() * 1000),
                end_time=int(end.timestamp() * 1000),
            ) or []
        except Exception as e:
            logger.warning("get_cancelled_orders failed: %s", e)
            return []

    def get_transactions(self, symbol: str, days: int = 30, limit: int = 100) -> List:
        """Per-fill executions for a single ticker (ms-precision timestamps).

        Tiger requires a symbol filter — to get all transactions across the
        portfolio, iterate over tickers from get_positions().
        """
        end = datetime.now()
        start = end - timedelta(days=days)
        try:
            result = self._trade_client.get_transactions(
                account=self._account,
                symbol=symbol,
                start_time=int(start.timestamp() * 1000),
                end_time=int(end.timestamp() * 1000),
                limit=limit,
            )
            # Could be a list, a TransactionsResponse wrapper, or None
            if result is None:
                return []
            if isinstance(result, list):
                return result
            if hasattr(result, "items") and isinstance(result.items, list):
                return result.items
            return [result]
        except Exception as e:
            logger.warning("get_transactions(%s) failed: %s", symbol, e)
            return []

    def get_order_transactions(self, order_id) -> List:
        """Fetch per-leg fills for a specific order id.

        Used to expand multi-leg combo orders (sec_type='MLEG' rolls / spreads)
        into their individual leg transactions. Each transaction has full
        contract details (strike, expiry, put_call) on the contract object.

        Tiger limits this endpoint to ~60 calls/min. On rate-limit errors we
        sleep and retry up to 3 times (with backoff).
        """
        import time
        for attempt in range(3):
            try:
                result = self._trade_client.get_transactions(
                    account=self._account,
                    order_id=order_id,
                    limit=50,
                )
                if result is None:
                    return []
                if isinstance(result, list):
                    return result
                if hasattr(result, "items") and isinstance(result.items, list):
                    return result.items
                return [result]
            except Exception as e:
                err = str(e).lower()
                if "rate limit" in err or "code=4" in err:
                    if attempt < 2:
                        wait = 30 * (attempt + 1)
                        logger.warning("Rate limit on order_transactions(%s); sleep %ds", order_id, wait)
                        time.sleep(wait)
                        continue
                logger.warning("get_order_transactions(%s) failed: %s", order_id, e)
                return []
        return []

    def get_all_transactions(self, days: int = 30):
        """Fetch transactions across every ticker we currently hold a position in.
        Returns a flat list. (Tiger API doesn't support a single 'all' query.)"""
        tickers = set()
        for p in self.get_all_positions():
            sym = getattr(p.contract, "symbol", "") if p.contract else ""
            if sym:
                tickers.add(sym)
        all_txns = []
        for sym in sorted(tickers):
            all_txns.extend(self.get_transactions(symbol=sym, days=days))
        return all_txns

    # ── Margin / multi-currency ──────────────────────────────────
    def get_prime_assets(self):
        """Detailed account state: segment balances, margin, BP, multi-currency.
        Returns a PortfolioAccount object. Has .segments dict keyed by 'S'/'C'/etc.
        """
        try:
            return self._trade_client.get_prime_assets(account=self._account)
        except Exception as e:
            logger.warning("get_prime_assets failed: %s", e)
            return None

    # ── Funding (deposits / withdrawals) ─────────────────────────
    def get_funding_history(self):
        """All deposits & withdrawals — across the lifetime of the account.

        Returns a pandas DataFrame with columns:
            id, ref_id, type, type_desc, currency, amount, business_date,
            completed_status, updated_at, created_at

        Solves the CSV's SGD/USD ambiguity — currency is explicit per row.
        """
        import pandas as pd

        try:
            df = self._trade_client.get_funding_history()
            if df is None:
                return pd.DataFrame()
            if isinstance(df, pd.DataFrame):
                return df
            # Some SDK versions return a list — coerce to DataFrame
            return pd.DataFrame(df)
        except Exception as e:
            logger.warning("get_funding_history failed: %s", e)
            import pandas as pd
            return pd.DataFrame()

    def get_segment_fund_history(self) -> List:
        """Internal segment transfers (e.g. SEC ↔ FUND, Securities ↔ Futures)."""
        try:
            return self._trade_client.get_segment_fund_history() or []
        except Exception as e:
            logger.warning("get_segment_fund_history failed: %s", e)
            return []

    # ── Quotes (spot prices + option chain / Greeks / briefs / bars) ──
    def _quote_client(self):
        """Lazy QuoteClient — only instantiated when first needed."""
        if not hasattr(self, "_qc") or self._qc is None:
            from tigeropen.quote.quote_client import QuoteClient
            self._qc = QuoteClient(self._client_config)
        return self._qc

    def get_spot_prices(self, symbols) -> dict:
        """Fetch current spot prices for a list of stock symbols.

        Returns {symbol: price}. Tickers without a quote are silently
        omitted from the result (caller falls back to position-based proxy).

        NOTE: this is for STOCK tickers only. Option contract identifiers
        (e.g. "MSTR  260718P00250000") look like long strings of digits
        and would be silently dropped by Tiger's stock briefs endpoint.
        We detect that shape and route the caller to get_option_briefs
        instead, with a structured error in the response.
        """
        if not symbols:
            return {}
        symbols = sorted({str(s).strip().upper() for s in symbols if s})
        if not symbols:
            return {}
        # Reject obvious option identifiers — anything that has digits in
        # the OCC-like position (chars 6-12 looking like YYMMDD) is almost
        # certainly an option, not a stock ticker.
        option_shaped = [s for s in symbols if _looks_like_option_identifier(s)]
        if option_shaped:
            raise ValueError(
                f"get_spot_prices is for stock tickers only. Detected "
                f"option-shaped identifiers: {option_shaped[:3]}. Use "
                f"get_option_briefs(contracts=[...]) for option quotes."
            )
        try:
            qc = self._quote_client()
            briefs = qc.get_briefs(symbols=symbols)
        except Exception as e:
            logger.warning("get_spot_prices failed: %s", e)
            return {}

        out = {}
        if briefs is None:
            return out
        # tigeropen returns a DataFrame in modern versions
        if hasattr(briefs, "iterrows"):
            for _, row in briefs.iterrows():
                sym = str(row.get("symbol") or row.get("ticker") or "").upper()
                price = row.get("latest_price") or row.get("last_price") or row.get("last") or row.get("close")
                try:
                    if sym and price is not None:
                        out[sym] = float(price)
                except (TypeError, ValueError):
                    continue
        elif isinstance(briefs, list):
            for b in briefs:
                sym = (getattr(b, "symbol", None) or "").upper()
                price = getattr(b, "latest_price", None) or getattr(b, "last_price", None) or getattr(b, "close", None)
                try:
                    if sym and price is not None:
                        out[sym] = float(price)
                except (TypeError, ValueError):
                    continue
        return out

    # ── NAV history ──────────────────────────────────────────────
    def get_nav_history(self, days: int = 30) -> dict:
        """Daily NAV / P&L / cash time series. Returns:
            {
                'summary': {'pnl': float, 'pnl_percentage': float, 'annualized_return': float},
                'history': [
                    {'date': ms, 'asset': float, 'pnl': float, 'cash_balance': float,
                     'gross_position_value': float, 'deposit': float, ...},
                    ...
                ]
            }
        """
        end = datetime.now().date()
        start = end - timedelta(days=days)
        try:
            return self._trade_client.get_analytics_asset(
                account=self._account,
                start_date=start.isoformat(),
                end_date=end.isoformat(),
            ) or {}
        except Exception as e:
            logger.warning("get_nav_history failed: %s", e)
            return {}

    # ── WRITE OPERATIONS (Phase 2c) ──────────────────────────────
    #
    # These methods place / cancel real orders against the configured
    # Tiger account. Callers MUST gate user intent before calling — the
    # MCP server tools wrap each one behind a preview-then-confirm
    # interaction. Argus's Streamlit UI does not call any of these.
    #
    # All methods raise on hard errors and log the raw SDK response so
    # production logs capture the audit trail.

    def place_option_order(
        self,
        symbol: str,
        expiry: str,            # YYYY-MM-DD
        strike: float,
        right: str,             # "PUT" | "CALL"
        side: str,              # "SELL_TO_OPEN" | "BUY_TO_CLOSE" | "BUY_TO_OPEN" | "SELL_TO_CLOSE"
        quantity: int,
        limit_price: float,
        time_in_force: str = "DAY",  # "DAY" | "GTC"
    ) -> dict:
        """Submit a single-leg option order with a limit price.

        Returns the placed order's id and status as a dict.
        """
        from tigeropen.common.consts import OrderType
        from tigeropen.common.util.contract_utils import option_contract
        from tigeropen.common.util.order_utils import limit_order

        action, open_close = _split_side(side)
        right_norm = right.strip().upper()
        if right_norm not in ("PUT", "CALL"):
            raise ValueError(f"right must be PUT or CALL, got {right!r}")
        if quantity <= 0:
            raise ValueError(f"quantity must be > 0, got {quantity}")
        if limit_price <= 0:
            raise ValueError(f"limit_price must be > 0, got {limit_price}")
        tif = time_in_force.strip().upper()
        if tif not in ("DAY", "GTC"):
            raise ValueError(f"time_in_force must be DAY or GTC, got {time_in_force!r}")

        contract = option_contract(
            symbol=symbol.strip().upper(),
            expiry=_tiger_expiry(expiry),
            strike=float(strike),
            put_call=right_norm,
            currency="USD",
        )
        order = limit_order(
            account=self._account,
            contract=contract,
            action=action,
            quantity=int(quantity),
            limit_price=float(limit_price),
        )
        # Tiger SDK fields not exposed via limit_order() helper
        order.time_in_force = tif
        if open_close is not None:
            order.open_close = open_close  # e.g. "OPEN" / "CLOSE"

        logger.info(
            "place_option_order: %s %s %s %s%s exp=%s qty=%s @ %s %s",
            self._account, side, symbol, right_norm, strike, expiry, quantity, limit_price, tif,
        )
        placed = self._trade_client.place_order(order)
        logger.info("place_option_order placed: id=%s", getattr(order, "id", None))
        return {
            "order_id": getattr(order, "id", None),
            "status": getattr(order, "status", None),
            "placed": bool(placed),
        }

    def cancel_order(self, order_id) -> dict:
        """Cancel a working order by id. Returns {ok, status}."""
        logger.info("cancel_order: %s", order_id)
        result = self._trade_client.cancel_order(account=self._account, id=order_id)
        return {"ok": bool(result), "order_id": order_id}

    def execute_combo_roll(
        self,
        symbol: str,
        close_expiry: str,      # YYYY-MM-DD
        close_strike: float,
        close_right: str,       # "PUT" | "CALL"
        new_expiry: str,        # YYYY-MM-DD
        new_strike: float,
        quantity: int,
        net_credit_limit: float,  # POSITIVE = receive net credit; NEGATIVE = pay net debit
        time_in_force: str = "DAY",
    ) -> dict:
        """Atomic two-leg combo: BUY_TO_CLOSE the existing short option leg and
        SELL_TO_OPEN a replacement at a different strike/expiry. Same underlying
        and same right (PUT or CALL) on both legs.

        `net_credit_limit > 0` requires receiving at least that net credit per
        contract on fill; `< 0` accepts paying that as a net debit.
        """
        from tigeropen.common.util.contract_utils import option_contract
        from tigeropen.common.util.order_utils import limit_order

        right_norm = close_right.strip().upper()
        if right_norm not in ("PUT", "CALL"):
            raise ValueError(f"right must be PUT or CALL, got {close_right!r}")
        if quantity <= 0:
            raise ValueError(f"quantity must be > 0, got {quantity}")

        sym = symbol.strip().upper()
        close_leg = option_contract(
            symbol=sym, expiry=_tiger_expiry(close_expiry),
            strike=float(close_strike), put_call=right_norm, currency="USD",
        )
        open_leg = option_contract(
            symbol=sym, expiry=_tiger_expiry(new_expiry),
            strike=float(new_strike), put_call=right_norm, currency="USD",
        )

        # Tiger MLEG combo: build a combo contract with both legs and one
        # net limit order. Net credit is expressed by the action+price sign
        # convention used by tigeropen — we route credits via a SELL action
        # on the combo with a positive premium, debits via BUY with positive.
        try:
            from tigeropen.common.util.contract_utils import combo_contract
        except ImportError as e:
            raise RuntimeError(
                "tigeropen build does not expose combo_contract — combo "
                "rolls require tigeropen with multi-leg support. Upgrade or "
                "switch to two sequential single-leg orders."
            ) from e

        if net_credit_limit >= 0:
            combo_action, combo_price = "SELL", float(net_credit_limit)
        else:
            combo_action, combo_price = "BUY", float(-net_credit_limit)

        combo = combo_contract(
            symbol=sym,
            contract_legs=[
                {"contract": close_leg, "action": "BUY",  "ratio": 1},
                {"contract": open_leg,  "action": "SELL", "ratio": 1},
            ],
        )
        order = limit_order(
            account=self._account,
            contract=combo,
            action=combo_action,
            quantity=int(quantity),
            limit_price=combo_price,
        )
        order.time_in_force = time_in_force.strip().upper()

        logger.info(
            "execute_combo_roll: %s %s close %s@%s exp=%s -> open %s@%s exp=%s qty=%s net_limit=%s",
            self._account, sym, right_norm, close_strike, close_expiry,
            right_norm, new_strike, new_expiry, quantity, net_credit_limit,
        )
        placed = self._trade_client.place_order(order)
        logger.info("execute_combo_roll placed: id=%s", getattr(order, "id", None))
        return {
            "order_id": getattr(order, "id", None),
            "status": getattr(order, "status", None),
            "placed": bool(placed),
            "net_limit_per_contract": net_credit_limit,
        }


    # ── Option chain / Greeks / quote depth (Phase 2d) ────────────
    #
    # All call into tigeropen.quote.QuoteClient. Returns are coerced to
    # plain list[dict] / dict so the MCP layer can JSON-serialize without
    # additional adapters.

    def get_option_expirations(self, symbols) -> dict:
        """Available expiry dates per underlying.

        Args:
          symbols: ticker or list of tickers (e.g. "MSTR" or ["MSTR","AAPL"])
        Returns: {symbol: [YYYY-MM-DD, ...]}
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        symbols = sorted({str(s).strip().upper() for s in symbols if s})
        if not symbols:
            return {}
        qc = self._quote_client()
        try:
            df = qc.get_option_expirations(symbols=symbols)
        except Exception as e:
            raise _wrap_tiger_error("get_option_expirations", e, hint=f"symbols={symbols}")
        return _expirations_to_dict(df)

    def get_option_chain(
        self,
        symbol: str,
        expiry: str,                  # YYYY-MM-DD
        include_greeks: bool = True,
    ) -> list[dict]:
        """Full option chain for one underlying + expiry.

        Tiger-only. Raises a clear exception if Tiger refuses (e.g. account
        lacks US market data permission). The MCP layer surfaces the
        exception text to the LLM so the failure mode is never silently
        empty.
        """
        sym = symbol.strip().upper()
        tiger_expiry = _tiger_expiry(expiry)
        qc = self._quote_client()
        try:
            df = qc.get_option_chain(
                symbol=sym,
                expiry=tiger_expiry,
                return_greek_value=bool(include_greeks),
            )
        except Exception as e:
            raise _wrap_tiger_error("get_option_chain", e, hint=f"symbol={sym} expiry={expiry}")
        return _option_rows_to_dicts(df)

    def get_option_briefs(self, contracts) -> list[dict]:
        """Real-time bid/ask/OI/HV/last for specific option contracts.

        Tiger-only. Raises a clear exception on failure (never returns empty
        for a real error).
        """
        ids = _build_option_identifiers(contracts)
        if not ids:
            raise ValueError(
                "get_option_briefs: contracts list is empty or all entries "
                "failed to convert to Tiger option identifiers."
            )
        qc = self._quote_client()
        try:
            df = qc.get_option_briefs(identifiers=ids)
        except Exception as e:
            raise _wrap_tiger_error("get_option_briefs", e, hint=f"{len(ids)} contracts")
        return _option_rows_to_dicts(df)

    def get_option_greeks(self, contracts) -> list[dict]:
        """Δ / Γ / Θ / ν / ρ + IV per contract. Tiger-only."""
        ids = _build_option_identifiers(contracts)
        if not ids:
            raise ValueError(
                "get_option_greeks: contracts list is empty or all entries "
                "failed to convert to Tiger option identifiers."
            )
        qc = self._quote_client()
        try:
            df = qc.get_option_briefs(identifiers=ids, return_greek_value=True)
        except Exception as e:
            raise _wrap_tiger_error("get_option_greeks", e, hint=f"{len(ids)} contracts")
        rows = _option_rows_to_dicts(df)
        keep = {
            "symbol", "identifier", "expiry", "strike", "right", "put_call",
            "delta", "gamma", "theta", "vega", "rho", "implied_vol", "iv",
            "underlying", "underlying_symbol",
        }
        return [{k: v for k, v in r.items() if k in keep or k.startswith("greek")}
                for r in rows]

    def get_option_bars(
        self,
        contracts,
        period: str = "day",   # "day" | "week" | "month" | "1min" | "5min" | "15min" | "30min" | "60min"
        limit: int = 60,
    ) -> dict:
        """OHLC bars per option contract. Tiger-only — raises on failure."""
        ids = _build_option_identifiers(contracts)
        if not ids:
            raise ValueError(
                "get_option_bars: contracts list is empty or all entries "
                "failed to convert to Tiger option identifiers."
            )
        qc = self._quote_client()
        try:
            df = qc.get_option_bars(identifiers=ids, period=period, limit=int(limit))
        except Exception as e:
            raise _wrap_tiger_error("get_option_bars", e, hint=f"{len(ids)} contracts period={period}")
        return _bars_to_dict(df)

    def get_option_depth(self, contracts) -> list[dict]:
        """L2 depth per option contract — bid/ask ladder. Tiger-only."""
        ids = _build_option_identifiers(contracts)
        if not ids:
            raise ValueError(
                "get_option_depth: contracts list is empty or all entries "
                "failed to convert to Tiger option identifiers."
            )
        qc = self._quote_client()
        try:
            df = qc.get_option_depth(identifiers=ids)
        except Exception as e:
            raise _wrap_tiger_error("get_option_depth", e, hint=f"{len(ids)} contracts")
        return _option_rows_to_dicts(df)

    def get_option_trade_ticks(self, contracts, limit: int = 50) -> dict:
        """Recent trade ticks per option contract. Tiger-only."""
        ids = _build_option_identifiers(contracts)
        if not ids:
            raise ValueError(
                "get_option_trade_ticks: contracts list is empty or all entries "
                "failed to convert to Tiger option identifiers."
            )
        qc = self._quote_client()
        try:
            df = qc.get_option_trade_ticks(identifiers=ids, limit=int(limit))
        except Exception as e:
            raise _wrap_tiger_error("get_option_trade_ticks", e, hint=f"{len(ids)} contracts")
        return _bars_to_dict(df, key_field="identifier")


# ── Helpers for option identifiers + row normalization ─────────────────────


def _format_option_identifier(symbol: str, expiry: str, strike: float, right: str) -> str:
    """Build a Tiger / OCC option identifier from components.

    Format: SYMBOL YYMMDD <C|P> STRIKE*1000 (8-digit zero-padded)
    Example: MSTR 250718 P 00250000 → "MSTR  250718P00250000"
    """
    sym = symbol.strip().upper()
    iso = _tiger_expiry(expiry)        # YYYYMMDD
    yymmdd = iso[2:]                   # YYMMDD
    right_n = right.strip().upper()
    if right_n not in ("PUT", "CALL", "P", "C"):
        raise ValueError(f"right must be PUT or CALL, got {right!r}")
    pc = "P" if right_n.startswith("P") else "C"
    strike_int = int(round(float(strike) * 1000))
    # Pad symbol to 6 chars for OCC convention
    return f"{sym:<6}{yymmdd}{pc}{strike_int:08d}"


def _looks_like_option_identifier(s: str) -> bool:
    """Heuristic: does this look like an OCC-style option identifier?

    OCC format is 21 chars: <6-char ticker><6 digits YYMMDD><P|C><8 digits>
    Anything that long with mostly digits in the tail is almost certainly
    an option, not a stock ticker (stock tickers are 1-6 chars, letters only).
    """
    s = s.strip()
    if len(s) < 14:
        return False
    # Check if last 8 chars are all digits (the strike encoding)
    if not s[-8:].isdigit():
        return False
    # And the char at -9 is P or C (put_call)
    if s[-9].upper() not in ("P", "C"):
        return False
    # And 6 digits before that (YYMMDD)
    if not s[-15:-9].isdigit():
        return False
    return True


def _build_option_identifiers(contracts) -> list[str]:
    """Normalize list[dict] OR list[str] into list[str] Tiger identifiers."""
    if not contracts:
        return []
    out = []
    for c in contracts:
        if isinstance(c, str):
            out.append(c.strip())
            continue
        if isinstance(c, dict):
            sym = c.get("symbol") or c.get("ticker") or ""
            exp = c.get("expiry") or c.get("expiration") or ""
            strike = c.get("strike")
            right = c.get("right") or c.get("put_call") or c.get("type") or ""
            if not (sym and exp and strike is not None and right):
                continue
            out.append(_format_option_identifier(sym, exp, float(strike), right))
            continue
        # Tuple / list: (symbol, expiry, strike, right)
        try:
            sym, exp, strike, right = c
            out.append(_format_option_identifier(sym, exp, float(strike), right))
        except (ValueError, TypeError):
            continue
    return out


def _option_rows_to_dicts(df) -> list[dict]:
    """Coerce a tigeropen DataFrame (or list) of option rows to list[dict].

    Many Tiger SDK responses come back as pandas DataFrames; some older
    versions return lists of objects. Handle both.
    """
    if df is None:
        return []
    if hasattr(df, "to_dict"):
        try:
            records = df.to_dict(orient="records")
            return [_jsonify_dict(r) for r in records]
        except Exception:
            pass
    if isinstance(df, list):
        out = []
        for item in df:
            if isinstance(item, dict):
                out.append(_jsonify_dict(item))
            else:
                # SDK domain object — pull readable attrs
                attrs = {}
                for k in dir(item):
                    if k.startswith("_"):
                        continue
                    try:
                        v = getattr(item, k)
                    except Exception:
                        continue
                    if callable(v):
                        continue
                    attrs[k] = v
                out.append(_jsonify_dict(attrs))
        return out
    return []


def _bars_to_dict(df, key_field: str = "identifier") -> dict:
    """Group bar/tick rows by identifier into {identifier: [row, ...]}."""
    rows = _option_rows_to_dicts(df)
    grouped: dict[str, list[dict]] = {}
    for r in rows:
        k = str(r.get(key_field) or r.get("symbol") or r.get("ticker") or "")
        grouped.setdefault(k, []).append(r)
    return grouped


def _expirations_to_dict(df) -> dict:
    """Group expiry rows by symbol into {symbol: [YYYY-MM-DD, ...]}."""
    rows = _option_rows_to_dicts(df)
    grouped: dict[str, list[str]] = {}
    for r in rows:
        sym = str(r.get("symbol") or r.get("underlying_symbol") or "").upper()
        exp = r.get("date") or r.get("expiry") or r.get("expiration")
        if not (sym and exp):
            continue
        s = str(exp)
        # Convert YYYYMMDD → YYYY-MM-DD when we got the compact form
        if len(s) == 8 and s.isdigit():
            s = f"{s[:4]}-{s[4:6]}-{s[6:]}"
        grouped.setdefault(sym, []).append(s)
    for k in grouped:
        grouped[k] = sorted(set(grouped[k]))
    return grouped


def _jsonify_dict(d: dict) -> dict:
    """Convert datetimes / Decimal / NaN-floats to JSON-safe types."""
    import math
    out = {}
    for k, v in d.items():
        if v is None:
            out[str(k)] = None
        elif isinstance(v, (bool, int, str)):
            out[str(k)] = v
        elif isinstance(v, float):
            out[str(k)] = None if math.isnan(v) or math.isinf(v) else v
        elif hasattr(v, "isoformat"):
            try:
                out[str(k)] = v.isoformat()
            except Exception:
                out[str(k)] = str(v)
        elif isinstance(v, (list, tuple)):
            out[str(k)] = [_jsonify_dict({"_": x}).get("_") for x in v]
        elif isinstance(v, dict):
            out[str(k)] = _jsonify_dict(v)
        else:
            out[str(k)] = str(v)
    return out


_SIDE_MAP = {
    "SELL_TO_OPEN":  ("SELL", "OPEN"),
    "BUY_TO_OPEN":   ("BUY",  "OPEN"),
    "SELL_TO_CLOSE": ("SELL", "CLOSE"),
    "BUY_TO_CLOSE":  ("BUY",  "CLOSE"),
}


def _split_side(side: str) -> tuple[str, str | None]:
    s = side.strip().upper().replace(" ", "_").replace("-", "_")
    if s in _SIDE_MAP:
        return _SIDE_MAP[s]
    if s in ("BUY", "SELL"):
        return s, None
    raise ValueError(
        f"side must be one of {sorted(_SIDE_MAP)} (or BUY/SELL), got {side!r}"
    )


def _tiger_expiry(iso_date: str) -> str:
    """Convert ISO YYYY-MM-DD to Tiger's YYYYMMDD expiry string."""
    s = iso_date.strip()
    if "-" in s:
        y, m, d = s.split("-")
        return f"{y}{m.zfill(2)}{d.zfill(2)}"
    if len(s) == 8 and s.isdigit():
        return s
    raise ValueError(f"expiry must be YYYY-MM-DD or YYYYMMDD, got {iso_date!r}")


# ── Tiger error translation ─────────────────────────────────────────────────
#
# Tiger SDK errors come back with code + msg patterns we can detect and
# rewrap into clear Python exceptions whose message text is meaningful to
# the LLM consuming the MCP tool result.


class TigerPermissionError(PermissionError):
    """Tiger returned 'permission denied' — typically the account lacks
    market data permission for the requested asset class."""


class TigerSessionError(RuntimeError):
    """Tiger session is missing or unauthenticated (e.g. config file gone,
    key invalid, token expired). Distinct from permission errors."""


class TigerAPIError(RuntimeError):
    """Catch-all for other Tiger SDK errors."""


def _wrap_tiger_error(method_name: str, exc: Exception, hint: str = "") -> Exception:
    """Translate a tigeropen SDK exception into a typed one with a clear
    message the LLM (and the user) can act on.

    Returned exception is meant to be `raise`-d by the caller.
    """
    raw = str(exc)
    low = raw.lower()
    suffix = f" [{hint}]" if hint else ""

    if "permission denied" in low or "code=4" in low or "no permission" in low:
        return TigerPermissionError(
            f"Tiger API permission denied on {method_name}{suffix}. "
            f"The Tiger account lacks the market data subscription needed "
            f"for this call. Original Tiger error: {raw}. "
            f"To fix: log in at tigerbrokers.com → Profile → Market Data "
            f"and enable US Level 1/2 for OpenAPI, OR contact Tiger support "
            f"quoting developer ID 20159040. Trade-side calls (positions, "
            f"orders, NAV) keep working because they're account data, not "
            f"market data."
        )

    if any(s in low for s in (
        "config not found", "private_key", "token", "unauthorized",
        "401", "session", "auth",
    )):
        return TigerSessionError(
            f"Tiger session/auth failed on {method_name}{suffix}. "
            f"Original error: {raw}. "
            f"Likely causes: TIGER_* env vars not loaded into the container "
            f"(check `gcloud run services describe argus-tiger-mcp` env), "
            f"or the private key in Secret Manager is corrupted, or the "
            f"Tiger config bootstrap at /tmp/argus_tiger_config/ failed at "
            f"startup."
        )

    return TigerAPIError(
        f"Tiger API call {method_name} failed{suffix}. "
        f"Original Tiger error: {raw}."
    )
