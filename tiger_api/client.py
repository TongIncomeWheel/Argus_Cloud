"""TigerClient — read-only wrapper around tigeropen SDK.

Single entry point for ARGUS to call Tiger Open API. Handles:
- Auth from .streamlit/tiger_openapi_config.properties (or path from $TIGER_CONFIG_PATH)
- Read-only fetches: account assets, stock + option positions, filled orders, funding
- Lazy client init (so importing this module doesn't hit the network)
- Defensive error handling with clear messages

This module NEVER places, modifies, or cancels orders. Read-only by design.
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


def _bootstrap_from_streamlit_secrets() -> bool:
    """If running on Streamlit Cloud (no local .properties file present),
    materialize the file from `st.secrets["tiger"]["properties"]` content.

    The user puts the ENTIRE .properties file content into a single multi-line
    secret in Streamlit Cloud's secrets manager:

        # .streamlit/secrets.toml on Cloud (set via Cloud UI)
        [tiger]
        properties = '''
        private_key=<base64 key content>
        private_key_pk8=<base64 key content>
        tiger_id=20159040
        account=50179929
        license=TBSG
        env=PROD
        '''

    Returns True if file was successfully materialized, False otherwise.
    """
    try:
        import streamlit as st
        if "tiger" not in st.secrets:
            return False
        tiger_secrets = st.secrets["tiger"]
        # Accept either 'properties' (whole file) or individual keys
        properties_content = tiger_secrets.get("properties", "")
        if not properties_content:
            # Fallback: rebuild from individual keys if the user prefers that style
            keys_in_order = ("private_key", "private_key_pk8", "tiger_id",
                             "account", "license", "env")
            lines = []
            for k in keys_in_order:
                v = tiger_secrets.get(k)
                if v:
                    lines.append(f"{k}={v}")
            properties_content = "\n".join(lines)
        if not properties_content:
            return False
        # Write to .streamlit/ relative to project root (same path the resolver expects)
        target_dir = Path(__file__).parent.parent / ".streamlit"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / "tiger_openapi_config.properties"
        target_file.write_text(properties_content.strip() + "\n", encoding="utf-8")
        logger.info("Bootstrapped Tiger config from st.secrets → %s", target_file)
        return True
    except Exception as e:
        logger.warning("Tiger secrets bootstrap failed: %s", e)
        return False


def _resolve_config_dir() -> Path:
    """Find directory containing tiger_openapi_config.properties.

    Resolution order:
      1. $TIGER_CONFIG_PATH env var
      2. .streamlit/tiger_openapi_config.properties (default local path)
      3. If neither exists, attempt bootstrap from Streamlit secrets and retry
    """
    cfg_rel = os.getenv("TIGER_CONFIG_PATH", _DEFAULT_CONFIG_REL)
    cfg_path = Path(cfg_rel)
    # If env var points to a file, use its parent dir; if it points to a dir, use it.
    if not cfg_path.is_absolute():
        # Resolve relative to ARGUS_Cloud root (this module's grandparent)
        cfg_path = Path(__file__).parent.parent / cfg_rel
    if cfg_path.is_file():
        return cfg_path.parent
    if cfg_path.is_dir():
        return cfg_path

    # Not found — try Streamlit Cloud secrets bootstrap
    if _bootstrap_from_streamlit_secrets():
        # Retry after writing the file
        if cfg_path.is_file():
            return cfg_path.parent
        if cfg_path.is_dir():
            return cfg_path

    raise FileNotFoundError(
        f"Tiger config not found. Set $TIGER_CONFIG_PATH, place "
        f"tiger_openapi_config.properties at {cfg_path}, or configure "
        f"`[tiger] properties = \"...\"` in Streamlit Cloud secrets."
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

    # ── Quotes (spot prices) ─────────────────────────────────────
    def get_spot_prices(self, symbols) -> dict:
        """Fetch current spot prices for a list of symbols.

        Returns {symbol: price}. Tickers without a quote are silently
        omitted from the result (caller falls back to position-based proxy).
        """
        if not symbols:
            return {}
        symbols = sorted({str(s).strip().upper() for s in symbols if s})
        if not symbols:
            return {}
        try:
            from tigeropen.quote.quote_client import QuoteClient
            qc = QuoteClient(self._client_config)
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
