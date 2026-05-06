"""Tiger Open API — connection smoke test.

Run from the ARGUS_Cloud root:
    python -m tiger_api.test_connection

Reads the native Tiger config file (tiger_openapi_config.properties).
Path is taken from $TIGER_CONFIG_PATH or defaults to
'.streamlit/tiger_openapi_config.properties'.

The .properties file (downloaded from developer.itigerup.com/profile) contains:
    private_key_pk1=<PKCS#1 key, base64>
    private_key_pk8=<PKCS#8 key, base64>     # ← used by Python SDK
    tiger_id=<your developer ID>
    account=<your trading account>
    license=TBSG | TBHK | TBNZ | TBAU
    env=PROD | SANDBOX

No writes, no orders. Three read-only calls:
  1. get_managed_accounts() — proves auth works
  2. get_assets()            — NAV / cash / position value
  3. get_positions()         — open positions count + first 10

Exit codes:
  0 — success
  1 — config / auth failure
  2 — API call failure
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Dict

from dotenv import load_dotenv


def _banner(s: str) -> None:
    print()
    print("=" * 70)
    print(s)
    print("=" * 70)


def _parse_properties(path: Path) -> Dict[str, str]:
    """Parse a Java-style .properties file (key=value, # comments)."""
    out: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith("!"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            out[key.strip()] = value.strip()
    return out


def main() -> int:
    load_dotenv()

    cfg_rel = os.getenv("TIGER_CONFIG_PATH", ".streamlit/tiger_openapi_config.properties")
    cfg_path = Path(cfg_rel)
    if not cfg_path.is_absolute():
        cfg_path = Path(__file__).parent.parent / cfg_rel

    _banner("TIGER OPEN API — CONNECTION TEST")
    print(f"Config file : {cfg_path}")

    if not cfg_path.exists():
        print(f"\n[FAIL] Config file not found at: {cfg_path}")
        print("       Save tiger_openapi_config.properties from developer.itigerup.com/profile")
        print(f"       to: {cfg_path}")
        return 1

    print(f"Size        : {cfg_path.stat().st_size} bytes")
    props = _parse_properties(cfg_path)

    tiger_id = props.get("tiger_id", "").strip()
    account = props.get("account", "").strip()
    license_ = props.get("license", "TBSG").strip().upper()
    env_name = props.get("env", "PROD").strip().upper()
    pk8 = props.get("private_key_pk8", "").strip()

    print(f"Tiger ID    : {tiger_id or '(MISSING)'}")
    print(f"Account     : {account or '(MISSING)'}")
    print(f"License     : {license_}")
    print(f"Environment : {env_name}")
    print(f"PK8 key     : {'OK (' + str(len(pk8)) + ' chars)' if pk8 else '(MISSING)'}")

    missing = [k for k, v in (("tiger_id", tiger_id), ("account", account), ("private_key_pk8", pk8)) if not v]
    if missing:
        print(f"\n[FAIL] .properties file missing keys: {', '.join(missing)}")
        return 1

    # ── Build client config ──────────────────────────────────────
    try:
        from tigeropen.tiger_open_config import TigerOpenClientConfig
        from tigeropen.common.consts import Language
        from tigeropen.trade.trade_client import TradeClient
    except ImportError as e:
        print(f"\n[FAIL] tigeropen SDK not importable: {e}")
        print("       Run: pip install tigeropen")
        return 1

    # tigeropen reads tiger_id/account/license/env from a directory containing
    # tiger_openapi_config.properties. Pass the *directory* (not the file path).
    client_config = TigerOpenClientConfig(
        sandbox_debug=(env_name == "SANDBOX"),
        props_path=str(cfg_path.parent),
    )
    # Belt-and-braces: explicitly set fields too, in case SDK version doesn't auto-load
    client_config.private_key = pk8
    client_config.tiger_id = tiger_id
    client_config.account = account
    client_config.language = Language.en_US
    try:
        client_config.license = license_
    except Exception:
        pass  # older SDK doesn't expose this — fine, default routing works

    trade_client = TradeClient(client_config)

    # ── Call 1: get_managed_accounts (auth proof) ────────────────
    _banner("CALL 1 — Managed Accounts (auth proof)")
    try:
        accounts = trade_client.get_managed_accounts()
        if not accounts:
            print("  (no accounts returned — credentials valid but no accounts linked?)")
        else:
            for a in accounts:
                print(
                    f"  account={a.account}  type={getattr(a, 'account_type', '?')}  "
                    f"status={getattr(a, 'status', '?')}  capability={getattr(a, 'capability', '?')}"
                )
    except Exception as e:
        print(f"  [FAIL] {type(e).__name__}: {e}")
        print("\n  Most likely causes:")
        print("    - tiger_id wrong (recheck developer.itigerup.com/profile)")
        print("    - private_key_pk8 doesn't match the public key on Tiger's portal")
        print("    - License mismatch (TBSG vs TBHK vs TBNZ vs TBAU)")
        return 2

    # ── Call 2: get_assets (NAV / cash) ─────────────────────────
    _banner("CALL 2 — Account Assets (NAV)")
    try:
        assets = trade_client.get_assets(account=account)
        if not assets:
            print("  (empty response)")
        else:
            for a in assets:
                summary = getattr(a, "summary", None)
                if summary:
                    print(f"  Currency             : {getattr(summary, 'currency', '?')}")
                    print(f"  NAV (net liquidation): ${getattr(summary, 'net_liquidation', 0):,.2f}")
                    print(f"  Cash                 : ${getattr(summary, 'cash', 0):,.2f}")
                    print(f"  Stock value          : ${getattr(summary, 'gross_position_value', 0):,.2f}")
                    print(f"  Realized P&L (today) : ${getattr(summary, 'realized_pl', 0):,.2f}")
                    print(f"  Unrealized P&L       : ${getattr(summary, 'unrealized_pl', 0):,.2f}")
                else:
                    print(f"  {a}")
    except Exception as e:
        print(f"  [FAIL] {type(e).__name__}: {e}")
        return 2

    # ── Call 3a: get_positions (STOCK) ──────────────────────────
    from tigeropen.common.consts import SecurityType

    _banner("CALL 3a — Stock Positions (STK)")
    try:
        stk_positions = trade_client.get_positions(account=account, sec_type=SecurityType.STK)
        n = len(stk_positions) if stk_positions else 0
        print(f"  Total stock positions: {n}")
        for p in (stk_positions or []):
            sym = getattr(p, "contract", None)
            qty = getattr(p, "quantity", "?")
            avg = getattr(p, "average_cost", "?")
            mkt = getattr(p, "market_price", "?")
            mv = getattr(p, "market_value", "?")
            print(f"    {sym}  qty={qty}  avg={avg}  mkt={mkt}  value={mv}")
    except Exception as e:
        print(f"  [FAIL] {type(e).__name__}: {e}")
        return 2

    # ── Call 3b: get_positions (OPTIONS — CSPs, CCs, LEAPs) ─────
    _banner("CALL 3b — Option Positions (OPT)")
    try:
        opt_positions = trade_client.get_positions(account=account, sec_type=SecurityType.OPT)
        n = len(opt_positions) if opt_positions else 0
        print(f"  Total option positions: {n}")
        for p in (opt_positions or []):
            sym = getattr(p, "contract", None)
            qty = getattr(p, "quantity", "?")
            avg = getattr(p, "average_cost", "?")
            mkt = getattr(p, "market_price", "?")
            mv = getattr(p, "market_value", "?")
            unrl = getattr(p, "unrealized_pnl", "?")
            print(f"    {sym}  qty={qty}  avg={avg}  mkt={mkt}  value={mv}  unrl_pnl={unrl}")
    except Exception as e:
        print(f"  [FAIL] {type(e).__name__}: {e}")
        return 2

    _banner("ALL CALLS PASSED — Tiger API is wired up correctly")
    print("Next: I'll build the 'Tiger Live' reconcile page.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
