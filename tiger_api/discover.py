"""Discovery script — calls every relevant Tiger API method and prints the
shape/fields of the first returned object. Read-only. Saves a markdown report
to docs/plans/2026-05-05-tiger-api-discovery.md so we can design adapters
against actual data, not guesses.

Run from the ARGUS_Cloud root:
    python -m tiger_api.discover
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from tiger_api.client import TigerClient


def _attrs(obj: Any) -> dict:
    """Public, non-callable attributes of an object."""
    out = {}
    for a in sorted(dir(obj)):
        if a.startswith("_"):
            continue
        try:
            v = getattr(obj, a)
        except Exception:
            continue
        if callable(v):
            continue
        out[a] = v
    return out


def _fmt(obj: Any, indent: int = 2) -> str:
    """One-line-per-field representation of an object's attrs."""
    if obj is None:
        return "(None)"
    pad = " " * indent
    lines = []
    for k, v in _attrs(obj).items():
        rep = repr(v)
        if len(rep) > 200:
            rep = rep[:200] + "..."
        lines.append(f"{pad}{k:30} = {rep}")
    return "\n".join(lines) if lines else f"{pad}(no public attrs — {type(obj).__name__})"


def _section(title: str) -> str:
    return f"\n\n## {title}\n"


def _summarize(name: str, items: Iterable, max_examples: int = 2) -> str:
    items = list(items or [])
    out = [f"**Method:** `{name}`", f"**Returned:** {len(items)} item(s)", ""]
    if not items:
        out.append("_(empty — endpoint may be unavailable for this license/account)_")
        return "\n".join(out)
    out.append(f"**Type:** `{type(items[0]).__name__}`")
    out.append("")
    for i, item in enumerate(items[:max_examples]):
        out.append(f"### Example {i + 1}")
        out.append("```")
        out.append(_fmt(item, indent=0))
        # If the item has a nested 'contract' or 'summary' attr, expand it
        for nested_attr in ("contract", "summary", "transactions", "fills"):
            inner = getattr(item, nested_attr, None)
            if inner is not None and not isinstance(inner, (str, int, float, bool)):
                if isinstance(inner, list) and inner:
                    out.append(f"\n  Nested .{nested_attr}[0]:")
                    out.append(_fmt(inner[0], indent=4))
                elif not isinstance(inner, list):
                    out.append(f"\n  Nested .{nested_attr}:")
                    out.append(_fmt(inner, indent=4))
        out.append("```")
    return "\n".join(out)


def main() -> int:
    client = TigerClient()
    parts = []
    parts.append(f"# Tiger API Discovery — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    parts.append(f"\nAccount: `{client.account}`  License: `{client.license}`  Sandbox: `{client.is_sandbox}`")

    # ── Assets / NAV ─────────────────────────────────────────────
    parts.append(_section("1. get_assets() — NAV & cash"))
    try:
        a = client._trade_client.get_assets(account=client.account)
        parts.append(_summarize("get_assets", a, max_examples=1))
    except Exception as e:
        parts.append(f"\n[FAIL] {e}")

    # ── Prime assets (margin detail) ─────────────────────────────
    parts.append(_section("2. get_prime_assets() — Margin / buying power detail"))
    try:
        pa = client._trade_client.get_prime_assets(account=client.account)
        parts.append(_summarize("get_prime_assets", [pa] if pa else [], max_examples=1))
    except Exception as e:
        parts.append(f"\n[FAIL] {e}")

    # ── Aggregate assets (multi-currency) ────────────────────────
    parts.append(_section("3. get_aggregate_assets() — Multi-currency totals"))
    try:
        aa = client._trade_client.get_aggregate_assets(account=client.account)
        parts.append(_summarize("get_aggregate_assets", [aa] if aa else [], max_examples=1))
    except Exception as e:
        parts.append(f"\n[FAIL] {e}")

    # ── Filled orders (last 30 days) ─────────────────────────────
    parts.append(_section("4. get_filled_orders() — Filled orders (last 30 days)"))
    try:
        end = datetime.now()
        start = end - timedelta(days=30)
        fills = client._trade_client.get_filled_orders(
            account=client.account,
            start_time=int(start.timestamp() * 1000),
            end_time=int(end.timestamp() * 1000),
        )
        parts.append(_summarize("get_filled_orders", fills, max_examples=2))
    except Exception as e:
        parts.append(f"\n[FAIL] {e}")

    # ── All orders (last 30 days, all statuses) ──────────────────
    parts.append(_section("5. get_orders() — All orders, last 30 days"))
    try:
        end = datetime.now()
        start = end - timedelta(days=30)
        orders = client._trade_client.get_orders(
            account=client.account,
            start_time=int(start.timestamp() * 1000),
            end_time=int(end.timestamp() * 1000),
            limit=10,
        )
        parts.append(_summarize("get_orders", orders, max_examples=2))
    except Exception as e:
        parts.append(f"\n[FAIL] {e}")

    # ── Open orders ──────────────────────────────────────────────
    parts.append(_section("6. get_open_orders() — Currently working orders"))
    try:
        oo = client._trade_client.get_open_orders(account=client.account)
        parts.append(_summarize("get_open_orders", oo, max_examples=2))
    except Exception as e:
        parts.append(f"\n[FAIL] {e}")

    # ── Transactions (per-fill with fees) ────────────────────────
    parts.append(_section("7. get_transactions() — Per-fill executions with FEES ⭐"))
    try:
        end = datetime.now()
        start = end - timedelta(days=14)
        txns = client._trade_client.get_transactions(
            account=client.account,
            start_time=int(start.timestamp() * 1000),
            end_time=int(end.timestamp() * 1000),
            limit=20,
        )
        # Could be a list or a TransactionsResponse wrapper
        if hasattr(txns, "items"):
            txns_list = txns.items
        elif isinstance(txns, list):
            txns_list = txns
        else:
            txns_list = [txns] if txns else []
        parts.append(_summarize("get_transactions", txns_list, max_examples=3))
    except Exception as e:
        parts.append(f"\n[FAIL] {e}")

    # ── Funding history ──────────────────────────────────────────
    parts.append(_section("8. get_funding_history() — Deposits / withdrawals"))
    try:
        fh = client._trade_client.get_funding_history()
        parts.append(_summarize("get_funding_history", fh, max_examples=3))
    except Exception as e:
        parts.append(f"\n[FAIL] {e}")

    # ── Segment fund history ─────────────────────────────────────
    parts.append(_section("9. get_segment_fund_history() — Securities ↔ Futures transfers"))
    try:
        sfh = client._trade_client.get_segment_fund_history()
        parts.append(_summarize("get_segment_fund_history", sfh, max_examples=2))
    except Exception as e:
        parts.append(f"\n[FAIL] {e}")

    # ── Analytics asset (NAV history) ────────────────────────────
    parts.append(_section("10. get_analytics_asset() — Historical NAV curve"))
    try:
        end = datetime.now().date()
        start = end - timedelta(days=30)
        an = client._trade_client.get_analytics_asset(
            account=client.account,
            start_date=start.isoformat(),
            end_date=end.isoformat(),
        )
        if an is None:
            parts.append("_(returned None)_")
        else:
            parts.append(_summarize("get_analytics_asset", [an], max_examples=1))
    except Exception as e:
        parts.append(f"\n[FAIL] {e}")

    # ── Write report ─────────────────────────────────────────────
    out_path = Path(__file__).parent.parent / "docs" / "plans" / "2026-05-05-tiger-api-discovery.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join(parts)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(body)
    print(f"Report written: {out_path}")
    print(f"  {len(body):,} chars")
    return 0


if __name__ == "__main__":
    sys.exit(main())
