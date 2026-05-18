"""Text-form 4-block daily review formatter.

Doctrine §10: every review states, in order:
  1. Regime cell
  2. Posture mandated by that cell
  3. Tripwire status
  4. Position-level marks (with §12 scorecard for any candidate trade)
  5. Aggregate book metrics
  6. Action — only if tripwires breached, with full §5 decomposition + §12 scorecard

This module returns plain text — useful for log files, CLI runs, and the
"copy to clipboard" path in the UI. The Streamlit UI in `ui.py` renders the
same content visually.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Mapping, Optional, Sequence


def _fmt_money(v) -> str:
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return "—"


def _fmt_pct(v) -> str:
    try:
        return f"{float(v)*100:.1f}%"
    except (TypeError, ValueError):
        return "—"


def render_review(
    ticker: str,
    spot: float,
    cell: Mapping,
    aggregate: Mapping,
    positions: Sequence[Mapping],
    tripwires: Sequence,
    action: Optional[Mapping] = None,
    yield_ratio: Optional[float] = None,
    timestamp: Optional[datetime] = None,
) -> str:
    """Build a 4-block text review per §10."""
    ts = timestamp or datetime.utcnow()
    lines = []

    # ── Plain-English summary ─────────────────────────
    lines.append("══════════════════════════════════════════════════════════════")
    lines.append(f"  PMCC Engine Review — {ticker}  ·  {ts:%Y-%m-%d %H:%M UTC}")
    lines.append("══════════════════════════════════════════════════════════════")
    lines.append("")
    summary = _summary_paragraph(ticker, spot, cell, aggregate, tripwires, action)
    lines.append("PLAIN-ENGLISH SUMMARY")
    lines.append(summary)
    lines.append("")

    # ── Block 1: Market State ─────────────────────────
    lines.append("BLOCK 1 — MARKET STATE")
    lines.append(f"  Ticker:     {ticker}")
    lines.append(f"  Spot:       {_fmt_money(spot)}")
    lines.append(f"  Vol axis:   {cell.get('vol_axis', '—')}  current={cell.get('current_vol', '—')}  median={cell.get('median_vol', '—')}")
    lines.append(f"  IVR:        {cell.get('ivr', '—')}  ({cell.get('ivr_band', '—')})")
    lines.append(f"  Regime:     {cell.get('cell_label', '—')}")
    lines.append(f"  Posture:    {cell.get('posture', '—')}")
    if cell.get("dte_weeks"):
        dw = cell["dte_weeks"]
        lines.append(f"  DTE band:   {dw[0]}-{dw[1]} weeks")
    if cell.get("array"):
        lines.append(f"  Array:      {cell['array']}")
    if cell.get("description"):
        lines.append(f"  Doctrine:   {cell['description']}")
    lines.append("")

    # ── Block 2: Position Table ───────────────────────
    lines.append("BLOCK 2 — POSITION TABLE")
    if not positions:
        lines.append("  (no positions for this ticker)")
    else:
        header = f"  {'Type':<6}{'Strike':>9}{'DTE':>6}{'Mark':>9}{'Δ':>8}{'Θ/d':>9}{'Ext':>9}{'$/spot':>9}"
        lines.append(header)
        lines.append("  " + "-" * (len(header) - 2))
        for p in positions:
            t = p.get("type", "—")[:5]
            strike = p.get("strike", 0.0)
            dte = p.get("dte", "—")
            mark = p.get("mark", 0.0)
            delta = p.get("delta")
            theta = p.get("theta_per_day")
            ext = p.get("extrinsic", 0.0)
            dollars_from_spot = (strike - spot) if spot else 0.0
            lines.append(
                f"  {t:<6}{strike:>9.2f}{str(dte):>6}{mark:>9.2f}"
                f"{(delta if delta is not None else 0):>8.2f}"
                f"{(theta if theta is not None else 0):>9.3f}"
                f"{ext:>9.2f}{dollars_from_spot:>9.2f}"
            )
    lines.append("")

    # ── Block 3: Aggregate ────────────────────────────
    lines.append("BLOCK 3 — AGGREGATE")
    lines.append(f"  Net delta:        {aggregate.get('net_delta', 0):.1f}  ($ per $1 underlying)")
    lines.append(f"  Net theta:        ${aggregate.get('net_theta', 0):.2f} / day")
    tpd = aggregate.get("theta_per_delta", 0)
    rating = aggregate.get("theta_per_delta_rating", "—")
    lines.append(f"  Theta/Delta:      {tpd:.2f}  ({rating})")
    if yield_ratio is not None:
        lines.append(f"  Yield ratio:      {yield_ratio:.2f}  (≥1.0 = above hurdle)")
    cov = aggregate.get("coverage", {})
    if cov:
        lines.append(f"  Contract cover:   long {cov.get('long_total', 0)} / short {cov.get('short_total', 0)}")
        lines.append(f"  Chassis cover:    chassis {cov.get('chassis_qty', 0)} / short {cov.get('short_total', 0)}")
    lines.append("")

    # Tripwire status
    lines.append("  Tripwire status:")
    for t in tripwires:
        flag = "🔴 BREACH" if bool(t) else "✅ pass"
        lines.append(f"    {flag}  {t.name}: {t.detail}")
    lines.append("")

    # ── Block 4: Action ───────────────────────────────
    lines.append("BLOCK 4 — ACTION")
    if action:
        lines.append("  ⚠️  Action required (see §5 decomposition + §12 scorecard below).")
        for k, v in action.items():
            lines.append(f"    {k}: {v}")
    else:
        any_breach = any(bool(t) for t in tripwires)
        if any_breach:
            lines.append("  ⚠️  Tripwire breach but no action proposed — review manually.")
        else:
            lines.append("  ✅ No action. Silent days are good days.")
    lines.append("")
    lines.append("══════════════════════════════════════════════════════════════")
    return "\n".join(lines)


def _summary_paragraph(ticker, spot, cell, aggregate, tripwires, action) -> str:
    any_breach = any(bool(t) for t in tripwires)
    parts = []
    parts.append(
        f"{ticker} @ {_fmt_money(spot)} — regime {cell.get('cell_label', '—')}; "
        f"posture {cell.get('posture', '—')}."
    )
    tpd = aggregate.get("theta_per_delta", 0)
    parts.append(
        f"Net Δ ${aggregate.get('net_delta', 0):.0f}, Θ ${aggregate.get('net_theta', 0):.0f}/d, "
        f"Θ/Δ ${tpd:.2f} ({aggregate.get('theta_per_delta_rating', '—').lower()})."
    )
    if action:
        parts.append("Action proposed — see Block 4.")
    elif any_breach:
        parts.append("Tripwire(s) breached — manual review required.")
    else:
        parts.append("All tripwires green. Hold.")
    return " ".join(parts)
