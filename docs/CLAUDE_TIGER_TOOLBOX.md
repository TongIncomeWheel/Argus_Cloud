# Tiger MCP Toolbox — what Claude can call, and what it should NEVER recompute

**Audience:** Claude itself, running in a claude.ai project that has the
`Tiger MCPv7` connector attached. This file is the project-knowledge entry
that tells Claude which tools to call instead of deriving things in context.

**Drop this into the claude.ai project as a knowledge file.** From then on
every chat in that project sees it.

---

## Core principle

> **Use the tools. Never recompute what a tool can answer.**

Claude is the executive. The MCP server is the calculator. When the user
asks "what's my net theta?", do NOT write Python to solve Black-Scholes in
context — call `compute_portfolio_greeks` and quote the answer.

Why this matters: every token spent re-deriving math the server already
does is wasted budget that could go toward portfolio strategy. The server
runs Python on Cloud Run; tokens are free there.

---

## Data freshness protocol (READ BEFORE EVERY COMPUTE STEP)

Tiger MCP tools have **three different freshness profiles**. Mixing them
silently produces wrong answers. Before any analytical step, decide which
freshness tier you actually need and call the matching tool.

| Tier | Source | Latency | Tools |
|---|---|---|---|
| **Live quote** | Tiger L1 push | sub-second | `get_option_briefs`, `get_option_chain`, `get_option_depth`, `get_option_trade_ticks` |
| **Position snapshot** | Tiger account cache | seconds–minutes (depends on Tiger's internal refresh) | `get_option_positions`, `get_stock_positions`, `get_account_summary`, `get_prime_assets` |
| **Underlying spot (equity)** | yfinance (server-side, inside compute_portfolio_greeks) | ~15 min delayed | embedded in `compute_portfolio_greeks` |

Rules:

1. **Position state (qty, avg_cost, symbol/expiry/strike) is reliable**
   from `get_option_positions`. Treat it as ground truth.
2. **`market_price` on a position is NOT reliable for execution.** It's a
   Tiger-cached mark, age unknown. Whenever the user is about to act on
   it — sizing a roll, deciding to BTC, computing real-time P&L — re-quote
   via `get_option_briefs` first. Burn the extra round-trip; it's cheap.
3. **For book-wide Greeks** call `compute_portfolio_greeks` directly. It
   fetches yfinance spot per call (no cache), but uses position.market_price
   as the option price. If you need contract-level *freshness* for the
   Greeks (not just spot), tell the user the IV solve is based on a
   possibly-stale position mark and ask whether they want a `briefs`
   re-quote first.
4. **For "what does my book actually look like RIGHT NOW"** the canonical
   sequence is:
   ```
   get_option_positions          → identifiers + quantities
   get_option_briefs(those ids)  → fresh bid/ask/mid per contract
   ```
   Mark-to-market = qty × 100 × briefs.mid. Do not trust
   positions.market_value for live decisions.
5. **Never claim a number is "current" or "live" without naming the
   source and its latency.** "Net theta ≈ $X/day (yfinance spot ~15 min
   delayed; IV solved from Tiger position marks of unknown age)" is the
   correct level of disclosure.

If the user runs a project-layer command (e.g., `/md-pacing`, `/md-yield`)
that needs freshness, the FIRST action of that command MUST be a refresh
sweep matching the tier above. If the command spec doesn't say so,
treat that as a charter bug and flag it before running.

---

## Decision tree (read first)

| User asks about… | Tool to call FIRST |
|---|---|
| NAV / cash / today's P&L | `get_account_summary` |
| Buying power, margin, multi-currency balances | `get_prime_assets` |
| What positions do I hold? | `get_stock_positions` + `get_option_positions` |
| What orders are working? | `get_open_orders` |
| What filled / cancelled in the last N days? | `get_filled_orders(days=N)` / `get_cancelled_orders(days=N)` |
| Per-leg fills for an order (especially MLEG rolls) | `get_order_transactions(order_id)` |
| Per-ticker executions / fills | `get_transactions(symbol, days, limit)` |
| Deposits / withdrawals history | `get_funding_history` |
| Daily NAV / equity curve | `get_nav_history(days)` |
| **Net Δ / Net Θ across the whole option book** | **`compute_portfolio_greeks`** ← new |
| What expiries are available for XYZ? | `get_option_expirations([symbols])` |
| Full chain for one expiry | `get_option_chain(symbol, expiry)` |
| Quote on specific contracts I already know | `get_option_briefs([contracts])` |
| Greeks on specific contracts (Tiger-source, may 403) | `get_option_greeks([contracts])` |
| OHLC bars for one or more contracts | `get_option_bars([contracts], period, limit)` |
| L2 depth / bid-ask ladder | `get_option_depth([contracts])` |
| Recent trade ticks on a contract | `get_option_trade_ticks([contracts], limit)` |
| Place a stock order | `place_stock_order(...)` then `confirm=True` after preview |
| Place a single-leg option order | `place_option_order(...)` then `confirm=True` |
| Roll a short option (atomic) | `execute_roll(...)` then `confirm=True` |
| Cancel a working order | `cancel_order(order_id)` then `confirm=True` |

When more than one tool fits, prefer the one with the smallest payload.
Don't fetch the full chain if `get_option_briefs` for the contracts the
user actually mentioned will do.

---

## Tool catalog

### Account / book state

- **`get_account_summary()`** → NAV, cash, gross stock value, today's
  realized P&L, today's unrealized P&L. Single round-trip. Use for "how
  much cash do I have", "what's my NAV today".
- **`get_prime_assets()`** → segment balances, buying power, margin used,
  multi-currency state. Use for sizing decisions and currency hedging.
- **`get_stock_positions()`** → all stock/ETF positions with avg cost,
  market value, unrealized P&L. Quantities are normalized for fractional
  shares (Tiger reports fractional NVDA as a scaled int — server corrects).
- **`get_option_positions()`** → all option positions with strike, expiry,
  right, qty (negative = short), market_price, avg_cost, unrealized P&L.
- **`get_nav_history(days=30)`** → daily NAV / cash / P&L time series with
  a `summary` block (pnl, pnl_percentage, annualized_return).
- **`get_funding_history()`** → all deposits + withdrawals with currency.

### Orders / executions

- **`get_open_orders()`** → currently working orders.
- **`get_filled_orders(days=7)`** → fills in the last N days. Auto-chunks
  into 30-day windows under Tiger's 90-day cap.

  **MANDATORY: filter by `fill_type` before any P&L / yield / premium
  averaging.** Each order has a `fill_type` field:
    - `"normal"` — real economic fills. Use these for premium collected,
      avg price, fill count for performance attribution.
    - `"expiration"` — worthless-expiry / auto-exercise events. These
      have `avg_fill_price=0`, `limit_price=0`, `commission=0` but a
      nonzero quantity, because Tiger records expiry as a "filled" order.

  Pseudocode any command MUST use when computing premium / yield:
  ```
  fills = get_filled_orders(days=N)
  real  = [f for f in fills if f["fill_type"] == "normal"]
  expiries = [f for f in fills if f["fill_type"] == "expiration"]
  # Premium / yield math goes over `real`.
  # Expiry events count toward win-rate / cycle completion separately.
  ```

  If a command counts expiry events into "avg premium per fill" or "fill
  count for yield", numbers will be silently wrong — premiums get divided
  by an inflated denominator. This is a known footgun; treat as a
  charter bug if you see a command spec missing the filter.
- **`get_cancelled_orders(days=7)`** → cancelled orders.
- **`get_transactions(symbol, days=30, limit=100)`** → per-fill executions
  for one ticker. Tiger requires a symbol filter — for portfolio-wide
  fills iterate over tickers from positions.
- **`get_order_transactions(order_id)`** → per-leg fills for one order.
  Use this to expand MLEG combo rolls into BTC + STO legs with strikes.

### Option market data (US Option L1)

- **`get_option_expirations([symbols])`** → `{symbol: ["YYYY-MM-DD", ...]}`.
- **`get_option_chain(symbol, expiry, include_greeks=True)`** → full chain
  for one underlying + expiry. Each row has strike, right, bid, ask,
  volume, open_interest, and (default) Greeks + implied_vol.
- **`get_option_briefs([contracts])`** → real-time bid/ask/last/OI/HV per
  contract. Use for mark-to-market and execution pricing on contracts
  whose identifiers you already know.
- **`get_option_greeks([contracts])`** → Δ/Γ/Θ/ν/ρ + IV per contract.
  ⚠️ **Tiger denies this for retail TBSG accounts (this account is TBSG).
  EXPECT a 403 / permission error.** Do not list this tool as a primary
  source in any project-layer command — it WILL fail silently and produce
  degraded output with no error surfaced upward. Route every Greeks
  request to one of:
    1. `compute_portfolio_greeks` — whole-book Δ + Θ, local BS solve.
    2. `get_option_chain(..., include_greeks=True)` — per-contract Greeks
       served from the chain endpoint (separate Tiger code path, works on
       TBSG when the L1 entitlement is active).
  If a charter still calls `get_option_greeks` directly, that is a
  charter bug — flag it and substitute one of the routes above before
  proceeding.
- **`get_option_bars([contracts], period, limit)`** → OHLC bars.
- **`get_option_depth([contracts])`** → L2 ladder.
- **`get_option_trade_ticks([contracts], limit)`** → recent prints.

Contract dict shape used by `briefs`/`greeks`/`bars`/`depth`/`ticks`:
```json
{"symbol": "MSTR", "expiry": "2026-07-18", "strike": 380, "right": "PUT"}
```

### Analytics — local compute (E1)

- **`compute_portfolio_greeks(risk_free_rate=0.045, dividend_yield=0.0)`** →
  net + gross Δ and Θ across all option positions. Spot from yfinance
  (~15-min delayed, free); IV solved per-leg from market price; signs
  flipped for shorts.

  Returns:
  ```text
  positions[]:     per-leg rows with delta, theta_per_day, iv,
                   delta_shares (= delta * 100 * |qty|),
                   theta_per_day_usd (= theta_per_day * 100 * |qty|)
  aggregates:      net_delta_shares, net_theta_per_day_usd,
                   gross_delta_shares, gross_theta_per_day_usd,
                   priced_positions, total_positions
  skipped[]:       positions we couldn't price + a reason
  notes:           caveats (delayed spot, missing tickers, etc.)
  ```

  **Use this when the user asks:** "what's my delta?", "how much theta am
  I collecting per day?", "what's my net option exposure?". It is the
  ONLY way to get Greeks on a TBSG account without Tiger throwing 403.

### Write tools — preview-by-default, confirm explicitly

ALL write tools work in two steps. **Never skip the preview.**

1. Call without `confirm=True` → returns `{preview: True, placed: False,
   summary: ..., spec: ..., next_step: "Call again with confirm=True"}`.
2. Show the user the preview summary verbatim. Ask "ok to submit?".
3. Only on explicit yes, call again with `confirm=True`.

- **`place_stock_order(symbol, side, quantity, order_type, limit_price,
  stop_price, time_in_force, outside_rth, currency, confirm)`**
  — side ∈ {BUY, SELL}; order_type ∈ {LMT, MKT, STP, STP_LMT}; LMT and
  STP_LMT require limit_price; STP and STP_LMT require stop_price.
- **`place_option_order(symbol, expiry, strike, right, side, quantity,
  limit_price, time_in_force, confirm)`**
  — side ∈ {SELL_TO_OPEN (CSP/CC entry), BUY_TO_CLOSE (close short),
    BUY_TO_OPEN (long entry), SELL_TO_CLOSE (close long)}. limit_price is
  per share — Tiger multiplies by 100 internally.
- **`execute_roll(symbol, close_expiry, close_strike, close_right,
  new_expiry, new_strike, quantity, net_credit_limit, time_in_force,
  confirm)`** — atomic MLEG combo: BTC old + STO new for one net credit.
  Positive net_credit_limit = require collecting at least that net
  premium; negative = accept a net debit of that magnitude.
- **`cancel_order(order_id, confirm)`** — cancel a working order.

---

## What is NOT yet exposed (and what to say if asked)

These engines exist as Python modules in the repo but are not yet wired
to MCP. If the user asks for them, name what's missing and offer to wire
it via a "Phase E1 follow-up". Do not try to recompute them in chat.

| Capability | Module | Current status |
|---|---|---|
| Wheel-cycle state per ticker (CSP_OPEN / ASSIGNED / CC_OPEN / etc.) | `tiger_api/wheel_cycles.py` | Python only, no MCP wrapper |
| Win-rate by setup bucket | `tiger_api/win_rate.py` | Python only |
| Roll quality scoring (net credit, ΔΘ, Δrisk per roll) | `tiger_api/rolls.py` | Python only |
| IV rank / percentile scanner via realized vol proxy | `tiger_api/iv_scanner.py` | Python only |
| Stress test the book under spot + vol shocks | `tiger_api/stress.py` | Python only |
| CSP candidate scanner (theta_scanner) | `theta_scanner/scan.py` | Python only |
| PMCC engine (regime, doctrine, scorecard) | `pmcc_engine/` | Python only |
| Forecast income (theta carry over horizon) | (designed, not built) | Not implemented |

When the user asks one of these:
- "Could compute that, but it's not wired to MCP yet. Want me to add a
  Phase E1 follow-up so you can call it from chat? Until then I'd have
  to load positions into context and run BS by hand — slow and lossy."
- The roadmap order is in `BACKLOG.md` (Phase E1). The first one shipped
  was `compute_portfolio_greeks` (2026-06-25). Next priorities are
  `get_portfolio_snapshot`, `get_wheel_state`, `get_roll_candidates`.

---

## Command-author checklist (run this against every /md-* or /aegis-* spec)

Before executing ANY project-layer command, mentally walk this list. If
the command spec violates any item, FIRST flag it to the user, THEN
substitute the correct path. Do not run a broken command silently.

1. **Freshness sweep declared?** First action should be a refresh matching
   the data tier (live quote vs. position snapshot vs. spot). See "Data
   freshness protocol" above.
2. **No `get_option_greeks` as a primary source.** Tiger 403s for TBSG.
   Use `compute_portfolio_greeks` or chain-Greeks.
3. **Spot source correct for the project?** Tiger-only project →
   yfinance-via-`compute_portfolio_greeks` or Alpaca (when E2 lands).
   IBKR-attached project → `get_price_snapshot`. Never invent a price.
4. **`fill_type == "normal"` filter applied to any premium/yield/avg
   math?** Expiry events are filled-with-zero-price and will dilute
   averages otherwise.
5. **All numbers attributed to a source + latency?** "Net Δ ≈ X (yfinance
   spot ~15 min delayed; IV from Tiger position mark, unknown age)".
6. **Write tools: preview before confirm, always.** No exceptions for
   urgency.

If the command passes all six, run it. If it fails any, fix it in
conversation with the user before pushing through.

---

## What Claude should NEVER do

1. **Never re-derive Greeks in context.** If asked, call
   `compute_portfolio_greeks`. If it's not available (connector down),
   say so — don't pretend by computing from delta-times-100 yourself
   without the IV solve.
2. **Never invent OAuth tokens, account numbers, or fill prices.** If the
   tool didn't return it, it doesn't exist.
3. **Never submit a trade without explicit user confirmation of the
   preview spec.** Preview → show the user → wait for explicit yes →
   `confirm=True`. This is a hard rule, even when the user sounds urgent.
4. **Never call `get_option_greeks` as a first attempt for portfolio
   Greeks.** Tiger returns 403 for retail TBSG. Use
   `compute_portfolio_greeks` directly.
5. **Never fetch equity spot from a Tiger tool.** The server intentionally
   doesn't expose one (Tiger US Equity L1 is a separate subscription we
   don't carry). Correct spot routes, by context:
   - **Inside the Greeks workflow** → already handled. `compute_portfolio_greeks`
     fetches yfinance spot per call server-side. Don't do a separate spot pull.
   - **Tiger-only project (e.g. Project MD)** → for charts, watchlists,
     scenario inputs, use `compute_portfolio_greeks` if the symbol is
     already in the option book (read its `spot` field from the response),
     otherwise note "spot not available from Tiger MCP — would need
     Alpaca (Phase E2)". Do **not** invent prices.
   - **Project with IBKR connector also attached (e.g. Aegis)** → use
     IBKR's `get_price_snapshot` for clean real-time spot. This is the
     ONLY context where `get_price_snapshot` applies.

---

## Sign conventions for `compute_portfolio_greeks`

Already handled internally — but cross-check before quoting numbers:

| Position | Delta sign | Theta sign |
|---|---|---|
| Long call | positive | negative (decay hurts) |
| Long put | negative | negative |
| Short call (covered call) | negative | **positive** (you collect) |
| Short put (cash-secured put) | **positive** | **positive** |

So a wheel-style book (short puts + short calls) should generally show:
- Mixed-sign per-position delta; net delta close-to-zero or slightly long
- Strongly positive net theta in $/day → that's the carry

If you see strongly negative net theta on a wheel book, something is
wrong — likely an IV solve failure on a few large positions. Check
`skipped[]`.

---

## Connector troubleshooting (tell the user, don't try to fix it yourself)

| Symptom | What to tell the user |
|---|---|
| Tool not in the list | `claude.ai → Settings → Connectors → Tiger MCPv7` → toggle off/on. Forces tool list refresh. |
| 401 unauthorized on every call | Same — disconnect/reconnect. Owner password persists; you don't need to re-enter. |
| `IV solve failed` in `skipped[]` for many positions | Likely stale market_price (after-hours). Try again in market hours. |
| `no spot price for XYZ` in `skipped[]` | yfinance miss. Will be cleaner once Alpaca (Phase E2) lands. |

---

*Last updated: 2026-06-26. Source of truth for what's wired:
`mcp_servers/tiger/server.py`. Source of truth for what's queued:
`BACKLOG.md` Phase E1.*
