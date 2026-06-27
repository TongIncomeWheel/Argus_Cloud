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

Tiger MCP tools have **four different freshness profiles**. Mixing them
silently produces wrong answers. Before any analytical step, decide which
freshness tier you actually need and call the matching tool.

| Tier | Source | Latency | Tools |
|---|---|---|---|
| **Live quote** | Tiger L1 push | sub-second | `get_option_briefs`, `get_option_chain`, `get_option_depth`, `get_option_trade_ticks` |
| **Position snapshot** | Tiger account cache | seconds–minutes (depends on Tiger's internal refresh) | `get_option_positions`, `get_stock_positions`, `get_account_summary`, `get_prime_assets` |
| **Underlying spot (equity)** | yfinance (server-side, inside compute_portfolio_greeks) | ~15 min delayed | embedded in `compute_portfolio_greeks` |
| **Canonical fill history** | Google Sheets "Data Table" tab (or Tiger 90-day fallback) | manual write latency (Tiger ETL pipeline; could be hours-to-days behind real fills) | embedded in `get_position_roc`; quarterly snapshots in `Archive Q<N>-<YYYY>` tabs |

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
6. **For RoC / harvested / juiced math, ALWAYS read `entry_source` and
   relay it.** `get_position_roc` will tag every response as either
   `entry_source: "google_sheets"` (canonical full history) or
   `entry_source: "tiger_mcp_fallback"` (Tiger's 90-day rolling window —
   positions older than 90 days will have `entry_fill_found: false` and a
   null `annualised_roc`). If you see the fallback tag, surface that fact
   when reporting numbers so the user knows the picture may be partial.

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
| **Net Δ / Net Θ across the whole option book** | **`compute_portfolio_greeks`** |
| **Per-position RoC, harvested %, juiced flag** | **`get_position_roc(juiced_only, pot)`** |
| **PMCC §12 scorecard for one candidate** | **`score_pmcc_candidate(symbol, strike, expiry, side, premium, spot, hv30, ...)`** ← new |
| **HV30 / HV-N realised vol for an underlying** | **`compute_hv(symbol, lookback_days=30)`** ← new |
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

- **`get_position_roc(juiced_only=False, pot="all")`** →
  per-position RoC analytics for every open SHORT option (CSP + CC).
  Joins live Tiger position state to entry STO records and computes:
  days held, DTE at entry, DTE remaining, premium yield on notional,
  % of premium harvested (theta captured), annualised RoC, and a
  `juiced` boolean (≥ 65% harvested = candidate to BTC or roll).

  Args:
    - `juiced_only` (bool): return only positions ≥ 65% harvested.
    - `pot` (str): `"all"` | `"core"` | `"active"` | `"sidecar"` — filter
      by pot membership.

  Data sources (in priority order, surfaced as `entry_source`):
    1. **PRIMARY** — Google Sheets `Data Table` tab (full unbounded fill
       history since strategy inception). Match key: ticker + strike +
       expiry + right + STO direction. For rolled positions the **most
       recent** matching STO row wins.
    2. **FALLBACK** — Tiger MCP `get_filled_orders(days=90)`. Used
       automatically when Sheets is unreachable or unconfigured. Tagged
       `entry_source: "tiger_mcp_fallback"`. Positions older than 90 days
       will report `entry_fill_found: false`.

  Pot routing (locked, do not invent):
    - **core** = {MARA, CRCL}
    - **active** = {BE, COIN, DELL, MSFT, MP, SLB}
    - **sidecar** = {ECHO, INTC}
    - **excluded entirely** = {KO, MCD, NVDA, SPY} (never appear in output)
    - anything else → `pot: "unknown"` (visible in positions list but
      NOT counted in `aggregates.by_pot`)

  Returns:
  ```text
  positions[]:               per-position rows with full RoC payload
  aggregates:                {by_pot: {core, active, sidecar},
                              total_notional, total_premium_received,
                              total_pnl_to_date, portfolio_yield_on_notional,
                              portfolio_pct_harvested, juiced_count,
                              total_positions, positions_missing_entry}
  juiced_positions[]:        subset of positions with juiced=True
  missing_entry_positions[]: positions where neither source had an entry
  asof_date:                 today's ISO date used for days-held math
  entry_source:              "google_sheets" | "tiger_mcp_fallback"
  juiced_threshold:          0.65 (echoed for transparency)
  notes[]:                   human-readable caveats / skip reasons
  ```

  **Use this when the user asks:** "which positions are juiced?",
  "what's my annualised RoC on MARA?", "how much have I harvested in the
  core pot?", "are any positions ready to BTC?". Sign convention is
  short-position-friendly: `pct_harvested = 1.0` means the position has
  decayed to ~zero (free money to take off the table).

- **`compute_hv(symbol, lookback_days=30)`** →
  annualised realised volatility from yfinance daily closes.

  Returns: `{symbol, lookback_days, hv, sample_size, source, asof_date, notes[]}`.
  `hv` is a decimal (`0.17` = 17%). Pass it straight as the `hv30` arg to
  `score_pmcc_candidate`. If the fetch fails or returns too few bars,
  `hv=0.0` and `notes[]` explains why.

  **Use when:** any time the doctrine wants HV30 (or HV-N) and you don't
  already have a hot value from FMP or elsewhere. ~250ms latency; safe
  to call inline at the top of a review.

- **`score_pmcc_candidate(symbol, strike, expiry, side, premium, spot,
    hv30, risk_free_rate=0.045, n_paths=5000, seed=None)`** →
  the **PMCC Master Doctrine v3 §12 Trade Evaluation Scorecard**.

  Runs the full §12 block server-side: Greeks (Δ, Θ, vega, gamma) via
  Black-Scholes from the supplied premium-implied IV; §2 theta hurdle
  check; 5,000-path geometric Brownian motion simulation; CVaR; verdict
  with §12 cutoffs. No external API calls inside the tool — pure compute
  from the inputs Claude pastes in.

  Args:
    - `side`: `"STO_PUT"` or `"STO_CALL"` (v1 — BTC/ROLL scorecards
      tracked in BACKLOG)
    - `premium`: current option mark per share (from `get_option_briefs`
      or the live chain)
    - `spot`: current underlying spot (from `mcp__FMP__quote` for SPY,
      or `mcp__Interactive_Brokers_IBKR__get_price_snapshot` if available)
    - `hv30`: annualised realised vol (decimal). Use `compute_hv` or
      compute from FMP chart bars
    - `seed`: optional, makes the MC reproducible across calls

  Returns (matches the §12 scorecard structure):
  ```text
  trade:        {action, symbol, strike, expiry, dte_days, premium}
  greeks:       {delta, theta_per_day, theta_hurdle, theta_pass,
                 vega, gamma, gamma_level, iv_solved, daily_1sigma_usd}
  distribution: {n_paths, hv30, r,
                 p_profit_50, p_profit_80, p_loss, p_assignment,
                 expected_pnl, pnl_stdev, cvar_5, max_profit}
  risk_adjusted:{annualised_return, annualised_vol, sharpe_equiv,
                 capital_efficiency}
  verdict:      "pass" | "conditional" | "fail"
  verdict_reasons[]: list of cutoff messages (theta hurdle, Sharpe,
                     CVaR/expected ratio, auto-reject reason)
  capital_at_risk:   strike × 100 (conservative ceiling)
  ```

  **Use when:** the user asks "score this trade", "should I sell the SPY
  720P 30 DTE", "run the scorecard on STRIKE EXPIRY". Always paste the
  scorecard output to the user verbatim — that IS the §12 block in §16
  output format.

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
| §5.1 Earning Power Test (PMCC roll decision math) | (doctrine only) | Not wired — compute in chat from current vs. new leg θ + roll debit |
| §12 BTC / ROLL scorecard variants | (doctrine only) | `score_pmcc_candidate` is STO-only in v1 |

When the user asks one of these:
- "Could compute that, but it's not wired to MCP yet. Want me to add a
  Phase E1 follow-up so you can call it from chat? Until then I'd have
  to load positions into context and run BS by hand — slow and lossy."
- The roadmap order is in `BACKLOG.md` (Phase E1). Shipped so far:
  `compute_portfolio_greeks` (2026-06-25), `get_position_roc` (2026-06-27),
  `score_pmcc_candidate` + `compute_hv` (2026-06-27). Next priorities:
  `get_wheel_state`, `get_roll_candidates`, BTC/ROLL scorecard variants,
  §5.1 Earning Power Test as an MCP tool.

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
6. **Never compute RoC, harvested %, juiced status, or annualised return
   manually.** Call `get_position_roc`. The math (yield_on_notional,
   pct_harvested, annualised_roc, juiced @ 0.65) lives server-side and is
   the single source of truth. If you recompute in chat with different
   assumptions you'll drift from what the dashboards say.
7. **Never invent the entry date of a position.** Either Sheets or Tiger
   fallback gave you one (entry_fill_found=true) or it didn't. If it
   didn't, that's data the user has to enter — say so and quote the
   `missing_entry_positions[]` block; don't guess.

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

## PMCC review recipe — §15 sequence mapped to tool calls

The SPY PMCC book lives in the Tiger account. Per the PMCC Master
Doctrine v3 §15 Review Sequence, every session walks these steps. Below
is the canonical tool-call recipe for each step — follow it exactly.

**Step 1 — Timestamp.** State SGT + ET. Run a quick `WebSearch` for
"FOMC CPI earnings this week" to confirm what has already happened.

**Step 2 — Live data pull.** In one round-trip, parallel:
1. `mcp__FMP__quote("SPY")` → SPY spot (live).
2. `mcp__FMP__quote("^VIX")` → VIX live + 52w high/low for IVR.
3. `compute_hv("SPY", lookback_days=30)` → HV30 for §2 hurdle.

Compute IVR inline: `(vix_price - vix_52w_low) / (vix_52w_high - vix_52w_low) × 100`.

**Step 3 — Regime classification.** Vol band from VIX vs 18 (SPY median):
L<18, M 18-25, H 25-36, X >36. IVR band: <25 Cheap, 25-50 Neutral, 50-75
Rich, >75 Extreme. State the cell explicitly and the mandated posture
(§1 grid). Mismatched posture is a flag for the next roll cycle, not an
immediate action.

**Step 4 — Position marks.**
1. `get_option_positions()` → full LEAPS + shorts table.
2. For per-contract live quotes: `get_option_briefs([contracts])` on the
   shorts where dying-leg math matters.
3. For per-contract chain Greeks: `get_option_chain(SPY, expiry,
   include_greeks=True)` then filter to the held strikes.

Per leg, check refresh triggers (§9) for LEAPS and dying/roll triggers
(§5.3) for shorts. Surface flags: DYING, ROLL, HARVEST, CLOSE, EX-DIV,
BRICK, REFRESH.

**Step 5 — Aggregate math.**
- `compute_portfolio_greeks()` → net Δ + net Θ across the entire option
  book in one call. Already sign-flipped for shorts.
- theta/delta ratio = `aggregates.net_theta_per_day_usd /
  aggregates.net_delta_shares` (watch the sign — net delta is shares,
  net theta is $/day; the ratio is $/share).
- Array drift = `abs(spot - mean(short strikes)) / spot × 100`.
- Yield ratio = `Σ|theta_per_day| / Σ(per-short hurdle)`.

**Step 6 — Tripwire check.** All 6 gates from §11. None breached → silent
day, state theta accrual only.

**Step 7 — Posture vs regime check.** Does the array's current ITM:OTM
match the regime cell's mandated posture? Mismatch → flag for next roll
cycle. Do NOT correct on the spot.

**Step 8 — Action or hold.**
- No trigger → "Hold. Engine earning $X/day. Next trigger: [condition]."
- Trigger fired → run `score_pmcc_candidate(...)` per candidate strike
  (top 5 candidates by extrinsic + DTE fit). Paste the scorecard block
  verbatim. Apply §5.1 Earning Power Test on the new vs. current leg
  (Claude does this in chat — no MCP tool yet). Spec the ticket only when
  `verdict in ("pass", "conditional")` AND Earning Power payback < 50% of
  new DTE.

### What the recipe deliberately does NOT cover

- `score_pmcc_candidate` is **STO-only in v1**. BTC/ROLL scorecards are
  in BACKLOG. For now, score the new leg only; the BTC side is judged
  by §5.1 Earning Power math.
- §5.1 Earning Power Test (`payback_days < new_leg_DTE × 0.5`) has no
  MCP tool yet — compute in chat from the current vs. new leg's
  `theta_per_day` and the roll debit.
- Spot from FMP is real-time; from `compute_portfolio_greeks` it's
  yfinance ~15-min delayed. If precision matters, prefer FMP. If you
  already have the Greeks block, the spot inside it is fine for context.

---

## Operational pieces (not tools, but Claude should know they exist)

These are pipelines / artifacts that produce the data the MCP tools read.
Claude doesn't run them, but should reference them correctly when the
user mentions them or when a tool's output points back to them.

### Google Sheets "Data Table" — canonical fill history

Every CSP / CC entry, close, and roll the user makes is written to the
`Data Table` tab of the Income Wheel spreadsheet via the Tiger ETL
pipeline (`tiger_etl.py`). This is the **unbounded** history — grows
forever, never rolls. `get_position_roc` reads it as primary source so
positions older than Tiger's 90-day fill window still get proper entry
dates and annualised-RoC math.

Schema (the columns that matter for tools):
`TradeID, Ticker, Date_open, Expiry_Date, Status,
Option_Strike_Price_(USD), OptPremium, Quantity, Direction, StrategyType,
Pot, Tiger_Row_Hash`. `Direction ∈ {Sell, OpenShort, Buy, Close}` —
short-option STO rows are filtered by `Direction in {Sell, OpenShort}`
AND `TradeType == "OPT"`. The `Pot` column is informational; the tool
derives pot from ticker via the locked CORE/ACTIVE/SIDECAR mapping.

Access: the Cloud Run runtime service account reads via Application
Default Credentials. If the user says "the connector isn't using the
sheet" or you see `entry_source: "tiger_mcp_fallback"`, the cause is
almost certainly one of (a) the `MCP_INCOME_WHEEL_SHEET_ID` Secret
Manager secret isn't set to the real spreadsheet id (still on the
`NOT_SET` sentinel), or (b) the runtime SA email
(`<project-number>-compute@developer.gserviceaccount.com`) isn't shared
on the sheet as Viewer.

### Quarterly archive tabs

A scheduled GitHub Action (`.github/workflows/quarterly-archive.yml`)
snapshots `Data Table` into a permanent `Archive Q<N>-<YYYY>` tab at
02:00 UTC on the last day of every calendar quarter. Manual triggers
via `workflow_dispatch` with an optional `quarter_label` input.

Use these tabs for **point-in-time analysis** — "what did the book look
like at end-Q1?" — never as live data. They're frozen snapshots.
Tools don't read them by default; reference them by name (`Archive Q2-2026`)
if the user wants to compare current state to a prior close.

### What Claude does NOT operate

- The Tiger CSV ETL (`tiger_etl.py run_migration`) — this is the user's
  daily/weekly housekeeping pipeline. Never trigger it from chat.
- The quarterly archive workflow — runs on cron + manual UI. Never
  trigger from chat unless the user explicitly asks.
- Anything that writes to the Sheet (`append_trade`, `update_trade`,
  `delete_trades`) — these are upstream of the MCP server, not exposed
  as tools. Sheet edits happen in the Streamlit UI or via ETL.

---

## Connector troubleshooting (tell the user, don't try to fix it yourself)

| Symptom | What to tell the user |
|---|---|
| Tool not in the list | `claude.ai → Settings → Connectors → Tiger MCPv7` → toggle off/on. Forces tool list refresh. |
| 401 unauthorized on every call | Same — disconnect/reconnect. Owner password persists; you don't need to re-enter. |
| `IV solve failed` in `skipped[]` for many positions | Likely stale market_price (after-hours). Try again in market hours. |
| `no spot price for XYZ` in `skipped[]` | yfinance miss. Will be cleaner once Alpaca (Phase E2) lands. |
| `entry_source: "tiger_mcp_fallback"` and positions are missing entries | Sheets isn't reachable. Either (a) `MCP_INCOME_WHEEL_SHEET_ID` is still `NOT_SET` (operator needs to update the Secret Manager value to the real spreadsheet id, then redeploy), or (b) the runtime SA isn't shared on the sheet. Both must be done — tell the user, don't try to fix from chat. |
| `missing_entry_positions[]` non-empty even with `entry_source: "google_sheets"` | Schema drift or a position whose STO row was deleted from the sheet. The user has to fix the sheet; the math will recompute on next call. |

---

*Last updated: 2026-06-26. Source of truth for what's wired:
`mcp_servers/tiger/server.py`. Source of truth for what's queued:
`BACKLOG.md` Phase E1.*
