# Tiger API Integration — Design & Migration Plan

**Date:** 2026-05-04
**Status:** Design — implementation pending user approval
**Trigger:** Tiger Activity Statement CSV exports lag real broker state by 1–3 days. Pilot day-1 found multiple cases where broker UI showed positions that the CSV did not. CSV is unacceptable as a sole source of truth.

---

## Goals

1. **Real-time data source** — replace lagging CSV with live broker state (positions, orders, P&L, cash).
2. **No manual gap-filling** — once API is wired, the dashboard always reflects today's broker reality without "wait for the next CSV."
3. **Native data fidelity** — pull cash flows in their actual currency (no SGD/USD ambiguity), order fills with exact fees, fills with realized P&L per leg.
4. **Phased migration** — keep CSV path working until the API path is trusted. Then deprecate CSV.

---

## Capabilities the Tiger API gives us (validated against [docs](https://docs-en.itigerup.com/docs/))

### Account / Cash
- `get_assets()` — total NAV, cash, stock value, option value, futures, by segment & currency
- `get_prime_assets()` — Prime account assets (relevant for Tiger Singapore retail)
- `get_funding_history()` — every deposit/withdrawal with native currency and date
- `get_segment_fund_history()` — internal transfers between Securities and Futures segments

### Positions
- `get_positions()` — current open positions with average cost, market value, qty, unrealized P&L. Filterable by security type, currency, market, ticker, expiry, strike.

### Orders
- `tigeropen trade order list` (CLI proxy for the SDK) — historical filled orders
- `place_order` / `modify_order` / `cancel_order` — full order lifecycle (we won't auto-place; for future)
- `get_contract` — option contract metadata for matching

### Real-time
- WebSocket push for **order status changes** (fills, cancels, partial fills)
- WebSocket push for **position updates** (qty changes, cost basis recalc)
- WebSocket push for **account changes** (cash balance moves)
- Quote streaming (already partially solved by Alpaca; could consolidate)

### Market data
- Options chain + Greeks (could replace Alpaca for SPY)
- Stock quotes
- Historical OHLCV

---

## What the API doesn't fully replace

- **Realized P&L per closed-trade pair** — Tiger's order list returns fills, not paired round-trips. We still need our pairing logic (which we already have, working).
- **Cross-broker view** — only Tiger. (Acceptable; user is single-broker.)
- **Some derived analytics** — e.g., per-pot allocation. These remain ARGUS-side calcs.

---

## Architecture

### Module structure

```
argus_cloud/
├── tiger_api/
│   ├── __init__.py
│   ├── client.py              # TigerClient — wraps tigeropen SDK; auth, retry, rate limit
│   ├── adapters.py            # Convert Tiger API objects → ARGUS-shaped dicts
│   ├── sync.py                # Orchestrator: pull current state, diff, write to gSheet
│   ├── push_listener.py       # WebSocket subscriber (background task; future)
│   └── tests/
│       ├── test_client.py     # Mock-based unit tests
│       └── test_adapters.py   # Mapping tests
```

### Data flow

```
Tiger Broker
   │
   ▼  REST poll (10–60s) + WebSocket push
TigerClient (auth, throttle, retry)
   │
   ▼
Adapters (Tiger schema → ARGUS schema)
   │
   ▼
Sync engine — diff vs current Data Table
   │
   ├── New trades found → append to Data Table + Audit_Table
   ├── Position update → update Data Table row
   └── Cash event → update Settings + Reconciliation Log
   │
   ▼
gSheet (single source of truth — same as today)
   │
   ▼
Streamlit app reads Data Table → renders dashboard
```

**Key principle: gSheet remains the source of truth for ARGUS.** API just keeps it fresh. This means the existing dashboard, calculations, and reports keep working unchanged.

---

## Authentication setup (one-time, ~30 min)

You as a Tiger Brokers Singapore retail user already have everything needed to activate the Open API. The flow:

1. **Log into Tiger app** → Account → Open API → Apply
2. **Generate keypair**:
   - Tiger creates a `tiger_id` (your application ID)
   - You generate a **public/private RSA keypair** locally
   - Upload public key to Tiger's portal
   - Keep private key (`private_key.pem`) on your machine — NEVER commit
3. **Get your account ID**: shown in Tiger app, e.g., `50179929`
4. **Configure ARGUS**:
   - Add to `.env` (local): `TIGER_ID=...`, `TIGER_ACCOUNT=50179929`, `TIGER_PRIVATE_KEY_PATH=/path/to/private_key.pem`
   - Add to Streamlit Cloud secrets (production): same fields under `[tiger_api]` section
5. **Test connection**: a small script `tiger_api/test_connection.py` validates we can call `get_assets()` and get a response

The credentials work the same way for Sandbox (paper) and Production. We start in **Sandbox** for testing — no real money risk.

---

## Sync strategy

### Phase 1: Read-only reconciliation view (week 1)

Build the API client + a new "Tiger Live" sidebar nav page that **reads** but **doesn't write**. Shows side-by-side:

| Metric | ARGUS (Data Table) | Tiger API (live) | Drift |
|---|---|---|---|
| Total NAV | $X | $Y | $Z |
| Open positions | N | M | diff |
| Cash | $X | $Y | $Z |
| Realized P&L (period) | $X | $Y | $Z |

Build user trust without changing data. Identifies CSV gaps quantitatively.

### Phase 2: Pull-mode sync (week 2)

Add a "Sync from Tiger API" button that:
1. Pulls current `get_positions()` and recent `get_filled_orders()` (last 7 days)
2. Diffs against Data Table
3. Shows preview (same UX as Tiger Import diff preview today)
4. User clicks Apply → writes to Data Table + Audit_Table

This effectively replaces Tiger Import (CSV) but using API instead of file upload.

### Phase 3: Auto-sync on schedule (week 3)

A background poll every 60s (when app is open) calls Phase 2 silently. If new fills detected, banner appears: "3 new fills since last sync — review?"

### Phase 4: WebSocket push (week 4+)

Subscribe to Account Change Push. New fills appear in Data Table within seconds of execution. CSV uploads become unnecessary.

---

## Data integrity guardrails

The sync logic mirrors what tiger_etl_update.py does today:

1. **Idempotency** — every Tiger order has an `order_id` (Tiger's unique identifier). We store it in `Tiger_Row_Hash`. Re-syncs of the same order are no-ops.
2. **Backup before write** — every sync creates a snapshot tab in the gSheet (same pattern as Tiger Update today).
3. **Audit JSON** — every sync writes a JSON to `data/etl_audit/api_sync_<run_id>.json` with before/after for every row touched.
4. **Conservative split** for partial fills — if API reports a partial close that doesn't match an existing row's qty exactly, we split (same logic as today).
5. **Rollback** — `tiger_etl_rollback.py` already handles restoring Data Table from any backup tab. No changes needed.

---

## Migration plan (CSV → API)

| Phase | Duration | What happens |
|---|---|---|
| 0 | Today | Both CSV and API working in parallel. CSV = primary, API = read-only reconcile view |
| 1 | Week 1 | API "Sync" page replaces "Tiger Import" page. CSV upload still possible but rarely used |
| 2 | Week 2 | CSV upload disabled by feature flag. Only API sync active |
| 3 | Week 3 | Remove CSV-related code (`tiger_etl_update.py`, file uploader, `tiger_samples/`). Repo simplified |
| 4 | Week 4 | WebSocket push for live updates. ARGUS feels real-time |

We can compress this aggressively if pilot validates quickly. The phasing exists to mitigate risk, not to delay.

---

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| API credentials leak | Private key never committed; `.env` and st.secrets only. `.gitignore` already excludes |
| API rate limit hit | tigeropen SDK has built-in throttle; we add retry-with-backoff |
| API returns unexpected data shape | Adapters layer catches schema changes; falls back to "no data" rather than corrupting Data Table |
| Sandbox vs Production confusion | Explicit env flag `TIGER_ENV=sandbox` vs `production`; visible warning banner in UI when sandbox |
| Account compromised through API | Tiger requires 2FA on API key generation; private key on your machine only |
| Bug in sync writes wrong data | Same backup-before-write + rollback CLI as today. Worst case: revert to last good state in 60 sec |
| WebSocket disconnects | Reconnect with exponential backoff; on reconnect, pull current state to fill any missed events |
| Cost / billing surprise | Trading commissions stay app-rate; only market data tiers might cost — we'll start with the free tier |

---

## Out of scope (for now)

- **Auto-placing orders** via the API. Hard requirement: ARGUS only READS. Order placement stays in Tiger's UI/app to keep human in the loop.
- **Multi-broker support**. Single-broker assumption is fine for v1.
- **Tax-lot accounting**. Tiger reports its lot accounting; we use it as-is.
- **Options exercise simulation** beyond what Tiger reports.

---

## Implementation effort estimate

| Phase | Effort | What you'd do | What I'd do |
|---|---|---|---|
| Setup | 30 min | Generate keypair, register in Tiger portal, give me `TIGER_ID` and `TIGER_ACCOUNT` | Wire env vars, write test_connection.py |
| Phase 1 (read-only reconcile) | 4–6 hours | Review, test in sandbox | Build TigerClient + adapters + Tiger Live page |
| Phase 2 (pull-mode sync) | 4–6 hours | Test against your real positions | Build sync orchestrator + diff/apply (reuse existing partial-fill logic) |
| Phase 3 (auto-sync) | 2 hours | None | Background polling loop + UI banner |
| Phase 4 (WebSocket) | 4 hours | None | Push subscriber + reconnection logic |
| CSV decommission | 1 hour | Confirm no regressions | Delete CSV-related code, simplify repo |

**Total: ~2 days of my work, plus ~30 min from you for credentials.**

---

## Open questions for you

1. **Sandbox vs Production**: do you want me to develop against your Production account directly (read-only, very low risk), or set up a Sandbox first? Sandbox is safer; Production has real data already so no setup needed.
2. **Pace**: do all 4 phases now (~2 days continuous) or week-by-week? Week-by-week lets you validate each before next.
3. **Scope check**: anything you want INCLUDED that I missed? (e.g., margin balance from API, dividend forecasts, etc.)
4. **Order placement** — should ARGUS ever PLACE orders, or is it strictly read-only forever? My recommendation: strictly read-only (you trade in Tiger's UI; ARGUS is the analytics layer).

Once you answer these, I'll start with the credential setup and Phase 1.

---

## Pilot day-1 lessons baked into this design

- **Currency awareness** — adapters explicitly read native currency from API (no parser ambiguity)
- **Idempotency by order_id** — Tiger's stable IDs replace our content-based hash heuristic
- **Real-time OR fast catch-up** — both paths designed; user picks which one is on
- **gSheet stays primary** — the dashboard never has to learn about the API; it just reads Data Table as today
- **Manual entry forms still useful** — e.g., for trades user wants to log before settlement, or cross-broker positions if they ever appear

---

## 2026-05-05 — API discovery findings (live account validation)

After credential setup + smoke tests against the production account, every endpoint we need is confirmed working. Data quality is materially **better than CSV**, not just equal.

### Endpoints confirmed (TBSG retail Prime account)

| Endpoint | Returns | What we now have that CSV didn't |
|---|---|---|
| `get_assets()` | NAV, cash, gross PV, realized/unrealized PnL | Live, real-time |
| `get_prime_assets()` | Per-segment balances, BP, capability, multi-currency | **Tiger's actual BP** ($120,690 in S-segment) |
| `get_positions(STK)` | Stock lots w/ avg cost & MV | Live, no lag |
| `get_positions(OPT)` | All options (CSPs, CCs, LEAPs) in one call | Signed qty distinguishes long/short |
| `get_filled_orders()` | Per-order: price, commission, gst, fill_time, **realized_pnl** | **Native realized PnL** — drop FIFO pairing for new orders |
| `get_open_orders()` | Live working orders | Real-time visibility on resting orders |
| `get_cancelled_orders()` | Cancelled orders w/ reason | Audit trail of cancels |
| `get_transactions(symbol)` | Per-fill executions (ms timestamps) | Sub-order detail; per-symbol query |
| `get_funding_history()` | DataFrame of all deposits/withdrawals **w/ explicit currency** | **Solves SGD/USD ambiguity** |
| `get_segment_fund_history()` | Internal segment transfers | Visibility into FUND→SEC moves |
| `get_nav_history()` (analytics_asset) | Daily NAV/PnL/cash/deposits time series | **NEW capability** for performance charts |

### Endpoints unavailable on TBSG retail (not blocking)

- `get_aggregate_assets()` — institution-only. `get_prime_assets()` covers our needs.

### Quirks worth noting

1. **`get_assets()` summary shows `inf` for some fields when options are present.** Cosmetic SDK bug. Numeric fields we use (`net_liquidation`, `cash`, `realized_pl`, `unrealized_pl`) are correct.
2. **`get_transactions()` requires a `symbol` filter.** No "all transactions" endpoint. Workaround: `client.get_all_transactions()` iterates over position tickers.
3. **`get_funding_history()` returns a pandas DataFrame**, not a list.
4. **`get_open_orders()` doesn't accept time-range filters.** Returns whatever's currently working.
5. **Fractional shares** (NVDA from rewards) report as `qty=45155, value=89.26` (5-decimal scaled int × 100,000 = 0.45155 shares). Adapters normalize.

### What this means for the build

1. **Drop the `:dup<N>` row hash heuristic.** Tiger orders have stable `id` and `external_id` — idempotency is trivial.
2. **Use Tiger's `realized_pnl` for new orders.** Keep our pairing logic only for legacy CSV-imported trades.
3. **Add a "Tiger Broker State" panel** showing live `get_prime_assets()` data alongside our cash-secured policy view.
4. **Funding parser is now obsolete.** `get_funding_history()` is clean.
5. **Performance / NAV charts use `get_nav_history()`.** Daily snapshots with deposits + position value + cash in one call.

### Real numbers from live account (validation)

```
NAV (net liquidation)  : $330,965.50
Cash                   : $ 38,406.45
Buying Power (S-seg)   : $120,690.83
Gross position value   : $293,950.51
Unrealized PnL         : -$76,387.75

Stock positions        : 4
Option positions       : 37  (6 SPY LEAP contracts ✓ matches expectation)
Filled orders (7d)     : 20
Funding events         : 32 (all SGD; total $470,316 — matches broker UI)
```

### Implementation status (as of 2026-05-05)

- ✅ Credentials wired (`.streamlit/tiger_openapi_config.properties`)
- ✅ `tiger_api/__init__.py`, `tiger_api/test_connection.py`, `tiger_api/discover.py`
- ✅ `tiger_api/client.py` — TigerClient with 9 working endpoints + chunked order fetch
- ✅ `tiger_api/adapters.py` — position + order → ARGUS row, side-aware classifier
- ✅ `tiger_api/tiger_data.py` — top-level cached DataFrame loaders
- ✅ `app_v2.py` — greenfield rebuild with 5 top tabs, per-ticker pacing
- ⏳ Phase 1 Tiger Live reconcile page (deprecated — superseded by app_v2)
- ⏳ Build out Ladder, P&L Slicer, Risk pages (next)
- ⏳ Decommission CSV path (cleanup phase)

---

## Backlog (post-MVP)

### Archive module — long-term history retention

**Problem:** Tiger API only retains ~16 months of order history (oldest fill: 2025-01-03). For a multi-year shelf life, ARGUS needs to archive older fills locally before they fall out of Tiger's window.

**Proposal:** Background "snapshot" job that:
1. Pulls a wide window (e.g. 12 months) from `get_filled_orders()` weekly
2. Appends new fills to a local `data/archive/orders.parquet` (deduped by `order_id`)
3. Loaders read live from API + union with archive for periods Tiger no longer covers
4. Same pattern for `get_nav_history()` (daily snapshots) and `get_funding_history()`

**Storage:** Parquet files in `data/archive/` (compact, fast). Or push to a small SQLite DB for queryability.

**On Streamlit Cloud:** ephemeral filesystem means archive must live in gSheet. Trade-off: bigger gSheet but cheap durability. Alternative: write archive to S3/R2 if Cloud-hosted.

**Trigger:** Build when ARGUS hits 12 months of operational use, OR when a chart needs >16 months of history. Not blocking for MVP.

### Other backlog items
- Tiger MCP server (~200 lines) for portfolio queries from Claude Desktop
- Per-ticker yield assumption (some tickers carry higher premium yield than others)
- Wheel-cycle decomposition view (CSP → assignment → CC → called away)
- Stress test: spot −10/−20/−30% scenarios per ticker
