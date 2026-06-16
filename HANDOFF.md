# ARGUS — Complete Handoff Document

> **Last updated:** 2026-05-16
> **Author:** Claude Code sessions (cumulative)
> **Purpose:** Full rebuild context so a fresh Claude session can continue development without loss

---

## 1. What Is ARGUS

ARGUS is a **Streamlit-based Income Wheel options trading command terminal** for a single user (Ash) who trades US options via **Tiger Brokers Singapore (TBSG)**. It connects to the Tiger Open API (read-only) and displays live positions, P&L analytics, expiry ladder, cash flow, and risk management tools.

**Deployed to:** Streamlit Community Cloud from `TongIncomeWheel/Argus_Cloud` repo, `main` branch.
**URL:** Managed via Streamlit Cloud dashboard.

### Income Wheel Strategy (context for all logic)
- **CSP** (Cash-Secured Put): sell put, collect premium. If assigned → buy stock.
- **CC** (Covered Call): own stock, sell call against it, collect premium. If called away → sell stock.
- **LEAP** (Long-dated call): used for PMCC (Poor Man's Covered Call) on SPY.
- **Cycle:** CASH → sell CSP → get assigned → own STOCK → sell CC → get called away → back to CASH.
- Goal: generate weekly premium income, manage risk, never use naked margin.

---

## 2. Repository & Branches

```
Repo:   https://github.com/TongIncomeWheel/Argus_Cloud.git
Branch: main          ← production (Streamlit Cloud auto-deploys from here)
Branch: pre-pilot-stable  ← legacy snapshot before Tiger API rebuild
```

### Git Workflow (from CLAUDE.md)
- **Never commit directly to main in dev.** The CLAUDE.md in ARGUS_Dev worktree says use `dev` branch, but the Cloud repo currently works directly on `main` since it's the deploy target.
- User triggers: `"push dev"`, `"merge to Github Main"`, etc.
- Always `git add -A` → commit → push. Don't auto-commit without being told.

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Streamlit Cloud                        │
│                                                           │
│  app.py  ←── main entry point (~4300 lines)              │
│    ├── render_header()     persistent top bar              │
│    ├── render_cockpit()    home — pacing, inventory, BP    │
│    ├── render_risk()       risk & roll candidates           │
│    ├── render_positions()  open positions + Greeks           │
│    ├── render_transactions() filled orders + filters         │
│    ├── render_ladder()     expiry calendar + quotes           │
│    ├── render_pl()         P&L analytics (6 sub-tabs)        │
│    ├── render_iv_scanner() IV Rank scanner                   │
│    ├── render_config()     settings persistence              │
│    └── render_cash()       cash maximization panel           │
│                                                               │
│  tiger_api/                                                   │
│    ├── client.py        TigerClient wrapper (read-only)       │
│    ├── tiger_data.py    all cached data loaders               │
│    ├── adapters.py      Tiger SDK → ARGUS row dicts           │
│    ├── archive.py       two-tier persistence (gSheet+parquet) │
│    ├── greeks.py        Black-Scholes (IV solve + Greeks)     │
│    ├── wheel_cycles.py  cycle state machine                   │
│    ├── win_rate.py      win-rate by setup bucket              │
│    ├── rolls.py         roll quality tracker                  │
│    ├── iv_scanner.py    IV Rank via realized vol proxy        │
│    └── stress.py        stress test via BS reprice            │
│                                                               │
│  mcp_servers/                                                  │
│    └── tiger/                                                  │
│      ├── server.py      FastMCP — 12 read-only tools           │
│      │                  --transport stdio|sse|streamable-http  │
│      ├── auth.py        env-var bootstrap + BearerTokenVerifier│
│      └── deploy/        Cloud Run Dockerfile + yaml + README   │
│                                                               │
│  persistence.py     settings (local JSON + gSheet canonical)  │
│  gsheet_handler.py  gspread CRUD for Google Sheets            │
│  config.py          paths, secrets, constants                 │
│  contract_price_lookup.py  Alpaca option pricer (Lookup tab)  │
│  unified_calculations.py   capital calculator classes          │
└───────────────────────────────────────────────────────────────┘
```

### Data Flow (cold start)
```
1. TigerClient connects (config from st.secrets on Cloud)
2. load_account_summary()  → Tiger get_assets + get_prime_assets
3. load_open_positions()   → Tiger get_all_positions
4. load_orders(90d)        → Tiger get_filled_orders (3 chunks × 30d)
   └─ MLEG combos: parquet cache → fallback seed from gSheet archive → Tiger API expansion
5. auto_archive_if_stale() → check gSheet archive age, auto-append if >80d stale
6. load_spot_prices()      → yfinance (primary) → Alpaca → Tiger (fallback chain)
7. Vault pull alert        → Tiger get_segment_fund_history
8. Render active tab
```

---

## 4. Key Files — What Each Does

### `app.py` (~4300 lines)
The monolithic Streamlit app. All UI rendering.

**9 tabs** (stateful radio, not st.tabs — to prevent tab-bounce on rerun):
| Tab | Key | Function | Data Source |
|-----|-----|----------|-------------|
| 🎯 Cockpit | cockpit | Pacing, inventory, BP, CC coverage | df_open, df_orders |
| ⚠️ Risk & Rolls | risk | Roll/close candidates, stress test, expiring-soon, roll history | df_open, df_orders_full |
| 📦 Positions | positions | Open positions with Greeks, Alpaca quotes | df_open |
| 📜 Transactions | transactions | Filled order history with filters | df_orders (90d) |
| 📅 Ladder | ladder | Expiry calendar grouped by week | df_open |
| 📊 P&L | pl | 6 sub-tabs: By Type, By Ticker, Pivot, Wheel Cycles, Win Rate, Rolls | df_orders_full (archive) |
| 🔎 Lookup | lookup | Contract price lookup (Alpaca) + IV Rank Scanner | Alpaca, yfinance |
| ⚙️ Config | config | Ticker list, deposits, capital allocation, PMCC, archive | persistence.py |
| 💰 Cash | cash | Cash flow, carry analysis, FX trades, MMF vault | Tiger fund_details |

**Critical patterns in app.py:**
- `@st.fragment` on every render function — scoped reruns preserve active tab
- Stateful tab: `st.session_state["active_tab"]` + CSS-styled horizontal radio
- All filter dropdowns are `st.multiselect` with `placeholder="All ..."` (empty = no filter = show all)
- Filter logic: `if filter_list: df = df[df[col].isin(filter_list)]`
- Mobile CSS: `@media (max-width: 768px)` responsive layout
- Dollar signs escaped as `\$` in markdown to prevent LaTeX rendering

### `tiger_api/client.py`
- `TigerClient` class: lazy singleton, read-only
- Cloud bootstrap: `_bootstrap_from_streamlit_secrets()` reads `st.secrets["tiger"]["properties"]` → writes to /tmp
- Auto-chunks `get_filled_orders()` in 30-day windows (Tiger caps at 90d, 100 fills/call)
- Rate limit aware: MLEG expansion uses 1.05s sleep between calls

### `tiger_api/tiger_data.py`
All `@st.cache_data` loaders. Cache TTLs:
| Function | TTL | What |
|----------|-----|------|
| load_account_summary | 30s | NAV, cash, BP, margin |
| load_open_positions | 30s | Open positions |
| load_orders | 300s | 90-day filled orders + MLEG expansion |
| load_orders_full | 300s | Live 90d + archive merged |
| load_spot_prices | 60s | yfinance → Alpaca → Tiger fallback |
| load_option_quotes | 120s | Alpaca option snapshots (bid/ask/last/mid/IV/Greeks) |
| load_earnings_calendar | 3600s | yfinance earnings dates |
| load_vault_history | 600s | Tiger fund_details (MMF) |
| load_fx_trades | 600s | Tiger fund_details (FX) |
| load_nav_history | 600s | Tiger daily NAV series |
| read_archive_from_gsheet | 600s | gSheet archive (cached to avoid 3× redundant reads) |

**MLEG cache cold-start optimization (new, uncommitted):**
- Parquet at `data/mleg_cache.parquet` is wiped on Cloud sleep
- `_load_mleg_cache()` now auto-seeds from gSheet archive on miss
- Archive rows carry `Source = "TigerAPI-LEG (combo <order_id>)"` → reconstructed into cache
- Saves ~60-90s on cold start (was re-expanding all combos via Tiger API)

### `tiger_api/archive.py`
Two-tier archive:
- **Tier 1 (canonical):** gSheet tab `Orders_Archive` — survives forever
- **Tier 2 (cache):** `data/archive/orders.parquet` — fast but ephemeral on Cloud
- Read: parquet first → gSheet fallback → save parquet for next time
- Write: always BOTH gSheet AND parquet
- `read_archive_from_gsheet()` is now `@st.cache_data(ttl=600)` to avoid redundant reads
- `auto_archive_if_stale()`: if archive latest > 80 days old, auto-appends to prevent data loss (Tiger only keeps 90 days)

### `tiger_api/adapters.py`
Converts Tiger SDK objects into flat ARGUS row dicts.
- `positions_to_argus_rows()`: open positions
- `orders_to_argus_rows()`: filled orders (single-leg)
- `txn_to_argus_row()`: MLEG combo leg expansion
- `classify_combo_type()`: determines if a combo is CC, CSP, or LEAP roll
- PMCC logic: if ticker in PMCC set (default SPY) and DTE > 180d → LEAP; else short option

### `tiger_api/greeks.py`
Black-Scholes implementation (Tiger denies Greeks for retail TBSG):
- `implied_vol()`: Newton-Raphson IV solver with bisection fallback
- `bs_price()`: European option pricing
- `bs_delta_theta()`: first-order Greeks
- `compute_greeks()`: top-level API — takes spot/strike/dte/market_price → returns delta/theta/iv

### `tiger_api/wheel_cycles.py`
State machine: CASH → CSP → STOCK → CC → CASH
- Traces per-ticker lifecycle through archive
- Separates Premium P&L (always realized) from Stock P&L (only on cycle close)
- Open cycles show Stock P&L = $0 (not the unrealized mark-to-market)

### `tiger_api/win_rate.py`
- Buckets closed trades by: Type (CSP/CC) × DTE bucket (0-7d/8-21d/22-45d/46+d) × Moneyness (~Δ proxy via premium/strike ratio)
- Win = Actual_Profit > 0
- Period-filterable (applies to closing date, not opening date)

### `tiger_api/rolls.py`
- Detects roll pairs: same day, same ticker, same right, BTC leg + STO leg
- Classifies: ✅ Strong (credit + better strike), 🟡 OK, 🔴 Defensive
- Period/ticker/pot filterable

### `tiger_api/iv_scanner.py`
- IV Rank proxy via realized volatility (RV30 rolling 30-day annualized from yfinance)
- IV Rank = (current - 52w low) / (52w high - 52w low) × 100
- Regime labels: 🟢 SELL aggressively (≥70) → 🔴 AVOID (<30)
- Cached 1hr (yfinance rate-limited)

### `tiger_api/stress.py`
- Reprices book at shocked spots (-30% to +10%)
- Uses BS with current IV solved from market price
- Reports: Stock impact, Option impact, Total NAV change, ITM count after shock

### `persistence.py`
- Settings stored in `data/user_settings.json` (local, ephemeral on Cloud)
- **Canonical persistence: gSheet Settings tab** — `save_settings()` writes to BOTH
- On Cloud restart, `load_settings()` auto-restores from gSheet if local file missing
- Portfolio-keyed: `income_wheel_*` prefix for all keys

### `gsheet_handler.py`
- Uses `gspread` + service account auth
- `GSheetHandler(sheet_id)` class
- `read_settings()` / `write_settings()` for the Settings tab
- Other CRUD for trades/positions (legacy, not used by current app)

### `config.py`
- `get_secret(key)`: tries `st.secrets` first, then `.env`
- Paths: BASE_DIR, DATA_DIR, BACKUP_DIR, LOGS_DIR
- Sheet IDs: `INCOME_WHEEL_SHEET_ID`, `ACTIVE_CORE_SHEET_ID`

### `contract_price_lookup.py`
- Standalone Alpaca option pricer
- User enters ticker/strike/expiry/type → fetches live bid/ask/last/mid/IV/Greeks
- Rendered in Lookup tab

### `unified_calculations.py`
- `UnifiedCapitalCalculator`: per-ticker capital breakdown
  - STOCK = shares × avg_buy_price (cost basis, NOT market price)
  - CSP = strike × 100 × contracts
  - LEAP = premium × 100 × contracts
  - CC = $0 (covered)
- `UnifiedPacingCalculator`: weekly deployment pacing vs planned allocation

---

## 5. Credentials & Secrets

### Streamlit Cloud Secrets (`.streamlit/secrets.toml` on Cloud dashboard)

```toml
INCOME_WHEEL_SHEET_ID = "your_google_sheet_id"
ALPACA_API_KEY = "..."
ALPACA_SECRET_KEY = "..."

[tiger]
properties = """
tiger_id=your_tiger_id
private_key=-----BEGIN RSA PRIVATE KEY-----
...key content...
-----END RSA PRIVATE KEY-----
account=your_account_number
"""
```

### Google Sheets Service Account
- `gsheet_credentials.json` at repo root (gitignored)
- Service account email must have Editor access to the Income Wheel Google Sheet
- On Streamlit Cloud: this file must exist in the repo or be generated from secrets

### Tiger Open API
- Config bootstrapped from `st.secrets["tiger"]["properties"]` on Cloud
- Written to `/tmp/argus_tiger_config/` if `.streamlit/` is read-only
- Read-only access only — ARGUS never places/modifies/cancels orders
- Rate limits: 60 calls/min for `get_order_transactions`, 10/min for `get_fund_details`

### Alpaca
- Free tier (IEX feed, ~15min lag for stocks)
- Option snapshots via `OptionHistoricalDataClient` — real-time-ish
- Env vars: `ALPACA_API_KEY`, `ALPACA_SECRET_KEY`

---

## 6. Key Design Decisions & Why

### Tab persistence (stateful radio vs st.tabs)
**Problem:** `st.tabs` is stateless — any full rerun (filter click, form save) bounces user back to first tab.
**Solution:** `st.session_state["active_tab"]` + `st.radio(horizontal=True)` + CSS to look like tabs. Every render function is `@st.fragment` so widget interactions only rerun the fragment, not the whole page.

### MLEG combo expansion + parquet cache
**Problem:** Tiger's `get_filled_orders()` returns one row per combo order with no per-leg detail. Need `get_order_transactions(order_id)` per combo = rate-limited 60/min. 59 combos = 80s cold start.
**Solution:** Permanent parquet cache keyed by order_id (`data/mleg_cache.parquet`). Each combo expanded exactly once in its lifetime. Daily cold start = 0-3 new combos = ~3s.
**Cloud problem:** Ephemeral filesystem wipes parquet on sleep. **New fix:** `_load_mleg_cache()` auto-seeds from gSheet archive on miss (archive already has expanded legs).

### Two-tier archive (gSheet + parquet)
**Problem:** Tiger only keeps 90 days of fill history. Need permanent archive for YTD/lifetime analytics.
**Solution:** gSheet `Orders_Archive` tab = canonical (survives forever). Local parquet = fast cache (ephemeral on Cloud). Read: try parquet → fallback gSheet → populate parquet. Write: always both.
**Auto-archive:** If archive latest > 80 days old, `auto_archive_if_stale()` auto-appends to prevent data loss.

### gSheet archive read caching
**Problem:** `read_archive_from_gsheet()` (1135 rows) was called 3× on cold start: auto_archive check, header archive badge, data coverage strip. Each read ~10-20s = 30-60s wasted.
**Solution:** `@st.cache_data(ttl=600)` on `read_archive_from_gsheet()`. Cache-bust on write.

### NAV summing ALL segments
**Problem:** `get_assets()` only returns SEC segment. After MMF subscription (SEC→FUND transfer), NAV showed wildly wrong negative number.
**Solution:** Use `get_prime_assets().segments` and sum `net_liquidation` across S (Securities), F (Fund/MMF), C (Futures).

### Greeks computed locally (not from Tiger)
**Problem:** Tiger's quote endpoint returns "permission denied" for retail TBSG accounts.
**Solution:** Solve IV from market option price via Newton-Raphson, then compute Delta/Theta from BS. Alpaca Greeks are also fetched as a second opinion.

### Spot prices: yfinance primary
**Problem:** Need real-time-ish spot prices. Tiger `get_briefs` often denied for retail.
**Solution:** Multi-source fallback: yfinance (free, low-lag) → Alpaca (IEX, ~15min lag) → Tiger (last resort).

### Stock at BUY price for BP (not market price)
**Decision:** `stock_locked = shares × avg_buy_price` because that's the cash that left the account. Market price affects unrealized P&L (separate metric) but NOT cash deployed. This is a user preference for capital planning, not accounting standard.

### All filters are multiselect
**Decision:** Every filter dropdown across ARGUS uses `st.multiselect` with `placeholder="All ..."`. Empty selection = show all. Filter logic: `if filter_list: df = df[df[col].isin(filter_list)]`.

### Wheel cycle P&L separation
**Decision:** Open cycles show Premium P&L (always realized from options) but Stock P&L = $0. Stock P&L only counts when the cycle closes (stock is called away or sold). This prevents misleading mark-to-market swings on open stock positions.

### Settings persistence via gSheet
**Problem:** Streamlit Cloud's filesystem is ephemeral. JSON settings get wiped on restart.
**Solution:** `save_settings()` writes to BOTH local JSON AND gSheet Settings tab. On restart, `load_settings()` auto-restores from gSheet.

---

## 7. Tiger API Specifics (TBSG Quirks)

- **Account type:** Cash + Margin (TBSG retail), NOT portfolio margin
- **Margin rate:** ~6.5-7.5% tiered, NOT flat 6%
- **FX trades:** NOT in `get_filled_orders()`. Found via `get_fund_details(seg_types=['SEC'])` with `type='Currency Exchange - Base/Quotation Currency'`
- **MMF (Tiger Vault):** Auto-sweep feature. Tiger auto-redeems MMF when margin pressure hits with ZERO notification. ARGUS detects this via `get_segment_fund_history()` (FUND→SEC transfers).
- **MLEG combos:** Tiger records rolls/spreads as single MLEG order. Must expand via `get_order_transactions(order_id)` to get per-leg detail.
- **Rate limits:** 60/min for transactions, 10/min for fund_details (7s sleep between pages + 65s retry on rate limit)
- **Config format:** Java properties file (`tiger_openapi_config.properties`) with `tiger_id`, `private_key`, `account`

---

## 8. Google Sheet Structure

**Sheet:** Income Wheel (ID in `INCOME_WHEEL_SHEET_ID` secret)

| Tab | Purpose | Who writes |
|-----|---------|------------|
| `Orders_Archive` | Canonical trade history (1135+ rows) | ARGUS archive system |
| `Settings` | User config (deposits, allocations, tickers) | ARGUS save_settings() |
| `Orders` | Legacy manual trade log (from pre-Tiger era) | Not used by current app |

---

## 9. Uncommitted Changes (as of 2026-05-16)

Four files modified, not yet committed:

### `app.py` — Multiselect conversion + Historical Ladder
All remaining `st.selectbox` filter dropdowns converted to `st.multiselect`:
- Positions: Type, Strategy, Ticker
- Transactions: Type, Event, Ticker
- Ladder: Type, Ticker, Moneyness
- Risk & Rolls: Type in Roll/Close Candidates
- Filter logic updated from `if x != "All": df = df[df[col] == x]` to `if x: df = df[df[col].isin(x)]`
- Bonus fix: Transaction reset had stale key `txn_side_filter` → `txn_event_filter`
- **NOT converted** (intentionally single-select): Pacing week viewer, P&L month picker, Sort-by control

**Historical Ladder** added below forward-looking ladder:
- Shows closed option trades bucketed by Friday week-end for YTD
- Same chart styles (By Ticker / By Type / Aggregated) using realized P&L
- Color-coded bars (red = loss weeks, green = profit weeks)
- Headline metrics: YTD Realized P&L, Avg per week, Trades closed, Win rate
- Weekly summary table: trades count, CSP/CC/LEAP breakdown, Close Premium, Realized P&L
- Respects same Type/Ticker/Pot filters as forward ladder
- Data source: `load_orders_full()` (archive + live merged)

### `tiger_api/archive.py` — gSheet read caching
- `read_archive_from_gsheet()` now `@st.cache_data(ttl=600)` — reads gSheet at most once per 10 min
- Internal raw reader renamed to `_read_archive_from_gsheet_uncached()`
- Cache busted on `write_archive_to_gsheet()` success

### `tiger_api/tiger_data.py` — MLEG cache cold-start seeding
- New `_seed_mleg_cache_from_archive()`: reconstructs MLEG cache from gSheet archive on cold start
- `_load_mleg_cache()`: tries parquet → falls back to archive seeding → saves parquet locally
- Eliminates ~60-90s of Tiger API calls on every Cloud cold start

---

## 10. Pending / Planned Work

### Capital & BP Refactor (planned, not started)
Full plan exists at `C:\Users\ashtz\.claude\plans\hidden-dancing-avalanche.md`:
1. Remove dead Margin Percentage section from Config
2. Lock in cash-secured policy: STOCK at buy price, CSP = strike × 100, LEAP = premium × 100, CC = $0
3. Add Tiger broker margin estimator as reference panel
4. Delete dead helper functions `calculate_capital_breakdown()` and `calculate_margin_by_position()`
5. Clean up `persistence.py` margin functions

### Performance
- Cold start should be ~30-40s after the MLEG + gSheet caching fixes (down from ~5 min)
- Consider: pre-warming archive read in background on startup

---

## 11. File-by-File Status

### Active (core app)
| File | Lines | Status |
|------|-------|--------|
| `app.py` | ~4300 | Active, main entry point |
| `tiger_api/client.py` | ~460 | Active, Tiger API wrapper |
| `tiger_api/tiger_data.py` | ~1350 | Active, all data loaders |
| `tiger_api/adapters.py` | ~520 | Active, SDK→ARGUS conversion |
| `tiger_api/archive.py` | ~300 | Active, two-tier persistence |
| `tiger_api/greeks.py` | ~200 | Active, BS Greeks |
| `tiger_api/wheel_cycles.py` | ~250 | Active, cycle tracker |
| `tiger_api/win_rate.py` | ~195 | Active, win-rate analytics |
| `tiger_api/rolls.py` | ~175 | Active, roll tracker |
| `tiger_api/iv_scanner.py` | ~95 | Active, IV rank scanner |
| `tiger_api/stress.py` | ~135 | Active, stress tester |
| `mcp_servers/tiger/server.py` | ~260 | Active, Tiger MCP server (stdio + SSE) |
| `mcp_servers/tiger/auth.py` | ~120 | Active, env-var bootstrap + bearer verifier |
| `mcp_servers/tiger/deploy/` | ~290 | Active, Cloud Run deploy artifacts |
| `.mcp.json` | ~6 | Active, declares the stdio tiger MCP server |
| `persistence.py` | ~695 | Active, settings persistence |
| `gsheet_handler.py` | ~500 | Active, gSheet CRUD |
| `config.py` | ~65 | Active, config/secrets |
| `contract_price_lookup.py` | ~150 | Active, Lookup tab |
| `unified_calculations.py` | ~350 | Active, capital calculator |
| `requirements.txt` | 16 | Active |

### Legacy (preserved, not used by current app)
| File | Notes |
|------|-------|
| `excel_handler.py` | Old Excel-based data handler |
| `calculations.py` | Old capital calculations |
| `pnl_calculator.py` | Old P&L calculator |
| `tiger_to_argus.py` | Old Tiger ETL |
| `tiger_etl*.py` | Old ETL scripts |
| `data_access.py`, `data_schema.py` | Old data layer |
| `market_data/` | Old multi-provider market data service |
| `strategy_*.py` | Old strategy UI |
| `income_scanner_ui.py` | Old scanner |
| `sheets_handler.py` | Old sheets handler |
| `web_search.py` | Old web search |

---

## 12. How to Resume Development

### Fresh session checklist:
1. Clone `https://github.com/TongIncomeWheel/Argus_Cloud.git`
2. Ensure `gsheet_credentials.json` is at repo root (service account with Editor access)
3. Create `.env` with `INCOME_WHEEL_SHEET_ID`, `ALPACA_API_KEY`, `ALPACA_SECRET_KEY`
4. Tiger config: `.streamlit/tiger_openapi_config.properties` (or env var `TIGER_CONFIG_PATH`)
5. `pip install -r requirements.txt`
6. `streamlit run app.py`

### Cloud deployment:
- Push to `main` branch → Streamlit Cloud auto-redeploys
- Secrets configured in Streamlit Cloud dashboard (not in repo)
- Tiger secrets format: `[tiger]` section with `properties = """..."""` triple-quote block

### When continuing with Claude:
- Read this HANDOFF.md first
- The CLAUDE.md in the repo root has branch strategy rules
- Check `git status` for uncommitted work
- Check `git log --oneline -5` for recent changes
- The app is ~4300 lines — read specific sections as needed, not the whole file

---

## 13. Known Gotchas

1. **Dollar signs in markdown:** Use `\$` not `$` — Streamlit renders `$...$` as LaTeX math
2. **st.tabs bounces:** Never use `st.tabs` for top-level navigation — use stateful radio
3. **Tiger `get_assets()` only returns SEC segment:** Always use `get_prime_assets()` for NAV
4. **FX trades invisible to `get_filled_orders()`:** Must use `get_fund_details(seg_types=['SEC'])`
5. **Cloud filesystem is ephemeral:** All parquet caches get wiped on sleep. gSheet is canonical.
6. **yfinance `fast_info` inconsistent:** Some versions use `last_price`, others `lastPrice`. Code tries both.
7. **Tiger rate limits are hard:** 60/min for transactions, 10/min for fund_details. Sleep between calls.
8. **MLEG cache order_id as string:** Tiger order IDs are large ints. Always cast to string for dict keys.
9. **`is_opening` in archive is string "True"/"False":** Must convert: `str(v).strip().lower() == "true"`
10. **Multiselect filter pattern:** Empty list = no filter = show all. Logic: `if values: df = df[df[col].isin(values)]`
