# ARGUS v2 Committee Review Report
**Date:** 2026-04-24 | **Reviewers:** APP/UI Expert, Quant Options Expert, Data Engineer, QA Lead

---

## EXECUTIVE SUMMARY

After 1 month of MVP operation, the committee identified **3 Critical bugs, 5 High bugs, 6 Medium bugs**, and significant architectural gaps in cash visibility, risk metrics, and data persistence. The Google Sheet schema has unused columns and missing P&L/margin fields that prevent standalone use as a backup. Two modules (CIO Report, Strategy Instructions) are confirmed safe to remove.

---

## PART 1: IMMEDIATE BUG FIXES (Do First)

### Critical (Money at Risk)

| ID | Bug | Impact | Fix |
|----|-----|--------|-----|
| BUG-01 | **Roll: orphaned closed position on failure** — if `append_trade` fails after `update_trade` succeeds, old position is permanently closed with no replacement | Lost position | Use `atomic_transaction()` for all Roll operations |
| BUG-02 | **BTC partial-close guard bypassed** — widget key `btc_partial_confirm_{fv}` vs lookup `btc_partial_confirm` (no suffix) | Unconfirmed splits | Fix key to include `_{fv}` suffix |
| BUG-03 | **Duplicate TradeIDs** — `generate_trade_id()` uses stale `df_trades` from session cache | Data corruption | Add timestamp suffix to TradeID or re-read sheet before ID generation |
| BUG-16 | **NaT written as literal "NaT" string** — `_serialize_value` checks NaT after strftime, not before | Sheet pollution | Swap NaT check to come first in `_serialize_value` |

### High (Incorrect Data)

| ID | Bug | Impact | Fix |
|----|-----|--------|-----|
| BUG-04 | **Partial Roll uses full quantity** — `_qty_save` from `original['Quantity']` ignores `quantity_to_roll` | Overstated P&L | Use `quantity_to_roll` for all write fields |
| BUG-05 | **CC Exercise: `UnboundLocalError`** — `stock_trade_id` referenced when no stock position exists | Crash mid-operation | Initialize `stock_trade_id = None` before if-block |
| BUG-06 | **Roll: bare `float()` on GSheet cell** — no guard for empty/error values | Crash on bad data | Use `pd.to_numeric(..., errors='coerce')` |
| BUG-10 | **CC Exercise: stock P&L not recorded** — no `Actual_Profit_(USD)` written on stock close | Lost P&L | Calculate `(strike - avg_cost) * shares` and write it |

---

## PART 2: MODULE CLEANUP

### Remove: CIO Report
| File | Action |
|------|--------|
| `app.py:43` | Remove `from daily_report import render_daily_report_panel` |
| `app.py:525` | Remove `"CIO Report"` from sidebar nav |
| `app.py:5058-5059` | Remove route handler |
| `daily_report.py` | Delete entire file |
| `ai_chat.py:1166-1247` | Remove embedded CIO Report generator expander |
| `persistence.py:566+` | Remove `load_daily_report` / `save_daily_report` |

### Remove: Strategy Instructions
| File | Action |
|------|--------|
| `app.py:40` | Remove `from strategy_ui import render_strategy_instructions` |
| `app.py:526` | Remove `"Strategy Instructions"` from sidebar nav |
| `app.py:5072-5073` | Remove route handler |
| `strategy_ui.py` | Delete entire file |
| `strategy_instructions.py` | Delete entire file |
| `ai_chat.py` | Remove `get_strategy_instructions` import and usage |

### Clean Sidebar (10 pages)
Dashboard, Daily Helper, Entry Forms, Expiry Ladder, Performance, All Positions, Margin Config, Income Scanner, Market Data, Contract Lookup

---

## PART 3: CASH & CAPITAL VISIBILITY (New Features)

### Current State
The dashboard shows **one aggregate BP number**. Per-ticker breakdown exists in `capital_data['by_ticker']` but is hidden inside a collapsed expander. No soft thresholds are enforced or displayed.

### What to Build

**A. Dashboard Cash Panel** (always visible, not in expander):

| Ticker | Allocated (Soft Cap) | CSP Reserved | Stock Locked | LEAP Sunk | Total Used | Remaining | % Used |
|--------|---------------------|--------------|-------------|-----------|------------|-----------|--------|
| MARA   | $50,000             | $32,000      | $15,000     | $0        | $47,000    | $3,000    | 94%    |
| SPY    | $80,000             | $0           | $45,000     | $28,000   | $73,000    | $7,000    | 91%    |

**B. Soft Threshold Alerts:**
- Single ticker > 20% of portfolio = amber warning
- Single ticker > 30% = red alert  
- Crypto cluster (MARA+CRCL+ETHA+SOL) combined > 40% = red alert
- Single sector > 30% = amber

**C. Assignment Exposure Panel:**
- "If all ITM puts assigned today: $X stock received at $Y cost"
- Per-ticker assignment risk breakdown

### Data Sources
All data already exists in `capital_data['by_ticker']` from `UnifiedCapitalCalculator`. Soft thresholds are in `persistence.py` (`capital_allocation`). Just needs a display layer.

---

## PART 4: STRATEGY TYPE — Add "ActiveCore"

### Changes Required

| File | Change |
|------|--------|
| `app.py:1966` | Add `"ActiveCore"` to `render_strategy_selector()` dropdown |
| `app.py:555` | Add `"ActiveCore"` to sidebar strategy filter radio |
| `app.py` (5 filter blocks) | Add `elif strategy_filter == 'ActiveCore'` branch |
| `ai_chat.py:287` | Update AI prompt: `StrategyType: WHEEL, PMCC, or ActiveCore` |
| All filter blocks | Fix existing `'Wheel'` vs `'WHEEL'` casing inconsistency |
| Income Scanner | Auto-tag Active bucket results as `StrategyType = "ActiveCore"` |

---

## PART 5: GOOGLE SHEET SCHEMA OVERHAUL

### Unused Columns (Safe to Remove)
- `DTE` — defined in schema, never written or read
- `Stock PNL` — defined in schema, never used
- `Cash_required_per_position_(USD)` — written on CSP open, never read
- `Opt_Premium_%` — written on open, never read in calculations

### Missing Columns to Add

| Column | Purpose | When Written |
|--------|---------|-------------|
| `Cost_Basis` | Stock average cost per share | On stock creation / assignment |
| `Unrealized_PnL` | Mark-to-market P&L | Optionally on each session (or keep computed) |
| `BTC_Price` | Close price on BTC (currently buried in Remarks) | On BTC close |
| `Net_Credit` | Net credit on Roll (currently in Remarks) | On Roll |
| `Ticker` on Audit_Table | Direct reference (currently requires join) | On every audit write |
| `TradeType` on Audit_Table | Direct reference | On every audit write |
| `PnL` on Audit_Table | Numeric P&L (currently in Remarks string) | On BTC/Roll/Exercise |

### Audit Table Typos to Fix
- `TImeStamp` -> `Timestamp`
- `ScriptNAme` -> `ScriptName`

### Settings Tab (Add to Each Sheet)
Persist to Google Sheet so the sheet is self-sufficient:
- Portfolio deposit (USD/SGD)
- FX rate
- Stock average prices (CRITICAL — currently only in local JSON, lost if machine dies)
- Capital allocation targets per ticker
- PMCC ticker flags

---

## PART 6: RISK METRICS (Quant Recommendations)

### Missing Portfolio-Level Metrics

| Metric | Priority | Description |
|--------|----------|-------------|
| **Portfolio Delta** | P1 | Sum of all position deltas — primary risk number |
| **Portfolio Theta** | P1 | Daily premium decay — "my portfolio earns $X/day" |
| **Assignment Exposure** | P1 | Total $ if all ITM puts assigned today |
| **Portfolio Vega** | P2 | IV sensitivity — critical for MARA/CRCL/ETHA/SOL |
| **Crypto Correlation Group** | P2 | Combined exposure of BTC-correlated names |
| **MAE (Max Adverse Excursion)** | P3 | Worst-case unrealized loss per position |
| **Expiry Concentration** | P3 | Alert when >X% of positions expire same week |

### P&L Calculation Fixes

| Issue | Current | Correct |
|-------|---------|---------|
| LEAP valuation | Intrinsic only (`max(0, price-strike)`) | Use live option mid-price for mark-to-market |
| Premium stats fallback | BTC at breakeven ($0 profit) falls back to full premium | Check for explicit $0 profit vs missing data |
| Weekly pacing | Hardcoded `capital * 0.25 * 0.02` (magical 2%) | Use actual premium collected this week |
| Coverage ratio | Compares CC contracts to LEAP lots (off by 100x if Open_lots used) | Always compare contracts to contracts |

### Scanner Threshold Adjustments

| Parameter | Current | Recommended |
|-----------|---------|-------------|
| Core \|Delta\| | 0.35 (PoP ~65%) | 0.25 (PoP ~75%) — safer for core positions |
| Open Interest min | 100 | 250-500 for fill assurance |
| Volume min | 50 | 100+ |
| Sector cap | 40% | 30% |
| T1 position size | 22% | 15% max per single CSP |
| IV Spike → T1 | Allowed | Force to T2 (elevated event risk) |

---

## PART 7: DATA ENGINEERING FIXES

### Performance
- Cache worksheet headers in `GSheetHandler.__init__` — eliminates 2+ redundant `row_values(1)` API calls per operation
- A single BTC triggers ~5 API calls; with 60 req/min quota, batch operations risk rate limiting

### Reliability
- Add retry with exponential backoff for `APIError 429`
- Trigger backup before `append_trade()` (currently only before update/delete)
- Back up Audit_Table (currently only Data Table is backed up)

### Type Safety
- Apply numeric coercion only to known numeric columns (not heuristic 50% survival)
- The `Trade` dataclass in `models.py` is never instantiated — either use it for validation or remove it
- Add `.copy()` in `UnifiedPacingCalculator.calculate_pacing()` to prevent DataFrame mutation

---

## PART 8: PRIORITIZED ACTION PLAN

### Phase 1: Stabilize (Bug Fixes) — ~2 days
1. Fix BUG-01 through BUG-06, BUG-10, BUG-16
2. Fix NaT serialization
3. Fix StrategyType casing inconsistency

### Phase 2: Clean Up — ~1 day
1. Remove CIO Report module
2. Remove Strategy Instructions module
3. Clean sidebar to 10 pages
4. Fix audit column typos (`TImeStamp`, `ScriptNAme`)

### Phase 3: Cash & Capital Visibility — ~2 days
1. Build per-ticker cash panel on Dashboard
2. Add soft threshold alerts
3. Add assignment exposure panel
4. Add "ActiveCore" strategy type

### Phase 4: Sheet Schema — ~1 day
1. Add missing columns (Cost_Basis, BTC_Price, Net_Credit)
2. Add Ticker/TradeType/PnL to Audit_Table
3. Add Settings tab to Google Sheet
4. Migrate `stock_average_prices` from local JSON to sheet
5. Remove unused columns

### Phase 5: Risk & Quant — ~3 days
1. Add portfolio Delta/Theta/Vega to dashboard
2. Fix LEAP valuation (use live mid-price)
3. Add assignment exposure calculation
4. Fix weekly pacing formula
5. Add crypto correlation group tracking

### Phase 6: Engineering Hardening — ~1 day
1. Cache GSheet headers
2. Add API retry logic
3. Whitelist numeric coercion columns
4. Route all column access through `DataAccess`

---

## PART 9: UX REVIEW — DASHBOARD & LAYOUT

### Sidebar
- **AI Assistant dominates 60% of sidebar** — nav is hidden in a collapsible expander. Should be flipped: nav always visible, AI chat collapsible.
- 12 pages is too many — removing CIO Report + Strategy Instructions brings it to 10.

### Capital Cockpit (Summary Row)
- **7 columns too cramped** — metric subtitles are tiny grey text, unreadable at lower resolutions.
- **"Total Capital Used" is redundant** — it's Capital Held + CSP Reserved, both already shown in the row below.
- **Negative BP (-$55k) buried in last column** — the most alarming number gets least visual emphasis. Should be a full-width red alert banner.
- **Two rows show same data** — Summary row and "Stock capital & LEAP/PMCC" section repeat the same numbers with different labels.
- **Recommendation:** Merge into 5 metrics: Deposit | Capital Held | CSP Reserved | Liquid Cash | BP. Remove duplicate section. Add red alert banner for negative BP.

### Profit & Loss
- Nett P/L card (red bordered box) is good visual treatment.
- Labels too verbose — "COIN — Premium collected from closed CC/CSP" repeated 4x wastes space. Just show ticker name with row header.
- MARA Stock P/L (-$74,571) dominates portfolio but gets same visual weight as smaller numbers. Largest loss/gain should be emphasized.

### Detail Breakdowns
- Two collapsed expanders always hidden — if the data matters, show it. If not, remove it.
- Replace with the per-ticker cash panel (Part 3) — always visible.

### Premium Collected
- 4-column weekly/MTD/YTD layout is well-designed — keep.
- Sign convention unclear — "Collected This Week: (-$1,430)" — is this a loss or premium received?
- CRCL YTD (-$10,244) has no alert or threshold indicator.

### Position Inventory
- Clean table with emoji coverage indicators — good.
- Missing $ amounts per ticker (only shows contract counts).
- COIN has 0 stock, 3 CSP (naked puts) — no alert for uncovered exposure.

### Overall Ratings
| Aspect | Score | Notes |
|--------|-------|-------|
| Readability | 5/10 | Too many small metrics, verbose labels |
| Actionability | 4/10 | No alerts, no thresholds, BP buried |
| Info hierarchy | 3/10 | Most important number gets least emphasis |
| Layout efficiency | 4/10 | Duplicate sections, collapsed useful info |
| Premium tracking | 7/10 | Weekly/MTD/YTD table well done |
| Risk visibility | 3/10 | No assignment exposure, no portfolio Greeks |

---

## PART 10: PACING UX REVIEW — "Today's Selling Target"

### Current State (Daily Helper — bottom section)

Two side-by-side tables:
- **CSP Selling Target:** Ticker | Allocated | Used | Remaining | Price | Weekly | Daily | Sold
- **CC Selling Target:** Ticker | Stock (shares) | Weekly | Daily | Sold | Remaining

### CSP Pacing — Critical Problems

1. **"Used" is always $0** — the capital-used calculation at `app.py:1584-1591` is commented out. "Remaining" shows raw allocated capital / price with no awareness of existing CSP positions.
2. **"Remaining" = contracts you COULD sell, not SHOULD sell** — MARA shows 34.1 remaining but already has 32 open CSPs reserving $104k+. Not reflected.
3. **Daily target ignores buying power** — MARA Daily = 11.6 contracts (~$130k/day). Meanwhile Dashboard shows BP = (-$55k). The table tells you to deploy capital you don't have.
4. **COIN: $0 allocated but 1 sold** — no warning, no threshold breach indicator.
5. **No link between pacing and BP** — Dashboard and pacing tables completely contradict each other.

### CC Pacing — Critical Problems

1. **Simplistic formula:** `shares / 4 / 100 / 5` — ignores how many CCs are already open, what coverage % is, what DTE mix is needed.
2. **"Remaining" = 0.0 for ALL tickers** — carries no useful information. Can't distinguish "fully covered" from "nothing to cover."
3. **Redundant with CC Coverage Planner** — the 5-week planner above with Full/Partial/Empty status is a far better CC pacing tool. This table adds confusion.

### What Pacing Should Answer

The daily question: **"What should I sell today, and how much?"**

**CSP pacing logic:**
```
Available CSP Capital = max(0, BP) - buffer_reserve
Per-ticker budget     = min(Available * ticker_alloc%, soft_cap - already_deployed)
Today's contracts     = floor(budget / (current_price * 100))
```

**CC pacing logic:**
```
Uncovered shares     = total_shares - (open_CC_contracts * 100)
Target this week     = weekly_target - sold_this_week
Today's CC contracts = ceil(target / remaining_trading_days)
```

**Hard stops:**
- BP negative → "0 to sell" for all CSPs
- Per-ticker allocation exceeded → amber warning
- Crypto cluster >40% → red alert
- Earnings inside DTE → flag ticker

### Recommended Design — "Daily Action Plan"

Replace both tables with **one unified action card per ticker:**

```
┌─────────────────────────────────────────────────────┐
│  MARA                              BP Available: $0 │
│  Stock: 15,900 shares | $11.85                      │
│                                                     │
│  CC:  88% covered | 0 to sell this week   ✅ Done   │
│  CSP: $47k/$275k deployed (17%)                     │
│       Sell today: 0 contracts             🔴 No BP  │
│                                                     │
│  ⚠️ Crypto cluster at 62% (cap: 40%)               │
└─────────────────────────────────────────────────────┘
```

**Principles:**
1. One card per ticker — all decision info in one place
2. Action-oriented — "Sell today: X" or "Done" or "No BP"
3. Hard stops prominent — red for negative BP, amber for threshold breach
4. References CC Coverage Planner data — no redundant computation
5. Respects buying power — if BP negative, every ticker shows 0

### Pacing Gap Summary

| Problem | Current | Should Be |
|---------|---------|-----------|
| CSP "Used" always $0 | Calc commented out | Sum open CSP reserved per ticker |
| Daily target ignores BP | Suggests selling with -$55k BP | Hard stop: "No BP available" |
| CC table redundant | Same info as CC Coverage Planner | Remove or merge |
| No cross-reference | Dashboard BP and pacing don't talk | Unified view with constraints |
| No earnings check | Pacing ignores upcoming ER | Flag tickers with ER inside DTE |
| No concentration alerts | Silent on crypto cluster | Show cluster % with threshold |
| Simplistic daily calc | shares/4/100/5 | remaining_target / remaining_trading_days |

---

## PART 11: IMPLEMENTATION PLAN — ARGUS v2

### Architecture Principles
1. **Dashboard = health check** — am I safe? any alerts?
2. **Daily Helper = action plan** — what to sell today, how many, constraints
3. **Entry Forms = execution** — do the trade, record it
4. **Everything else = analysis** — performance, history, scanning

### Phase 1: Stabilize (Bug Fixes) — ~2 days

**1.1 Critical bugs (BUG-01, 02, 03, 16)**
- [ ] Roll: wrap all 3 operations in `atomic_transaction()`
- [ ] BTC: fix widget key mismatch (`btc_partial_confirm` → `btc_partial_confirm_{fv}`)
- [ ] TradeID: add timestamp suffix to prevent duplicates (`T-562-1714000000`)
- [ ] NaT serialization: swap check order in `_serialize_value`

**1.2 High bugs (BUG-04, 05, 06, 10)**
- [ ] Partial Roll: use `quantity_to_roll` not `original['Quantity']`
- [ ] CC Exercise: init `stock_trade_id = None` before if-block
- [ ] Roll float: guard with `pd.to_numeric(..., errors='coerce')`
- [ ] CC Exercise stock P&L: write `Actual_Profit_(USD)` = `(strike - avg_cost) * shares`

**1.3 Data fixes**
- [ ] Fix StrategyType casing: standardize all writes to `"WHEEL"` (not `"Wheel"`)
- [ ] Fix `_serialize_value` NaT → empty string ordering

### Phase 2: Clean Up — ~1 day

**2.1 Remove modules**
- [ ] Delete `daily_report.py`, `strategy_ui.py`, `strategy_instructions.py`
- [ ] Remove imports, nav entries, route handlers from `app.py`
- [ ] Remove CIO Report expander from `ai_chat.py`
- [ ] Remove `load_daily_report`/`save_daily_report` from `persistence.py`

**2.2 Sidebar restructure**
- [ ] Nav radio buttons always visible at top (10 items)
- [ ] AI Assistant in collapsible expander below nav
- [ ] Remove icon duplicates (Dashboard/Performance both use 📊)

**2.3 Audit column fixes**
- [ ] `TImeStamp` → `Timestamp` in all write payloads
- [ ] `ScriptNAme` → `ScriptName` in all write payloads
- [ ] Add `Ticker`, `TradeType`, `PnL` columns to audit writes

### Phase 3: Dashboard Redesign — ~2 days

**3.1 Alert banner (NEW)**
- [ ] Full-width red banner if BP negative: "WARNING: Buying Power is negative (-$55,308)"
- [ ] Amber banner if any ticker exceeds soft cap
- [ ] Amber banner if crypto cluster >40%

**3.2 Capital summary (SIMPLIFY)**
- [ ] Merge 7+3 metrics into 5: Deposit | Capital Held | CSP Reserved | Liquid Cash | BP
- [ ] Remove duplicate "Stock capital & LEAP/PMCC" section

**3.3 Per-ticker cash panel (NEW, replace collapsed expanders)**
- [ ] Always-visible table: Ticker | Soft Cap | CSP Reserved | Stock | LEAP | Total | Remaining | % Used
- [ ] Color-code rows: green <60%, amber 60-85%, red >85%
- [ ] Data source: `capital_data['by_ticker']` + `capital_allocation` from persistence

**3.4 Premium collected (KEEP, minor fixes)**
- [ ] Clarify sign convention (add "(BTC losses)" label for negative values)
- [ ] Add threshold alert for YTD losses >$5k per ticker

**3.5 Position inventory (ENHANCE)**
- [ ] Add "CSP Reserved $" column alongside contract counts
- [ ] Add "Uncovered" alert for tickers with CSPs but no stock/LEAP

### Phase 4: Pacing Redesign — "Daily Action Plan" — ~2 days

**4.1 Fix CSP pacing engine**
- [ ] Uncomment and fix capital-used calculation (`app.py:1584-1591`)
- [ ] Used = sum of `strike * 100 * qty` for all open CSPs per ticker
- [ ] Remaining = min(allocated - used, max(0, BP)) / (price * 100)
- [ ] Hard stop: if BP ≤ 0, all CSP targets = 0

**4.2 Build unified action cards**
- [ ] One card per ticker replacing both CSP and CC tables
- [ ] CC line: pull from CC Coverage Planner data (coverage %, remaining to sell)
- [ ] CSP line: show deployed vs allocated, contracts to sell today
- [ ] Badge system: ✅ Done | 🟡 Partial | 🔴 No BP | ⚠️ Threshold

**4.3 Add constraint checks**
- [ ] Per-ticker soft cap enforcement (from `capital_allocation`)
- [ ] Crypto cluster aggregation (MARA+CRCL+ETHA+SOL combined %)
- [ ] Earnings flag: if any open ticker has ER within 14 days, show ⚠️
- [ ] BP cross-reference: link to Dashboard BP figure

**4.4 Remove redundant CC Selling Target table**
- [ ] CC pacing fully handled by CC Coverage Planner + action cards

### Phase 5: Sheet Schema & Data Persistence — ~1 day

**5.1 New columns on Data Table**
- [ ] `Cost_Basis` — stock average cost per share
- [ ] `BTC_Price` — close price on BTC (extract from Remarks)
- [ ] `Net_Credit` — net credit on Roll (extract from Remarks)

**5.2 Enhanced Audit_Table**
- [ ] Add `Ticker`, `TradeType` columns to all audit writes
- [ ] Add numeric `PnL` column (no more freetext in Remarks)

**5.3 Settings tab on Google Sheet**
- [ ] Create "Settings" worksheet in each spreadsheet
- [ ] Auto-sync: portfolio deposit, FX rate, stock avg prices, capital allocations
- [ ] Load on startup, save on change
- [ ] This replaces the local-only `user_settings.json` as primary store

**5.4 Cleanup**
- [ ] Remove unused columns: `DTE`, `Stock PNL`, `Cash_required_per_position_(USD)`, `Opt_Premium_%`
- [ ] Backfill `Cost_Basis` from current `user_settings.json` values

### Phase 6: Strategy Type — "ActiveCore" — ~0.5 day

- [ ] Add `"ActiveCore"` to `render_strategy_selector()` dropdown
- [ ] Add to sidebar strategy filter radio
- [ ] Add `elif` branches in all 5 filter blocks
- [ ] Update AI prompt in `ai_chat.py`
- [ ] Income Scanner: auto-tag Active bucket results as ActiveCore

### Phase 7: Risk & Quant Enhancements — ~3 days

**7.1 Portfolio-level Greeks (Dashboard)**
- [ ] Sum all position deltas → "Portfolio Delta: -X"
- [ ] Sum all position thetas → "Daily Theta Income: $X"
- [ ] Sum all position vegas → "Portfolio Vega: X"
- [ ] Data source: existing `market_data` service (Alpaca Greeks)

**7.2 Assignment exposure (Dashboard)**
- [ ] Calculate: for each ITM put, `(strike - current_price) * 100 * contracts`
- [ ] Sum total: "If all ITM puts assigned: $X stock received at $Y cost"
- [ ] Show per-ticker breakdown

**7.3 Fix LEAP valuation**
- [ ] Replace intrinsic-only with live option mid-price
- [ ] Route through existing `market_data.service.get_open_positions_data()`
- [ ] Remove `spy_leap_pl` manual override hack

**7.4 Fix weekly pacing formula**
- [ ] Remove hardcoded `capital * 0.25 * 0.02` (magical 2%)
- [ ] Use actual premiums collected this week vs dollar target

**7.5 Scanner threshold adjustments**
- [ ] Core delta: 0.35 → 0.25
- [ ] OI minimum: 100 → 250
- [ ] IV Spike tickers: force to T2
- [ ] T1 position size: 22% → 15%

### Phase 8: Engineering Hardening — ~1 day

- [ ] Cache GSheet worksheet headers in `GSheetHandler.__init__`
- [ ] Add retry with exponential backoff for API 429 errors
- [ ] Whitelist numeric coercion to known columns only
- [ ] Add `.copy()` in `UnifiedPacingCalculator.calculate_pacing()`
- [ ] Back up Audit_Table alongside Data Table
- [ ] Trigger backup before `append_trade()` (not just update/delete)

---

### Phase Summary

| Phase | Scope | Duration | Dependency |
|-------|-------|----------|------------|
| 1. Stabilize | 8 bug fixes + data fixes | ~2 days | None |
| 2. Clean Up | Remove 3 modules, restructure sidebar, fix audit | ~1 day | None |
| 3. Dashboard Redesign | Alert banner, capital summary, per-ticker panel | ~2 days | Phase 2 |
| 4. Pacing Redesign | Fix CSP engine, unified action cards, constraints | ~2 days | Phase 3 |
| 5. Sheet Schema | New columns, Settings tab, data persistence | ~1 day | Phase 1 |
| 6. ActiveCore | New strategy type across app | ~0.5 day | Phase 2 |
| 7. Risk & Quant | Greeks, assignment exposure, LEAP fix, scanner | ~3 days | Phase 3 |
| 8. Engineering | Caching, retry, type safety | ~1 day | Phase 1 |

**Total: ~12.5 days** | Phases 1+2 can run in parallel. Phases 3+4 are sequential. Phases 5-8 can interleave.

---

*Report generated by ARGUS Review Committee — APP/UI Expert, Quant Options Expert, Data Engineer, QA Lead, UX Reviewer*
