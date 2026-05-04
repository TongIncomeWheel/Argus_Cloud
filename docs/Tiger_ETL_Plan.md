# Tiger → ARGUS ETL Plan (Final)

## Primary Use Case — Travel-Friendly Reconciliation

**The pattern this is designed for:**

User is travelling. Manual trade entry sometimes happens, sometimes doesn't. End of trip / end of month, user downloads Tiger statement (any date range) and uploads to ARGUS. ETL handles **everything** in one operation:

- New trades user missed → created automatically
- Existing manual entries → enriched with Tiger's exact numbers
- Rolls user forgot to mark → auto-detected from same-day BTC + new OpenShort pairs
- Fees, margin interest, dividends → captured cumulatively
- Orphan rows (manual entries that don't match Tiger) → flagged for review
- Idempotent: re-uploading same file is safe (no dups)

**What success looks like:** After each Tiger upload, the dashboard reflects the broker's truth. User trusts the numbers without doing manual reconciliation work.

---


## Locked Decisions

1. **Tiger is fully golden** — historic and ongoing
2. **Start fresh from Tiger** — Data Table rebuilt entirely from Tiger CSVs
3. **One-way ETL** — Tiger → ARGUS, no reconciliation engine needed
4. **Existing schema preserved** — app needs no refactor; gSheet column structure stays
5. **Per-statement upload tracking** — file hash + date range ledger to prevent duplicate imports
6. **Cumulative import** — fees, margin interest, dividends accumulate across uploads

## What Gets Lost (Acknowledged)

- Custom `Remarks` you've manually entered (replaced by Tiger-derived comments + roll inference)
- Existing `StrategyType` tags on individual trades (will be inferred + bulk re-tagged)
- Manual `Pot` tags (re-derived from StrategyType + Margin Config PMCC list)
- The 40 orphaned "Rolled to" rows (correctly resolved via Tiger pair detection)

## What Gets Preserved

- Margin Config settings (deposits, allocations, PMCC ticker list, FX rate)
- Stock average prices (Settings tab)
- All Audit_Table history (kept as-is, archival)

---

## Migration (One-Time)

### Step 1 — Backup
```
Data Table              → "Data Table (Pre-Tiger 2026-04-29)"  [archival]
Audit_Table             → "Audit_Table (Pre-Tiger 2026-04-29)" [archival]
```
JSON snapshots also written to `data/backups/` per existing handler logic.

### Step 2 — Wipe & Rebuild Data Table
1. Parse all Tiger CSVs in chronological order
2. Run ETL transform → ARGUS-format rows
3. Apply default metadata (rules below)
4. Write rebuilt Data Table

### Step 3 — Verify
Sanity checks before declaring complete:
- Row count matches Tiger fill count (post-dedup)
- Open positions count matches Tiger Holdings section
- Closed P&L total matches Tiger's Realized P&L Cash Report total
- Cumulative fees match Tiger's "Commissions" line
- Open positions tickers match Tiger Holdings tickers

---

## ETL Transform Rules

### A — Map Tiger Activity → ARGUS TradeType

| Tiger Activity Type | Tiger Section | Tiger Right | Tiger Qty | → ARGUS TradeType | → ARGUS Direction |
|---|---|---|---|---|---|
| OpenShort | Trades | PUT | negative | CSP | Sell |
| OpenShort | Trades | CALL | negative | CC | Sell |
| OpenLong | Trades | CALL (DTE>180) | positive | LEAP | Buy |
| OpenLong | Trades | CALL (DTE<180) | positive | CC | Buy (rare) |
| Close | Trades | PUT or CALL | positive | (closes existing CC/CSP) | — |
| Close | Trades | PUT or CALL | negative | (closes existing LEAP) | — |
| Buy | Trades | — | positive | STOCK | Buy |
| Sell | Trades | — | negative | STOCK | Sell (closes existing) |
| Option Expired Worthless | Exercise & Exp. | PUT | — | (closes CSP, full premium kept) | — |
| Option Expired Worthless | Exercise & Exp. | CALL | — | (closes CC, full premium kept) | — |
| Option Exercise | Exercise & Exp. | PUT | — | (closes CSP + creates STOCK at strike) | 2 rows |
| Option Exercise | Exercise & Exp. | CALL | — | (closes CC + closes existing STOCK at strike) | 2 rows |

### B — TradeID Generation

Sequential `T-1`, `T-2`, `T-3`... in chronological order of Tiger trade date.

### C — StrategyType Inference

```python
def infer_strategy_type(ticker, trade_type, expiry, pmcc_tickers):
    # PMCC: ticker is on the PMCC list AND TradeType is CC, LEAP, or STOCK underlying
    if ticker in pmcc_tickers and trade_type in ('CC', 'LEAP', 'STOCK'):
        return 'PMCC'
    # ActiveCore: harder to infer from Tiger alone — default to WHEEL,
    # user can re-tag specific trades via app forms after migration
    return 'WHEEL'
```

PMCC ticker list comes from Margin Config (`pmcc_tickers` setting) — currently `["SPY"]`.

### D — Pot Derivation

```python
def derive_pot(strategy_type):
    return 'Active' if strategy_type == 'ActiveCore' else 'Base'
```

### E — Roll Detection

Iterate through closed trades chronologically. For each `Close` (BTC) on day D for ticker T:
1. Look for an `OpenShort` on the same day D for ticker T with **different strike or expiry**
2. If found — pair them as a roll:
   - Old position `Remarks = "Rolled to T-NEW"`
   - New position `Remarks = "Rolled from T-OLD"`
3. If no pair found — leave as standalone BTC

### F — Field Mapping

| ARGUS Column | Tiger Source |
|---|---|
| `TradeID` | Generated (T-N) |
| `Ticker` | Parsed from option symbol or stock name |
| `StrategyType` | Inferred (Rule C) |
| `Pot` | Derived (Rule D) |
| `Direction` | Mapped (Rule A) |
| `TradeType` | Mapped (Rule A) |
| `Quantity` | `abs(Tiger Quantity)` for options; signed for stock |
| `Open_lots` | `Quantity × 100` for STOCK |
| `Option_Strike_Price_(USD)` | Parsed from option symbol |
| `OptPremium` | Tiger `Trade Price` for opens |
| `Date_open` | Tiger `Trade Time` (date portion) for opens |
| `Expiry_Date` | Parsed from option symbol |
| `Date_closed` | Tiger `Trade Time` for closes/expires/exercises |
| `Close_Price` | Tiger `Trade Price` for closes; `0.00` for expirations; strike for exercises |
| `Status` | `Closed` if subsequent close event found; else `Open` |
| `Actual_Profit_(USD)` | Tiger `Realized P/L` (already net of fees) |
| `Fee` | Sum of all Tiger fee columns (positive number) |
| `Tiger_Trade_ID` | Tiger source row hash for traceability |
| `Remarks` | Roll markers + transaction notes |

### G — New Columns to Add to Data Table

- `Fee` — total fees paid on this trade (USD)
- `Pot` — Base or Active (currently derived from StrategyType, but explicit storage avoids re-computation)
- `Tiger_Trade_ID` — foreign key to Tiger Statement tab

---

## New gSheet Tabs

### `Tiger Statement` (append-only, raw)

Every Tiger fill, schema-locked. Imported on every upload. Never modified.

```
Source_File | Trade_Date | Settle_Date | Symbol_Raw | Ticker | Asset_Class |
Expiry | Right | Strike | Activity_Type | Quantity | Trade_Price | Amount |
Fee_Total | Realized_PL | Notes | Currency | Market | Tiger_Trade_ID
```

### `Tiger Cash` (append-only)

Non-trade events. Critical for cumulative fees/dividends/margin interest tracking.

```
Source_File | Date | Type | Description | Amount | Currency
```

Where `Type` ∈ {Deposit, Withdrawal, Dividend, Interest, Fee, Allowance, FX_Conversion, Securities_Lending}

### `Tiger Imports` (append-only ledger)

Per-statement tracking. Prevents duplicate imports.

```
Filename | File_Hash | Date_Range_From | Date_Range_To | Imported_At |
Trades_Imported | Cash_Events_Imported | Status
```

Status ∈ {Success, Partial, Failed}

### `Tiger Holdings Snapshot` (latest snapshot only)

Tiger's view of current open positions at end of latest statement period. Used for sanity check — ARGUS open positions should match.

```
As_Of_Date | Symbol | Ticker | Asset_Class | Strike | Expiry | Right |
Quantity | Cost_Price | Close_Price | Unrealized_PL | Currency
```

---

## Cumulative Tracking on Dashboard (Simplified)

User wants ONE bucket — not per-category breakdowns.

### Tiger Cash Tab (still kept as durable record)

Records every non-trade cash event but the app doesn't display them individually:

```
Date | Type | Description | Amount | Currency
```

Type is one of: Dividend, Interest, Fee, Allowance, Securities_Lending, Deposit, Withdrawal.

### Single Dashboard Metric

```
Tiger Cash Adjustment = SUM(Tiger Cash.Amount) for all event types EXCEPT Deposit/Withdrawal
```

(Deposits/withdrawals affect deposit base, not P&L; everything else nets against P&L.)

### Account Value Display

```
Realized P&L (from closed trades)  : $39,363
Tiger Cash Adjustment              : -$13,567   ← single net line
Unrealized P&L (Stock + LEAP MTM)  : -$45,000
─────────────────────────────────────────
Total P&L                          : -$19,204
+ Deposit                          : $367,188
═════════════════════════════════════════
NAV                                : $347,984
```

Caption under "Tiger Cash Adjustment": *"Net of fees, margin interest, dividends, and allowances. Hover for breakdown."*

Hover tooltip (optional, low priority): one-line breakdown if user wants detail.

### Per-Trade Fee (still kept)

The `Fee` column on Data Table still gets populated by ETL. This makes per-trade `Realized P&L` accurate. It's NOT separately displayed — Tiger's `Realized P/L` field is already net of fees, so when ETL writes `Actual_Profit_(USD) = Tiger.Realized_PL`, fees are baked in.

The standalone `Fee` column is for traceability only (user can see what Tiger charged on any individual trade if they care).

---

## File Plan

### New files

| File | Purpose | LoC estimate |
|---|---|---|
| `tiger_parser.py` | Parse multi-section Tiger CSV → structured Python objects | ~250 |
| `tiger_to_argus.py` | Transform `TigerTrade` objects → ARGUS rows (Rules A-F) | ~200 |
| `tiger_etl.py` | Orchestrator — read CSV, dedup, transform, detect rolls, write to gSheet | ~150 |
| `render_tiger_import.py` | Streamlit page: upload + run + report | ~200 |

### Modified files

| File | Change |
|---|---|
| `gsheet_handler.py` | Add `archive_table(name, suffix)` for backups; helpers for new tabs |
| `app.py` | Add "Tiger Import" page to sidebar nav |
| `data_schema.py` | Add `Fee`, `Pot`, `Tiger_Trade_ID` to schema |

---

## Build Order

### Phase 1 — Parser (Standalone, ~1 day)
- `tiger_parser.py` produces JSON dump from CSV
- CLI tool: `python tiger_parser.py path/to/file.csv > parsed.json`
- Unit test against your 2 sample files
- **Checkpoint:** You inspect parsed.json — verify all 1,762 trades, 209 exercises, 41 holdings captured correctly

### Phase 2 — Transform (Standalone, ~1 day)
- `tiger_to_argus.py` converts parsed Tiger → ARGUS row dicts
- CLI: `python tiger_to_argus.py parsed.json > argus_rows.json`
- Roll detection logic + StrategyType inference
- Unit tests for each event type
- **Checkpoint:** You inspect argus_rows.json — spot-check 10 trades vs Tiger statement, including a roll, an expiration, an assignment

### Phase 3 — gSheet Migration (~1 day)
- Add new tabs (`Tiger Statement`, `Tiger Cash`, `Tiger Imports`, `Tiger Holdings Snapshot`)
- Add Fee/Pot/Tiger_Trade_ID columns to Data Table
- Backup existing Data Table to archival tab
- Run one-time migration: process both your Tiger CSVs, write all data
- **Checkpoint:** Open the gSheet, verify new tabs populated, Data Table rebuilt with row counts matching Tiger

### Phase 4 — Sanity Verification (~0.5 day)
- ARGUS app loads, shows correct portfolio
- Dashboard NAV matches Tiger Account Overview
- Realized P&L matches Tiger Cash Report
- Open positions match Tiger Holdings
- Margin interest + fees show on Dashboard
- **Checkpoint:** Numbers tie out within $50 of Tiger statement

### Phase 5 — Streamlit Upload Page (~1 day)
- New "Tiger Import" page in sidebar
- Drag-and-drop CSV upload
- Idempotent: refuses duplicate file hashes; warns on date-range overlap
- Run ETL on upload, show summary + diff vs current state
- **Checkpoint:** End-to-end test with a third Tiger statement

### Phase 6 — Cumulative Metrics on Dashboard (~0.5 day)
- Surface Total Fees, Margin Interest, Dividends in Account Value section
- Net Realized Income card
- **Checkpoint:** Dashboard reads correctly from Tiger Cash tab

**Total: ~5 working days**

---

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| ETL bug corrupts Data Table | Backup to `(Pre-Tiger ...)` tab before every migration; can restore in seconds |
| Tiger CSV format changes in future | Parser uses header-name lookup, not column index; tolerant of column reordering |
| Roll detection mis-pairs same-day trades | Conservative: require different strike OR expiry on same ticker same day; ambiguous cases left as standalone (user can manually link via app) |
| StrategyType inference wrong for migrated trades | Default to WHEEL is safe; user re-tags ActiveCore trades via Performance/All Positions edit (existing functionality) |
| Audit_Table history "lost" because Data Table rebuilt | Audit_Table preserved unchanged; old TradeID references still resolvable via archival Data Table |
| Manual trade entered today, Tiger upload tomorrow overwrites | Acceptable per user decision — Tiger is golden, manual entries are temporary placeholders |

---

## Open Items Before Starting

None. All decisions locked. Ready to begin Phase 1 (parser).

The next step is for me to write `tiger_parser.py` against your CSVs, produce a JSON dump, and have you eyeball the parsed output before we do anything destructive.
