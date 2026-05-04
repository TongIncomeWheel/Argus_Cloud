# Tiger Brokers Reconciliation & ETL Plan

---

## CRITICAL — ARGUS Event Model (verified against live data)

**ARGUS already captures every Tiger event** — but in different rows/columns than Tiger's section structure. Reconciliation must understand the mapping below or it will be wrong.

### How ARGUS represents each event (confirmed by querying the live gSheet)

| Event | ARGUS rows affected | Identifying signature |
|---|---|---|
| **Expiration (worthless)** | 1 existing CC/CSP row marked Closed | `Status=Closed`, `Close_Price=0.00`, `Actual_Profit_(USD)=full premium`, `Remarks="CC Expired Worthless"` or `"CSP Expired Worthless"` |
| **BTC (Buy to Close)** | 1 existing CC/CSP row marked Closed | `Status=Closed`, `Close_Price=BTC fill price`, `Actual_Profit=(orig_prem − BTC) × 100 × qty`, `Remarks="BTC"` or contains BTC info |
| **Roll** | 2 rows: 1 closed + 1 new opened | OLD: `Status=Closed`, `Remarks="Rolled to T-NEW"`. NEW: New TradeID, `Status=Open`, `Remarks="Rolled from T-OLD"` |
| **CSP Assignment** | 2 rows: CSP closed + STOCK created | OLD CSP: `Status=Closed`, `Close_Price=0`, full premium kept, `Remarks="CSP Assigned"`. NEW STOCK: New TradeID, `TradeType=STOCK`, `Direction=Buy`, `Quantity=qty×100`, `Open_lots=qty×100`, `Price=strike`, `Remarks="Exercised CSP from T-OLD"` |
| **CC Called Away** | 2 rows: CC closed + existing STOCK closed | OLD CC: `Status=Closed`, `Close_Price=0`, full premium kept, `Remarks="CC Called"`. STOCK: `Status=Closed`, `Close_Price=strike`, `Actual_Profit=(strike − cost_basis) × shares`, `Remarks="Called away by T-OLD"` |
| **OpenShort (sell to open option)** | 1 new row | `Status=Open`, `TradeType=CC` or `CSP`, `Direction=Sell` |
| **OpenLong (buy LEAP)** | 1 new row | `Status=Open`, `TradeType=LEAP`, `Direction=Buy` |
| **Buy Stock** | 1 new row | `Status=Open`, `TradeType=STOCK`, `Direction=Buy` |
| **Sell Stock** | 1 existing STOCK row marked Closed | `Status=Closed`, `Close_Price=sell price` |

### Counts in your live data (validates the model)

- **327 rows** with `Close_Price=0` and CC/CSP TradeType (= expirations)
- **55 "Rolled to" rows** (old positions closed in a roll)
- **15 "Rolled from" rows** (new positions opened in a roll)
- **73 rows** with Assignment/Exercise/Called keywords
- **626 total trades** (open + closed)

### ⚠️ Data Quality Red Flag — Roll Imbalance

55 "Rolled to" rows but only 15 "Rolled from" rows. **Either:**
- 40 partial-roll old positions were closed but the new positions were created differently (without "Rolled from" remark), OR
- 40 rolls have orphaned old positions and the new trades were entered as fresh OpenShort

This is one of the FIRST things reconciliation will surface — and it likely explains a chunk of your portfolio drift.

---

## Source File Analysis

**File:** `Statement_<account>_<from>_<to>.csv` — Tiger's annual Activity Statement
**Format:** Multi-section CSV with section name as column 1
**Encoding:** UTF-8 with BOM
**Date range covered:** 2024-04-01 to 2026-03-31 across two files

### Sections Found (and how ARGUS already handles them)

**Important context:** ARGUS captures every Tiger trade event (expirations, rolls, assignments) — it just spreads them across the same Data Table with different `Remarks` and `Status` values rather than putting them in a separate sheet. See the **ARGUS Event Model** section above for the precise mapping.

| Section | Rows (combined) | Relevance to ARGUS | Notes |
|---|---:|---|---|
| **Trades** | 1,762 | **CRITICAL** — every option/stock fill | Section #1 priority |
| **Exercise and Expiration** | 209 | **CRITICAL** — assignments + worthless expiries | Realized P/L events |
| **Holdings** | 67 | **CRITICAL** — current positions snapshot at end of period | Used to verify ARGUS open positions match |
| **Account Overview** | 8 | **HIGH** — beginning/ending NAV per period | Reconciles total portfolio value |
| **Cash Report** | 109 | HIGH — fees, commissions, net trades, deposits | Shows total fees per category |
| **Deposits & Withdrawals** | 38 | HIGH — actual cash flow events | Used for FX/deposit timing |
| **Dividends** | 4 | MEDIUM — SPY dividends paid | Adds to realized P&L |
| **Interest** | 23 | MEDIUM — MMF interest earned/charged | Adds to realized P&L |
| **Allowance** | 352 | LOW — Tiger promotional credits | Counts as cash credits |
| **Securities Lent** | 94 | LOW — share lending program income | Negligible $ but tracked |
| **Base Currency Exchange Rate** | 40 | HIGH — daily FX SGD/USD rates | Needed for SGD reconciliation |
| **Transfer** | 11 | LOW — gifted stock in (fractional NVDA) | Outside trading flow |
| **Segment Transfer** | 13 | LOW — internal cash↔fund swaps | Already net |
| **Card Transactions** | 46 | IGNORE — debit card spending | Personal expenses |
| **Financial Instrument Information** | 421 | IGNORE — security metadata | Reference only |
| **Interest Accruals** | 535 | IGNORE — daily accrual snapshots | Subsumed by Interest section |

---

## Critical Parsing Rules

### Rule 1 — Symbol Inheritance / De-duplication

Tiger reports each trade **twice** in the Trades section: once with the symbol filled, once with a blank symbol but identical data. Empirically verified: COIN sample shows 8 DATA rows totaling Qty=−2, but TOTAL row says Qty=−2 (so blank-symbol rows must be skipped).

**Parser rule:** *When `Symbol` column is blank but `Activity Type` and `Quantity` are populated, SKIP the row — it's a duplicate of the previous DATA row.*

### Rule 2 — Multi-Section Layout

Each section starts with a header row (column 1 = section name, columns 2+ = column headers) and continues with `DATA` rows (column 4 = "DATA"). Some sections also have `TOTAL` rows (column 4 = "TOTAL") — skip these for reconciliation.

**Parser approach:** Scan once, switch state machine when section header changes.

### Rule 3 — Option Symbol Format

Tiger writes options as: `Coinbase Global, Inc. (COIN 20260424 PUT 170.0)`

Format: `<Company> (<Ticker> <YYYYMMDD> <PUT|CALL> <Strike>)`

Parse with regex: `r'\(([A-Z]+) (\d{8}) (PUT|CALL) ([\d.]+)\)'`

### Rule 4 — Activity Types Mapping

| Tiger | ARGUS TradeType | ARGUS Direction | Notes |
|---|---|---|---|
| `OpenShort` (Option) | `CC` if right=CALL, `CSP` if right=PUT | `Sell` | Most common |
| `OpenLong` (Option) | `LEAP` (if expiry > 6 months) | `Buy` | LEAP detection by DTE |
| `Close` (Option) | (closes existing) | — | Match to open trade by symbol |
| `BuyToOpen` / `SellToClose` (Stock) | `STOCK` | `Buy` / `Sell` | Standard |
| `Option Exercise` (Exercise & Expiration) | (closes option, opens stock) | — | Special case — creates 2 ARGUS rows |
| `Option Expired Worthless` (Exercise & Expiration) | (closes option) | — | Realized P/L = full premium |

### Rule 5 — Fee Aggregation

A single Tiger trade has **15+ fee columns**. For ARGUS we collapse them into a single `Fee` field:

```
Fee = abs(sum of: Transaction Fee, Other Tripartite, Settlement Fee, SEC Fee,
                   Option Regulatory Fee, Stamp Duty, Transaction Levy,
                   Clearing Fee, Trading Activity Fee, Exchange Fee,
                   Commission, Platform Fee, Option Settlement Fee,
                   Consolidated Audit Trail Fee, GST))
```

(Always positive, regardless of direction)

### Rule 6 — Date/Time Parsing

Tiger format: `"2026-03-23\n12:32:58, US/Eastern"` (multiline cell with timezone)

Parser: split on `\n`, strip, parse first part as date, ignore time/TZ for ARGUS purposes (date precision only).

### Rule 7 — Realized P/L Field

Already populated by Tiger on closing trades — **trust this column** as authoritative for closed-position P/L. Don't recompute. This eliminates a whole class of P&L drift bugs.

---

## Normalized Trade Schema (Output of Parser)

```python
@dataclass
class TigerTrade:
    trade_date: date              # 2026-03-23
    settle_date: date             # 2026-03-24
    symbol_raw: str               # "Coinbase Global, Inc. (COIN 20260424 PUT 170.0)"
    ticker: str                   # "COIN"
    asset_class: str              # "Option" | "Stock"
    expiry: Optional[date]        # 2026-04-24 (None for stock)
    right: Optional[str]          # "PUT" | "CALL" | None
    strike: Optional[float]       # 170.0 | None
    activity_type: str            # "OpenShort" | "Close" | "BuyToOpen" | etc.
    quantity: int                 # signed: -1 = sold 1 contract / share
    price: float                  # 5.80 (per contract premium / per share)
    amount: float                 # -580.00 (signed cash flow)
    fee_total: float              # 0.41 (always positive, sum of all fee columns)
    realized_pl: Optional[float]  # 658.51 (only on closing trades)
    notes: str                    # any annotation
    currency: str                 # "USD" | "SGD"
    market: str                   # "US" | "SG" | "HK"
    source_row: int               # CSV line number for traceability
```

---

## ETL Pipeline Design

### Stage 1 — Extract (Parse Tiger CSV)

**File:** `tiger_parser.py`

```
parse_statement(filepath) → {
    'trades':         list[TigerTrade],
    'exercises':      list[TigerExercise],
    'holdings':       list[TigerHolding],
    'cash_events':    list[TigerCashEvent],   # Deposits, Dividends, Interest, Allowance
    'fx_rates':       dict[date, dict[ccy, rate]],
    'account_overview': {begin: dict, end: dict},
}
```

Single-pass scan with state machine. ~150 lines of code.

### Stage 2 — Transform (Normalize to ARGUS Format)

**File:** `tiger_to_argus.py`

For each `TigerTrade`:
1. **Map activity type** to ARGUS TradeType + Direction (Rule 4)
2. **Parse option symbol** to ticker/expiry/right/strike (Rule 3)
3. **Sum fees** (Rule 5)
4. **Detect LEAP vs short option** (DTE ≥ 180 + Buy direction = LEAP)
5. **Infer StrategyType** from existing ARGUS rules:
   - In `pmcc_tickers` set → PMCC
   - Tiger says it's an ActiveCore label (none for now) → ActiveCore
   - Otherwise → WHEEL

For each `TigerExercise`:
- Create 2 ARGUS rows: close the option (Status=Closed), open the stock (Status=Open) — or close the stock if CC was assigned.

Output: `list[ArgusProposal]` — dicts ready for `gsheet_handler.append_trade()`.

### Stage 3 — Load (Write to gSheet)

Two write paths:

**3a — Tiger Statement tab (raw, append-only):** Every parsed Tiger trade gets persisted as-is in a new `Tiger Statement` tab. This is the durable off-broker record. If ARGUS dies, you have your Tiger history in your own GSheet.

**3b — Data Table tab (ARGUS format):** Reconciliation finds:
- ✅ **Matches** — Tiger trade has corresponding ARGUS row → backfill `Fee` and `Tiger_Trade_ID` fields
- ❌ **Missing in ARGUS** — Tiger trade has no ARGUS counterpart → enqueue as proposal for review queue
- ⚠️ **Drift** — matched but P&L/qty/price differ → flag in report
- ❓ **Missing in Tiger** — ARGUS has it, Tiger doesn't → likely manual error, flag for review

User reviews proposals in the new Reconciliation page → approves → batch write via `atomic_transaction`.

---

## Tiger Event → ARGUS Row Mapping (the heart of the reconciler)

### Tiger Trades section (regular fills)

| Tiger Activity | Tiger Qty Sign | Right | → ARGUS Lookup → | Match Field |
|---|---|---|---|---|
| `OpenShort` | negative | PUT | New CSP open | (ticker, expiry, strike, Status=Open, Direction=Sell, TradeType=CSP) |
| `OpenShort` | negative | CALL | New CC open | (ticker, expiry, strike, Status=Open, Direction=Sell, TradeType=CC) |
| `OpenLong` | positive | CALL | New LEAP open (if DTE > 180d) | (ticker, expiry, strike, Status=Open, Direction=Buy, TradeType=LEAP) |
| `Close` | positive | PUT or CALL | Existing CSP/CC being closed (BTC or post-Roll close) | (ticker, expiry, strike, Status=Closed) — match `Close_Price` to Tiger's Trade Price |
| `BuyToOpen` (Stock) | positive | — | New STOCK open OR existing STOCK addition | (ticker, TradeType=STOCK, Direction=Buy, Status=Open) |
| `Close` (Stock) | negative | — | Existing STOCK being sold/called away | (ticker, TradeType=STOCK, Status=Closed) |

### Tiger Exercise and Expiration section (option lifecycle events)

| Tiger Transaction Type | Right | → ARGUS Lookup → | Identifying signature |
|---|---|---|---|
| `Option Expired Worthless` | PUT | Existing CSP marked Closed | `Close_Price=0`, `Remarks` contains "Expired" or "Expire" |
| `Option Expired Worthless` | CALL | Existing CC marked Closed | `Close_Price=0`, `Remarks` contains "Expired" or "Expire" |
| `Option Exercise` | PUT (CSP assigned) | TWO ARGUS rows: (1) closed CSP + (2) new STOCK row | OLD: `Remarks` contains "Assigned" or "Exercise". NEW: `TradeType=STOCK`, `Direction=Buy`, `Remarks` contains "from T-OLD" |
| `Option Exercise` | CALL (CC called away) | TWO ARGUS rows: (1) closed CC + (2) closed STOCK | OLD CC: `Remarks` contains "Called". STOCK: `Status=Closed`, `Remarks` contains "Called away" |

### Roll Detection (compound event spanning two Tiger rows)

A roll in Tiger appears as TWO independent rows:
- Row A: `Close` of old CC/CSP
- Row B: `OpenShort` of new CC/CSP (different strike/expiry, same ticker, same trade date)

In ARGUS this is ALSO two rows (old closed + new open) — reconciliation does NOT need to "detect" rolls. Each Tiger row matches one ARGUS row independently. The "Rolled to / Rolled from" remark linkage is ARGUS metadata that Tiger doesn't track.

**However** — when proposing missing trades:
- If we find a Tiger Close + Tiger OpenShort on same date / same ticker / different strike or expiry → mark them as a candidate roll pair
- The Trade Loader should suggest the roll relationship and let user approve/reject

---

## Match Strategy

### Match key by event type

For **OpenShort / OpenLong** options:
```
key = (ticker, expiry_date, right, strike, status="Open", direction)
```
- Quantity must be exact match
- Premium price ±$0.05 tolerance (rounding)

For **Close** options (BTC):
```
key = (ticker, expiry_date, right, strike, status="Closed")
```
- Match Tiger Close fill_price to ARGUS `Close_Price` ±$0.05
- Match trade date to ARGUS `Date_closed`

For **Expirations**:
```
key = (ticker, expiry_date, right, strike, status="Closed", close_price=0)
```
- Match Tiger expiry date (= Tiger trade date) to ARGUS `Expiry_Date`

For **Exercises (CSP assigned)**:
- Match the CSP close (same as expiration above with Close_Price=0)
- AND find the corresponding new STOCK row created on assignment date with strike as buy price

For **Exercises (CC called)**:
- Match the CC close (Close_Price=0, full premium kept)
- AND find the corresponding STOCK close at strike price

For **Stocks**:
```
key = (ticker, trade_date, abs(qty), direction)
```

### Primary Match Key (Stocks)

```
(ticker, trade_date, abs(quantity), side)
```

### Match Tolerance

- **Quantity:** exact match required
- **Price:** ±$0.05 (rounding tolerance)
- **Date:** exact match required (we use trade date, not settlement)
- **Fee:** not used for matching, just backfilled from Tiger

### Multi-fill Handling

If Tiger has 2 fills on same date/symbol (partial fills of same order), and ARGUS has 1 row with the combined qty — that's a 1-to-many match. Acceptable — reconciler aggregates Tiger fills before matching.

---

## gSheet Schema Changes

### `Data Table` (existing) — add 3 columns

| New Column | Purpose |
|---|---|
| `Fee` | Total fee from Tiger (USD), positive number |
| `Tiger_Trade_ID` | Foreign key to Tiger Statement tab (e.g., row index or hash) |
| `Reconciled` | `TRUE` if matched against Tiger, else blank |

### `Tiger Statement` (NEW tab) — raw Tiger fills

Columns mirror `TigerTrade` schema. Append-only. One row per parsed Tiger trade. Includes a `Run_Timestamp` so multiple reconciliation runs can be distinguished.

### `Tiger Cash` (NEW tab) — non-trade cash events

| Column | Source |
|---|---|
| Date | Cash event date |
| Type | `Deposit` / `Withdrawal` / `Dividend` / `Interest` / `Fee` / `Allowance` |
| Description | Tiger description |
| Amount | Signed (positive = credit, negative = debit) |
| Currency | USD / SGD |
| Run_Timestamp | When this row was imported |

### `Reconciliation Log` (NEW tab) — audit of every reconciliation run

| Column | Purpose |
|---|---|
| Run_Timestamp | When user ran reconciliation |
| Tiger_Trades_Parsed | Count |
| Matches | Count |
| Missing_In_ARGUS_Approved | Count of proposals user accepted |
| Missing_In_ARGUS_Rejected | Count of proposals user skipped |
| Drift_Flagged | Count of mismatches surfaced |
| Total_Fee_Backfilled_USD | Cumulative fees added on this run |

---

## Reconciliation Page UX (`render_reconciliation`)

### Section 1 — Upload + Parse Summary

```
[Drag CSV file here]                  [Parse]
✅ Parsed Tiger statement: 2025-04-01 → 2026-03-31
   - 1,762 trade rows (881 unique fills after dedup)
   - 209 exercises/expirations
   - 41 current holdings
   - Tiger NAV at end of period: $250,512.19 USD
```

### Section 2 — Snapshot Reconciliation

| Item | ARGUS | Tiger | Drift |
|---|---:|---:|---:|
| Total Deposits (USD) | $X | $Y | |
| Realized P&L | $X | $Y | |
| Stock at Cost | $X | $Y | |
| LEAP at Cost | $X | $Y | |
| CSP Reserved | $X | $Y | |
| Cumulative Fees | $0 | -$Y | (gSheet missing) |
| Cumulative Dividends | $0 | +$Y | (gSheet missing) |
| **NET ACCOUNT VALUE** | **$X** | **$Y** | **$Z** ← THIS IS WHAT WE'RE CHASING |

### Section 3 — Trade-by-Trade Drift Report

Sortable table:
- Tiger trade ID, date, ticker, activity, qty, price
- Match status (Matched / Drift / Missing)
- ARGUS TradeID (if matched)
- $ delta on price/qty/realized P&L
- Action button: "Investigate" / "Auto-fix"

### Section 4 — Trade Loader (Review Queue)

For each missing-in-ARGUS Tiger trade:
- Pre-filled ARGUS row with proposed StrategyType, Pot, etc.
- User can edit before approval
- Checkbox to include in batch write
- "Apply Selected" button → batch `atomic_transaction` write to Data Table

### Section 5 — Reconciliation History

Read from `Reconciliation Log` tab. Shows previous runs and what they changed. Useful for "did this drift exist before today's run?" investigations.

---

## Implementation Phases

### Phase A — Parser only (1 day)
- `tiger_parser.py` — parse the CSV, output normalized objects
- Standalone CLI for testing: `python tiger_parser.py path/to/file.csv` → JSON dump
- Unit tests against the 2 sample files
- **Deliverable:** can read Tiger CSV and produce structured data, no app integration yet

### Phase B — Schema migration (0.5 day)
- Add `Fee`, `Tiger_Trade_ID`, `Reconciled` columns to Data Table
- Create `Tiger Statement`, `Tiger Cash`, `Reconciliation Log` tabs
- Existing trades default to Fee=0, Reconciled=blank
- **Deliverable:** gSheet ready to receive enriched data

### Phase C — Reconciliation engine (1.5 days)
- `reconciliation.py` — match Tiger ↔ ARGUS, compute deltas
- Snapshot comparison (NAV, fees, dividends, etc.)
- Output: structured report dicts
- Unit tests with the sample data
- **Deliverable:** can compute drift programmatically

### Phase D — Trade Loader (1 day)
- `tiger_to_argus.py` — convert Tiger trades to ARGUS proposals
- StrategyType + Pot inference
- Exercise/Expiration handling (creates 2 rows for assignments)
- **Deliverable:** can generate ARGUS write proposals

### Phase E — Reconciliation UI (1.5 days)
- New `render_reconciliation()` page in app.py
- File uploader + parse-trigger button
- Snapshot, drift, review-queue, history tabs
- Approve-and-write button with `atomic_transaction`
- **Deliverable:** end-to-end reconciliation flow in app

### Phase F — Backfill historical data (0.5 day, manual user step)
- User runs reconciliation against ALL historical Tiger statements
- Approves missing trades in batches
- Reviews drift report and fixes any cost basis errors
- **Deliverable:** clean gSheet, zero drift vs Tiger

**Total: ~6 days of dev + 0.5 day user time = ~1 working week**

---

## Risk & Edge Cases

1. **Tiger CSV format changes** — Tiger may add/rename columns over time. Parser must be tolerant of column reordering (use header-name lookup, not positional indexing).

2. **Multi-currency trades (SGD stocks like 1B0.SI)** — Tiger reports them in the local currency. We need to use the FX rate from `Base Currency Exchange Rate` section to convert to USD for reconciliation.

3. **Exercise/Expiration creates implicit positions** — A CSP getting assigned creates an implicit STOCK position in ARGUS at the strike price. The parser must generate this automatically.

4. **Securities lending income** — Small ($0.13/day on CRCL shares lent). Negligible but should be captured as cash credit.

5. **Allowance / promotional credits** — Tiger gives small subsidies. Counted as cash credit.

6. **Account Overview NAV doesn't match Cash + Stock + Option breakdown** — Tiger's "Total" includes "Funds in Transit" and accruals. Reconciliation must match ARGUS NAV to the correct Tiger row.

7. **Multi-leg orders (rolls executed as combo)** — Tiger may book a roll as a single multi-leg order. Currently no evidence of this in samples but worth checking on more data.

8. **Trade date vs settle date** — Tiger uses trade date for P&L, settle date for cash flow. We use trade date throughout.

---

## Open Questions for User Before Implementation

1. **Pot inference:** When Tiger creates a missing trade proposal, default to which pot? (Suggest: Base Pot for all, user can change in review queue.)

2. **Historical Tiger statements:** Do you have CSVs going back to portfolio inception (early 2024)? If so, drop them in the same `tiger_samples/` folder and we'll reconcile from day 1.

3. **Frequency of reconciliation:** Will you run this monthly (matching Tiger statement cadence) or more often? Affects how aggressive the auto-write should be.

4. **Cost basis correction policy:** When reconciliation finds a price drift on a closed trade, do we (a) update gSheet to match Tiger, or (b) flag for review and manual edit?
