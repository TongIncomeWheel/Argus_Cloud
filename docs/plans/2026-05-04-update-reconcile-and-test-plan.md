# Update & Reconcile + Functional/Integration Test Plan

**Date:** 2026-05-04
**Status:** In progress
**Author:** Tiger ETL workstream

## Context

ARGUS now has a clean Tiger-rebuilt Data Table (623 rows, P&L reconciled to within $0.03 of Tiger's reported number). Schema includes new columns `Fee`, `Pot`, `Tiger_Row_Hash`. Trade entry forms updated to populate the new schema. 3-layer integrity verification passes all checks.

Two gaps remain before UAT:

1. **Update & Reconcile** is not built. Today the only ingestion path is destructive migration. Ongoing operations need an additive upload that diffs new Tiger data against existing ARGUS data and applies only the deltas.
2. **Functional/integration testing** has not been done end-to-end across the actual ARGUS app surface.

This document specifies both.

---

## Part 1 — Update & Reconcile

### Purpose
Allow the user to upload a fresh Tiger Activity Statement CSV and have ARGUS additively reconcile new trades, fees, and broker margin against the existing Data Table without destroying anything.

### Trigger surface
- **Primary**: Streamlit page `🐅 Tiger Import` (sidebar nav). Production path on Streamlit Cloud.
- **Secondary (dev only)**: `python tiger_etl_update.py path/to/new.csv` — terminal helper for local testing.

### Workflow
```
[CSV upload] → parse → diff vs ARGUS → preview → user approves → apply → audit JSON
```

### Idempotency (locked-in)
Re-uploading the same CSV (or an overlapping CSV) must produce a no-op diff. Achieved by:
- **Content-based row hashes** with within-file `:dup<N>` suffix for legitimate multi-fill orders. Fixed in `tiger_parser.compute_row_hash` on 2026-05-04.
- **File hash check** against `Tiger Imports` tab — if exact file already imported, page shows "Already imported on [date]" with empty diff.

### Diff categories (what the engine reports)
1. **New opens** — Tiger trades whose `row_hash` is not in ARGUS Data Table. Append as new rows with `Status=Open`.
2. **Closes matched to existing opens** — incoming Close fill / Exercise / Expire event whose contract key (ticker, right, strike, expiry) matches an existing open ARGUS row. Update `Status=Closed`, fill `Date_closed`, `Close_Price`, `Actual_Profit_(USD)`. Use partial-fill aggregation.
3. **Rolls** — same-day Close + new OpenShort with different strike/expiry. Pair them, mark Remarks.
4. **Orphans** — closes / exercises with no matching open. User reviews manually before approving.
5. **Cash events** — new dividends, interest accruals, allowances, deposits. Append to Tiger Cash tab.
6. **Holdings reconcile** — compare ARGUS open positions (count + qty by contract key) against Tiger's end-of-period Holdings. Drift report.
7. **NAV reconcile** — ARGUS computed NAV vs Tiger Account Overview `end` total. Drift report.
8. **Fee backfill** — for closes matched to existing opens, sum the Tiger close-side fees into the existing row's Fee field.

### Conservative split on partial closes (decision: 3b=A)
When an incoming Close has qty=4 but the matching existing ARGUS open has qty=10, split the open row:
- Existing row updated to `Quantity=4`, `Status=Closed`, `Tiger_Row_Hash` retained
- New sibling row appended: `Quantity=6`, `Status=Open`, `Tiger_Row_Hash = <original>:p2`, Remarks: "Partial remainder (6 of 10)"

Preserves audit lineage — both rows trace back to the same original open hash.

### Apply step (atomic, with backup)
1. Backup current `Data Table` → `Data Table (Pre-Update <timestamp>)` archival tab
2. Apply row updates and inserts via `gsheet_handler.update_trade()` and `append_trade()`
3. Append fact rows to `Tiger Statement`, `Tiger Cash`, `Tiger Imports` (no overwrite)
4. Append a row to `Reconciliation Log` with the diff summary + drift values
5. Save audit JSON to `data/etl_audit/etl_update_<run_id>.json`

### Streamlit page UX
| Section | Content |
|---|---|
| Top | File uploader (drag-drop, multi-file). On upload → spinner while parsing |
| Tab: 📊 Summary | Counts: N new opens, M closes matched, K rolls, P orphans. Status traffic light + NAV/holdings drift |
| Tab: ➕ New Trades | Table of incoming opens |
| Tab: 🔄 Updates | Existing rows that will be modified (TradeID + before/after diff per field) |
| Tab: ♻️ Rolls | Side-by-side close + new-open pairs |
| Tab: ⚠️ Orphans | Unmatched closes — user reviews and either approves anyway or cancels |
| Tab: 💰 Cash & Margin | New cash events table + Tiger NAV vs ARGUS NAV drift comparison |
| Bottom | "Apply Import" button (disabled if blocking issues) + "Cancel" |

### CLI helper interface
```bash
python tiger_etl_update.py path/to/new.csv [--auto-approve] [--dry-run]
```
- `--dry-run`: print diff, exit without applying
- `--auto-approve`: apply without confirmation prompt (for CI / scripting)
- Default: print diff, ask y/N, apply on yes

---

## Part 2 — Test Plan (Phases 0–3)

### Phase 0 — Data Ingestion
**Goal:** Validate Update & Reconcile end-to-end with a real fresh Tiger CSV.

Test fixture: `Statement_50179929_20260427_20260503.csv` (uploaded 2026-05-04). Contents:
- 11 new trades (9 OpenShort + 2 Close)
- 5 new exercises
- 41 holdings at end of period
- 0 cash events
- End NAV: $319,060.28
- Has 3 overlapping 2026-04-27 trades + 5 overlapping 2026-04-25 exercises with the prior April CSV (idempotency test built in)

| Check | Method | Pass criteria |
|---|---|---|
| Parser accepts file-like object | Python | Streamlit's `UploadedFile` object parses identically to file path |
| Cross-file dedup | Python | The 3 overlap trades + 5 overlap exercises detected as already-in-ARGUS, NOT proposed as new |
| Genuine new trades detected | Python | Exactly 11 trades + 5 exercises proposed as net-new |
| Streamlit page renders | Browser | Upload page loads, file uploader works, all 6 tabs render |
| Diff preview correct | Browser | Summary card shows correct counts. Each tab populated correctly |
| Apply button disabled if orphans | Browser | Force an orphan scenario → button disabled |
| Apply succeeds | Browser | Click Apply → backup tab created, Data Table grows by ~16 rows, audit JSON saved |
| Re-upload idempotent | Browser | Uploading same file again shows "already imported" with empty diff |
| NAV drift shown | Browser | Reconcile tab shows ARGUS NAV vs Tiger NAV $319,060.28 with drift in $ |
| Fees backfilled | Python | Closed paired rows have Fee = open_fee + close_fee_share |

### Phase 1 — Data Integrity & Math
**Goal:** The financial numbers on the Dashboard reflect reality.

| Check | Where | Method |
|---|---|---|
| NAV (USD) | Dashboard | Cross-check vs Tiger reported $319,060 ± unrealized MTM |
| NAV (SGD) | Dashboard | NAV_USD × FX rate |
| Selling Capacity / BP | Dashboard | `(deposit + realized PL) − stock_at_buy − leap_sunk − csp_reserved` formula matches expected |
| Capital Allocation Drill-Down | Dashboard | Pot split (Base vs Active) totals to deployed capital. Per-ticker subtotals reconcile |
| Tiger Broker Margin panel | Dashboard | Estimator value < cash-secured reserve. Headroom = positive |
| Realized P&L | Dashboard | Matches `Actual_Profit_(USD)` sum on closed rows ($27,329.73) |
| Total Fees | Dashboard | Matches Tiger `$2,562.47` |
| LEAP MTM | Dashboard | Alpaca live mid-price × 100 × qty matches per-row LEAP card |
| Pot configuration | Margin Config | Base + Active deposits sum correctly. Per-pot capital allocation editor saves |

### Phase 2 — Trade Lifecycle (live UI)
**Goal:** Each form does what it claims, against new schema, persisting to gSheet.

| Form | Smoke test |
|---|---|
| Insert CSP | Create test CSP on MARA → row appears with `Pot=Base, Fee=0, Tiger_Row_Hash=''` |
| Insert CC (ActiveCore) | Create test CC on COIN → row appears with `Pot=Active` |
| Roll CSP | Roll the test CSP → old row Closed, new row Open, both with "Rolled to/from" remarks linked |
| BTC partial close | Partial close the new CSP (5→3) → (A) row Closed, (B) row Open with `Tiger_Row_Hash=''` |
| Expire worthless | Pick an expiring CSP, mark expired → Status=Closed, premium = profit, Pot preserved |
| CSP Assignment | Run assignment → option Closed, new STOCK row created with inherited Pot |
| CC Called | Run called → CC Closed, existing STOCK row Closed at strike with computed P&L |

After each test trade, **delete the test row** so it doesn't pollute UAT.

### Phase 3 — Intelligence & Risk
**Goal:** Decision-support pages render and surface the right info.

| Surface | Smoke check |
|---|---|
| Daily Action Plan | Top tickers list. BP situation card. Actions suggested. No errors |
| Expiry Ladder | Positions expiring within 30d listed. DTE column correct |
| P&L Charts | Daily P&L, Cumulative P&L render. Time frame selector (1W/1M/3M/All) works |
| Allocation by Strategy | WHEEL / PMCC / ActiveCore breakdown sums to total |
| Tiger Statement view | New tab visible (if added to UI) — shows fact log |
| Audit Trail | Recent actions appear |

### Test execution method
- **Phase 0**: I drive Chrome MCP — navigate to Tiger Import page, file_upload (using the uploaded image API), inspect tabs, click Apply, screenshot key states
- **Phases 1–3**: I drive Chrome MCP for navigation and screenshots; you review with me as we go
- Python automated checks run alongside in parallel terminals

### Pass criteria
A phase passes if all checks in its table show ✓. Issues found are tracked in a "Phase X Issues" section appended to this doc.

### Rollback plan
Every phase has a backup: Update & Reconcile creates `Data Table (Pre-Update <timestamp>)` before apply. If anything goes wrong, restore that tab via Sheets → Tab → Duplicate.

---

## Acceptance: ready for UAT
- All Phase 0 checks pass
- All Phase 1 checks pass
- Phases 2 + 3 have at most "polish" issues (no data integrity issues)
- New Tiger CSVs reconcile cleanly going forward (idempotency verified)

Once accepted, the user takes over for User Acceptance Testing in their normal workflow.
