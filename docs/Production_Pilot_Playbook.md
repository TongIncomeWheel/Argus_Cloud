# ARGUS Production Pilot — Playbook

**Pilot start date:** 2026-05-04
**Pilot version:** Tiger ETL + Update & Reconcile + new schema (Fee/Pot/Tiger_Row_Hash)
**Pre-pilot stable code:** commit `7fac580` (2026-04-29) — what was on Cloud before this pilot
**Pre-pilot stable data:** backup tab `Data Table (Pre-Tiger Update_2026-05-04_11-54-13)` — 623 rows, P&L $27,329.73

---

## What's New in the Pilot

| Capability | Where |
|---|---|
| Tiger CSV → ARGUS reconciliation | Tiger Import page (sidebar nav) |
| Multi-fill aggregation in pairing | Internal — handles 5+3+2 contract orders correctly |
| Cross-file idempotency | Re-uploading same file is a no-op |
| Fee + NAV + Holdings reconciliation | Reconciliation Log tab + Update preview |
| New schema columns | `Fee`, `Pot` (Base/Active), `Tiger_Row_Hash` |
| Rollback CLI | `python tiger_etl_rollback.py --list` |

---

## Daily Operating Procedure

### When you have new Tiger trades to reconcile
1. Download fresh Tiger Activity Statement CSV (covering since last upload — overlap is safe, parser dedupes)
2. Open ARGUS app → sidebar → **🐅 Tiger Import**
3. Drag-drop the CSV. Wait for diff preview to render (~10 sec)
4. Review:
   - **Summary tab**: counts (new trades, updates, rolls, orphans)
   - **New Trades tab**: incoming opens
   - **Updates tab**: existing rows being closed by incoming closes
   - **Rolls tab**: paired close+open
   - **Orphans tab**: things that need your judgment (should be empty in normal use)
   - **Cash & Margin tab**: NAV drift vs Tiger
5. Click **"Apply Import"** when satisfied
6. After apply, refresh Dashboard to see updated NAV/positions

### Manual trade entry (CSP, CC, Roll, BTC, Expire, Exercise)
Same as before — Entry Forms page. New fields auto-populate (`Pot` derived from `StrategyType`, `Fee=0`, `Tiger_Row_Hash=''`).

---

## Rollback Procedures

### Scenario A: Data corruption (most common pilot risk)

If ARGUS shows wrong numbers after a Tiger Import or manual trade goes sideways:

```bash
cd C:\Users\ashtz\ARGUS_Cloud

# 1. List available backup tabs (newest first)
python tiger_etl_rollback.py --list

# 2. Restore to the May 3 pilot baseline (this is the cleanest stable point)
python tiger_etl_rollback.py --restore "Data Table (Pre-Tiger Update_2026-05-04_11-54-13)"

# Or restore to the most recent backup
python tiger_etl_rollback.py --restore latest
```

The script automatically:
- Snapshots the CURRENT Data Table to a `Pre-Rollback <timestamp>` tab (so you can undo the rollback if you change your mind)
- Wipes the live Data Table
- Writes the backup's contents back

After restore, refresh Streamlit Cloud (or your local app) to see the rolled-back state.

### Scenario B: Code bug (rare)

If the new code itself misbehaves (e.g., dashboard crashes, calculation errors):

```bash
cd C:\Users\ashtz\ARGUS_Cloud

# Revert to pre-pilot stable commit
git checkout 7fac580 -- .   # use last-good commit hash
git commit -m "rollback: revert to pre-pilot stable (7fac580)"
git push origin main

# Streamlit Cloud auto-redeploys in 1-2 min
```

This rolls back code only; data is untouched. If the bug also corrupted data, do Scenario A as well.

### Scenario C: Both code and data (full rollback)

```bash
# 1. Roll data back first (safer order)
python tiger_etl_rollback.py --restore "Data Table (Pre-Tiger Update_2026-05-04_11-54-13)"

# 2. Roll code back
git checkout 7fac580 -- .
git commit -m "rollback: full revert to pre-pilot stable"
git push origin main
```

---

## Daily Health Checks (first 1–2 weeks of pilot)

Run this every morning before trading to catch drift early:

```bash
cd C:\Users\ashtz\ARGUS_Cloud
python tiger_etl_verify.py 2>&1 | tail -10
```

Expected output (passing pilot):
```
Layer 1: PASS
Layer 2: PASS  
Layer 3: PASS
OVERALL: ALL LAYERS PASSED (with warnings)
```

If any layer fails → investigate before trading.

---

## Monitoring Targets

| Metric | Expected | Alert if |
|---|---|---|
| Realized P&L drift vs Tiger | < 1% | >1% |
| Open positions count | Match Tiger holdings within ±2 contracts | >5 contracts off |
| NAV drift vs Tiger end-of-period | < 5% (depends on FX/MTM lag) | >10% |
| Implicit expirations per import | 0–2 | >5 (suggests parser missed events) |
| Orphan closes per import | 0 | Any (review manually) |

The Reconciliation Log tab in the gSheet has one row per Tiger Import with all these metrics. Review it weekly.

---

## Key Files in the Codebase

| File | Purpose |
|---|---|
| `tiger_parser.py` | Parses Tiger CSV → structured data. Content-based row hash with `:dup<N>` for legitimate multi-fills |
| `tiger_to_argus.py` | Pure transform: Tiger event → ARGUS row dict |
| `tiger_etl.py` | Destructive migration orchestrator (DO NOT use in normal pilot operation) |
| `tiger_etl_update.py` | Additive Update & Reconcile (used by Tiger Import page + CLI) |
| `tiger_etl_rollback.py` | CLI to restore Data Table from a backup tab |
| `tiger_etl_verify.py` | 3-layer integrity verifier — run daily during pilot |
| `gsheet_handler.py` | gSheet read/write. `read_data_table()` uses get_all_values to avoid scientific-notation hash corruption |
| `app.py` | Streamlit app. Tiger Import page near bottom |
| `unified_calculations.py` | NAV, BP, Tiger margin, P&L formulas (single source of truth) |

---

## Backup Tabs in gSheet (as of pilot start)

| Tab | Rows | Purpose |
|---|---|---|
| `Data Table` | live | Current operating state |
| `Data Table (Pre-Tiger Update_2026-05-04_11-54-13)` | 623 | **Recommended rollback point** — May 3 stable |
| `Data Table (Pre-Tiger 2026-05-04_1122)` | 623 | Same content as above (alt rollback) |
| `Data Table (Pre-Tiger 2026-05-03_2216)` | 622 | Earlier May 3 state |
| `Data Table (Pre-Tiger 2026-04-30)` | 626 | Original pre-Tiger-ETL — **do NOT use as rollback**, lacks new schema |

The Tiger Update & Reconcile module also creates a fresh `Pre-Update <ts>` backup before every Apply, so you always have an immediate undo.

---

## Pilot Exit Criteria

After 1–2 weeks of clean operation:
- All 3 verifier layers pass daily
- NAV reconciles within 1% of Tiger weekly snapshot
- No data integrity rollbacks needed
- User confidence in Tiger Import workflow

→ Promote pilot to permanent prod (no special action — the code is already on `main`).

---

## Support / Escalation

If something breaks and you can't figure out what:
1. **Don't trade** until reconciled
2. Save a screenshot of the issue
3. Run `tiger_etl_verify.py` and save the output
4. Roll back data to May 3 baseline (Scenario A)
5. Reach out for diagnosis with the screenshot + verifier output

The audit JSON for every Update/Migration is saved to `data/etl_audit/` — these files are the forensic record of what happened.
