# ARGUS — E1 BACKLOG

**Repo:** TongIncomeWheel/Argus_Cloud
**Updated:** 27 Jun 2026
**Status:** Living document — update as items ship

---

## SHIPPED (wired to MCP — callable now)

| Tool | Shipped | Notes |
|---|---|---|
| `compute_portfolio_greeks` | 25 Jun 2026 | Full book delta + theta. BS local solve. yfinance spot ~15min delayed. |
| `get_position_roc` | 27 Jun 2026 | Per-position yield, % harvested, annualised RoC, juiced flag at 65%. Primary: Google Sheets. Fallback: Tiger 90-day. ms-epoch timestamp bug (1781-11-05) fixed same day. |
| `compute_hv` | 27 Jun 2026 | HV30 from yfinance daily closes. Input to PMCC §2 hurdle and §12 scorecard. |
| `score_pmcc_candidate` | 27 Jun 2026 | Full §12 scorecard server-side. BS Greeks + 5k-path MC distribution + verdict. STO only (v1). |
| `quarterly_archive` | 27 Jun 2026 | Google Sheets Data Table snapshot at quarter-end. GitHub Actions cron (Mar/Jun/Sep/Dec 30/31). |

---

## OUTSTANDING — PRIORITY ORDER

### Priority 1 — `get_wheel_state`
**File:** `tiger_api/wheel_cycles.py`
**What it does:** Returns per-ticker wheel cycle state — CSP_OPEN / ASSIGNED / CC_OPEN / EXPIRED / IDLE.
**Why it matters:** Claude currently cannot tell which phase of the wheel each ticker is in without reading raw position data and inferring. This makes `/md-pacing` imprecise.
**Output schema:**
```json
{
  "ticker": "MARA",
  "state": "CC_OPEN",
  "current_positions": [...],
  "cycle_start_date": "2026-06-05",
  "days_in_cycle": 22
}
```

---

### Priority 2 — `get_roll_candidates`
**File:** `tiger_api/rolls.py`
**What it does:** Surfaces all open short positions that meet roll criteria — dying leg definition, delta band breach, or juiced flag — with net credit/debit estimate for candidate strikes.
**Why it matters:** Currently Claude identifies roll candidates manually from Greeks output. This moves the logic server-side.
**Input:** `pot` filter, `urgency` (immediate / this-week / monitor)
**Output:** Ranked list of roll candidates with BTC cost, STO credit estimate, net debit/credit, payback days.

---

### Priority 3 — Stress Test MCP tool
**File:** `tiger_api/stress.py`
**What it does:** Runs the B, D, and B+D combined drawdown scenarios server-side against live positions. Returns NAV impact, excess liquidity after shock, zone classification, and reduction schedule.
**Why it matters:** Currently Claude computes margin scenarios inline — slow, token-heavy, prone to drift as positions change. Server-side stress test runs against live marks every morning automatically.
**Output:**
```json
{
  "scenarios": {
    "A": {"equity_loss": 40023, "buffer_after": 37956, "zone": "watch"},
    "B": {"equity_loss": 80200, "buffer_after": 0, "zone": "critical"},
    "D": {"pmcc_loss": 44390, "buffer_after": 33589, "zone": "watch"},
    "BD": {"total_loss": 153968, "buffer_after": -75989, "zone": "insolvent"}
  },
  "current_zone": "watch",
  "reduction_schedule": [...]
}
```
**Charter integration:** Replaces Claude inline margin math in morning run. `/margin` command calls this directly.

---

### Priority 4 — §5.1 Earning Power Test MCP tool
**What it does:** Given a dying short leg and a proposed replacement, runs the Portfolio Earning Power Test from PMCC Master Doctrine §5.1 server-side.
**Formula:**
```
daily_improvement = (new_theta + new_delta × drift) − (current_theta + current_delta × drift)
payback_days = roll_debit ÷ daily_improvement
verdict = ROLL if payback_days < new_DTE × 0.50 else HOLD
```
**Why it matters:** Currently computed inline by Claude from Greeks output. Error-prone and token-heavy on a 6-leg array.
**Input:** current leg Greeks + proposed leg Greeks + roll debit + new DTE + drift assumption
**Output:** daily_improvement, payback_days, verdict, justification

---

### Priority 5 — `score_pmcc_candidate` BTC/ROLL variants
**Current state:** `score_pmcc_candidate` is STO-only (v1).
**Needed:** BTC scorecard (should I close this position?) + ROLL scorecard (BTC old → STO new as a combined evaluation).
**Doctrine reference:** PMCC Master Doctrine §12 — "BTC/ROLL scorecard variants tracked in BACKLOG."

---

### Priority 6 — `get_win_rate`
**File:** `tiger_api/win_rate.py`
**What it does:** Win rate by setup bucket — by ticker, by delta band at entry, by DTE at entry, by pot.
**Why it matters:** Needed for honest performance attribution. Tells you whether the 0.30–0.35 delta entries outperform the 0.38–0.40 entries over time.
**Source:** Google Sheets Data Table (full history) — not Tiger 90-day window.

---

### Priority 7 — `get_iv_rank`
**File:** `tiger_api/iv_scanner.py`
**What it does:** IV rank/percentile for a given ticker using realised vol proxy from historical prices.
**Why it matters:** Active Pot CSP entry timing. High IV rank = rich premium. Currently estimated manually.

---

### Backlog (no priority assigned yet)
- Income forecast — theta carry over configurable horizon
- `ap-scan` — Active Pot CSP candidate screener (pull-based, PM-initiated only — never autopushed)
- Phase E2 — Alpaca spot + Greeks (free, 15-min delayed) replacing yfinance in `compute_portfolio_greeks` and `compute_hv`. Requires `ALPACA_API_KEY` + `ALPACA_SECRET_KEY` GH secrets; deploy workflow syncs to Cloud Run.
- Theta-scanner Phase 1 (`scan_csp_candidates`, `scan_csp_with_capital`) and Phase 2 (wheel-aware UX, capital sizing) — older scoping doc, still relevant. Pot-aware `wheel_state` annotation now subsumed by Priority 1.
- PR-D1: fix broken `test_refresh_token_rotation_invalidates_old` (PR #38 stopped rotating refresh tokens; test still asserts old behavior).
- PR-D2: boot-guard `_build_server()` — refuse to start if `MCP_OAUTH_OWNER_PASSWORD` set AND transport is sse/streamable-http AND `MCP_OAUTH_STORAGE != firestore`. Prevents overnight-disconnect class of bugs from recurring.
- PR-D3: delete `mcp_servers/tiger/deploy/cloud-run.yaml` — drifted from the GitHub Actions deploy (memory 256Mi vs 512Mi, missing OAuth env vars).

---

## KNOWN ISSUES / TECH DEBT

| Issue | Impact | Fix |
|---|---|---|
| `entry_fill_found: false` on some positions older than 90 days | Annualised RoC not computable for those positions. Yield % and harvested % still correct. | Once Sheets primary source is active (PR #51 wired auto-sync from GH secrets), most resolve. Remaining gaps need ETL backfill. |
| yfinance spot ~15min delayed | Greeks solve uses slightly stale spot | Phase E2: wire Alpaca for real-time spot. Low priority — delay acceptable for daily review. |
| Streamlit Community Cloud sleep | Argus UI goes cold after ~15 min inactivity | Separate issue — Streamlit hosting choice. MCP server itself is on Cloud Run with `min-instances=1` + `no-cpu-throttling`, always warm. |

---

## DEPLOYMENT NOTES (correct as of 27 Jun 2026)

- **Hosting:** the Tiger MCP server runs on Google Cloud Run (`tiger-mcp-499603` / `asia-southeast1`). Not Railway. `min-instances=1`, `no-cpu-throttling`, 60-min request timeout. Auto-deploys on every push to `main` via `.github/workflows/deploy-mcp.yml`.
- **Sheets auth:** the deploy workflow syncs `GOOGLE_SHEETS_CREDENTIALS` + `INCOME_WHEEL_SHEET_ID` GitHub repo secrets into Secret Manager on every push; Cloud Run binds both as env vars. Same SA the Streamlit Argus deploy uses. No manual sheet sharing required.
- **OAuth:** static client credentials (PR #42) in Firestore (PR #43+), 10-year token TTLs (PR #39), streamable-http transport (PR #45). The connector ID in claude.ai's form is `argus-tiger-mcp-claude`; the secret is `mcp-oauth-client-secret`.
- **Connector name:** `Tiger MCPv7` in claude.ai. URL `https://argus-tiger-mcp-686093261470.asia-southeast1.run.app/mcp`.

---

*E1 Backlog v2.0 | 27 Jun 2026 | Reflects shipped: compute_portfolio_greeks, get_position_roc, compute_hv, score_pmcc_candidate, quarterly_archive. Outstanding: get_wheel_state, get_roll_candidates, stress_test, §5.1 EPT, BTC/ROLL scorecard variants, get_win_rate, get_iv_rank.*
