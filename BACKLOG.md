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
| `get_wheel_state` | 27 Jun 2026 | Per-ticker wheel state — CSP_OPEN / ASSIGNED / CC_OPEN / LEAP_ONLY / MIXED / IDLE. cycle_start_date anchored via Sheets primary, Tiger 90-day fallback. |
| `earning_power_test` | 27 Jun 2026 | Pure-math §5.1 PMCC roll decision: daily_improvement + payback_days + ROLL/HOLD verdict + drift override. |
| `get_roll_candidates` | 27 Jun 2026 | Structural-anchor (swing / round / MA / consolidation) + ATR-14 buffer + ±5 strike chain ladder + ranked candidates with mid + net credit + Δ/Θ. FMP daily bars source. |
| `run_stress_test` | 27 Jun 2026 | Scenarios A (−15% core) / B (−30% core + put assignment + call offset) / D (SPY −20% LEAPS) / B+D combined. Zone classifier + MARA reduction schedule + PMCC hard stop. Live position-driven. |

---

## OUTSTANDING — PRIORITY ORDER

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
