# Backlog — Tiger MCP

Work queued for weekend execution (no active trading, lowest token contention).
This file is the source of truth — if a future Claude session asks "what's
next," point them here.

---

## Scoping decisions (locked, can change but call it out if you do)

- **Q1: Surface** — both Streamlit AND claude.ai MCP. MCP first, Streamlit second.
- **Q2: UX strategy** — layer wheel-cycle awareness + capital-aware sizing on top
  of the existing `theta_scanner/` module. Do NOT rebuild the existing UI from
  scratch.
- **Q3: "Active Pot"** = the Argus Active Portfolio target (the Tiger account
  portfolio selector that already exists in `app.py`).

## Pre-feature defense PRs (optional, can do any time)

Identified by the 2026-06-23 code review. Tiny, low-token, defensive. Worth
shipping any time — not gated to weekends. Status: TBD per user.

- **PR-D1:** Fix broken test `test_refresh_token_rotation_invalidates_old`
  in `mcp_servers/tiger/tests/test_oauth.py` — code intentionally no longer
  rotates refresh tokens (PR #38), test still asserts old behavior. ~20 min.
- **PR-D2:** Add boot guard in `mcp_servers/tiger/server.py` `_build_server()`:
  if `MCP_OAUTH_OWNER_PASSWORD` set AND transport is sse/streamable-http AND
  `MCP_OAUTH_STORAGE != firestore` → refuse to start. Prevents the entire
  class of overnight-disconnect bugs from recurring. ~30 min.
- **PR-D3:** Delete `mcp_servers/tiger/deploy/cloud-run.yaml`. Has drifted hard
  from the GitHub Actions deploy (memory 256Mi vs 512Mi, missing OAuth env
  vars). Better gone than wrong. ~5 min.

---

## Phase 1 — Tiger as primary chain source + MCP exposure

**Trigger:** weekend, US markets closed, Tiger US Option L1 entitlement
confirmed active for OpenAPI (see BUG 005 — Ash to action with Tiger support).

**Scope:**

- `theta_scanner/data.py`
  - Add Tiger chain loader using `TigerClient.get_option_chain(symbol, expiry,
    return_greek_value=True)` (already implemented in
    `tiger_api/client.py:777-...`).
  - Tiger becomes primary source. Alpaca stays as fallback (`alpaca_configured()`
    check) for when Tiger entitlement is in a bad state.
  - Single env var to override: `THETA_SCANNER_CHAIN_SOURCE=tiger|alpaca|auto`,
    default `auto` (tries Tiger, falls back to Alpaca on permission errors).

- New MCP tools in `mcp_servers/tiger/server.py`:
  - `scan_csp_candidates(universe, dte_min, dte_max, delta_min, delta_max,
                          min_iv, min_oi, min_volume, limit=20)`
    → returns ranked CSP scan results as `list[dict]`. Wraps
    `theta_scanner.scan.run_scan` with `option_type="put"`.
  - `scan_csp_with_capital(cash_available, max_per_position_pct,
                            universe, dte_min, dte_max, delta_min, delta_max,
                            limit=10)`
    → sizing-aware: filters out contracts where `strike * 100 * 1` exceeds
    the per-position cap, sorts by `theta * size_fit` rather than raw theta.

- New MCP resource: `theta-scanner://presets` (read-only) — exposes existing
  `presets.load_filter_presets()` to the LLM so it can use named scan configs.

**Out of scope for Phase 1:**
- One-click stage_csp action (that's Phase 2)
- Wheel-cycle awareness (that's Phase 2)
- Streamlit UI changes (Phase 2)

**Estimated tokens:** ~2-3 hours of focused work, no UI design churn.

---

## Phase 2 — Wheel-aware UX + capital sizing

**Trigger:** weekend, after Phase 1 is shipped and validated by hitting the
new MCP tools from claude.ai.

**Scope (in priority order):**

1. **Position-cross-reference in scanner output.** For every CSP candidate,
   annotate whether we already have:
   - A short put on this ticker (in `tiger_api.client.get_option_positions`)
   - The stock (assigned scenario)
   - A short call (CC scenario)
   Surfaces as `wheel_state` field on each result row: `"empty" / "csp_open" /
   "assigned" / "cc_open" / "called_away"`.

2. **Capital-aware sizing default.** Wire `get_account_summary` cash into
   `scan_csp_with_capital` as default `cash_available` if not supplied.
   `max_per_position_pct` defaults to a config setting (e.g., 15% per CSP).

3. **One-click stage action.**
   - New MCP tool: `stage_csp(symbol, expiry, strike, quantity, limit_price,
                              confirm=False)`
     → returns `place_option_order(symbol, expiry, strike, right="PUT",
     side="SELL_TO_OPEN", quantity, limit_price, confirm=False)` preview.
     Just a semantic shim — clearer name for the income-wheel workflow.
   - Optional: `stage_csps_from_scan(scan_results, top_n)` → returns N
     previews in one call.

4. **Streamlit UI integration:**
   - Add a "Scan & Stage" tab in `app.py` that wraps the MCP tools.
   - Display wheel_state badges next to each candidate.
   - Stage button → opens `place_option_order` preview in a confirm modal.
   - Doesn't touch the existing `render_theta_scanner` UI — sits next to it.

**Estimated tokens:** ~3-4 hours.

---

## Phase 3 — automation (deferred, not in this backlog)

Cron-driven scans, alert thresholds, auto-stage queued previews. Revisit
when Phase 1+2 are operational and we have real usage data on which alerts
would actually be useful.

---

## How to resume this work in a future session

> "Read `BACKLOG.md`. Status check: have Phase 1 PRs landed? If yes, start
> Phase 2 with the scoping decisions already locked. If no, start Phase 1
> from the `scan_csp_candidates` MCP tool — Tiger chain loader first, then
> MCP wrapper. Pre-feature defense PRs (PR-D1 through PR-D3) can be shipped
> any time — check with the user before doing them as part of a Phase batch
> to keep PRs small and reviewable."

---

*Last updated: 2026-06-23 by Claude Code session
01GTnrwWAp9CvXWRWkzqQy2X — scoped from user instruction during code-review
session. Phase 1+2 explicitly on hold pending weekend execution.*
