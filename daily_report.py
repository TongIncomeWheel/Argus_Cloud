"""
Daily CIO Report — Income Wheel / Active Core Portfolio
Generates a structured 4-section briefing using LLM + web search.
Persists the last report to disk so it survives app restarts.
"""
from __future__ import annotations

import re
import logging
from datetime import datetime
from typing import Optional

import pandas as pd
import streamlit as st

from persistence import load_daily_report, save_daily_report

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# REPORT PROMPT
# ─────────────────────────────────────────────

REPORT_PROMPT_TEMPLATE = """\
## DAILY CIO BRIEFING — {date}
Portfolio: {portfolio_name}

You are the senior CIO and Risk Analyst for this options income portfolio.
Generate the structured daily briefing below. Be concise, specific, and actionable.
Cite actual tickers, strikes, DTE, and prices. No vague generalities.
Keep the total output under 900 words.

{web_context}

---
## SECTION 1 — STRATEGY PULSE
Assess the current market regime and whether the portfolio is executing per strategy.
Provide 2–4 bullet points covering:
- Regime: risk-on / risk-off / neutral
- Portfolio alignment with strategy rules
- 1–2 strategic opportunities or threats visible today

## SECTION 2 — PORTFOLIO SNAPSHOT
Provide a concise summary of the portfolio state. Include:
- Capital deployed ($ and %) vs available buying power
- Floating P&L by ticker (highlight largest winners and losers)
- CC coverage status (flag any uncovered or over-covered stock positions)
- Any concentration risk (sector or single-ticker >25% of capital)

## SECTION 3 — POSITION ACTIONS
For EVERY open option position, provide a recommended action.
Output EXACTLY a markdown table with columns:
| Ticker | Type | Strike | Expiry | DTE | Action | Reason |

Action must be one of: HOLD / ROLL / CLOSE / MONITOR / ADD
Sort by urgency: positions with DTE ≤ 7 first, then at-risk (within 5% of strike), then all others.

## SECTION 4 — RISK DASHBOARD
- Risk posture: GREEN / AMBER / RED (bold) — one sentence rationale
- Positions at risk (underlying within 5% of short strike)
- Earnings announcements within any position's DTE window
- Beta / directional exposure commentary
- Key macro or news events today that affect held tickers
---
"""


# ─────────────────────────────────────────────
# WEB SEARCH PRE-QUERY
# ─────────────────────────────────────────────

def _fetch_web_context(tickers: list[str], today: str) -> str:
    """
    Run targeted web searches for each ticker and return a formatted
    context block for injection into the LLM prompt.
    Uses the existing WebSearch class from web_search.py.
    """
    try:
        from web_search import WebSearch, is_web_search_enabled
        if not is_web_search_enabled():
            return ""

        searcher = WebSearch()
        blocks = []

        for ticker in tickers[:8]:  # cap at 8 tickers to keep prompt size manageable
            results = searcher.search_financial(
                ticker,
                query_type="news",
                max_results=2,
            )
            if results:
                blocks.append(f"**{ticker} News/Context:**")
                blocks.append(searcher.format_results_for_ai(results, max_chars=300))

        if not blocks:
            return ""

        return (
            "\n## LIVE MARKET CONTEXT (web search)\n"
            + "\n".join(blocks)
            + "\n"
        )
    except Exception as e:
        logger.warning("Web search for report failed: %s", e)
        return ""


# ─────────────────────────────────────────────
# SECTION PARSER
# ─────────────────────────────────────────────

def _parse_report_sections(markdown: str) -> dict:
    """
    Split the raw LLM markdown output into the 4 named sections.
    Returns a dict with keys: strategy, portfolio, actions, risk.
    Falls back to the full text in 'strategy' if parsing fails.
    """
    sections = {"strategy": "", "portfolio": "", "actions": "", "risk": ""}

    # Match section headers: "## SECTION N — TITLE"
    pattern = re.compile(
        r'##\s+SECTION\s+(\d)\s*[—\-]\s*(.*?)(?=##\s+SECTION\s+\d|$)',
        re.DOTALL | re.IGNORECASE,
    )
    matches = list(pattern.finditer(markdown))

    key_map = {
        "1": "strategy",
        "2": "portfolio",
        "3": "actions",
        "4": "risk",
    }

    if not matches:
        # Fallback: dump everything into strategy tab
        sections["strategy"] = markdown
        return sections

    for match in matches:
        num = match.group(1)
        content = match.group(0)
        key = key_map.get(num)
        if key:
            sections[key] = content.strip()

    return sections


# ─────────────────────────────────────────────
# REPORT GENERATION
# ─────────────────────────────────────────────

def generate_report(
    ai_chat_client,
    df_open: Optional[pd.DataFrame],
    df_trades: Optional[pd.DataFrame],
    portfolio_deposit: float,
    portfolio_name: str,
    live_prices: dict,
    model_name: str,
    web_search_enabled: bool,
) -> str:
    """
    Generate the 4-section CIO daily briefing.
    Reuses AIChat.get_system_context() and get_data_summary() for portfolio context.
    Returns raw markdown string.
    """
    today = datetime.now().strftime("%A, %d %B %Y")

    # 1. Build portfolio context (reuse existing AIChat methods)
    system_ctx = ai_chat_client.get_system_context(
        df_trades=df_trades,
        df_open=df_open,
        portfolio_deposit=portfolio_deposit,
        current_page="CIO Report",
    )
    data_summary = ai_chat_client.get_data_summary(
        df_trades=df_trades,
        df_open=df_open,
        portfolio=portfolio_name,
    )

    # 2. Web search context
    tickers = []
    if df_open is not None and not df_open.empty and "Ticker" in df_open.columns:
        tickers = df_open["Ticker"].dropna().unique().tolist()

    web_context = _fetch_web_context(tickers, today) if web_search_enabled else ""

    # 3. Build the report prompt
    report_prompt = REPORT_PROMPT_TEMPLATE.format(
        date=today,
        portfolio_name=portfolio_name,
        web_context=web_context,
    )

    full_prompt = (
        system_ctx
        + "\n\n"
        + data_summary
        + "\n\n"
        + report_prompt
    )

    # 4. Resolve the correct model name — Gemini sets gemini_model_name, Claude sets model_name
    resolved_model = (
        getattr(ai_chat_client, "model_name", None)           # Claude path
        or getattr(ai_chat_client, "gemini_model_name", None) # Gemini path
        or model_name                                          # fallback: passed-in param
    )

    # Detect which backend to use via client_type (always set for both branches)
    client_type = getattr(ai_chat_client, "client_type", "claude")

    # 5. Call the LLM (reuse AIChat infrastructure)
    try:
        if client_type == "gemini":
            # Gemini path
            import google.genai.types as genai_types  # type: ignore

            tools = []
            if web_search_enabled:
                try:
                    tools = [genai_types.Tool(google_search=genai_types.GoogleSearch())]
                except Exception:
                    tools = []

            kwargs = dict(
                model=resolved_model,
                contents=full_prompt,
            )
            if tools:
                kwargs["config"] = genai_types.GenerateContentConfig(tools=tools)

            if getattr(ai_chat_client, 'use_new_genai_sdk', True):
                # New SDK: client is self.gemini_client
                response = ai_chat_client.gemini_client.models.generate_content(**kwargs)
                return response.text or "Report generation returned empty response."
            else:
                # Old SDK fallback: client is self.model
                response = ai_chat_client.model.generate_content(full_prompt)
                return response.text or "Report generation returned empty response."

        else:
            # Claude path — use Anthropic client directly
            import anthropic  # type: ignore

            claude_kwargs = dict(
                model=resolved_model,
                max_tokens=4096,
                system=system_ctx,
                messages=[{"role": "user", "content": data_summary + "\n\n" + report_prompt}],
            )
            if web_search_enabled:
                claude_kwargs["tools"] = [{"type": "web_search_20250305", "name": "web_search", "max_uses": 5}]

            response = ai_chat_client.client.messages.create(**claude_kwargs)

            # Extract text from content blocks
            text_parts = []
            for block in response.content:
                if hasattr(block, "text"):
                    text_parts.append(block.text)
            return "\n".join(text_parts) or "Report generation returned empty response."

    except Exception as e:
        logger.error("Report generation LLM call failed: %s", e)
        return f"⚠️ Report generation failed: {e}"


# ─────────────────────────────────────────────
# UI — REPORT PANEL (main content area)
# ─────────────────────────────────────────────

def render_daily_report_panel():
    """
    Render the persistent CIO Report page.
    Loads the last saved report and displays it in 4 tabs.
    Called from app.py routing when page == '📋 CIO Report'.
    """
    report = load_daily_report()

    st.title("📋 CIO Daily Briefing")

    if not report:
        st.info(
            "No report generated yet. Open the **📋 Daily CIO Report** expander "
            "in the AI Assistant sidebar and click **🔄 Generate Report**."
        )
        return

    # ── Header strip ──────────────────────────────────────
    col_p, col_m, col_t, col_age = st.columns([2, 2, 2, 1])
    col_p.markdown(f"**Portfolio:** {report.get('portfolio', '—')}")
    col_m.markdown(f"**Model:** {report.get('model', '—')}")
    col_t.markdown(f"**Generated:** {report.get('generated_at', '—')}")

    # Freshness indicator
    try:
        gen_dt = datetime.strptime(report.get("generated_at", ""), "%Y-%m-%d %H:%M")
        age_h = (datetime.now() - gen_dt).total_seconds() / 3600
        if age_h < 4:
            col_age.success("🟢 Fresh")
        elif age_h < 12:
            col_age.warning("🟡 Today")
        else:
            col_age.error("🔴 Stale")
    except Exception:
        col_age.caption("—")

    st.divider()

    # ── Parse and display sections ─────────────────────────
    markdown = report.get("markdown", "")
    sections = _parse_report_sections(markdown)

    tab_strategy, tab_portfolio, tab_actions, tab_risk = st.tabs([
        "🎯 Strategy Pulse",
        "📊 Portfolio Snapshot",
        "⚡ Position Actions",
        "🛡️ Risk Dashboard",
    ])

    with tab_strategy:
        if sections["strategy"]:
            st.markdown(sections["strategy"])
        else:
            st.info("Strategy Pulse section not found in report.")

    with tab_portfolio:
        if sections["portfolio"]:
            st.markdown(sections["portfolio"])
        else:
            st.info("Portfolio Snapshot section not found in report.")

    with tab_actions:
        if sections["actions"]:
            st.markdown(sections["actions"])
        else:
            st.info("Position Actions section not found in report.")

    with tab_risk:
        if sections["risk"]:
            st.markdown(sections["risk"])
        else:
            st.info("Risk Dashboard section not found in report.")

    # ── Raw report expander ────────────────────────────────
    with st.expander("📄 Full raw report", expanded=False):
        st.markdown(markdown)
