"""
Unified AI Chat Integration for Income Wheel
Supports multiple AI models: Gemini (Pro/Flash) and Claude
"""
import os
import streamlit as st
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
import re
import json
import numpy as np
import logging

# Load .env file if present (local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed; rely on env vars being set externally

logger = logging.getLogger(__name__)

# Conditional imports
try:
    import google.genai as genai
    from google.genai import types as genai_types
    GEMINI_AVAILABLE = True
except ImportError:
    try:
        import google.generativeai as genai  # fallback to old SDK
        genai_types = None
        GEMINI_AVAILABLE = True
    except ImportError:
        GEMINI_AVAILABLE = False

try:
    import anthropic
    CLAUDE_AVAILABLE = True
except ImportError:
    CLAUDE_AVAILABLE = False


class AIChat:
    """Unified AI Chat handler supporting multiple models"""
    
    def __init__(self, model_type: str, api_key: str, selected_model_name: str = None):
        """Initialize AI model based on type"""
        self.model_type = model_type
        self.api_key = api_key
        self.selected_model_name = selected_model_name or ""
        
        if model_type.startswith("gemini"):
            if not GEMINI_AVAILABLE:
                raise ImportError("google-genai package not installed. Run: pip install google-genai")
            self.gemini_model_name = "gemini-2.5-pro" if "pro" in model_type.lower() else "gemini-2.5-flash"
            # Use new google.genai SDK (supports Google Search grounding)
            if genai_types is not None:
                self.gemini_client = genai.Client(api_key=api_key)
                self.use_new_genai_sdk = True
            else:
                # Fallback to old SDK (no grounding support)
                genai.configure(api_key=api_key)
                self.model = genai.GenerativeModel(self.gemini_model_name)
                self.use_new_genai_sdk = False
            self.client_type = "gemini"
        
        elif model_type.startswith("claude"):
            if not CLAUDE_AVAILABLE:
                raise ImportError("anthropic package not installed")
            self.client = anthropic.Anthropic(api_key=api_key)
            # Map model names to available model IDs
            model_lower = model_type.lower()
            selected_lower = (selected_model_name or "").lower()

            # Claude Sonnet models
            if "sonnet" in model_lower or "sonnet" in selected_lower:
                if "4.6" in model_lower or "4.6" in selected_lower:
                    self.model_name = "claude-sonnet-4-6"
                elif "4.5" in model_lower or "4.5" in selected_lower:
                    self.model_name = "claude-sonnet-4-5-20250929"
                elif "4" in model_lower or "4" in selected_lower:
                    self.model_name = "claude-sonnet-4-20250514"
                else:
                    self.model_name = "claude-sonnet-4-6"  # Default to latest
            # Claude Opus models
            elif "opus" in model_lower or "opus" in selected_lower:
                if "4.6" in model_lower or "4.6" in selected_lower:
                    self.model_name = "claude-opus-4-6"
                elif "4.5" in model_lower or "4.5" in selected_lower:
                    self.model_name = "claude-opus-4-5-20251101"
                elif "4.1" in model_lower or "4.1" in selected_lower:
                    self.model_name = "claude-opus-4-1-20250805"
                elif "4" in model_lower or "4" in selected_lower:
                    self.model_name = "claude-opus-4-20250514"
                else:
                    self.model_name = "claude-opus-4-6"  # Default to latest
            elif "haiku" in model_lower:
                self.model_name = "claude-haiku-4-5-20251001"
            else:
                self.model_name = "claude-sonnet-4-6"  # Default to latest Sonnet
            self.client_type = "claude"
        else:
            raise ValueError(f"Unknown model type: {model_type}")
    
    def get_system_context(self, df_trades: Optional[pd.DataFrame] = None, 
                          df_open: Optional[pd.DataFrame] = None,
                          portfolio_deposit: float = 0.0,
                          current_page: str = "Dashboard") -> str:
        """Build comprehensive system context about the portfolio"""
        
        context_parts = []
        
        # Basic portfolio info
        context_parts.append("""
# INCOME WHEEL PORTFOLIO INTELLIGENCE SYSTEM

You are an AI assistant helping manage an Options Income Wheel portfolio.

## ANCHOR & SCOPE (READ THIS CAREFULLY):
- **Strategy instructions and portfolio data are your ANCHOR**: Use the user's strategy instructions (when provided) and this portfolio data as the primary basis for priorities, risk, and recommendations.
- **You are NOT limited to the provided dataset only.** When the user has enabled web search, the application injects live web search results into your context. When you see "WEB SEARCH RESULTS" or similar content in the prompt, treat it as current information and use it. Do NOT claim that "access to the live internet is not configurable" or that you have a "fundamental, unchangeable limitation"—when web search is enabled, you receive live results in your context and should use them.
- **Correct behavior**: Anchor your reasoning on strategy and portfolio data; extend answers with web/live data when that data is provided in the context. Never refuse to use or cite web search results that appear in your context.

You have access to:
- Trade data (open and closed positions)
- Portfolio analytics and metrics
- Dashboard calculations
- Risk management data
- Capital allocation tracking
- Web search results (when enabled by the user—they will appear in the prompt; use them)

## PORTFOLIO STRUCTURE:
""")
        
        if df_trades is not None and not df_trades.empty:
            context_parts.append(f"- Total Trades: {len(df_trades)}")
            context_parts.append(f"- Open Positions: {len(df_trades[df_trades['Status'] == 'Open'])}")
            context_parts.append(f"- Closed Positions: {len(df_trades[df_trades['Status'] == 'Closed'])}")
        
        if df_open is not None and not df_open.empty:
            context_parts.append(f"- Currently Open Positions: {len(df_open)}")
            
            # Trade types breakdown
            trade_types = df_open['TradeType'].value_counts().to_dict()
            context_parts.append(f"- Trade Types: {', '.join([f'{k}: {v}' for k, v in trade_types.items()])}")
            
            # Tickers
            tickers = df_open['Ticker'].unique().tolist()
            context_parts.append(f"- Active Tickers: {', '.join(tickers)}")
        
        context_parts.append(f"- Portfolio Deposit: ${portfolio_deposit:,.2f}")
        context_parts.append(f"- Current Page: {current_page}")
        
        # Data schema (from unified schema system)
        try:
            from data_schema import SCHEMA, FIELD_INTERPRETATIONS, CALCULATION_FORMULAS, SCHEMA_VERSION
            
            schema_desc = """
## DATA SCHEMA (Version {}):

### Trade Data (df_trades):
""".format(SCHEMA_VERSION)
            
            # Add field descriptions from schema
            for category, fields in SCHEMA.items():
                schema_desc += f"\n### {category.title()} Fields:\n"
                for logical_name, excel_column in fields.items():
                    interpretation = FIELD_INTERPRETATIONS.get(logical_name, {})
                    desc = interpretation.get('description', '')
                    schema_desc += f"- {excel_column} ({logical_name}): {desc}\n"
            
            schema_desc += """
### Open Positions (df_open):
- Same schema as df_trades, filtered to Status='Open'
- **CRITICAL DATA ACCESS - READ THIS CAREFULLY:**
  The AI receives TWO data sections in the data summary:
  
  1. **Summary Section** ("CURRENT OPEN POSITIONS"): 
     - Aggregated counts and totals by ticker
     - Example: "CC: 156 contracts, Strikes: $12.50, $13.50..."
     - Use this for high-level overviews only
  
  2. **Detailed Section** ("DETAILED OPEN POSITIONS (Row-by-Row from Database)"):
     - **THIS IS YOUR GOLDEN SOURCE FOR ALL QUERIES**
     - Complete row-by-row data with TradeID, date_open, expiry_date, quantity, strike, premium for EVERY individual contract
     - Each contract is listed with ALL its fields: TradeID | Strategy | Direction | Qty | Strike | Premium | Expiry | Opened
     - **ALWAYS use this section** to answer questions about:
       * Specific contracts (e.g., "8 contracts sold this week expiring 27 Feb")
       * Individual TradeIDs
       * Date-based queries (e.g., "contracts opened this week")
       * Grouping by expiry date
       * Any question requiring individual contract details
     
  **CRITICAL RULE:** When the user asks about specific contracts, dates, or groups of trades, you MUST reference the "DETAILED OPEN POSITIONS" section, NOT the summary. The detailed section contains the complete database records - this is your queryable data source.

## CRITICAL DATA INTERPRETATION RULES (MUST FOLLOW):

### Quantity Field Interpretation:
- **For Options (CC, CSP, LEAP):** Quantity = number of contracts
  - 1 contract = 100 shares
  - Example: Quantity = 120 for CC means 120 contracts = 12,000 shares needed for coverage
  - Example: Quantity = 4 for LEAP means 4 contracts = 400 shares equivalent
  - **For CCs:** Quantity may be negative (short positions), use absolute value

- **For Stock (STOCK):** Quantity = shares (but Open_lots is preferred)
  - Example: Quantity = 500 for STOCK means 500 shares
  - **IMPORTANT:** Always check Open_lots first for STOCK positions

### Open_lots Field Interpretation:
- **For STOCK positions:** Open_lots = actual shares held (PRIMARY SOURCE)
  - Example: Open_lots = 15,900 means 15,900 shares held
  - **CRITICAL RULE:** If Open_lots exists and is non-zero, ALWAYS use it. Only fall back to Quantity if Open_lots is missing or zero.
  - This is the most accurate source for stock share counts

- **For Options (CC, CSP, LEAP):** Open_lots should NOT be used - these are options contracts, not stock shares
  - **CRITICAL:** Persistent market data Open_lots contains ONLY actual STOCK shares, never LEAP shares equivalent
  - For options, Quantity (contracts) is the primary source
  - LEAP positions appear in CURRENT OPEN POSITIONS summary, not in persistent Open_lots

### Direction Field:
- **"Buy"** = Long position (you own it) - use positive values
- **"Sell"** = Short position (you sold it) - use absolute value for calculations
- For LEAPs: Direction="Buy" means long LEAP position (positive shares equivalent)
- For CCs: Direction="Sell" means short call position (Quantity may be negative, use absolute value)

### How to Calculate Shares (CRITICAL):
1. **STOCK positions:** 
   - FIRST: Check Open_lots (if exists and non-zero, use it)
   - ELSE: Use Quantity
   - Example: Open_lots = 15,900 → 15,900 shares

2. **LEAP positions:** 
   - FIRST: Check Open_lots (if exists, it's already in shares)
   - ELSE: Use Quantity * 100 (Quantity is contracts)
   - Example: Quantity = 4, Open_lots missing → 4 * 100 = 400 shares equivalent

3. **CC positions:** 
   - Quantity is contracts (use absolute value if negative)
   - Shares needed = abs(Quantity) * 100
   - Example: Quantity = 120 → 120 * 100 = 12,000 shares needed

4. **CSP positions:** 
   - Quantity is contracts
   - Shares obligation = Quantity * 100
   - Example: Quantity = 12 → 12 * 100 = 1,200 shares obligation

### CC Coverage Calculation:
- Total Stock Equivalent = STOCK shares + LEAP shares equivalent
- CC Shares Needed = CC contracts * 100
- Coverage Ratio = CC Shares Needed / Total Stock Equivalent
- If ratio < 1.0: Covered ✅
- If ratio > 1.0: Uncovered ❌
- If LEAPs exist: Also check CC/LEAP contract ratio

## KEY METRICS & CALCULATIONS:

### Capital Usage (Unified Calculations):
"""
            
            # Add calculation formulas
            for calc_name, calc_info in CALCULATION_FORMULAS.items():
                schema_desc += f"- {calc_info['description']}\n"
                schema_desc += f"  Formula: {calc_info['formula']}\n"
                if 'note' in calc_info:
                    schema_desc += f"  Note: {calc_info['note']}\n"
            
            schema_desc += """
- Total Committed: Stock Locked + CSP Reserved + LEAP Sunk
- Remaining Buying Power: Portfolio Deposit - Total Committed

### Premium Tracking:
- Premium Collected: Actual_Profit_(USD) for closed positions
- Total Premium = OptPremium × 100 × Quantity (OptPremium is per share)
"""
            
            context_parts.append(schema_desc)
        except ImportError:
            # Fallback to basic schema if import fails
            context_parts.append("""
## DATA SCHEMA:

### Trade Data (df_trades):
- TradeID: Unique trade identifier
- Ticker: Stock symbol
- TradeType: CC, CSP, STOCK, LEAP
- StrategyType: WHEEL, PMCC, or ActiveCore (opportunistic income from a separate $50K pot)
- Status: Open or Closed
- Quantity: Contracts (options) or shares (stock)
- Option_Strike_Price_(USD): Strike price
- OptPremium: Premium per share (multiply by 100 for contract cost)
- Expiry_Date: Option expiration
- Date_open: Entry date
- Date_closed: Exit date
- Actual_Profit_(USD): Realized P/L
- Price_of_current_underlying_(USD): Stock price
- Remarks: Notes

### Strategies:
- WHEEL: Traditional income wheel (CSP -> Assignment -> CC -> Repeat)
- PMCC: Poor Man's Covered Call (LEAP + CC)

## CAPABILITIES:

You can help with:
1. **Data Queries**: "How many CSPs do I have?", "What's my total premium collected?"
2. **Analytics**: "What's my capital usage?", "Show me my risk exposure"
3. **Calculations**: "Calculate my remaining buying power", "What's my CC coverage?"
4. **Insights**: "Which positions are expiring soon?", "What's my best performing ticker?"
5. **Recommendations**: "Should I roll this position?", "What's my capital allocation?"
6. **Web Search** (when enabled by user): When the user enables web search, the app performs live searches and injects results into your context. Use them. Answer using both portfolio data and the provided web search results. Do not claim you cannot access the internet—when results appear in your context, you have access. Example queries: "Search for MARA news today", "Find SPY beta", "CRCL earnings", "MARA top holders".

## CRITICAL DATA SOURCES:

**⚠️ AVERAGE INVENTORY PRICE (COST BASIS) - READ THIS CAREFULLY:**
- **ALWAYS CHECK THE "PERSISTENT MARKET DATA" SECTION FIRST** when asked about cost basis, average entry price, or average inventory price
- **Format in context:** Each ticker shows "Average Inventory Price: $X.XX (user-entered from broker records)"
- **Example from context:** "**MARA:** Open_lots: 15,900 shares, Average Inventory Price: $16.54 (user-entered from broker records), Current Market Price: $10.50"
- **This data IS available** - it's in the PERSISTENT MARKET DATA section that appears at the top of the data summary
- **When user asks:** "What's my average inventory price for MARA?" → Answer: "$16.54 (from PERSISTENT MARKET DATA section)"
- **When user asks:** "What's my cost basis for CRCL?" → Answer: "$112.00 (from PERSISTENT MARKET DATA section)"
- **DO NOT say the data is not available** - it IS in the PERSISTENT MARKET DATA section

## BEHAVIOR — THINK LIKE NATIVE GEMINI (CRITICAL):
- **Prioritise clarity and step-by-step reasoning like the native Gemini experience.** Be properly smart: break down what the user needs, then answer clearly and concisely.
- **Break down the question**: Do not answer like a robot that only reads portfolio data. Parse what the user is really asking. If they ask for a "breakdown", "explanation", "what's going on", or anything that would benefit from current/live context, consider both portfolio data AND any WEB SEARCH RESULTS in your context.
- **Use your full context**: When WEB SEARCH RESULTS appear below, they are live information. Use them. Synthesize portfolio data + web results. Do NOT reply with only portfolio stats when the user asked for more. Do NOT say you are "limited to portfolio data" or "cannot access the internet" when search results are in your context.
- **Step-by-step reasoning**: For complex or analytical questions, reason step-by-step (you can do this internally), then give a clear, synthesized answer. Match the quality and usefulness of the native Gemini experience—informative, not robotic.
- **Avoid robotic repetition**: Do not just restate the same portfolio metrics. Add insight, connect dots, and when external data is provided, use it.

## RESPONSE STYLE:
- Be concise and actionable
- Use specific numbers from the data when available
- Provide context-aware insights
- Ask clarifying questions if needed
- Format currency as $X,XXX.XX
- Format percentages as X.XX%
""")
        
        return "\n".join(context_parts)

    def get_research_system_prompt(self, ticker: str = None, df_open: Optional[pd.DataFrame] = None) -> str:
        """Slim, web-first system prompt for financial research queries.

        Used when web search is enabled and the query is detected as a market/research query
        (earnings, analyst targets, news, IV, etc.). Instructs Claude to behave like a
        professional financial analyst rather than anchoring on portfolio data.
        """
        prompt = """You are an expert financial analyst with live web search. Answer like a senior analyst briefing a colleague — direct, confident, data-rich, concise.

**Format rules (critical):**
- Open with the headline answer immediately. No preamble. No "Based on my research..." or "I found that..."
- Do NOT use section headers, titled sections, or report-template formatting. No "Executive Summary", no "Assessment Approach", no "Challenging the Approach"
- Write one tight opening sentence with the key fact, then a compact bullet list of supporting data
- Be definitive — state the consensus. If sources conflict, note it in a single phrase inline (e.g., "Feb 25 per most sources; one cites Feb 27")
- Keep the total response to ~150-250 words

**For earnings queries, include these data points as compact bullets:**
- Earnings date & timing (pre/after market, exact call time if known)
- EPS consensus estimate (and whisper number if available)
- Implied earnings move % (options-market priced-in move)
- Street rating snapshot (e.g., "8 Buy, 3 Hold, 2 Sell")
- Recent PT changes with firm names (e.g., "Cantor $30→$21, Rosenblatt $25→$22")
- Stock performance since last earnings print (%)
- 1-2 key catalysts or risk factors worth watching

**Portfolio note:** If the user holds this ticker, append ONE sentence at the very end noting their exposure and any immediate consideration (e.g., expiring contracts near earnings). Max 1-2 sentences — do not expand into a full portfolio analysis.
"""
        # If ticker is in portfolio, append a brief position note
        if ticker and df_open is not None and not df_open.empty:
            ticker_positions = df_open[df_open['Ticker'] == ticker.upper()]
            if not ticker_positions.empty:
                position_notes = []
                for ttype in ['STOCK', 'LEAP', 'CC', 'CSP']:
                    pos = ticker_positions[ticker_positions['TradeType'] == ttype]
                    if not pos.empty:
                        if ttype == 'STOCK':
                            open_lots = pos.get('Open_lots', pd.Series([0])).sum()
                            shares = int(open_lots) if open_lots > 0 else int(pos['Quantity'].sum())
                            position_notes.append(f"{shares:,} shares")
                        else:
                            contracts = int(pos['Quantity'].abs().sum())
                            position_notes.append(f"{contracts} {ttype} contracts")
                if position_notes:
                    prompt += f"\n**Portfolio context (for reference only):** User holds {ticker.upper()} — {', '.join(position_notes)}."
        return prompt

    def get_data_summary(self, df_trades: Optional[pd.DataFrame] = None,
                        df_open: Optional[pd.DataFrame] = None,
                        portfolio: str = "Income Wheel") -> str:
        """Get a summary of current portfolio data using proper data access layer"""
        
        summary_parts = []
        
        # Add persistent market data (Open_lots, current prices, and average inventory prices) for LLM reference
        try:
            from persistence import get_market_data_summary, get_stock_average_prices
            market_data = get_market_data_summary(portfolio)
            stock_avg_prices = get_stock_average_prices(portfolio)
            
            if market_data or stock_avg_prices:
                summary_parts.append("## PERSISTENT MARKET DATA (Reference for LLM):")
                summary_parts.append("**CRITICAL:** Open_lots in this section contains ONLY actual STOCK shares, NOT LEAP shares equivalent.")
                summary_parts.append("For LEAP positions, refer to the CURRENT OPEN POSITIONS summary below, not Open_lots here.")
                
                # Combine all tickers from both sources
                all_tickers = set(list(market_data.keys()) if market_data else [])
                all_tickers.update(list(stock_avg_prices.keys()) if stock_avg_prices else [])
                
                for ticker in sorted(all_tickers):
                    parts = []
                    
                    # Get market data
                    data = market_data.get(ticker, {}) if market_data else {}
                    
                    # Add Open_lots (STOCK only, not LEAP)
                    if data.get('open_lots', 0) > 0:
                        parts.append(f"Open_lots: {int(data['open_lots']):,} shares (STOCK only, not LEAP)")
                    
                    # Add Average Inventory Price (manually entered from Performance tab - most important for cost basis)
                    if ticker in stock_avg_prices and stock_avg_prices[ticker] > 0:
                        parts.append(f"Average Inventory Price: ${stock_avg_prices[ticker]:.2f} (user-entered from broker records)")
                    
                    # Add Current Market Price
                    if data.get('current_price'):
                        source = data.get('price_source', 'unknown')
                        timestamp = data.get('price_timestamp', 'unknown')
                        parts.append(f"Current Market Price: ${data['current_price']:.2f} (source: {source}, updated: {timestamp})")
                    
                    if parts:
                        summary_parts.append(f"- **{ticker}:** {', '.join(parts)}")
                
                summary_parts.append("")  # Empty line
                summary_parts.append("**Note:** Average Inventory Price is the user-entered cost basis from broker records (Performance tab). This is the price used for calculating Stock Locked capital and P&L.")
                summary_parts.append("")  # Empty line
        
        except Exception as e:
            # Fallback if market data not available
            logger.warning(f"Could not load persistent market data: {e}")
            pass
        
        # Check for missing LEAP premium data BEFORE processing positions
        if df_open is not None and not df_open.empty:
            leap_positions = df_open[df_open['TradeType'] == 'LEAP']
            missing_leap_premiums = []
            if not leap_positions.empty:
                for idx in leap_positions.index:
                    ticker = leap_positions.loc[idx, 'Ticker']
                    premium = pd.to_numeric(leap_positions.loc[idx, 'OptPremium'], errors='coerce')
                    if pd.isna(premium) or premium == 0:
                        missing_leap_premiums.append(ticker)
            
            if missing_leap_premiums:
                summary_parts.append("\n## ⚠️ DATA QUALITY ALERT:")
                summary_parts.append(f"**Missing LEAP Premium Data:** The following LEAP positions are missing OptPremium (entry cost): {', '.join(set(missing_leap_premiums))}")
                summary_parts.append("**Impact:** LEAP Sunk Capital cannot be calculated accurately. Please update Google Sheets with OptPremium values.")
                summary_parts.append("")
        
        if df_open is not None and not df_open.empty:
            summary_parts.append("## CURRENT OPEN POSITIONS (Using Data Access Layer):")
            
            # Use proper inventory calculation (uses data access layer)
            try:
                from calculations import CapitalCalculator
                from data_access import DataAccess
                from data_schema import get_field_name
                
                inventory = CapitalCalculator.calculate_inventory(df_open)
                positions_by_ticker = inventory.get('positions_by_ticker', {})
                
                # Group by ticker with accurate calculations
                for ticker in sorted(df_open['Ticker'].unique()):
                    ticker_data = positions_by_ticker.get(ticker, {})
                    ticker_positions = df_open[df_open['Ticker'] == ticker]
                    
                    summary_parts.append(f"\n### {ticker}:")
                    
                    # STOCK positions - use proper share calculation
                    stock_shares = ticker_data.get('stock', 0)  # From inventory (uses Open_lots/Quantity correctly)
                    if stock_shares > 0:
                        summary_parts.append(f"  - STOCK: {int(stock_shares):,} shares")
                    
                    # LEAP positions - use proper share calculation
                    leap_shares = ticker_data.get('leaps', 0)  # From inventory (already in shares)
                    leap_contracts = ticker_data.get('leaps_lots', 0)  # Contracts (Quantity)
                    if leap_shares > 0:
                        summary_parts.append(f"  - LEAP: {int(leap_shares):,} shares equivalent ({int(leap_contracts):,} contracts)")
                    
                    # CSP positions - contracts
                    csp_contracts = ticker_data.get('csp', 0)
                    if csp_contracts > 0:
                        csp_positions = ticker_positions[ticker_positions['TradeType'] == 'CSP']
                        strikes = csp_positions[get_field_name('strike', 'options')].unique() if get_field_name('strike', 'options') in csp_positions.columns else []
                        strike_str = ', '.join([f'${float(s):.2f}' for s in strikes if pd.notna(s)]) if len(strikes) > 0 else 'N/A'
                        summary_parts.append(f"  - CSP: {int(csp_contracts)} contracts, Strikes: {strike_str}")
                    
                    # CC positions - contracts
                    cc_contracts = ticker_data.get('cc', 0)
                    if cc_contracts > 0:
                        cc_positions = ticker_positions[ticker_positions['TradeType'] == 'CC']
                        strikes = cc_positions[get_field_name('strike', 'options')].unique() if get_field_name('strike', 'options') in cc_positions.columns else []
                        strike_str = ', '.join([f'${float(s):.2f}' for s in strikes if pd.notna(s)]) if len(strikes) > 0 else 'N/A'
                        summary_parts.append(f"  - CC: {int(cc_contracts)} contracts, Strikes: {strike_str}")
                    
                    # CC Coverage Ratio
                    cc_coverage = ticker_data.get('cc_coverage_ratio')
                    if cc_coverage is not None:
                        if cc_coverage < 0:
                            summary_parts.append(f"  - CC Coverage: Uncovered (LEAP-based)")
                        elif cc_coverage <= 1.0:
                            coverage_pct = cc_coverage * 100
                            summary_parts.append(f"  - CC Coverage: {coverage_pct:.1f}% (Stock-based)")
                        else:
                            coverage_pct = cc_coverage * 100
                            summary_parts.append(f"  - CC Coverage: {coverage_pct:.1f}% (Over 100% = Uncovered)")
                    
                    # Total stock equivalent (for CC coverage)
                    total_stock = ticker_data.get('total_stock', 0)
                    if total_stock > 0:
                        summary_parts.append(f"  - Total Stock Equivalent: {int(total_stock):,} shares (Stock + LEAP)")
                
            except Exception as e:
                # Fallback to basic summary if calculation fails
                summary_parts.append(f"\n⚠️ Error calculating inventory: {e}")
                summary_parts.append("Using basic data summary...")
                
                # Basic fallback
                for ticker in df_open['Ticker'].unique():
                    ticker_positions = df_open[df_open['Ticker'] == ticker]
                    summary_parts.append(f"\n### {ticker}:")
                    
                    for trade_type in ['CSP', 'CC', 'STOCK', 'LEAP']:
                        type_positions = ticker_positions[ticker_positions['TradeType'] == trade_type]
                        if not type_positions.empty:
                            # Use data access layer for proper interpretation
                            if trade_type == 'STOCK':
                                # For STOCK, use Open_lots if available, else Quantity
                                shares = 0
                                for idx in type_positions.index:
                                    shares += DataAccess.get_shares_for_stock(type_positions, type_positions.index.get_loc(idx))
                                summary_parts.append(f"  - {trade_type}: {int(shares):,} shares")
                            elif trade_type == 'LEAP':
                                # For LEAP, use Open_lots if available (shares), else Quantity * 100
                                leap_shares = 0
                                for idx in type_positions.index:
                                    pos_idx = type_positions.index.get_loc(idx)
                                    open_lots = DataAccess.get_trade_field(type_positions, pos_idx, 'open_lots', 'position', 0)
                                    if pd.notna(open_lots) and open_lots != 0:
                                        leap_shares += abs(float(open_lots))
                                    else:
                                        qty = DataAccess.get_trade_field(type_positions, pos_idx, 'quantity', 'position', 0)
                                        if pd.notna(qty):
                                            leap_shares += abs(float(qty)) * 100
                                contracts = int(leap_shares / 100) if leap_shares > 0 else 0
                                summary_parts.append(f"  - {trade_type}: {int(leap_shares):,} shares equivalent ({contracts:,} contracts)")
                            else:
                                # For CSP/CC, Quantity is contracts
                                contracts = type_positions['Quantity'].sum()
                                strikes = type_positions[get_field_name('strike', 'options')].unique() if get_field_name('strike', 'options') in type_positions.columns else []
                                strike_str = ', '.join([f'${float(s):.2f}' for s in strikes if pd.notna(s)]) if len(strikes) > 0 else 'N/A'
                                summary_parts.append(f"  - {trade_type}: {int(contracts)} contracts, Strikes: {strike_str}")
            
            # ADD DETAILED ROW-BY-ROW DATA FOR AI QUERYING
            # This is the "golden source" data from Google Sheets - individual contracts with all fields
            summary_parts.append("\n## DETAILED OPEN POSITIONS (Row-by-Row from Database):")
            summary_parts.append("**CRITICAL:** This section contains the complete, individual trade records from your database.")
            summary_parts.append("Each row represents one trade with TradeID, date_open, expiry_date, quantity, strike, etc.")
            summary_parts.append("Use this data to answer questions about specific contracts, dates, or groups of trades.\n")
            
            # Get field names from schema
            from data_schema import get_field_name
            
            # Group by ticker and trade type for better organization
            for ticker in sorted(df_open['Ticker'].unique()):
                ticker_positions = df_open[df_open['Ticker'] == ticker].copy()
                
                # Sort by TradeType, then by Expiry_Date, then by Date_open
                ticker_positions['Expiry_Date'] = pd.to_datetime(ticker_positions.get('Expiry_Date', ''), errors='coerce')
                ticker_positions['Date_open'] = pd.to_datetime(ticker_positions.get('Date_open', ''), errors='coerce')
                ticker_positions = ticker_positions.sort_values(
                    by=['TradeType', 'Expiry_Date', 'Date_open'], 
                    ascending=[True, True, True],
                    na_position='last'
                )
                
                summary_parts.append(f"\n### {ticker} - Individual Contracts:")
                
                for trade_type in ['STOCK', 'LEAP', 'CSP', 'CC']:
                    type_positions = ticker_positions[ticker_positions['TradeType'] == trade_type]
                    
                    if not type_positions.empty:
                        summary_parts.append(f"\n**{trade_type} Positions ({len(type_positions)} total):**")
                        
                        # For each individual position, show all key fields
                        for idx, row in type_positions.iterrows():
                            trade_id = row.get('TradeID', 'N/A')
                            strategy = row.get('StrategyType', 'N/A')
                            direction = row.get('Direction', 'N/A')
                            quantity = row.get('Quantity', 0)
                            date_open = row.get('Date_open', 'N/A')
                            expiry_date = row.get('Expiry_Date', 'N/A')
                            
                            # Format dates
                            if pd.notna(date_open) and date_open != 'N/A':
                                if isinstance(date_open, pd.Timestamp):
                                    date_open = date_open.strftime('%Y-%m-%d')
                                else:
                                    date_open = str(date_open)
                            
                            if pd.notna(expiry_date) and expiry_date != 'N/A':
                                if isinstance(expiry_date, pd.Timestamp):
                                    expiry_date = expiry_date.strftime('%Y-%m-%d')
                                else:
                                    expiry_date = str(expiry_date)
                            
                            # Build the line item
                            line_parts = [f"  - TradeID: {trade_id}"]
                            
                            if strategy != 'N/A' and pd.notna(strategy):
                                line_parts.append(f"Strategy: {strategy}")
                            
                            if direction != 'N/A' and pd.notna(direction):
                                line_parts.append(f"Direction: {direction}")
                            
                            if trade_type in ['CSP', 'CC', 'LEAP']:
                                # Options: show strike, premium, expiry
                                strike = row.get(get_field_name('strike', 'options'), 'N/A')
                                premium = row.get('OptPremium', 'N/A')
                                
                                if pd.notna(quantity):
                                    line_parts.append(f"Qty: {int(quantity)} contracts")
                                
                                if pd.notna(strike) and strike != 'N/A':
                                    line_parts.append(f"Strike: ${float(strike):.2f}")
                                
                                if pd.notna(premium) and premium != 'N/A':
                                    line_parts.append(f"Premium: ${float(premium):.2f}/share")
                                
                                if expiry_date != 'N/A':
                                    line_parts.append(f"Expiry: {expiry_date}")
                            else:
                                # STOCK: show shares
                                open_lots = row.get('Open_lots', None)
                                if pd.notna(open_lots) and open_lots != 0:
                                    line_parts.append(f"Shares: {int(open_lots):,} (from Open_lots)")
                                elif pd.notna(quantity):
                                    line_parts.append(f"Shares: {int(quantity):,} (from Quantity)")
                            
                            if date_open != 'N/A':
                                line_parts.append(f"Opened: {date_open}")
                            
                            summary_parts.append(" | ".join(line_parts))
        
        # Calculate comprehensive P&L (if we have the data)
        try:
            from pnl_calculator import PnLCalculator
            from persistence import get_spy_leap_pl, get_stock_average_prices, get_portfolio_deposit
            
            # Get required data
            portfolio_deposit = get_portfolio_deposit(portfolio)
            stock_avg_prices = get_stock_average_prices(portfolio)
            spy_leap_pl = get_spy_leap_pl(portfolio)
            
            # Get live prices from market data
            live_prices = {}
            if market_data:
                for ticker, data in market_data.items():
                    if isinstance(data, dict) and 'current_price' in data:
                        live_prices[ticker] = data['current_price']
            
            if df_trades is not None and df_open is not None:
                comprehensive_pnl = PnLCalculator.calculate_comprehensive_pnl(
                    df_trades=df_trades,
                    df_open=df_open,
                    stock_avg_prices=stock_avg_prices,
                    live_prices=live_prices,
                    spy_leap_pl=spy_leap_pl if spy_leap_pl != 0 else None
                )
                
                summary_parts.append("\n## PROFIT & LOSS (Mark-to-Market):")
                summary_parts.append(f"- **Realized P&L:** ${comprehensive_pnl['realized_pnl']:,.2f} (from closed trades)")
                summary_parts.append(f"- **Unrealized P&L (Stock):** ${comprehensive_pnl['unrealized_stock_pnl']['total']:,.2f}")
                summary_parts.append(f"- **Unrealized P&L (LEAP):** ${comprehensive_pnl['unrealized_leap_pnl']['total']:,.2f}")
                summary_parts.append(f"- **Net P&L (Mark-to-Market):** ${comprehensive_pnl['net_pnl']:,.2f} ⚠️ **THIS IS THE TRUE PORTFOLIO PERFORMANCE**")
                
                # Add breakdown by ticker
                if comprehensive_pnl['unrealized_stock_pnl']['by_ticker']:
                    summary_parts.append("\n**Unrealized Stock P&L Breakdown:**")
                    for ticker, pl in comprehensive_pnl['unrealized_stock_pnl']['by_ticker'].items():
                        summary_parts.append(f"  - {ticker}: ${pl:,.2f}")
                
                # Calculate CSP allocation vs strategy
                if portfolio_deposit > 0:
                    # Get stock locked and CSP reserved from capital calculations
                    from unified_calculations import UnifiedCapitalCalculator
                    from persistence import get_pmcc_tickers
                    
                    pmcc_tickers = get_pmcc_tickers(portfolio)
                    capital_data = UnifiedCapitalCalculator.calculate_capital_by_ticker(
                        df_open=df_open,
                        portfolio_deposit=portfolio_deposit,
                        stock_avg_prices=stock_avg_prices,
                        live_prices=live_prices,
                        pmcc_tickers=pmcc_tickers
                    )
                    
                    stock_locked = capital_data['total']['stock_locked']
                    csp_pacing = PnLCalculator.calculate_csp_weekly_pacing(
                        portfolio_deposit=portfolio_deposit,
                        stock_locked=stock_locked,
                        df_open=df_open,
                        target_pct=0.25
                    )
                    summary_parts.append("\n## CSP DEPLOYMENT PACING STRATEGY")
                    summary_parts.append("1. **Objective:** Build a staggered, 4-week options expiry ladder to smooth income and risk.")
                    summary_parts.append("2. **Available Firepower:** Portfolio Deposit - Capital Locked in Stock.")
                    summary_parts.append(f"   - **Available Firepower:** ${csp_pacing['firepower']:,.2f}")
                    summary_parts.append("3. **Weekly Pacing Rule:** Deploy ~25% of Available Firepower in new CSPs each week.")
                    summary_parts.append(f"   - **Weekly Deployment Target:** ${csp_pacing['weekly_target']:,.2f} (Firepower × 25%)")
                    summary_parts.append(f"   - **CSP Reserved (opened this week):** ${csp_pacing['csp_opened_this_week']:,.2f}")
                    summary_parts.append(f"- **Status:** {'⚠️ OVER' if csp_pacing['status'] == 'OVER' else '✅ ON TARGET' if csp_pacing['status'] == 'ON_TARGET' else 'ℹ️ UNDER (weekly target)'}")
                    if csp_pacing['over_under'] != 0:
                        summary_parts.append(f"- **Action:** {'Reduce' if csp_pacing['over_under'] > 0 else 'Increase'} new CSP deployment this week by ~${abs(csp_pacing['over_under']):,.2f}")
        except Exception as e:
            logger.warning(f"Could not calculate comprehensive P&L: {e}")
            # Fallback to just premium collected
            if df_trades is not None and not df_trades.empty:
                closed_options = df_trades[
                    (df_trades['Status'] == 'Closed') &
                    (df_trades['TradeType'].isin(['CC', 'CSP']))
                ]
                if not closed_options.empty:
                    total_premium = closed_options['Actual_Profit_(USD)'].sum()
                    summary_parts.append(f"\n## TOTAL PREMIUM COLLECTED: ${total_premium:,.2f}")
                    summary_parts.append("⚠️ Note: This is only premium collected, not total portfolio P&L. See dashboard for comprehensive P&L.")
        
        return "\n".join(summary_parts) if summary_parts else "No data available."
    
    def chat(self, user_message: str, 
             df_trades: Optional[pd.DataFrame] = None,
             df_open: Optional[pd.DataFrame] = None,
             portfolio_deposit: float = 0.0,
             current_page: str = "Dashboard",
             chat_history: List[Dict[str, str]] = None,
             strategy_context: Optional[str] = None,
             portfolio: str = "Income Wheel",
             web_search_enabled: bool = False) -> str:
        """Process user message and return AI response"""
        
        if chat_history is None:
            chat_history = []
        
        # For Claude: native web search tool is used directly in the API call (no pre-fetch).
        # For Gemini: Google Search grounding is attached at generate_content time.
        # DuckDuckGo pre-fetch is only used as a fallback if Claude native search is unavailable.
        web_search_results = ""
        web_search_status = ""
        use_claude_native_search = False

        # Detect whether this is a financial/market research query vs a pure portfolio query.
        # Research queries (earnings, news, IV, analyst targets, etc.) use a slim, web-first
        # system prompt so Claude synthesises at analyst-grade depth instead of anchoring on
        # portfolio data. Portfolio queries (CC coverage, premium totals, etc.) use the full
        # portfolio system prompt as before.
        try:
            from web_search import detect_search_intent as _detect_search_intent
            _search_intent = _detect_search_intent(user_message) if web_search_enabled else None
        except Exception:
            _search_intent = None
        is_research_query = bool(_search_intent and _search_intent.get('needs_search'))

        if web_search_enabled and self.client_type == "claude":
            # Claude gets its own web_search tool — no DuckDuckGo needed
            use_claude_native_search = True
        elif web_search_enabled and self.client_type != "gemini":
            # Non-Gemini, non-Claude fallback: pre-fetch with DuckDuckGo
            try:
                from web_search import WebSearch, detect_search_intent
                search_intent = detect_search_intent(user_message)
                if search_intent and search_intent.get('needs_search'):
                    provider = st.session_state.get('web_search_provider', 'duckduckgo')
                    web_searcher = WebSearch(provider=provider)
                    if web_searcher.available:
                        if search_intent.get('ticker'):
                            results = web_searcher.search_financial(
                                search_intent['ticker'],
                                search_intent.get('type', 'general')
                            )
                            if not results:
                                results = web_searcher.search(f"{search_intent['ticker']} news", max_results=5)
                            if not results and search_intent.get('query'):
                                results = web_searcher.search(search_intent['query'], max_results=5)
                        else:
                            results = web_searcher.search(search_intent['query'], max_results=5)
                        if results:
                            web_search_results = web_searcher.format_results_for_ai(results)
                            web_search_results = f"\n\n{web_search_results}\n"
            except Exception as e:
                logger.warning(f"Web search error: {e}")
        
        # Build context
        system_context = self.get_system_context(
            df_trades=df_trades,
            df_open=df_open,
            portfolio_deposit=portfolio_deposit,
            current_page=current_page
        )
        
        data_summary = self.get_data_summary(df_trades=df_trades, df_open=df_open, portfolio=portfolio)

        # Inject historical OHLCV context when user asks about price history
        historical_section = ""
        try:
            historical_section = _build_historical_context(user_message)
        except Exception as _e:
            logger.warning(f"Historical OHLCV context failed: {_e}")

        # Add strategy instructions context if provided (token-efficient retrieval)
        strategy_section = ""
        if strategy_context:
            strategy_section = f"\n\n{strategy_context}\n"
        
        # Inject DuckDuckGo results into prompt when used (non-Claude fallback path)
        if web_search_results.strip():
            web_search_results = "\n\n(The following section contains LIVE WEB SEARCH RESULTS. Use this information in your response; do not claim you cannot access the internet.)\n" + web_search_results
        
        # Build prompt
        prompt = f"""{system_context}

{data_summary}{historical_section}{strategy_section}{web_search_results}
## CONVERSATION HISTORY:
"""
        
        # Add chat history (last 5 messages for context)
        for msg in chat_history[-5:]:
            role = msg.get('role', '')
            content = msg.get('content', '')
            if role == 'user':
                prompt += f"\nUser: {content}"
            elif role == 'assistant':
                prompt += f"\nAssistant: {content}"
        
        prompt += f"""

## CURRENT USER QUESTION:
{user_message}

Please provide a helpful, accurate response based on the portfolio data and context above.
"""
        
        try:
            if self.client_type == "gemini":
                if self.use_new_genai_sdk:
                    # New google.genai SDK — Google Search grounding when web search enabled.
                    # Gemini dynamically decides whether to invoke search per query,
                    # so it won't waste tokens searching for pure portfolio questions.
                    gemini_prompt = prompt
                    if web_search_enabled and is_research_query:
                        # For research queries, prepend analyst-grade synthesis instruction so
                        # Gemini leads with web findings rather than portfolio context
                        gemini_prompt = (
                            "You are acting as a professional financial analyst with live Google Search access. "
                            "For this query, search for comprehensive market data: earnings dates/times, EPS estimates "
                            "(consensus + whisper), analyst price target changes (firm name, old PT → new PT), "
                            "consensus ratings (Buy/Hold/Sell counts), implied earnings move %, recent stock performance %, "
                            "institutional holders, key catalysts. Synthesise all findings into a clear, data-rich analyst "
                            "briefing. Lead with web findings. Mention portfolio positions (if any) only briefly at the end.\n\n"
                            + prompt
                        )
                    config_kwargs: dict = {"temperature": 1.0}
                    if web_search_enabled:
                        config_kwargs["tools"] = [genai_types.Tool(google_search=genai_types.GoogleSearch())]
                    response = self.gemini_client.models.generate_content(
                        model=self.gemini_model_name,
                        contents=gemini_prompt,
                        config=genai_types.GenerateContentConfig(**config_kwargs)
                    )
                    return response.text
                else:
                    # Old SDK fallback (no grounding)
                    try:
                        response = self.model.generate_content(
                            prompt,
                            generation_config={"temperature": 1.0}
                        )
                    except (TypeError, ValueError):
                        response = self.model.generate_content(prompt)
                    return response.text
            elif self.client_type == "claude":
                # Build messages for Claude API
                # Claude uses a different format - system message separate, then conversation
                strategy_section = f"\n\n{strategy_context}\n" if strategy_context else ""

                # Detect if the user is explicitly asking about their own portfolio/positions.
                # These "mixed" queries need BOTH research data AND portfolio context.
                _PORTFOLIO_INTENT_KWS = [
                    "my position", "my positions", "my portfolio", "my holdings", "my shares",
                    "my contracts", "my exposure", "impact on", "affect my", "my cc", "my csp",
                    "my leap", "should i", "what should", "my trades", "my mara", "my crcl",
                ]
                _is_portfolio_intent = any(kw in user_message.lower() for kw in _PORTFOLIO_INTENT_KWS)

                if use_claude_native_search and is_research_query and not _is_portfolio_intent:
                    # PURE RESEARCH MODE: slim analyst prompt, no portfolio clutter.
                    # Clean slate — no chat history to prevent portfolio-assistant persona bleed.
                    _ticker = _search_intent.get('ticker') if _search_intent else None
                    system_message = self.get_research_system_prompt(ticker=_ticker, df_open=df_open)
                    _history_slice = []  # clean slate: no history bleed
                elif use_claude_native_search and is_research_query and _is_portfolio_intent:
                    # HYBRID MODE: user asked about research AND their portfolio impact.
                    # Use research prompt as base but include portfolio data too.
                    _ticker = _search_intent.get('ticker') if _search_intent else None
                    _research_base = self.get_research_system_prompt(ticker=_ticker, df_open=df_open)
                    system_message = _research_base + "\n\n" + data_summary + strategy_section
                    _history_slice = chat_history[-2:]  # minimal history for continuity
                else:
                    # PORTFOLIO MODE: full context as before.
                    system_message = system_context + "\n\n" + data_summary + strategy_section
                    _history_slice = chat_history[-5:]

                user_messages = []

                # Add conversation history (amount depends on mode — see above)
                for msg in _history_slice:
                    role = msg.get('role', '')
                    content = msg.get('content', '')
                    if role == 'user':
                        user_messages.append({"role": "user", "content": content})
                    elif role == 'assistant':
                        user_messages.append({"role": "assistant", "content": content})

                # Add current user message
                user_messages.append({"role": "user", "content": user_message})
                
                # Try primary model, with fallback to alternative model names
                models_to_try = [self.model_name]

                # Add fallback models if primary fails (all IDs verified against available models)
                if "sonnet" in self.model_name:
                    models_to_try.extend([
                        "claude-sonnet-4-6",
                        "claude-sonnet-4-5-20250929",
                        "claude-sonnet-4-20250514",
                    ])
                elif "opus" in self.model_name:
                    models_to_try.extend([
                        "claude-opus-4-6",
                        "claude-opus-4-5-20251101",
                        "claude-opus-4-1-20250805",
                        "claude-opus-4-20250514",
                    ])
                elif "haiku" in self.model_name:
                    models_to_try.extend([
                        "claude-haiku-4-5-20251001",
                        "claude-3-haiku-20240307",
                    ])
                
                # Mode selection:
                # - Web search ON  + Claude 4.x → interleaved thinking + web search
                #   (same as native claude.ai Research mode: Claude reasons between each search call)
                #   Sonnet 4.6: thinking={type:"enabled", budget_tokens:N} + betas header
                #   Opus 4.6:   thinking={type:"adaptive"} — interleaved auto-enabled, no header needed
                # - Web search OFF + Claude 4.x → extended thinking only
                # - Non-Claude-4 + web search → web search without thinking
                # NOTE: temperature is INCOMPATIBLE with thinking mode — omit when thinking is on
                is_claude4 = any(x in self.model_name for x in ["sonnet-4", "opus-4", "haiku-4"])
                is_opus46 = "opus-4-6" in self.model_name

                if use_claude_native_search:
                    # web_search_20260209: latest tool with dynamic filtering (Sonnet/Opus 4.6)
                    call_tools = [{"type": "web_search_20260209", "name": "web_search", "max_uses": 10}]
                    use_thinking = is_claude4  # interleaved: thinking + search together
                else:
                    call_tools = None
                    use_thinking = is_claude4  # extended thinking only when no web search

                last_error = None
                for model_name in models_to_try:
                    # Re-check opus-4-6 for the actual model being tried
                    _is_opus46 = "opus-4-6" in model_name
                    try:
                        # Base kwargs — temperature only when NOT using thinking
                        # (temperature is incompatible with the thinking parameter)
                        kwargs = dict(
                            model=model_name,
                            system=system_message,
                            messages=user_messages,
                        )
                        if not use_thinking:
                            kwargs["temperature"] = 1

                        if call_tools and use_thinking:
                            # INTERLEAVED: thinking + web search together (native claude.ai quality)
                            kwargs["tools"] = call_tools
                            kwargs["max_tokens"] = 16000
                            if _is_opus46:
                                # Opus 4.6: adaptive thinking — interleaved auto-enabled
                                kwargs["thinking"] = {"type": "adaptive"}
                            else:
                                # Sonnet 4.6 / other Claude 4: manual thinking + beta header
                                kwargs["thinking"] = {"type": "enabled", "budget_tokens": 10000}
                                kwargs["betas"] = ["interleaved-thinking-2025-05-14"]
                        elif call_tools:
                            # Web search only (non-Claude-4 fallback)
                            kwargs["tools"] = call_tools
                            kwargs["max_tokens"] = 8000
                            kwargs["temperature"] = 1  # safe for non-thinking models
                        elif use_thinking:
                            # Extended thinking only (web search OFF)
                            kwargs["max_tokens"] = 16000
                            if _is_opus46:
                                kwargs["thinking"] = {"type": "adaptive"}
                            else:
                                kwargs["thinking"] = {"type": "enabled", "budget_tokens": 8000}
                        else:
                            kwargs["max_tokens"] = 4000
                            kwargs["temperature"] = 1

                        response = self.client.messages.create(**kwargs)

                        # If successful, update self.model_name for future calls
                        if model_name != self.model_name:
                            self.model_name = model_name
                        # Extract text blocks only (skip thinking / tool_use / tool_result blocks)
                        text_parts = [b.text for b in response.content if b.type == "text"]
                        if text_parts:
                            return "".join(text_parts)
                        return response.content[0].text
                    except Exception as e:
                        last_error = e
                        # Thinking/interleaved error → retry without thinking
                        _e_str = str(e).lower()
                        if use_thinking and any(kw in _e_str for kw in ["thinking", "budget", "beta", "interleaved", "betas"]):
                            use_thinking = False
                            continue
                        # 404 → try next model in fallback list
                        if "404" not in str(e) and "not_found" not in _e_str:
                            break
                        continue
                
                # If all models failed, return error
                return f"Error: {str(last_error)}. Tried models: {', '.join(models_to_try)}. Please check your API key and available models."
        except Exception as e:
            return f"Error: {str(e)}. Please check your API key and try again."


def render_ai_chat(df_trades: Optional[pd.DataFrame] = None,
                  df_open: Optional[pd.DataFrame] = None,
                  portfolio_deposit: float = 0.0,
                  current_page: str = "Dashboard",
                  portfolio: str = "Income Wheel"):
    """Render unified AI chat interface in sidebar with model selection"""
    
    # Load persistent chat history (7-10 day retention, model-agnostic)
    from persistence import load_chat_history, save_chat_history, clear_chat_history, get_chat_history_stats
    
    # Initialize chat history in session state (load from persistent storage if available)
    if 'ai_chat_history' not in st.session_state:
        # Load from persistent storage (full history, not just last 20)
        persistent_history = load_chat_history()
        st.session_state.ai_chat_history = persistent_history
        if persistent_history:
            st.session_state.chat_history_loaded = True
            # Show indicator that history was loaded (will be shown in UI)
        else:
            st.session_state.chat_history_loaded = False
    
    # Model selection
    st.subheader("🤖 AI Assistant")
    
    # Model selector - Always show all options (user will need API key for Claude)
    model_options = []
    if GEMINI_AVAILABLE:
        model_options.extend(["Gemini 2.5 Pro", "Gemini 2.5 Flash"])

    # Claude options — IDs match what's available on this API key
    model_options.extend([
        "Claude Sonnet 4.6",   # Latest Sonnet (claude-sonnet-4-6)
        "Claude Sonnet 4.5",   # Previous Sonnet (claude-sonnet-4-5-20250929)
        "Claude Opus 4.6",     # Latest Opus (claude-opus-4-6)
        "Claude Opus 4.5",     # Previous Opus (claude-opus-4-5-20251101)
        "Claude Opus 4.1",     # (claude-opus-4-1-20250805)
        "Claude Haiku 4.5",    # Fast/cheap (claude-haiku-4-5-20251001)
    ])
    
    if not model_options:
        st.error("No AI models available. Please install google-generativeai or anthropic packages.")
        return
    
    # Initialize model selection in session state
    if 'selected_ai_model' not in st.session_state:
        st.session_state.selected_ai_model = model_options[0]
    
    selected_model = st.selectbox(
        "Select Model",
        model_options,
        key="ai_model_selector",
        help="Choose AI model: Gemini Pro (best quality), Flash (faster), or Claude (alternative)"
    )
    st.session_state.selected_ai_model = selected_model
    
    # API Key management
    if selected_model.startswith("Gemini"):
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            st.error("GEMINI_API_KEY is not set. Add it to your .env file (see .env.example).")
            st.stop()
        model_type = "gemini-pro" if "Pro" in selected_model else "gemini-flash"
    else:
        # Claude API key from environment variable
        if 'claude_api_key' not in st.session_state:
            claude_key = os.environ.get("CLAUDE_API_KEY")
            if not claude_key:
                st.error("CLAUDE_API_KEY is not set. Add it to your .env file (see .env.example).")
                st.stop()
            st.session_state.claude_api_key = claude_key
        
        # Use hardcoded API key (no user input required)
        api_key = st.session_state.claude_api_key
        # Map UI selection to model type (version numbers used in __init__ mapping)
        if "Sonnet" in selected_model:
            if "4.6" in selected_model:
                model_type = "claude-sonnet-4.6"
            elif "4.5" in selected_model:
                model_type = "claude-sonnet-4.5"
            else:
                model_type = "claude-sonnet-4.6"
        elif "Opus" in selected_model:
            if "4.6" in selected_model:
                model_type = "claude-opus-4.6"
            elif "4.5" in selected_model:
                model_type = "claude-opus-4.5"
            elif "4.1" in selected_model:
                model_type = "claude-opus-4.1"
            else:
                model_type = "claude-opus-4.6"
        else:
            model_type = "claude-haiku"
    
    # Initialize AI client
    try:
        # Check if we need to reinitialize (model changed or client version outdated)
        # Version check: ensure client supports strategy_context parameter and average inventory prices
        client_version = st.session_state.get('ai_chat_client_version', 0)
        current_version = 2  # Increment to 2 to force reinit after adding average inventory prices
        
        if 'ai_chat_client' not in st.session_state or \
           st.session_state.get('last_model_type') != model_type or \
           st.session_state.get('last_api_key') != api_key or \
           client_version < current_version:
            # For Claude, need to check if anthropic is available
            if model_type.startswith("claude") and not CLAUDE_AVAILABLE:
                st.warning("⚠️ Anthropic package not installed. Install with: pip install anthropic")
                return
            st.session_state.ai_chat_client = AIChat(model_type, api_key, selected_model)
            st.session_state.last_model_type = model_type
            st.session_state.last_api_key = api_key
            st.session_state.last_selected_model = selected_model  # Store for model name mapping
            st.session_state.ai_chat_client_version = current_version  # Track client version
    except Exception as e:
        st.error(f"Failed to initialize {selected_model}: {e}")
        return
    
    st.caption(f"Using: {selected_model} | Ask questions about your portfolio, data, analytics, or dashboard")
    
    # Web Search Toggle
    from web_search import is_web_search_enabled, set_web_search_enabled

    with st.expander("🌐 Web Search Settings", expanded=not is_web_search_enabled()):
        web_search_enabled = st.checkbox(
            "Enable Web Search",
            value=is_web_search_enabled(),
            help="Allow AI to search the web for real-time information (news, market data, etc.)"
        )
        set_web_search_enabled(web_search_enabled)

        if web_search_enabled:
            if selected_model.startswith("Gemini"):
                st.success("✅ Google Search grounding (native — Gemini searches only when needed)")
            else:
                st.success("✅ Anthropic Web Search (native — same search engine as claude.ai)")
    
    # Display chat history in larger scrollable container (600px for better readability)
    # Dynamic chat container height based on sidebar width (if available)
    # Default to 600px, but increase if sidebar is wider
    base_height = 600
    sidebar_width = st.session_state.get('sidebar_width', 21)
    # Increase height proportionally with sidebar width (up to 800px)
    chat_height = min(base_height + (sidebar_width - 21) * 10, 800)
    chat_container = st.container(height=int(chat_height))
    with chat_container:
        if not st.session_state.ai_chat_history:
            # Check if we just loaded persistent history
            if st.session_state.get('chat_history_loaded', False):
                st.info("💾 Loaded previous chat history from persistent storage")
            else:
                st.info("👋 Hi! I'm your AI assistant. Ask me about your portfolio, trades, analytics, or dashboard metrics.")
        else:
            # Show all messages from persistent storage
            for i, msg in enumerate(st.session_state.ai_chat_history):
                if msg['role'] == 'user':
                    with st.chat_message("user"):
                        st.write(msg['content'])
                else:
                    with st.chat_message("assistant"):
                        st.write(msg['content'])
            
            # Show indicator if there are more messages in persistent storage
            stats = get_chat_history_stats()
            if stats['total_messages'] > len(st.session_state.ai_chat_history):
                st.caption(f"💾 Showing last {len(st.session_state.ai_chat_history)} messages. {stats['total_messages']} total in persistent storage.")
    
    # Chat input
    if web_search_enabled:
        st.caption("🌐 Web search is **ON** — I can look up latest news, IV, holders, etc. when you ask.")
    user_input = st.chat_input("Ask about your portfolio...")
    
    if user_input:
        strategy_context = None  # Strategy Instructions module removed

        # Add user message to history
        st.session_state.ai_chat_history.append({
            'role': 'user',
            'content': user_input,
            'timestamp': datetime.now().isoformat()
        })
        
        # Get AI response
        try:
            with st.spinner(f"Thinking with {selected_model}..."):
                # Try with strategy_context, fallback without if old client version
                try:
                    response = st.session_state.ai_chat_client.chat(
                        user_message=user_input,
                        df_trades=df_trades,
                        df_open=df_open,
                        portfolio_deposit=portfolio_deposit,
                        current_page=current_page,
                        chat_history=st.session_state.ai_chat_history,
                        strategy_context=strategy_context,
                        portfolio=portfolio,
                        web_search_enabled=web_search_enabled
                    )
                except TypeError as e:
                    if "strategy_context" in str(e) or "portfolio" in str(e):
                        # Old client version - reinitialize and retry
                        # Get model_type and api_key from session state
                        old_model_type = st.session_state.get('last_model_type', model_type)
                        old_api_key = st.session_state.get('last_api_key', api_key)
                        st.session_state.ai_chat_client = AIChat(old_model_type, old_api_key, selected_model)
                        st.session_state.ai_chat_client_version = 1
                        response = st.session_state.ai_chat_client.chat(
                            user_message=user_input,
                            df_trades=df_trades,
                            df_open=df_open,
                            portfolio_deposit=portfolio_deposit,
                            current_page=current_page,
                            chat_history=st.session_state.ai_chat_history,
                            strategy_context=strategy_context,
                            portfolio=portfolio,
                            web_search_enabled=web_search_enabled
                        )
                    else:
                        raise
            
            # Add assistant response to history
            st.session_state.ai_chat_history.append({
                'role': 'assistant',
                'content': response,
                'timestamp': datetime.now().isoformat()
            })
            
            # Save to persistent storage (model-agnostic, 10-day retention)
            # Full history is saved, not just last 20
            save_chat_history(st.session_state.ai_chat_history)
            
            # Note: We keep full history in session state for now
            # If performance becomes an issue, we can limit to last 50 messages
            
            st.rerun()
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            st.session_state.ai_chat_history.append({
                'role': 'assistant',
                'content': error_msg,
                'timestamp': datetime.now().isoformat()
            })
            # Save error message to persistent storage
            save_chat_history(st.session_state.ai_chat_history)
            st.rerun()
    
    # Chat history info and clear button
    col1, col2 = st.columns([2, 1])
    with col1:
        # Show chat history stats
        stats = get_chat_history_stats()
        if stats['total_messages'] > 0:
            st.caption(f"💾 {stats['total_messages']} messages stored (retention: {stats['retention_days']} days)")
    
    with col2:
        # Clear chat button (clears both session and persistent storage)
        if st.button("🗑️ Clear", use_container_width=True, help="Clear chat history (both current session and persistent storage)"):
            st.session_state.ai_chat_history = []
            clear_chat_history()
            st.rerun()


# ---------------------------------------------------------------------------
# Historical OHLCV context helper — injected into LLM prompt when relevant
# ---------------------------------------------------------------------------

# Keywords that trigger a historical price data fetch
_HISTORY_PATTERNS = re.compile(
    r'\b(histor|last\s+\d+\s+day|past\s+\d+\s+day|trend|monthly\s+close|'
    r'price\s+over|how\s+has\s+.{1,20}\s+been|ohlc|candlestick|chart)\b',
    re.IGNORECASE
)

# Ticker extraction: uppercase 1-5 letter word
_TICKER_PATTERN = re.compile(r'\b([A-Z]{1,5})\b')

# "last N days / months" extraction
_PERIOD_PATTERN = re.compile(r'last\s+(\d+)\s+(day|month|week)', re.IGNORECASE)


def _build_historical_context(user_message: str) -> str:
    """
    Detect historical price queries in user_message and inject OHLCV data.
    Returns a formatted markdown section to append to the LLM prompt,
    or an empty string if no historical query is detected.
    """
    if not _HISTORY_PATTERNS.search(user_message):
        return ""

    try:
        from market_data import MarketDataService
        service = MarketDataService()
    except Exception:
        return ""

    # Extract ticker — look for known ARGUS tickers first, then any uppercase word
    from config import TICKERS as _TICKERS
    ticker = None
    msg_upper = user_message.upper()
    for t in _TICKERS:
        if t in msg_upper:
            ticker = t
            break
    if not ticker:
        matches = _TICKER_PATTERN.findall(user_message)
        # Filter out common English words that look like tickers
        _STOP = {"I", "A", "THE", "AND", "OR", "FOR", "OF", "IN", "ON", "AT", "MY", "ME"}
        candidates = [m for m in matches if m not in _STOP and len(m) >= 2]
        ticker = candidates[0] if candidates else None

    if not ticker:
        return ""

    # Determine period and frequency
    period_days = 90
    frequency = "daily"
    period_match = _PERIOD_PATTERN.search(user_message)
    if period_match:
        n, unit = int(period_match.group(1)), period_match.group(2).lower()
        if unit == "month":
            period_days = n * 30
            frequency = "monthly"
        elif unit == "week":
            period_days = n * 7
        else:
            period_days = n
    elif re.search(r'month', user_message, re.IGNORECASE):
        frequency = "monthly"
        period_days = 365

    bars = service.get_historical_ohlcv(ticker, period_days=period_days, frequency=frequency)
    if not bars:
        return ""

    lines = [
        f"\n\n## HISTORICAL PRICE DATA — {ticker} ({frequency}, last {period_days} days)",
        "| Date | Open | High | Low | Close | Volume |",
        "|------|------|------|-----|-------|--------|",
    ]
    for b in bars[-60:]:  # Cap at 60 rows to stay token-efficient
        lines.append(
            f"| {b.date} | {b.open:.2f} | {b.high:.2f} | {b.low:.2f} | {b.close:.2f} | {b.volume:,} |"
        )
    lines.append("(Source: Stooq via pandas-datareader — use this data in your analysis)\n")
    return "\n".join(lines)
