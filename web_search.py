"""
Web Search Integration for AI Chat
Enables AI to search the web for real-time information
"""
import streamlit as st
import logging
from typing import List, Dict, Optional
import time

logger = logging.getLogger(__name__)

# Try to import web search libraries
try:
    from duckduckgo_search import DDGS
    DUCKDUCKGO_AVAILABLE = True
except ImportError:
    DUCKDUCKGO_AVAILABLE = False

try:
    from googlesearch import search as google_search
    GOOGLE_SEARCH_AVAILABLE = True
except ImportError:
    GOOGLE_SEARCH_AVAILABLE = False
    # Note: googlesearch-python package is available but may be rate-limited


class WebSearch:
    """Web search handler for AI chat"""
    
    def __init__(self, provider: str = "duckduckgo"):
        """
        Initialize web search
        
        Args:
            provider: 'duckduckgo' or 'google'
        """
        self.provider = provider
        self.available = False
        
        if provider == "duckduckgo" and DUCKDUCKGO_AVAILABLE:
            try:
                self.ddgs = DDGS()
                self.available = True
            except Exception as e:
                logger.warning(f"DuckDuckGo search not available: {e}")
                self.available = False
        elif provider == "google" and GOOGLE_SEARCH_AVAILABLE:
            self.available = True
        else:
            self.available = False
    
    def search(self, query: str, max_results: int = 5) -> List[Dict[str, str]]:
        """
        Search the web for information
        
        Args:
            query: Search query
            max_results: Maximum number of results to return
        
        Returns:
            List of dicts with 'title', 'url', 'snippet' keys
        """
        if not self.available:
            return []
        
        results = []
        
        try:
            if self.provider == "duckduckgo":
                # DuckDuckGo search (free, no API key needed)
                search_results = self.ddgs.text(query, max_results=max_results)
                for result in search_results:
                    results.append({
                        'title': result.get('title', ''),
                        'url': result.get('href', ''),
                        'snippet': result.get('body', '')
                    })
            
            elif self.provider == "google":
                # Google search (free, but may be rate-limited)
                search_results = google_search(query, num_results=max_results, lang='en')
                for url in search_results:
                    results.append({
                        'title': url,
                        'url': url,
                        'snippet': ''  # Google search doesn't provide snippets easily
                    })
            
            logger.info(f"Web search completed: {len(results)} results for query: {query}")
            return results
            
        except Exception as e:
            logger.error(f"Web search error: {e}")
            return []
    
    def search_financial(self, ticker: str, query_type: str = "news") -> List[Dict[str, str]]:
        """
        Search for financial information about a ticker
        
        Args:
            ticker: Stock ticker symbol
            query_type: 'news', 'analysis', 'earnings', etc.
        
        Returns:
            List of search results
        """
        if query_type == "news":
            query = f"{ticker} stock news"
        elif query_type == "analysis":
            query = f"{ticker} stock analysis price target"
        elif query_type == "earnings":
            query = f"{ticker} earnings report"
        elif query_type == "beta":
            query = f"{ticker} implied volatility IV beta stock"
        elif query_type == "ownership":
            query = f"{ticker} stock top holders institutional insider ownership"
        else:
            query = f"{ticker} stock {query_type}"
        
        return self.search(query, max_results=5)
    
    def format_results_for_ai(self, results: List[Dict[str, str]]) -> str:
        """
        Format search results for AI context
        
        Args:
            results: List of search result dicts
        
        Returns:
            Formatted string for AI prompt
        """
        if not results:
            return "No web search results found."
        
        formatted = "## WEB SEARCH RESULTS:\n\n"
        for i, result in enumerate(results, 1):
            formatted += f"{i}. **{result.get('title', 'No title')}**\n"
            formatted += f"   URL: {result.get('url', 'N/A')}\n"
            if result.get('snippet'):
                formatted += f"   Summary: {result.get('snippet', '')[:200]}...\n"
            formatted += "\n"
        
        return formatted


def is_web_search_enabled() -> bool:
    """Check if web search is enabled in session state. Default True so live queries work without user enabling."""
    return st.session_state.get('web_search_enabled', True)


def set_web_search_enabled(enabled: bool):
    """Set web search enabled state"""
    st.session_state.web_search_enabled = enabled


def get_web_search_provider() -> str:
    """Get current web search provider"""
    return st.session_state.get('web_search_provider', 'duckduckgo')


def set_web_search_provider(provider: str):
    """Set web search provider"""
    st.session_state.web_search_provider = provider


def detect_search_intent(user_message: str) -> Optional[Dict[str, str]]:
    """
    Detect if user message requires web search.
    Broad triggers so ARGUS behaves like native Gemini: breakdowns, explanations,
    ticker context, and explicit search requests all get live data when possible.
    """
    message_lower = user_message.lower().strip()
    if len(message_lower) < 2:
        return None

    # Explicit search / live-data keywords (broad so "live IV", "MARA news" etc. trigger)
    search_keywords = [
        'search', 'find', 'look up', 'google', 'web', 'internet',
        'current', 'latest', 'today', 'recent', 'news', 'update', 'live',
        'real-time', 'real time', 'realtime', 'iv', 'implied volatility',
        'beta', 'volatility', 'finbox', 'gfinance', 'correlation',
        'earnings', 'report', 'analysis', 'price target', 'target',
        'breakdown', 'break down', 'explain', 'why', 'what\'s going on',
        'holders', 'institutional', 'insider', 'ownership', 'top 10',
        'sentiment', 'catalyst', 'catalysts', 'driving', 'macro',
        'fomc', 'sec', 'filing', 'filings', 'key information', 'key info'
    ]

    # Question-style: user asking for explanation/context about something (often a ticker)
    question_words = ['what', 'why', 'how', 'who', 'when', 'explain', 'tell me', 'break down', 'breakdown', 'key info']
    looks_like_question = any(message_lower.startswith(w) or f' {w} ' in message_lower for w in question_words)

    # Common tickers (portfolio + common symbols)
    common_tickers = ['MARA', 'CRCL', 'SPY', 'AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA', 'AMD', 'META', 'AMZN']
    ticker_in_message = None
    for t in common_tickers:
        if t.lower() in message_lower:
            ticker_in_message = t
            break

    # Trigger 1: explicit search-style keywords
    needs_search = any(kw in message_lower for kw in search_keywords)

    # Trigger 2: ticker + question/context request (so "break down MARA" or "what should I know about CRCL" gets search)
    if not needs_search and ticker_in_message and (looks_like_question or len(message_lower.split()) >= 4):
        needs_search = True

    if not needs_search:
        return None

    # Determine search type
    search_type = "general"
    if any(w in message_lower for w in ['news', 'update', 'today', 'recent']):
        search_type = "news"
    elif any(w in message_lower for w in ['iv', 'implied volatility', 'volatility', 'beta', 'finbox', 'correlation']):
        search_type = "beta"   # beta/vol/IV use same financial query
    elif any(w in message_lower for w in ['earnings', 'report']):
        search_type = "earnings"
    elif any(w in message_lower for w in ['analysis', 'price target', 'target']):
        search_type = "analysis"
    elif any(w in message_lower for w in ['holders', 'institutional', 'insider', 'ownership', 'top 10']):
        search_type = "ownership"

    return {
        'needs_search': True,
        'query': user_message,
        'type': search_type,
        'ticker': ticker_in_message
    }
