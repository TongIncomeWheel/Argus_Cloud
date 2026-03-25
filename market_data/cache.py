"""
TTL-based in-memory cache.
Pure stdlib — no Streamlit dependency so the service works outside of Streamlit.
"""
import time
from typing import Any, Optional


class TTLCache:
    """
    Simple time-to-live cache backed by a plain dict.
    Thread-safety is not required for single-process Streamlit apps.
    """

    def __init__(self, ttl_seconds: int = 300):
        self._ttl = ttl_seconds
        self._store: dict = {}

    def get(self, key: str) -> Optional[Any]:
        """Return cached value if still within TTL, else None."""
        entry = self._store.get(key)
        if entry is None:
            return None
        value, ts = entry
        if time.time() - ts < self._ttl:
            return value
        del self._store[key]
        return None

    def set(self, key: str, value: Any) -> None:
        """Store value with current timestamp."""
        self._store[key] = (value, time.time())

    def clear(self) -> None:
        """Invalidate all cached entries."""
        self._store.clear()

    def invalidate(self, key: str) -> None:
        """Invalidate a single cache entry."""
        self._store.pop(key, None)
