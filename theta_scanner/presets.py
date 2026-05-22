"""Saved-state for the Scanner — filter presets, column layouts, watchlist.

Persists through ARGUS's `persistence` layer (local JSON + Google Sheets
backup), namespaced under dedicated settings keys so it survives Cloud
restarts. Degrades to a no-op if persistence is unavailable.
"""
from __future__ import annotations

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

_FILTERS_KEY = "theta_scanner_filter_presets"
_LAYOUTS_KEY = "theta_scanner_column_layouts"
_WATCHLIST_KEY = "theta_scanner_watchlist"


def _load_settings() -> dict:
    try:
        import persistence
        return persistence.load_settings() or {}
    except Exception as e:
        logger.warning("presets: load_settings unavailable: %s", e)
        return {}


def _save_settings(settings: dict) -> bool:
    try:
        import persistence
        persistence.save_settings(settings)
        return True
    except Exception as e:
        logger.warning("presets: save_settings failed: %s", e)
        return False


def _get_map(key: str) -> dict:
    val = _load_settings().get(key)
    return dict(val) if isinstance(val, dict) else {}


def _put_map(key: str, value: dict) -> bool:
    settings = _load_settings()
    settings[key] = value
    return _save_settings(settings)


# ─── Filter presets ────────────────────────────────────────────────


def load_filter_presets() -> Dict[str, dict]:
    return _get_map(_FILTERS_KEY)


def save_filter_preset(name: str, state: dict) -> bool:
    name = (name or "").strip()
    if not name:
        return False
    presets = load_filter_presets()
    presets[name] = dict(state)
    return _put_map(_FILTERS_KEY, presets)


def delete_filter_preset(name: str) -> bool:
    presets = load_filter_presets()
    if name in presets:
        del presets[name]
        return _put_map(_FILTERS_KEY, presets)
    return False


# ─── Column layouts ────────────────────────────────────────────────


def load_column_layouts() -> Dict[str, List[str]]:
    return _get_map(_LAYOUTS_KEY)


def save_column_layout(name: str, columns: List[str]) -> bool:
    name = (name or "").strip()
    if not name:
        return False
    layouts = load_column_layouts()
    layouts[name] = list(columns)
    return _put_map(_LAYOUTS_KEY, layouts)


def delete_column_layout(name: str) -> bool:
    layouts = load_column_layouts()
    if name in layouts:
        del layouts[name]
        return _put_map(_LAYOUTS_KEY, layouts)
    return False


# ─── Watchlist ─────────────────────────────────────────────────────


def load_watchlist() -> List[str]:
    val = _load_settings().get(_WATCHLIST_KEY)
    if isinstance(val, list):
        return [str(t).upper() for t in val]
    return []


def save_watchlist(tickers: List[str]) -> bool:
    clean = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    settings = _load_settings()
    settings[_WATCHLIST_KEY] = clean
    return _save_settings(settings)
