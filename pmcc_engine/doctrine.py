"""Doctrine constants — the math is here, not in business logic.

Anything that's a numeric threshold from the PMCC doctrine lives in this
module so callers can import the named constant rather than scattering
magic numbers. If a threshold changes in a future doctrine revision, this
is the only file that needs to move.
"""
from __future__ import annotations

# ─── §1 Regime Stack ───────────────────────────────────────────────

# Vol bands are multiples of the ticker's 5-year median vol. The bands
# themselves are universal — the median is per-ticker state.
VOL_BAND_L_MAX = 1.0   # below 1× median = low
VOL_BAND_M_MAX = 1.4   # 1.0-1.4× = medium
VOL_BAND_H_MAX = 2.0   # 1.4-2.0× = high; above 2.0× = extreme

# IVR bands are universal — applied to a 52-week implied vol rank.
IVR_CHEAP_MAX = 25     # IVR < 25 → cheap
IVR_NEUTRAL_MAX = 50   # IVR 25-50 → neutral
IVR_RICH_MAX = 75      # IVR 50-75 → rich; above 75 → extreme


# Regime grid: (vol_band, ivr_band) → posture metadata.
# `dte_weeks` is a (min, max) tuple in weeks for short DTE selection.
# `posture` is a short code; `array` describes ITM/OTM allocation.
REGIME_GRID = {
    ("L", "cheap"):    {"posture": "defensive_flip",       "dte_weeks": (3, 4),  "array": "all_itm_3pct_below"},
    ("L", "neutral"):  {"posture": "standard_lean_theta",  "dte_weeks": (6, 6),  "array": "2_2"},
    ("L", "rich"):     {"posture": "standard",             "dte_weeks": (4, 5),  "array": "2_2"},
    ("L", "extreme"):  {"posture": "otm_skew",             "dte_weeks": (3, 4),  "array": "all_otm"},
    ("M", "cheap"):    {"posture": "otm_lean",             "dte_weeks": (5, 6),  "array": "2_2"},
    ("M", "neutral"):  {"posture": "base_case",            "dte_weeks": (4, 6),  "array": "3_3"},
    ("M", "rich"):     {"posture": "itm_harvest",          "dte_weeks": (4, 4),  "array": "itm_lean"},
    ("M", "extreme"):  {"posture": "fade_vol",             "dte_weeks": (3, 4),  "array": "all_otm"},
    ("H", "cheap"):    {"posture": "otm_lean",             "dte_weeks": (4, 4),  "array": "2_2_otm_lean"},
    ("H", "neutral"):  {"posture": "otm_lean",             "dte_weeks": (4, 4),  "array": "2_2_otm_lean"},
    ("H", "rich"):     {"posture": "all_otm",              "dte_weeks": (3, 3),  "array": "all_otm"},
    ("H", "extreme"):  {"posture": "all_otm_half_size",    "dte_weeks": (2, 3),  "array": "all_otm"},
    ("X", "cheap"):    {"posture": "stand_down",           "dte_weeks": None,    "array": None},
    ("X", "neutral"):  {"posture": "stand_down_or_half",   "dte_weeks": None,    "array": "all_otm_half"},
    ("X", "rich"):     {"posture": "all_otm_half_size",    "dte_weeks": (2, 2),  "array": "all_otm"},
    ("X", "extreme"):  {"posture": "all_otm_half_gamma",   "dte_weeks": (2, 2),  "array": "all_otm"},
}

POSTURE_DESCRIPTIONS = {
    "defensive_flip":      "Defensive flip: all 3% below spot, 3–4w DTE, recycle fast.",
    "standard_lean_theta": "Standard 2/2 array, lean 6w to harvest theta in low vol.",
    "standard":            "Standard 2/2 array, 4–5w DTE.",
    "otm_skew":            "Skew OTM, shortened DTE — mean-revert play.",
    "otm_lean":            "2/2 array OTM-leaning, 4-6w DTE.",
    "base_case":           "Base case: standard 2/2 ITM + 2/2 OTM array, 4–6w. Doctrine's sweet spot.",
    "itm_harvest":         "2/2 ITM-leaning, 4w — harvest rich premium.",
    "fade_vol":            "All OTM, 3–4w, fade extreme vol expansion.",
    "all_otm":             "All OTM array, 3w DTE.",
    "all_otm_half_size":   "All OTM, 2–3w, half size — preserve capital.",
    "stand_down":          "Stand down — wait for re-rank. Vol regime too extreme to deploy.",
    "stand_down_or_half":  "Stand down or all OTM half-size at most.",
    "all_otm_half_gamma":  "All OTM, 2w, half size, gamma-aware.",
}


# Plain-English description of array codenames used in REGIME_GRID["array"].
# Used by the UI so users never see cryptic codes like "2_2" or "all_itm_3pct_below".
ARRAY_DESCRIPTIONS = {
    "2_2":                  "2 ITM + 2 OTM short calls",
    "3_3":                  "3 ITM + 3 OTM short calls",
    "2_2_otm_lean":         "2 ITM + 2 OTM short calls (OTM-weighted)",
    "itm_lean":             "ITM-leaning short array",
    "all_otm":              "All shorts OTM",
    "all_otm_half":         "All shorts OTM, half size",
    "all_itm_3pct_below":   "All shorts ITM (~3% below spot, defensive flip)",
}


def array_description(code) -> str:
    """Translate a regime-grid array codename to plain English for UI display."""
    if not code:
        return "—"
    return ARRAY_DESCRIPTIONS.get(code, code)


# ─── §2 Theta Hurdle ───────────────────────────────────────────────

# Daily theta hurdle = (strike × HV30 / √252) × HURDLE_CAPTURE_RATE.
# Default 4% capture rate; tunable per regime if needed.
HURDLE_CAPTURE_RATE = 0.04
TRADING_DAYS_PER_YEAR = 252

# Yield ratio thresholds for book-level reporting.
YIELD_RATIO_PASS = 1.00
YIELD_RATIO_REGIME_CAVEAT_FLOOR = 0.80  # in sustained low-vol, this is acceptable


# ─── §3 Strike Selection ───────────────────────────────────────────

STRIKE_TARGET_PCT_BELOW_SPOT = 0.03   # ITM target band: 3% below spot
STRIKE_TARGET_PCT_ABOVE_SPOT = 0.03   # OTM target band: 3% above spot
STRIKE_BAND_MIN = 0.01
STRIKE_BAND_MAX = 0.05

# Hard floors
MIN_OPEN_INTEREST = 100               # contracts
MAX_BID_ASK_SPREAD_PCT = 0.05         # 5% of mid
MIN_TIME_VALUE_FLOOR = 6.0            # $6 extrinsic floor (per share)


# ─── §4 Capital Efficiency ─────────────────────────────────────────

CHASSIS_DELTA_BASELINE_MIN = 0.78
CHASSIS_DELTA_BASELINE_MAX = 0.82
CHASSIS_DELTA_DRIFT_FLOOR = 0.70      # below → refresh trigger
BRICK_DELTA_THRESHOLD = 0.95          # ≥ 0.95 → brick, run extraction math


# ─── §6 Ex-Dividend ────────────────────────────────────────────────

EX_DIV_TRIGGER_MULTIPLIER = 1.25     # extrinsic < 1.25 × dividend → mandatory roll
EX_DIV_WINDOW_TRADING_DAYS = 2       # within 2 TD of ex-div → check trigger


# ─── §8 Defensive Flip ─────────────────────────────────────────────

DEFENSIVE_FLIP_PCT_BELOW_SPOT = 0.97  # all shorts ≤ spot × 0.97


# ─── §9 LEAPS Maintenance ──────────────────────────────────────────

LEAPS_REFRESH_TARGET_DTE_MIN = 365
LEAPS_REFRESH_TARGET_DELTA_MIN = 0.78
LEAPS_REFRESH_TARGET_DELTA_MAX = 0.82
LEAPS_SURVIVAL_FLOOR_DTE = 180        # < 180 DTE → forced refresh regardless of tape


# ─── §11 Execution ─────────────────────────────────────────────────

CLUSTER_SLIPPAGE_BUDGET = 0.01        # ~1% of cluster cash flow
ORDER_QUOTE_REFRESH_SECONDS = 60
LEG_FILL_TIMEOUT_SECONDS = 30
VIX_INTRADAY_ABORT_DELTA = 1.5        # index vol spike abort
SS_IV_INTRADAY_ABORT_PCT = 0.10       # single-stock IV30 +10% abort


# ─── §12 Scorecard ─────────────────────────────────────────────────

MC_DEFAULT_PATHS = 5000
MC_DEFAULT_RISK_FREE_RATE = 0.045    # ~1Y Treasury yield (caller can override)
SHARPE_FLAG_THRESHOLD = 1.0
P_LOSS_AUTO_REJECT = 0.50
CVAR_FLAG_MULTIPLIER = 3.0           # CVaR > 3× expected return → flag


# ─── §13 Array Optimization ────────────────────────────────────────

THETA_PER_DELTA_OPTIMAL = 1.50
THETA_PER_DELTA_ACCEPTABLE = 0.75    # below this → suboptimal
NET_DELTA_TARGET_MIN = 30.0           # $30 per $1 of underlying move
NET_DELTA_TARGET_MAX = 80.0
SHORT_DELTA_PCT_OF_LONG_MIN = 0.80
SHORT_DELTA_PCT_OF_LONG_MAX = 0.90
ARRAY_RECENTER_SPOT_DRIFT = 0.05      # spot moves >5% from array center → re-optimize
ITM_DEAD_WEIGHT_EXTRINSIC_FLOOR = 2.0  # extrinsic < $2 → dead weight


# ─── Default ticker state seeds ────────────────────────────────────

# Used when a ticker has no engine state yet. The vol_median values are
# rough seeds — operators should refine with empirical 5y data.
DEFAULT_TICKER_STATE = {
    "SPY":  {"vol_median_5yr": 18.0, "vol_axis": "VIX",  "quarterly_dividend": 1.85},
    "QQQ":  {"vol_median_5yr": 22.0, "vol_axis": "VIX",  "quarterly_dividend": 0.65},
    "IWM":  {"vol_median_5yr": 24.0, "vol_axis": "VIX",  "quarterly_dividend": 0.60},
    "MSFT": {"vol_median_5yr": 28.0, "vol_axis": "IV30", "quarterly_dividend": 0.83},
    "GOOG": {"vol_median_5yr": 32.0, "vol_axis": "IV30", "quarterly_dividend": 0.20},
    "AAPL": {"vol_median_5yr": 30.0, "vol_axis": "IV30", "quarterly_dividend": 0.25},
    "NVDA": {"vol_median_5yr": 45.0, "vol_axis": "IV30", "quarterly_dividend": 0.04},
}
