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
# `posture` is a short code; `shape` describes the ITM/OTM LEAN direction —
# NOT a count. The count comes from how many LEAPS the operator chooses to
# cover (typically N LEAPS → N shorts for 100% coverage). The regime tells
# you how to SPLIT those shorts (lean ITM, balanced, lean OTM, all ITM, etc.).
#
# Shape vocabulary (see SHAPE_DESCRIPTIONS):
#   centered         equal ITM and OTM
#   lean_itm         more ITM than OTM (harvest mode)
#   lean_otm         more OTM than ITM (growth-participation mode)
#   all_itm          all shorts ITM (defensive flip)
#   all_otm          all shorts OTM
#   all_otm_half     all shorts OTM, half size
#   stand_down       no new short deployment
REGIME_GRID = {
    ("L", "cheap"):    {"posture": "defensive_flip",       "dte_weeks": (3, 4),  "shape": "all_itm"},
    ("L", "neutral"):  {"posture": "standard_lean_theta",  "dte_weeks": (6, 6),  "shape": "centered"},
    ("L", "rich"):     {"posture": "standard",             "dte_weeks": (4, 5),  "shape": "centered"},
    ("L", "extreme"):  {"posture": "otm_skew",             "dte_weeks": (3, 4),  "shape": "all_otm"},
    ("M", "cheap"):    {"posture": "otm_lean",             "dte_weeks": (5, 6),  "shape": "lean_otm"},
    ("M", "neutral"):  {"posture": "base_case",            "dte_weeks": (4, 6),  "shape": "centered"},
    ("M", "rich"):     {"posture": "itm_harvest",          "dte_weeks": (4, 4),  "shape": "lean_itm"},
    ("M", "extreme"):  {"posture": "fade_vol",             "dte_weeks": (3, 4),  "shape": "all_otm"},
    ("H", "cheap"):    {"posture": "otm_lean",             "dte_weeks": (4, 4),  "shape": "lean_otm"},
    ("H", "neutral"):  {"posture": "otm_lean",             "dte_weeks": (4, 4),  "shape": "lean_otm"},
    ("H", "rich"):     {"posture": "all_otm",              "dte_weeks": (3, 3),  "shape": "all_otm"},
    ("H", "extreme"):  {"posture": "all_otm_half_size",    "dte_weeks": (2, 3),  "shape": "all_otm_half"},
    ("X", "cheap"):    {"posture": "stand_down",           "dte_weeks": None,    "shape": "stand_down"},
    ("X", "neutral"):  {"posture": "stand_down_or_half",   "dte_weeks": None,    "shape": "all_otm_half"},
    ("X", "rich"):     {"posture": "all_otm_half_size",    "dte_weeks": (2, 2),  "shape": "all_otm_half"},
    ("X", "extreme"):  {"posture": "all_otm_half_gamma",   "dte_weeks": (2, 2),  "shape": "all_otm_half"},
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


# Plain-English descriptions of SHAPE codes used in REGIME_GRID["shape"].
# These are direction-of-lean labels — the COUNT of shorts is set by the
# operator (typically = number of LEAPS for 100% coverage).
SHAPE_DESCRIPTIONS = {
    "centered":       "Centered (equal short calls ITM and OTM)",
    "lean_itm":       "ITM-lean (more shorts below spot — harvest mode)",
    "lean_otm":       "OTM-lean (more shorts above spot — growth-participation mode)",
    "all_itm":        "All shorts ITM (defensive flip — premium harvest only)",
    "all_otm":        "All shorts OTM (defensive — growth participation only)",
    "all_otm_half":   "All shorts OTM, half-size (preserve capital in shock regime)",
    "stand_down":     "Stand down (no new short deployment until regime re-ranks)",
}


def shape_description(code) -> str:
    """Translate a regime-grid shape codename to plain English."""
    if not code:
        return "—"
    return SHAPE_DESCRIPTIONS.get(code, code)


def classify_shape(itm_count: int, otm_count: int) -> str:
    """Classify the operator's current short array into a doctrine shape code.

    The shape is purely about LEAN direction; the count comes from coverage.
    """
    if itm_count == 0 and otm_count == 0:
        return "empty"
    if otm_count == 0:
        return "all_itm"
    if itm_count == 0:
        return "all_otm"
    if itm_count == otm_count:
        return "centered"
    if itm_count > otm_count:
        return "lean_itm"
    return "lean_otm"


def shape_guidance(current_itm: int, current_otm: int, target_shape: str) -> dict:
    """Compare operator's current short shape to the regime's target shape.

    Returns plain-English guidance: headline, actions, tradeoffs. The count
    of shorts (4 vs 6 vs 8) is irrelevant — only the lean direction matters.

    Output dict:
        match (bool)
        current_shape (str)        — operator's actual classified shape
        target (str)               — regime's target shape description
        actions (list[str])        — what to shift to align (rolls, not closes)
        tradeoffs (list[str])
        headline (str)
    """
    current_shape = classify_shape(current_itm, current_otm)
    target_desc = shape_description(target_shape)

    if not target_shape or target_shape == "stand_down":
        return {
            "match": False,
            "current_shape": current_shape,
            "target": target_desc,
            "actions": ["Regime calls for **stand down** — close or let expire; do not deploy new shorts until vol re-ranks."],
            "tradeoffs": ["Standing down means zero income for the duration. Doctrine accepts this cost to avoid deploying into a regime that can't be trusted."],
            "headline": "🛑 Stand down regime — no new shorts.",
        }

    # Exact-match shapes
    if current_shape == target_shape:
        return {
            "match": True,
            "current_shape": current_shape,
            "target": target_desc,
            "actions": [f"On doctrine — your shape is **{shape_description(current_shape)}**, matching the regime target."],
            "tradeoffs": [],
            "headline": f"✅ {shape_description(current_shape)}.",
        }

    # Compute the shift needed
    total = current_itm + current_otm
    if total == 0:
        return {
            "match": False,
            "current_shape": current_shape,
            "target": target_desc,
            "actions": [f"No shorts deployed. Regime calls for **{target_desc}** at your chosen coverage count."],
            "tradeoffs": [],
            "headline": f"No shorts yet — regime target is {target_desc}.",
        }

    actions = []
    tradeoffs = []
    headline = f"⚠️ Your shape is **{shape_description(current_shape)}** — regime calls for **{target_desc}**."

    if target_shape == "centered":
        # Need equal counts. Calculate the move.
        if current_otm > current_itm:
            move = (current_otm - current_itm) // 2 or 1
            actions.append(f"**Roll {move} OTM short(s) ITM** (above-spot strikes to below-spot, e.g. into the 1-3% ITM band).")
            tradeoffs.append("Rolling OTM→ITM trades growth participation for higher theta capture.")
        else:
            move = (current_itm - current_otm) // 2 or 1
            actions.append(f"**Roll {move} ITM short(s) OTM** (below-spot strikes to above-spot, e.g. into the 1-3% OTM band).")
            tradeoffs.append("Rolling ITM→OTM gives back some theta to uncap LEAPS upside.")
        actions.append("Alternative: keep current count, accept off-doctrine, document rationale in your §10 review.")

    elif target_shape == "lean_itm":
        if current_otm >= current_itm:
            move = max(1, (current_otm - current_itm + 1) // 2)
            actions.append(f"**Roll {move} OTM short(s) ITM** to tilt the array toward harvest mode.")
            tradeoffs.append("ITM-lean trades growth headroom for more reliable theta capture (regime is paying for harvest right now).")

    elif target_shape == "lean_otm":
        if current_itm >= current_otm:
            move = max(1, (current_itm - current_otm + 1) // 2)
            actions.append(f"**Roll {move} ITM short(s) OTM** to tilt the array toward growth-participation.")
            tradeoffs.append("OTM-lean trades theta capture for less assignment risk + LEAPS upside participation.")

    elif target_shape == "all_otm":
        actions.append(f"**Roll all {current_itm} ITM short(s) to OTM strikes.** Regime can't be trusted for ITM theta-harvest.")
        tradeoffs.append("All-OTM gives up most theta in exchange for minimizing assignment risk in a high-vol regime.")

    elif target_shape == "all_itm":
        actions.append(f"**Roll all {current_otm} OTM short(s) to ITM strikes (~3% below spot).** Defensive flip — extract what theta is available.")
        tradeoffs.append("All-ITM caps LEAPS upside completely but maximizes theta capture when premium is otherwise dead.")

    elif target_shape == "all_otm_half":
        if current_itm > 0 or total > max(1, total // 2):
            actions.append(f"**Roll all {current_itm} ITM short(s) OTM AND close ~half the OTM legs.** Half-size all-OTM in shock regime.")
            tradeoffs.append("Half-size preserves capital while maintaining minimum directional cover.")

    if not actions:
        actions = [f"Adjust array toward **{target_desc}**."]

    actions.append("Alternative: hold current shape, accept off-doctrine, document rationale in your §10 review.")

    return {
        "match": False,
        "current_shape": current_shape,
        "target": target_desc,
        "actions": actions,
        "tradeoffs": tradeoffs,
        "headline": headline,
    }


# ─── Legacy aliases (kept for backwards-compat during refactor) ───
# Older callers may reference these. They simply delegate to the new
# shape-based helpers so the engine stays unified.

def array_description(code) -> str:   # legacy
    return shape_description(code)


def array_guidance(current_itm, current_otm, target_code):   # legacy
    return shape_guidance(current_itm, current_otm, target_code)


def parse_array_code(code):   # legacy — no longer used internally
    if not code:
        return None
    if code in ("all_otm", "all_otm_half"):
        return (0, None)
    if code == "all_itm":
        return (None, 0)
    return None


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

# θ/Δ rating thresholds. The doctrine §13 table prescribes Optimal ≥$1.50 /
# Acceptable ≥$0.75 / Suboptimal <$0.75, but real PMCC books on liquid
# index underlyings rarely clear $1.50 in normal regimes. Calibration below
# matches the operator's live advisor experience:
#   Optimal     ≥ $0.75      (book is well-tuned)
#   Acceptable  $0.40-$0.75  (working as intended; doctrine §13 transition state)
#   Suboptimal  < $0.40      (carrying directional risk without adequate income)
THETA_PER_DELTA_OPTIMAL = 0.75
THETA_PER_DELTA_ACCEPTABLE = 0.40    # below this → suboptimal
NET_DELTA_TARGET_MIN = 30.0           # $30 per $1 of underlying move
NET_DELTA_TARGET_MAX = 80.0
SHORT_DELTA_PCT_OF_LONG_MIN = 0.80
SHORT_DELTA_PCT_OF_LONG_MAX = 0.90
ARRAY_RECENTER_SPOT_DRIFT = 0.05      # spot moves >5% from array center → re-optimize
ITM_DEAD_WEIGHT_EXTRINSIC_FLOOR = 2.0  # extrinsic < $2 → dead weight


# ─── Default ticker state seeds ────────────────────────────────────

# Used when a ticker has no engine state yet. The vol_median values are
# rough seeds — operators should refine with empirical 5y data.
#
# ex_div_calendar entries are SPY/QQQ/IWM quarterly dividend dates (3rd Friday
# of Mar / Jun / Sep / Dec). Dividend estimates are placeholders close to
# recent prints — refine as actual ex-divs print and rates step.
DEFAULT_TICKER_STATE = {
    "SPY": {
        "vol_median_5yr": 18.0, "vol_axis": "VIX", "quarterly_dividend": 1.85,
        "ex_div_calendar": [
            {"date": "2026-06-19", "est_dividend": 1.85},
            {"date": "2026-09-18", "est_dividend": 1.85},
            {"date": "2026-12-18", "est_dividend": 1.85},
            {"date": "2027-03-20", "est_dividend": 1.90},
            {"date": "2027-06-18", "est_dividend": 1.90},
            {"date": "2027-09-17", "est_dividend": 1.90},
            {"date": "2027-12-17", "est_dividend": 1.95},
            {"date": "2028-03-17", "est_dividend": 1.95},
        ],
    },
    "QQQ": {
        "vol_median_5yr": 22.0, "vol_axis": "VIX", "quarterly_dividend": 0.85,
        "ex_div_calendar": [
            {"date": "2026-06-19", "est_dividend": 0.85},
            {"date": "2026-09-18", "est_dividend": 0.85},
            {"date": "2026-12-18", "est_dividend": 0.85},
            {"date": "2027-03-20", "est_dividend": 0.90},
            {"date": "2027-06-18", "est_dividend": 0.90},
            {"date": "2027-09-17", "est_dividend": 0.90},
            {"date": "2027-12-17", "est_dividend": 0.95},
        ],
    },
    "IWM": {
        "vol_median_5yr": 24.0, "vol_axis": "VIX", "quarterly_dividend": 0.55,
        "ex_div_calendar": [
            {"date": "2026-06-19", "est_dividend": 0.55},
            {"date": "2026-09-18", "est_dividend": 0.55},
            {"date": "2026-12-18", "est_dividend": 0.60},
            {"date": "2027-03-20", "est_dividend": 0.60},
            {"date": "2027-06-18", "est_dividend": 0.60},
            {"date": "2027-09-17", "est_dividend": 0.65},
            {"date": "2027-12-17", "est_dividend": 0.65},
        ],
    },
    "MSFT": {"vol_median_5yr": 28.0, "vol_axis": "IV30", "quarterly_dividend": 0.83},
    "GOOG": {"vol_median_5yr": 32.0, "vol_axis": "IV30", "quarterly_dividend": 0.20},
    "AAPL": {"vol_median_5yr": 30.0, "vol_axis": "IV30", "quarterly_dividend": 0.25},
    "NVDA": {"vol_median_5yr": 45.0, "vol_axis": "IV30", "quarterly_dividend": 0.04},
}
