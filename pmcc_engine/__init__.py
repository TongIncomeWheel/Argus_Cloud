"""ARGUS PMCC Engine — operationalizes the PMCC Operating Doctrine v2.

Single source of truth for the math: regime classification, theta hurdle,
tripwires, strike candidate filtering, roll decomposition, Monte Carlo scorecard,
and posture optimization. Ticker-portable: the same engine runs SPY, MSFT, GOOG,
or any liquid optionable underlying. Per-ticker calibration (vol regime levels,
ex-div calendar, tripwire numerics) lives in ARGUS settings under
`pmcc_engine_state.{ticker}`.

Reference documents (in repo or in user's planning notes):
  pmccdoctrine.md     — strategy framework (v2)
  pmccapphandoff.md   — engineering spec

Modules:
  doctrine     constants: regime grid, hurdle capture rate, IVR cutoffs
  theta_math   HV30, theta hurdle, yield ratio, extrinsic, book greeks, theta/delta
  regime       vol band + IVR classification → regime cell lookup
  triggers     tripwires, short roll triggers, LEAPS refresh triggers
  strikes      ITM/OTM candidate filters with hurdle pass/fail flags
  rolls        roll cost & decomposition template
  scorecard    Monte Carlo trade evaluation
  posture      array optimization checks + ex-div protocol
  data_io      daily bars (yfinance), VIX, option chain (Alpaca wrapper)
  state        per-ticker engine state load/save (settings-backed)
  review       4-block daily review formatter (text version)
  ui           Streamlit render function (PMCC tab in ARGUS)

Hard rule from doctrine: state the regime cell explicitly on every review.
No analysis is valid without it.
"""
from __future__ import annotations

__version__ = "1.0.0"
