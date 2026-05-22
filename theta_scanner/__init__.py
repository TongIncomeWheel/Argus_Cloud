"""ARGUS Theta Scanner — finds cash-secured-put (CSP) candidates worth selling.

'Good' CSP, per the operator's definition:
  - juicy premium       — high return relative to the collateral tied up
  - good RoR            — annualized return on the cash secured
  - safe distance       — strike comfortably OTM (below spot)
  - delta in the band   — assignment probability in a sane range

The scanner pulls live put chains (Alpaca), scores every candidate on those
four axes, blends them into a 0-100 composite, and ranks.

Modules:
  universe   candidate ticker universe — bundled liquid large-cap list,
             auto-upgrades to a live FMP market-cap/volume screen if an
             FMP_API_KEY is configured
  scoring    pure CSP scoring math — RoR, annualized yield, PoP, distance,
             delta score, composite
  data       Alpaca put-chain fetcher + batch spot
  ui         Streamlit sub-tab (rendered inside the Lookup tab)
"""
from __future__ import annotations

__version__ = "1.0.0"
