"""Tiger Open API integration for ARGUS.

Read-only PULL data source. Replaces lagging CSV uploads with live broker state.

Modules (forthcoming):
- client.py           : TigerClient — auth wrapper around tigeropen SDK
- adapters.py         : Tiger schema → ARGUS row dicts
- sync.py             : Diff & apply against gSheet Data Table
- push_listener.py    : WebSocket subscriber (Phase 4)

Configuration is read from environment via python-dotenv:
  TIGER_ID                   — developer ID from developer.itigerup.com/profile
  TIGER_ACCOUNT              — trading account (e.g. 50179929 for TBSG Prime)
  TIGER_LICENSE              — TBSG | TBHK | TBNZ | TBAU
  TIGER_PRIVATE_KEY_PATH     — path to PKCS#8 private key (.pem)
  TIGER_ENV                  — 'production' (default) or 'sandbox'
"""
