"""Bootstrap Tiger config from environment variables.

The Tiger MCP server runs in a Claude Code on the web container that has
neither the user's laptop `.streamlit/` directory nor the Streamlit Cloud
`st.secrets` runtime. Credentials therefore have to arrive as cloud-env
variables set in the claude.ai/code environment dashboard.

Recognized env vars (any subset that produces a complete .properties file):
  TIGER_PROPERTIES_CONTENT   verbatim contents of tiger_openapi_config.properties
  TIGER_PRIVATE_KEY_PK8      PKCS#8 private key (preferred)
  TIGER_PRIVATE_KEY_PK1      PKCS#1 private key (legacy fallback)
  TIGER_ID                   developer id from developer.itigerup.com/profile
  TIGER_ACCOUNT              trading account number
  TIGER_LICENSE              TBSG | TBHK | TBNZ | TBAU
  TIGER_ENV                  PROD | SANDBOX

The function writes the assembled .properties file under /tmp and points
`TIGER_CONFIG_PATH` at its parent directory so that
`tiger_api.client.TigerClient` discovers it without further wiring.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_TARGET_DIR = Path("/tmp/argus_tiger_config")
_TARGET_FILE = "tiger_openapi_config.properties"

# Env var name → .properties key
_KEY_MAP = (
    ("TIGER_PRIVATE_KEY_PK8", "private_key_pk8"),
    ("TIGER_PRIVATE_KEY_PK1", "private_key_pk1"),
    ("TIGER_ID", "tiger_id"),
    ("TIGER_ACCOUNT", "account"),
    ("TIGER_LICENSE", "license"),
    ("TIGER_ENV", "env"),
)


def bootstrap_from_env() -> Optional[Path]:
    """Materialize tiger_openapi_config.properties from env vars.

    Returns the path written, or None when no TIGER_* env vars are set
    (caller then falls back to TigerClient's own resolution order).
    """
    raw = os.environ.get("TIGER_PROPERTIES_CONTENT", "").strip()
    if raw:
        return _write(raw if raw.endswith("\n") else raw + "\n")

    lines = []
    for env_key, prop_key in _KEY_MAP:
        v = os.environ.get(env_key, "").strip()
        if v:
            lines.append(f"{prop_key}={v}")
    if not lines:
        logger.info("No TIGER_* env vars present — skipping bootstrap")
        return None
    return _write("\n".join(lines) + "\n")


def _write(content: str) -> Path:
    _TARGET_DIR.mkdir(parents=True, exist_ok=True)
    target = _TARGET_DIR / _TARGET_FILE
    target.write_text(content, encoding="utf-8")
    os.environ["TIGER_CONFIG_PATH"] = str(_TARGET_DIR)
    logger.info("Tiger config materialized at %s; TIGER_CONFIG_PATH=%s", target, _TARGET_DIR)
    return target
