"""Bootstrap Tiger config + bearer-token verifier for the MCP server.

Two responsibilities, both driven by environment variables:

1. `bootstrap_from_env()` — materialize `tiger_openapi_config.properties` so
   `tiger_api.client.TigerClient` discovers credentials without further wiring.
2. `BearerTokenVerifier` — gate HTTP-transport requests against `MCP_BEARER_TOKEN`.

Recognized env vars for Tiger (any subset that produces a complete .properties file):
  TIGER_PROPERTIES_CONTENT   verbatim contents of tiger_openapi_config.properties
  TIGER_PRIVATE_KEY_PK8      PKCS#8 private key (preferred)
  TIGER_PRIVATE_KEY_PK1      PKCS#1 private key (legacy fallback)
  TIGER_ID                   developer id from developer.itigerup.com/profile
  TIGER_ACCOUNT              trading account number
  TIGER_LICENSE              TBSG | TBHK | TBNZ | TBAU
  TIGER_ENV                  PROD | SANDBOX

Recognized env vars for bearer auth:
  MCP_BEARER_TOKEN           shared secret; required on every HTTP request as
                             `Authorization: Bearer <token>`
"""
from __future__ import annotations

import hmac
import logging
import os
from pathlib import Path
from typing import Optional

from mcp.server.auth.provider import AccessToken

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


class BearerTokenVerifier:
    """Verify `Authorization: Bearer <token>` against a single configured secret.

    Phase 2a auth — no OAuth, no per-client tracking, no token rotation. Suitable
    for single-user gating where Claude Code or curl supplies the same secret on
    every call. Phase 2b will replace this with proper OAuth 2.1 + PKCE so the
    Claude.ai consumer Custom Connectors flow can authenticate.
    """

    def __init__(self, expected: str) -> None:
        if not expected:
            raise ValueError("BearerTokenVerifier requires a non-empty expected token")
        self._expected = expected

    async def verify_token(self, token: str) -> AccessToken | None:
        if not token:
            return None
        # Constant-time compare prevents leaking length info via timing attacks.
        if not hmac.compare_digest(token, self._expected):
            return None
        return AccessToken(
            token=token,
            client_id="argus-tiger-mcp",
            scopes=["tiger:read"],
            subject="owner",
        )

