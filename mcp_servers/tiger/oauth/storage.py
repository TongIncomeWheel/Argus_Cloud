"""In-memory OAuth state for the Tiger MCP server.

Cloud Run with default scale-to-zero means tokens are wiped on cold start;
that's acceptable for a single-user deployment because the Claude app
will re-issue an OAuth flow and the user re-consents. If/when persistence
is needed, swap the dicts for a Firestore or GCS-backed store with the
same interface.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Dict, Optional

from mcp.server.auth.provider import AccessToken, AuthorizationCode, RefreshToken
from mcp.shared.auth import OAuthClientInformationFull


@dataclass
class PendingAuthRequest:
    """Authorization request waiting for the user to complete consent."""

    client_id: str
    scopes: list[str]
    code_challenge: str
    redirect_uri: str
    redirect_uri_provided_explicitly: bool
    state: Optional[str]
    resource: Optional[str]
    expires_at: float  # epoch seconds


@dataclass
class InMemoryStorage:
    """Single-process, asyncio-safe OAuth state."""

    clients: Dict[str, OAuthClientInformationFull] = field(default_factory=dict)
    auth_codes: Dict[str, AuthorizationCode] = field(default_factory=dict)
    access_tokens: Dict[str, AccessToken] = field(default_factory=dict)
    refresh_tokens: Dict[str, RefreshToken] = field(default_factory=dict)
    pending: Dict[str, PendingAuthRequest] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
