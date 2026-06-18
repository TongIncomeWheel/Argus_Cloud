"""Storage layer for the Tiger MCP OAuth provider.

Two implementations both satisfying `OAuthStorage`:

  - InMemoryStorage   dicts. Fast, simple, lost on container restart.
                      Used for tests + stdio dev.

  - FirestoreStorage  state persisted in Google Cloud Firestore. Required
                      for any HTTPS deployment on Cloud Run / similar
                      where the process can be recycled. Survives cold
                      starts so claude.ai's OAuth tokens keep working
                      across multi-hour idle gaps — closes the
                      "connector silently disconnects" failure mode.

`provider.py` codes against the protocol and doesn't care which backend
is wired. Selection happens in `server._build_server()` based on
`MCP_OAUTH_STORAGE` env var.
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Protocol

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

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PendingAuthRequest":
        return cls(
            client_id=d["client_id"],
            scopes=list(d.get("scopes") or []),
            code_challenge=d["code_challenge"],
            redirect_uri=d["redirect_uri"],
            redirect_uri_provided_explicitly=bool(d["redirect_uri_provided_explicitly"]),
            state=d.get("state"),
            resource=d.get("resource"),
            expires_at=float(d["expires_at"]),
        )


class OAuthStorage(Protocol):
    """Async key-value store for the five OAuth entity types."""

    async def get_client(self, client_id: str) -> Optional[OAuthClientInformationFull]: ...
    async def store_client(self, client_info: OAuthClientInformationFull) -> None: ...

    async def get_pending(self, request_id: str) -> Optional[PendingAuthRequest]: ...
    async def store_pending(self, request_id: str, pending: PendingAuthRequest) -> None: ...
    async def pop_pending(self, request_id: str) -> Optional[PendingAuthRequest]: ...

    async def get_code(self, code: str) -> Optional[AuthorizationCode]: ...
    async def store_code(self, code: AuthorizationCode) -> None: ...
    async def pop_code(self, code_str: str) -> None: ...

    async def get_access(self, token: str) -> Optional[AccessToken]: ...
    async def store_access(self, token: AccessToken) -> None: ...
    async def pop_access(self, token_str: str) -> None: ...

    async def get_refresh(self, token: str) -> Optional[RefreshToken]: ...
    async def store_refresh(self, token: RefreshToken) -> None: ...
    async def pop_refresh(self, token_str: str) -> None: ...


# ── In-memory implementation (tests, stdio) ──────────────────────────────────


class InMemoryStorage:
    """Single-process, asyncio-safe OAuth state. Lost on restart.

    The fields stay public dicts so older test code that pokes the storage
    directly still works; the new method API is what `provider.py` uses.
    """

    def __init__(self) -> None:
        self.clients: Dict[str, OAuthClientInformationFull] = {}
        self.pending: Dict[str, PendingAuthRequest] = {}
        self.auth_codes: Dict[str, AuthorizationCode] = {}
        self.access_tokens: Dict[str, AccessToken] = {}
        self.refresh_tokens: Dict[str, RefreshToken] = {}
        self.lock = asyncio.Lock()

    async def get_client(self, client_id: str) -> Optional[OAuthClientInformationFull]:
        async with self.lock:
            return self.clients.get(client_id)

    async def store_client(self, client_info: OAuthClientInformationFull) -> None:
        async with self.lock:
            self.clients[client_info.client_id] = client_info

    async def get_pending(self, request_id: str) -> Optional[PendingAuthRequest]:
        async with self.lock:
            return self.pending.get(request_id)

    async def store_pending(self, request_id: str, pending: PendingAuthRequest) -> None:
        async with self.lock:
            self.pending[request_id] = pending

    async def pop_pending(self, request_id: str) -> Optional[PendingAuthRequest]:
        async with self.lock:
            return self.pending.pop(request_id, None)

    async def get_code(self, code: str) -> Optional[AuthorizationCode]:
        async with self.lock:
            return self.auth_codes.get(code)

    async def store_code(self, code: AuthorizationCode) -> None:
        async with self.lock:
            self.auth_codes[code.code] = code

    async def pop_code(self, code_str: str) -> None:
        async with self.lock:
            self.auth_codes.pop(code_str, None)

    async def get_access(self, token: str) -> Optional[AccessToken]:
        async with self.lock:
            return self.access_tokens.get(token)

    async def store_access(self, token: AccessToken) -> None:
        async with self.lock:
            self.access_tokens[token.token] = token

    async def pop_access(self, token_str: str) -> None:
        async with self.lock:
            self.access_tokens.pop(token_str, None)

    async def get_refresh(self, token: str) -> Optional[RefreshToken]:
        async with self.lock:
            return self.refresh_tokens.get(token)

    async def store_refresh(self, token: RefreshToken) -> None:
        async with self.lock:
            self.refresh_tokens[token.token] = token

    async def pop_refresh(self, token_str: str) -> None:
        async with self.lock:
            self.refresh_tokens.pop(token_str, None)
