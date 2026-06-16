"""OAuthAuthorizationServerProvider implementation for Tiger MCP.

Implements the MCP SDK's protocol so FastMCP can advertise the
required OAuth metadata, accept Dynamic Client Registration from the
Claude consumer app, and gate every tool call by a bearer access token
issued through the consent flow.
"""
from __future__ import annotations

import secrets
import time
from typing import Optional, Union

from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    OAuthAuthorizationServerProvider,
    RefreshToken,
)
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken

from mcp_servers.tiger.oauth.storage import InMemoryStorage, PendingAuthRequest


_ACCESS_TTL = 3600  # 1 hour
_REFRESH_TTL = 30 * 24 * 3600  # 30 days
_CODE_TTL = 600  # 10 minutes
_PENDING_TTL = 600  # 10 minutes for the consent step


class TigerOAuthProvider(OAuthAuthorizationServerProvider):
    """OAuth 2.1 + PKCE + DCR for the single-owner Tiger MCP server."""

    def __init__(self, storage: InMemoryStorage, base_url: str) -> None:
        self._storage = storage
        self._base_url = base_url.rstrip("/")

    # ── Client registration ──────────────────────────────────────────────

    async def get_client(self, client_id: str) -> Optional[OAuthClientInformationFull]:
        async with self._storage.lock:
            return self._storage.clients.get(client_id)

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        async with self._storage.lock:
            self._storage.clients[client_info.client_id] = client_info

    # ── Authorize → consent → callback ───────────────────────────────────

    async def authorize(
        self,
        client: OAuthClientInformationFull,
        params: AuthorizationParams,
    ) -> str:
        """Persist the request and redirect the user to the consent page."""
        request_id = secrets.token_urlsafe(24)
        pending = PendingAuthRequest(
            client_id=client.client_id,
            scopes=list(params.scopes or []),
            code_challenge=params.code_challenge,
            redirect_uri=str(params.redirect_uri),
            redirect_uri_provided_explicitly=params.redirect_uri_provided_explicitly,
            state=params.state,
            resource=params.resource,
            expires_at=time.time() + _PENDING_TTL,
        )
        async with self._storage.lock:
            self._storage.pending[request_id] = pending
        return f"{self._base_url}/consent?request_id={request_id}"

    async def get_pending_request(self, request_id: str) -> Optional[PendingAuthRequest]:
        async with self._storage.lock:
            pending = self._storage.pending.get(request_id)
            if pending is None:
                return None
            if pending.expires_at < time.time():
                self._storage.pending.pop(request_id, None)
                return None
            return pending

    async def finalize_authorization(self, request_id: str) -> tuple[str, Optional[str], str]:
        """After the user passes the consent step, mint an auth code and
        return (code, state, redirect_uri) so the route handler can build
        the redirect back to the OAuth client."""
        async with self._storage.lock:
            pending = self._storage.pending.pop(request_id, None)
        if pending is None or pending.expires_at < time.time():
            raise ValueError("Authorization request is invalid or expired")

        code_str = secrets.token_urlsafe(32)
        code = AuthorizationCode(
            code=code_str,
            scopes=pending.scopes,
            expires_at=time.time() + _CODE_TTL,
            client_id=pending.client_id,
            code_challenge=pending.code_challenge,
            redirect_uri=pending.redirect_uri,
            redirect_uri_provided_explicitly=pending.redirect_uri_provided_explicitly,
            resource=pending.resource,
            subject="owner",
        )
        async with self._storage.lock:
            self._storage.auth_codes[code_str] = code
        return code_str, pending.state, pending.redirect_uri

    # ── Token endpoint ───────────────────────────────────────────────────

    async def load_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: str,
    ) -> Optional[AuthorizationCode]:
        async with self._storage.lock:
            code = self._storage.auth_codes.get(authorization_code)
            if code is None:
                return None
            if code.client_id != client.client_id:
                return None
            if code.expires_at < time.time():
                self._storage.auth_codes.pop(authorization_code, None)
                return None
            return code

    async def exchange_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: AuthorizationCode,
    ) -> OAuthToken:
        access = secrets.token_urlsafe(32)
        refresh = secrets.token_urlsafe(32)
        now = int(time.time())
        access_tok = AccessToken(
            token=access,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=now + _ACCESS_TTL,
            subject=authorization_code.subject,
            resource=authorization_code.resource,
        )
        refresh_tok = RefreshToken(
            token=refresh,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=now + _REFRESH_TTL,
            subject=authorization_code.subject,
        )
        async with self._storage.lock:
            self._storage.access_tokens[access] = access_tok
            self._storage.refresh_tokens[refresh] = refresh_tok
            # One-time use: invalidate the auth code.
            self._storage.auth_codes.pop(authorization_code.code, None)
        return OAuthToken(
            access_token=access,
            token_type="Bearer",
            expires_in=_ACCESS_TTL,
            refresh_token=refresh,
            scope=" ".join(authorization_code.scopes),
        )

    async def load_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: str,
    ) -> Optional[RefreshToken]:
        async with self._storage.lock:
            rt = self._storage.refresh_tokens.get(refresh_token)
            if rt is None or rt.client_id != client.client_id:
                return None
            if rt.expires_at is not None and rt.expires_at < time.time():
                self._storage.refresh_tokens.pop(refresh_token, None)
                return None
            return rt

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        access = secrets.token_urlsafe(32)
        new_refresh = secrets.token_urlsafe(32)
        now = int(time.time())
        # If client requested narrower scopes that are a subset of the
        # refresh token's, honour them; otherwise carry the original.
        if scopes and set(scopes).issubset(set(refresh_token.scopes)):
            effective_scopes = scopes
        else:
            effective_scopes = refresh_token.scopes
        access_tok = AccessToken(
            token=access,
            client_id=client.client_id,
            scopes=effective_scopes,
            expires_at=now + _ACCESS_TTL,
            subject=refresh_token.subject,
        )
        new_refresh_tok = RefreshToken(
            token=new_refresh,
            client_id=client.client_id,
            scopes=effective_scopes,
            expires_at=now + _REFRESH_TTL,
            subject=refresh_token.subject,
        )
        async with self._storage.lock:
            self._storage.access_tokens[access] = access_tok
            self._storage.refresh_tokens[new_refresh] = new_refresh_tok
            # Rotation: revoke the consumed refresh token.
            self._storage.refresh_tokens.pop(refresh_token.token, None)
        return OAuthToken(
            access_token=access,
            token_type="Bearer",
            expires_in=_ACCESS_TTL,
            refresh_token=new_refresh,
            scope=" ".join(effective_scopes),
        )

    # ── Resource server side ─────────────────────────────────────────────

    async def load_access_token(self, token: str) -> Optional[AccessToken]:
        async with self._storage.lock:
            at = self._storage.access_tokens.get(token)
            if at is None:
                return None
            if at.expires_at is not None and at.expires_at < time.time():
                self._storage.access_tokens.pop(token, None)
                return None
            return at

    async def revoke_token(self, token: Union[AccessToken, RefreshToken]) -> None:
        async with self._storage.lock:
            self._storage.access_tokens.pop(token.token, None)
            self._storage.refresh_tokens.pop(token.token, None)
