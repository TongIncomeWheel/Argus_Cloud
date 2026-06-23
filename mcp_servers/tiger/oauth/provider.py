"""OAuthAuthorizationServerProvider implementation for Tiger MCP.

Implements the MCP SDK's protocol so FastMCP can advertise the
required OAuth metadata, accept Dynamic Client Registration from the
Claude consumer app, and gate every tool call by a bearer access token
issued through the consent flow.

Storage backend is injected (see oauth/storage.py): InMemoryStorage for
tests/stdio, FirestoreStorage for HTTPS deploys. The provider only
touches storage through the OAuthStorage protocol methods, so swapping
backends never requires changing this file.
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

from mcp_servers.tiger.oauth.storage import OAuthStorage, PendingAuthRequest


_ACCESS_TTL = 24 * 3600  # 24 hours — keep claude.ai's refresh cadence low
_REFRESH_TTL = 30 * 24 * 3600  # 30 days
_CODE_TTL = 600  # 10 minutes
_PENDING_TTL = 600  # 10 minutes for the consent step


class TigerOAuthProvider(OAuthAuthorizationServerProvider):
    """OAuth 2.1 + PKCE + DCR for the single-owner Tiger MCP server."""

    def __init__(self, storage: OAuthStorage, base_url: str) -> None:
        self._storage = storage
        self._base_url = base_url.rstrip("/")

    # ── Client registration ──────────────────────────────────────────────

    async def get_client(self, client_id: str) -> Optional[OAuthClientInformationFull]:
        return await self._storage.get_client(client_id)

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        await self._storage.store_client(client_info)

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
        await self._storage.store_pending(request_id, pending)
        return f"{self._base_url}/consent?request_id={request_id}"

    async def get_pending_request(self, request_id: str) -> Optional[PendingAuthRequest]:
        pending = await self._storage.get_pending(request_id)
        if pending is None:
            return None
        if pending.expires_at < time.time():
            await self._storage.pop_pending(request_id)
            return None
        return pending

    async def finalize_authorization(self, request_id: str) -> tuple[str, Optional[str], str]:
        """After the user passes the consent step, mint an auth code and
        return (code, state, redirect_uri) so the route handler can build
        the redirect back to the OAuth client."""
        pending = await self._storage.pop_pending(request_id)
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
        await self._storage.store_code(code)
        return code_str, pending.state, pending.redirect_uri

    # ── Token endpoint ───────────────────────────────────────────────────

    async def load_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: str,
    ) -> Optional[AuthorizationCode]:
        code = await self._storage.get_code(authorization_code)
        if code is None:
            return None
        if code.client_id != client.client_id:
            return None
        if code.expires_at < time.time():
            await self._storage.pop_code(authorization_code)
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
        await self._storage.store_access(access_tok)
        await self._storage.store_refresh(refresh_tok)
        # One-time use: invalidate the auth code.
        await self._storage.pop_code(authorization_code.code)
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
        rt = await self._storage.get_refresh(refresh_token)
        if rt is None or rt.client_id != client.client_id:
            return None
        if rt.expires_at is not None and rt.expires_at < time.time():
            await self._storage.pop_refresh(refresh_token)
            return None
        return rt

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        """Mint a new access token (and a new refresh token) from a valid
        refresh token.

        Intentionally does NOT delete the consumed refresh token. The
        OAuth 2.1 spec recommends rotation, but in practice claude.ai's
        consumer connector occasionally retries refresh requests (network
        jitter, race conditions, sleep/wake transitions). If we delete the
        old token the moment a new one is issued, the retry comes in with
        a token we just invalidated and the connector silently disconnects.

        Refresh tokens still expire on their own (30-day TTL), so the
        "tokens accumulate forever" risk is bounded. revoke_token() can
        also remove them explicitly when the user signs out.
        """
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
        await self._storage.store_access(access_tok)
        await self._storage.store_refresh(new_refresh_tok)
        # NOTE: NOT deleting refresh_token.token here. See docstring.
        return OAuthToken(
            access_token=access,
            token_type="Bearer",
            expires_in=_ACCESS_TTL,
            refresh_token=new_refresh,
            scope=" ".join(effective_scopes),
        )

    # ── Resource server side ─────────────────────────────────────────────

    async def load_access_token(self, token: str) -> Optional[AccessToken]:
        at = await self._storage.get_access(token)
        if at is None:
            return None
        if at.expires_at is not None and at.expires_at < time.time():
            await self._storage.pop_access(token)
            return None
        return at

    async def revoke_token(self, token: Union[AccessToken, RefreshToken]) -> None:
        await self._storage.pop_access(token.token)
        await self._storage.pop_refresh(token.token)
