"""Firestore-backed OAuth state for the Tiger MCP server.

Each entity gets its own collection. Document IDs are the natural keys
(client_id, code string, token string, request_id) so single-document
get/set/delete is the entire data path — no queries, no indexes to
manage.

Collections:
  oauth_clients          → OAuthClientInformationFull
  oauth_codes            → AuthorizationCode  (one-time use; pop on exchange)
  oauth_access_tokens    → AccessToken        (bearer presented by MCP clients)
  oauth_refresh_tokens   → RefreshToken       (rotated on exchange)
  oauth_pending          → PendingAuthRequest (consent step in flight)

Auth is via Application Default Credentials on Cloud Run — no key files.
The runtime service account just needs `roles/datastore.user`.

Cleanup
-------
Expired access/refresh tokens, codes, and pending requests are never
deleted by this code — that runs on the read side (`provider.py` checks
expires_at and skips). Optional follow-up: set a Firestore TTL policy on
`expires_at` so docs auto-delete; not required for correctness.
"""
from __future__ import annotations

import logging
from typing import Optional

from mcp.server.auth.provider import AccessToken, AuthorizationCode, RefreshToken
from mcp.shared.auth import OAuthClientInformationFull

from mcp_servers.tiger.oauth.storage import PendingAuthRequest

logger = logging.getLogger(__name__)

_COL_CLIENTS = "oauth_clients"
_COL_PENDING = "oauth_pending"
_COL_CODES = "oauth_codes"
_COL_ACCESS = "oauth_access_tokens"
_COL_REFRESH = "oauth_refresh_tokens"


def _dump(model) -> dict:
    """Pydantic v2 model → dict. Falls back to .dict() for older shims."""
    if hasattr(model, "model_dump"):
        return model.model_dump(mode="json")
    return model.dict()


def _validate(cls, data: dict):
    """dict → Pydantic v2 model."""
    if hasattr(cls, "model_validate"):
        return cls.model_validate(data)
    return cls(**data)


class FirestoreStorage:
    """OAuth state persisted in Cloud Firestore (Native mode)."""

    def __init__(self, project: Optional[str] = None, database: str = "(default)") -> None:
        from google.cloud.firestore import AsyncClient
        # `database="(default)"` matches the default Firestore database
        # created by `gcloud firestore databases create` without --database flag.
        self._db = AsyncClient(project=project, database=database)
        logger.info("FirestoreStorage initialized (project=%s, db=%s)",
                    project or "<ADC>", database)

    def _doc(self, collection: str, doc_id: str):
        return self._db.collection(collection).document(doc_id)

    # ── Clients ──────────────────────────────────────────────────────────

    async def get_client(self, client_id: str) -> Optional[OAuthClientInformationFull]:
        snap = await self._doc(_COL_CLIENTS, client_id).get()
        if not snap.exists:
            return None
        return _validate(OAuthClientInformationFull, snap.to_dict())

    async def store_client(self, client_info: OAuthClientInformationFull) -> None:
        await self._doc(_COL_CLIENTS, client_info.client_id).set(_dump(client_info))

    # ── Pending (consent step in flight) ─────────────────────────────────

    async def get_pending(self, request_id: str) -> Optional[PendingAuthRequest]:
        snap = await self._doc(_COL_PENDING, request_id).get()
        if not snap.exists:
            return None
        return PendingAuthRequest.from_dict(snap.to_dict())

    async def store_pending(self, request_id: str, pending: PendingAuthRequest) -> None:
        await self._doc(_COL_PENDING, request_id).set(pending.to_dict())

    async def pop_pending(self, request_id: str) -> Optional[PendingAuthRequest]:
        ref = self._doc(_COL_PENDING, request_id)
        snap = await ref.get()
        if not snap.exists:
            return None
        await ref.delete()
        return PendingAuthRequest.from_dict(snap.to_dict())

    # ── Authorization codes (one-time use) ───────────────────────────────

    async def get_code(self, code: str) -> Optional[AuthorizationCode]:
        snap = await self._doc(_COL_CODES, code).get()
        if not snap.exists:
            return None
        return _validate(AuthorizationCode, snap.to_dict())

    async def store_code(self, code: AuthorizationCode) -> None:
        await self._doc(_COL_CODES, code.code).set(_dump(code))

    async def pop_code(self, code_str: str) -> None:
        await self._doc(_COL_CODES, code_str).delete()

    # ── Access tokens ────────────────────────────────────────────────────

    async def get_access(self, token: str) -> Optional[AccessToken]:
        try:
            snap = await self._doc(_COL_ACCESS, token).get()
        except Exception as e:
            logger.error("FirestoreStorage.get_access failed: %s", e)
            raise
        if not snap.exists:
            return None
        try:
            return _validate(AccessToken, snap.to_dict())
        except Exception as e:
            logger.error("get_access: failed to deserialize doc: %s", e)
            raise

    async def store_access(self, token: AccessToken) -> None:
        try:
            await self._doc(_COL_ACCESS, token.token).set(_dump(token))
        except Exception as e:
            logger.error("store_access failed: %s", e)
            raise

    async def pop_access(self, token_str: str) -> None:
        try:
            await self._doc(_COL_ACCESS, token_str).delete()
        except Exception as e:
            logger.warning("pop_access: %s", e)

    # ── Refresh tokens ───────────────────────────────────────────────────

    async def get_refresh(self, token: str) -> Optional[RefreshToken]:
        try:
            snap = await self._doc(_COL_REFRESH, token).get()
        except Exception as e:
            logger.error("FirestoreStorage.get_refresh failed: %s", e)
            raise
        if not snap.exists:
            logger.info("get_refresh: token not found in Firestore (prefix=%s)",
                        token[:8] if token else "")
            return None
        try:
            return _validate(RefreshToken, snap.to_dict())
        except Exception as e:
            logger.error("get_refresh: failed to deserialize doc: %s", e)
            raise

    async def store_refresh(self, token: RefreshToken) -> None:
        try:
            await self._doc(_COL_REFRESH, token.token).set(_dump(token))
        except Exception as e:
            logger.error("store_refresh failed (prefix=%s): %s",
                         token.token[:8], e)
            raise

    async def pop_refresh(self, token_str: str) -> None:
        try:
            await self._doc(_COL_REFRESH, token_str).delete()
        except Exception as e:
            logger.warning("pop_refresh: %s", e)
