"""Tests for the TigerOAuthProvider — covers the critical flows without
hitting any live MCP transport."""
from __future__ import annotations

import asyncio
import unittest
from typing import cast

from mcp.server.auth.provider import AuthorizationParams
from mcp.shared.auth import OAuthClientInformationFull

from mcp_servers.tiger.oauth.provider import TigerOAuthProvider
from mcp_servers.tiger.oauth.storage import InMemoryStorage


def _make_client(client_id: str = "claude-test-client") -> OAuthClientInformationFull:
    return OAuthClientInformationFull(
        client_id=client_id,
        client_secret=None,
        redirect_uris=[cast(object, "https://claude.ai/api/mcp/auth_callback")],
        client_name="Claude Test",
        scope="tiger:read",
    )


def _make_params(
    code_challenge: str = "x" * 43,
    redirect_uri: str = "https://claude.ai/api/mcp/auth_callback",
    state: str = "opaque-state-xyz",
) -> AuthorizationParams:
    return AuthorizationParams(
        state=state,
        scopes=["tiger:read"],
        code_challenge=code_challenge,
        redirect_uri=cast(object, redirect_uri),
        redirect_uri_provided_explicitly=True,
        resource=None,
    )


class TigerOAuthProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.storage = InMemoryStorage()
        self.provider = TigerOAuthProvider(self.storage, "https://argus-tiger-mcp.example.com")

    def test_register_then_get_client_round_trip(self) -> None:
        client = _make_client()
        asyncio.run(self.provider.register_client(client))
        got = asyncio.run(self.provider.get_client("claude-test-client"))
        self.assertIsNotNone(got)
        assert got is not None
        self.assertEqual(got.client_id, "claude-test-client")

    def test_authorize_returns_consent_url_with_request_id(self) -> None:
        client = _make_client()
        asyncio.run(self.provider.register_client(client))
        url = asyncio.run(self.provider.authorize(client, _make_params()))
        self.assertTrue(url.startswith("https://argus-tiger-mcp.example.com/consent?"))
        self.assertIn("request_id=", url)
        # The request must be retrievable
        request_id = url.split("request_id=")[1]
        pending = asyncio.run(self.provider.get_pending_request(request_id))
        self.assertIsNotNone(pending)
        assert pending is not None
        self.assertEqual(pending.client_id, "claude-test-client")
        self.assertEqual(pending.state, "opaque-state-xyz")

    def test_full_auth_code_flow_issues_access_and_refresh_tokens(self) -> None:
        client = _make_client()
        asyncio.run(self.provider.register_client(client))
        url = asyncio.run(self.provider.authorize(client, _make_params()))
        request_id = url.split("request_id=")[1]

        code, state, redirect_uri = asyncio.run(
            self.provider.finalize_authorization(request_id)
        )
        self.assertEqual(state, "opaque-state-xyz")
        self.assertEqual(redirect_uri, "https://claude.ai/api/mcp/auth_callback")

        loaded_code = asyncio.run(self.provider.load_authorization_code(client, code))
        self.assertIsNotNone(loaded_code)
        assert loaded_code is not None
        self.assertEqual(loaded_code.scopes, ["tiger:read"])

        token = asyncio.run(self.provider.exchange_authorization_code(client, loaded_code))
        self.assertIsNotNone(token.access_token)
        self.assertIsNotNone(token.refresh_token)
        self.assertEqual(token.token_type, "Bearer")
        self.assertGreater(token.expires_in or 0, 0)

        # Access token must be loadable and scoped
        at = asyncio.run(self.provider.load_access_token(token.access_token))
        self.assertIsNotNone(at)
        assert at is not None
        self.assertIn("tiger:read", at.scopes)

    def test_auth_code_is_single_use(self) -> None:
        client = _make_client()
        asyncio.run(self.provider.register_client(client))
        url = asyncio.run(self.provider.authorize(client, _make_params()))
        request_id = url.split("request_id=")[1]
        code, _, _ = asyncio.run(self.provider.finalize_authorization(request_id))
        loaded = asyncio.run(self.provider.load_authorization_code(client, code))
        assert loaded is not None
        asyncio.run(self.provider.exchange_authorization_code(client, loaded))
        # Second load must be None — code consumed
        again = asyncio.run(self.provider.load_authorization_code(client, code))
        self.assertIsNone(again)

    def test_refresh_token_rotation_invalidates_old(self) -> None:
        client = _make_client()
        asyncio.run(self.provider.register_client(client))
        url = asyncio.run(self.provider.authorize(client, _make_params()))
        request_id = url.split("request_id=")[1]
        code, _, _ = asyncio.run(self.provider.finalize_authorization(request_id))
        loaded = asyncio.run(self.provider.load_authorization_code(client, code))
        assert loaded is not None
        first = asyncio.run(self.provider.exchange_authorization_code(client, loaded))

        rt = asyncio.run(self.provider.load_refresh_token(client, first.refresh_token or ""))
        self.assertIsNotNone(rt)
        assert rt is not None
        second = asyncio.run(self.provider.exchange_refresh_token(client, rt, ["tiger:read"]))
        self.assertNotEqual(first.refresh_token, second.refresh_token)
        self.assertNotEqual(first.access_token, second.access_token)
        # Old refresh token must be gone
        rt_after = asyncio.run(self.provider.load_refresh_token(client, first.refresh_token or ""))
        self.assertIsNone(rt_after)

    def test_finalize_with_unknown_request_id_raises(self) -> None:
        with self.assertRaises(ValueError):
            asyncio.run(self.provider.finalize_authorization("does-not-exist"))

    def test_load_authorization_code_rejects_wrong_client(self) -> None:
        client_a = _make_client("client-a")
        client_b = _make_client("client-b")
        asyncio.run(self.provider.register_client(client_a))
        asyncio.run(self.provider.register_client(client_b))
        url = asyncio.run(self.provider.authorize(client_a, _make_params()))
        request_id = url.split("request_id=")[1]
        code, _, _ = asyncio.run(self.provider.finalize_authorization(request_id))
        # Wrong client trying to load the code
        wrong = asyncio.run(self.provider.load_authorization_code(client_b, code))
        self.assertIsNone(wrong)


if __name__ == "__main__":
    unittest.main()
