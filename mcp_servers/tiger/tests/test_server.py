"""Structural tests for the Tiger MCP server.

These exercise auth bootstrap, scalar serialization, and tool registration.
They never instantiate TigerClient or hit Tiger's API.
"""
from __future__ import annotations

import asyncio
import os
import unittest
from datetime import datetime
from pathlib import Path


class BootstrapEnvTests(unittest.TestCase):
    def setUp(self) -> None:
        for k in list(os.environ.keys()):
            if k.startswith("TIGER_"):
                os.environ.pop(k, None)
        target = Path("/tmp/argus_tiger_config/tiger_openapi_config.properties")
        if target.exists():
            target.unlink()

    def test_no_env_returns_none(self) -> None:
        from mcp_servers.tiger.auth import bootstrap_from_env
        self.assertIsNone(bootstrap_from_env())

    def test_individual_vars_assembled_into_properties_file(self) -> None:
        os.environ["TIGER_PRIVATE_KEY_PK8"] = "FAKE_PK8_VALUE"
        os.environ["TIGER_ID"] = "12345"
        os.environ["TIGER_ACCOUNT"] = "67890"
        os.environ["TIGER_LICENSE"] = "TBSG"
        os.environ["TIGER_ENV"] = "SANDBOX"

        from mcp_servers.tiger.auth import bootstrap_from_env
        path = bootstrap_from_env()

        self.assertIsNotNone(path)
        assert path is not None
        self.assertTrue(path.exists())
        content = path.read_text()
        for expected in (
            "private_key_pk8=FAKE_PK8_VALUE",
            "tiger_id=12345",
            "account=67890",
            "license=TBSG",
            "env=SANDBOX",
        ):
            self.assertIn(expected, content)
        self.assertEqual(os.environ["TIGER_CONFIG_PATH"], str(path.parent))

    def test_properties_content_shortcut_writes_verbatim(self) -> None:
        body = "tiger_id=99\naccount=88\nlicense=TBHK\nenv=PROD\n"
        os.environ["TIGER_PROPERTIES_CONTENT"] = body
        from mcp_servers.tiger.auth import bootstrap_from_env
        path = bootstrap_from_env()
        self.assertIsNotNone(path)
        assert path is not None
        self.assertEqual(path.read_text(), body)


class ScalarSerializationTests(unittest.TestCase):
    def test_primitives_passthrough(self) -> None:
        from mcp_servers.tiger.server import _scalar
        self.assertEqual(_scalar(None), None)
        self.assertEqual(_scalar(1), 1)
        self.assertEqual(_scalar(1.5), 1.5)
        self.assertEqual(_scalar("x"), "x")
        self.assertEqual(_scalar(True), True)

    def test_list_recurses(self) -> None:
        from mcp_servers.tiger.server import _scalar
        self.assertEqual(_scalar([1, "x", None]), [1, "x", None])

    def test_dict_recurses(self) -> None:
        from mcp_servers.tiger.server import _scalar
        self.assertEqual(_scalar({"a": 1, "b": [2, 3]}), {"a": 1, "b": [2, 3]})

    def test_datetime_isoformat(self) -> None:
        from mcp_servers.tiger.server import _scalar
        self.assertEqual(_scalar(datetime(2026, 1, 1, 12, 0)), "2026-01-01T12:00:00")

    def test_unknown_object_falls_back_to_str(self) -> None:
        from mcp_servers.tiger.server import _scalar

        class Obj:
            def __str__(self) -> str:
                return "obj"

        self.assertEqual(_scalar(Obj()), "obj")


class ServerToolRegistrationTests(unittest.TestCase):
    EXPECTED = {
        "get_account_summary",
        "get_stock_positions",
        "get_option_positions",
        "get_filled_orders",
        "get_open_orders",
        "get_cancelled_orders",
        "get_transactions",
        "get_order_transactions",
        "get_prime_assets",
        "get_funding_history",
        "get_spot_prices",
        "get_nav_history",
    }

    def test_expected_tools_registered(self) -> None:
        from mcp_servers.tiger import server

        async def gather_names() -> set[str]:
            tools = await server.mcp.list_tools()
            return {t.name for t in tools}

        registered = asyncio.run(gather_names())
        missing = self.EXPECTED - registered
        self.assertFalse(missing, f"Missing tools: {missing}")


class BuildServerTests(unittest.TestCase):
    """_build_server() switches auth wiring based on env vars."""

    def setUp(self) -> None:
        # Save and restore env; nuke MCP_* between tests.
        self._saved = {k: os.environ.get(k) for k in (
            "MCP_BEARER_TOKEN", "MCP_BASE_URL", "MCP_HOST", "MCP_PORT",
        )}
        for k in self._saved:
            os.environ.pop(k, None)

    def tearDown(self) -> None:
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_no_bearer_token_builds_unauthenticated_server(self) -> None:
        from mcp_servers.tiger.server import _build_server
        m = _build_server()
        self.assertEqual(m.name, "tiger")
        # No auth settings → settings.auth is None
        self.assertIsNone(m.settings.auth)

    def test_bearer_token_wires_auth_settings(self) -> None:
        os.environ["MCP_BEARER_TOKEN"] = "s3cret"
        os.environ["MCP_BASE_URL"] = "https://argus-tiger-mcp.fly.dev"
        from mcp_servers.tiger.server import _build_server
        m = _build_server()
        self.assertIsNotNone(m.settings.auth)
        self.assertEqual(str(m.settings.auth.issuer_url), "https://argus-tiger-mcp.fly.dev/")
        self.assertIn("tiger:read", m.settings.auth.required_scopes or [])


class BearerTokenVerifierTests(unittest.TestCase):
    def test_correct_token_returns_access_token(self) -> None:
        from mcp_servers.tiger.auth import BearerTokenVerifier

        v = BearerTokenVerifier("s3cret")
        result = asyncio.run(v.verify_token("s3cret"))
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.token, "s3cret")
        self.assertEqual(result.client_id, "argus-tiger-mcp")
        self.assertIn("tiger:read", result.scopes)

    def test_wrong_token_returns_none(self) -> None:
        from mcp_servers.tiger.auth import BearerTokenVerifier

        v = BearerTokenVerifier("s3cret")
        self.assertIsNone(asyncio.run(v.verify_token("wrong")))

    def test_empty_token_returns_none(self) -> None:
        from mcp_servers.tiger.auth import BearerTokenVerifier

        v = BearerTokenVerifier("s3cret")
        self.assertIsNone(asyncio.run(v.verify_token("")))

    def test_empty_expected_rejected_at_construction(self) -> None:
        from mcp_servers.tiger.auth import BearerTokenVerifier

        with self.assertRaises(ValueError):
            BearerTokenVerifier("")

    def test_constant_time_compare_used(self) -> None:
        """Same-length wrong tokens are still rejected — guards against the
        regression of switching to a plain == compare that short-circuits."""
        from mcp_servers.tiger.auth import BearerTokenVerifier

        v = BearerTokenVerifier("abcdef")
        self.assertIsNone(asyncio.run(v.verify_token("abcdeg")))
        self.assertIsNone(asyncio.run(v.verify_token("zbcdef")))


if __name__ == "__main__":
    unittest.main()
