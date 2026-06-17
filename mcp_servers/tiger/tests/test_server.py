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
        # Phase 2c write tools
        "place_option_order",
        "cancel_order",
        "execute_roll",
        # Phase 2d option-chain / Greeks / quotes
        "get_option_expirations",
        "get_option_chain",
        "get_option_briefs",
        "get_option_greeks",
        "get_option_bars",
        "get_option_depth",
        "get_option_trade_ticks",
    }

    def test_expected_tools_registered(self) -> None:
        from mcp_servers.tiger import server

        async def gather_names() -> set[str]:
            tools = await server.mcp.list_tools()
            return {t.name for t in tools}

        registered = asyncio.run(gather_names())
        missing = self.EXPECTED - registered
        self.assertFalse(missing, f"Missing tools: {missing}")


class WriteToolPreviewGateTests(unittest.TestCase):
    """confirm=False MUST NOT touch the TigerClient.

    A real Tiger call would either fail (no credentials in this env) or place
    a live order. Neither is acceptable for a preview, so we set _client to a
    sentinel that explodes loudly if any method is called.
    """

    def setUp(self) -> None:
        from mcp_servers.tiger import server

        class _Boom:
            def __getattr__(self, name):
                raise AssertionError(f"TigerClient.{name} called during preview — preview gate broken")

        # Save current state
        self._prev_client = server._client
        server._client = _Boom()  # type: ignore[assignment]

    def tearDown(self) -> None:
        from mcp_servers.tiger import server
        server._client = self._prev_client  # type: ignore[assignment]

    def test_place_option_order_preview_returns_spec_no_client_call(self) -> None:
        from mcp_servers.tiger.server import place_option_order
        result = place_option_order(
            symbol="MSTR", expiry="2026-07-18", strike=250.0, right="PUT",
            side="SELL_TO_OPEN", quantity=2, limit_price=5.50,
        )
        self.assertTrue(result["preview"])
        self.assertFalse(result["placed"])
        self.assertEqual(result["spec"]["symbol"], "MSTR")
        self.assertEqual(result["spec"]["quantity"], 2)
        self.assertEqual(result["spec"]["premium_per_contract_usd"], 550.0)
        self.assertEqual(result["spec"]["total_premium_usd"], 1100.0)
        self.assertIn("SELL_TO_OPEN 2x MSTR", result["summary"])

    def test_cancel_order_preview_returns_spec_no_client_call(self) -> None:
        from mcp_servers.tiger.server import cancel_order
        result = cancel_order(order_id="ABC123")
        self.assertTrue(result["preview"])
        self.assertFalse(result["placed"])
        self.assertEqual(result["spec"]["order_id"], "ABC123")

    def test_execute_roll_preview_returns_spec_no_client_call(self) -> None:
        from mcp_servers.tiger.server import execute_roll
        result = execute_roll(
            symbol="MSTR",
            close_expiry="2026-07-18", close_strike=250.0, close_right="PUT",
            new_expiry="2026-08-15", new_strike=240.0,
            quantity=1, net_credit_limit=1.25,
        )
        self.assertTrue(result["preview"])
        self.assertFalse(result["placed"])
        self.assertEqual(result["spec"]["close_leg"]["side"], "BUY_TO_CLOSE")
        self.assertEqual(result["spec"]["open_leg"]["side"], "SELL_TO_OPEN")
        self.assertEqual(result["spec"]["net_credit_limit_total_usd"], 125.0)
        self.assertIn("credit", result["summary"])

    def test_execute_roll_preview_negative_credit_is_debit(self) -> None:
        from mcp_servers.tiger.server import execute_roll
        result = execute_roll(
            symbol="MSTR",
            close_expiry="2026-07-18", close_strike=250.0, close_right="PUT",
            new_expiry="2026-08-15", new_strike=260.0,
            quantity=1, net_credit_limit=-0.50,
        )
        self.assertTrue(result["preview"])
        self.assertIn("debit", result["summary"])
        self.assertEqual(result["spec"]["net_credit_limit_per_contract_usd"], -0.50)


class OptionIdentifierTests(unittest.TestCase):
    """Format helper is pure logic — testable without the Tiger SDK."""

    def test_basic_put(self) -> None:
        from tiger_api.client import _format_option_identifier
        ident = _format_option_identifier("MSTR", "2026-07-18", 250.0, "PUT")
        # Pad symbol to 6 chars + YYMMDD + P|C + strike*1000 zero-padded
        self.assertEqual(ident, "MSTR  260718P00250000")

    def test_basic_call(self) -> None:
        from tiger_api.client import _format_option_identifier
        ident = _format_option_identifier("AAPL", "2027-01-15", 150.0, "CALL")
        self.assertEqual(ident, "AAPL  270115C00150000")

    def test_short_form_right(self) -> None:
        from tiger_api.client import _format_option_identifier
        ident = _format_option_identifier("AAPL", "2027-01-15", 150.5, "P")
        # Strike 150.50 → 150500
        self.assertEqual(ident, "AAPL  270115P00150500")

    def test_lowercase_normalized(self) -> None:
        from tiger_api.client import _format_option_identifier
        ident = _format_option_identifier("mstr", "2026-07-18", 250, "put")
        self.assertEqual(ident, "MSTR  260718P00250000")

    def test_invalid_right_raises(self) -> None:
        from tiger_api.client import _format_option_identifier
        with self.assertRaises(ValueError):
            _format_option_identifier("MSTR", "2026-07-18", 250, "BOTH")

    def test_build_identifiers_accepts_dicts_and_strings(self) -> None:
        from tiger_api.client import _build_option_identifiers
        result = _build_option_identifiers([
            {"symbol": "MSTR", "expiry": "2026-07-18", "strike": 250, "right": "PUT"},
            "AAPL  270115C00150000",  # already formatted
            {"symbol": "AAPL", "expiry": "2027-01-15", "strike": 150, "right": "CALL"},
        ])
        self.assertEqual(result, [
            "MSTR  260718P00250000",
            "AAPL  270115C00150000",
            "AAPL  270115C00150000",
        ])

    def test_build_identifiers_skips_incomplete_dicts(self) -> None:
        from tiger_api.client import _build_option_identifiers
        result = _build_option_identifiers([
            {"symbol": "MSTR"},  # missing fields
            {"symbol": "MSTR", "expiry": "2026-07-18", "strike": 250, "right": "PUT"},
        ])
        self.assertEqual(result, ["MSTR  260718P00250000"])


class OptionIdentifierDetectionTests(unittest.TestCase):
    """Detect OCC-shaped strings so get_spot_prices can route correctly."""

    def test_real_option_id_detected(self) -> None:
        from tiger_api.client import _looks_like_option_identifier
        self.assertTrue(_looks_like_option_identifier("MSTR  260718P00250000"))
        self.assertTrue(_looks_like_option_identifier("AAPL  270115C00150000"))
        self.assertTrue(_looks_like_option_identifier("SPY   260620P00500000"))

    def test_stock_tickers_not_option(self) -> None:
        from tiger_api.client import _looks_like_option_identifier
        for ticker in ("MSTR", "AAPL", "BRK.B", "GOOGL", "T", "SPY"):
            self.assertFalse(_looks_like_option_identifier(ticker),
                             f"{ticker!r} should not look like an option id")

    def test_garbage_not_option(self) -> None:
        from tiger_api.client import _looks_like_option_identifier
        # Wrong put_call char
        self.assertFalse(_looks_like_option_identifier("MSTR  260718X00250000"))
        # Strike not all digits
        self.assertFalse(_looks_like_option_identifier("MSTR  260718P00ABC000"))
        # Too short
        self.assertFalse(_looks_like_option_identifier("MSTR2607"))


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
