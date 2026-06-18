"""Consent page handlers for the Tiger MCP OAuth flow.

The Claude consumer app opens our `/authorize` URL in the user's browser,
which we redirect to `/consent?request_id=...`. This module renders the
form and processes the password submission. On success, we mint an
authorization code and redirect back to the OAuth client's redirect_uri.
"""
from __future__ import annotations

import hmac
import logging
import os
from typing import Awaitable, Callable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse, Response

from mcp_servers.tiger.oauth.provider import TigerOAuthProvider

logger = logging.getLogger("tiger-mcp.oauth.consent")


_CONSENT_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Authorize Tiger MCP</title>
  <style>
    body{{font-family:-apple-system,BlinkMacSystemFont,system-ui,sans-serif;
         max-width:480px;margin:60px auto;padding:0 20px;color:#222}}
    h1{{font-size:1.4em;margin-bottom:24px}}
    .box{{border:1px solid #e0e0e0;border-radius:8px;padding:24px;background:#fafafa}}
    .client{{font-weight:600;font-size:1.05em}}
    .scope{{background:#fff;border:1px solid #e0e0e0;padding:10px 12px;
            border-radius:6px;font-size:0.9em;margin:16px 0;color:#444}}
    input[type=password]{{width:100%;padding:10px;font-size:16px;margin:12px 0;
                          box-sizing:border-box;border:1px solid #ccc;border-radius:6px}}
    button{{width:100%;padding:12px;font-size:16px;background:#0066cc;color:#fff;
            border:0;border-radius:6px;cursor:pointer;font-weight:500}}
    button:hover{{background:#0055aa}}
    .err{{color:#c00;margin:8px 0;font-size:0.9em}}
    .muted{{color:#666;font-size:0.85em;margin-top:20px;text-align:center}}
  </style>
</head>
<body>
  <h1>Authorize access to your Tiger account</h1>
  <div class="box">
    <p><span class="client">{client_name}</span> is requesting access to your
    Tiger Brokers account through the Tiger MCP server.</p>
    <div class="scope">
      <strong>Read</strong> — positions, orders, funding, NAV history, option Greeks/chain<br>
      <strong>Trade</strong> — place / modify / cancel stock and option orders, execute combo rolls<br>
      <em>Every write tool defaults to preview mode; orders only submit when the
      caller explicitly passes confirm=True.</em>
    </div>
    {error}
    <form method="post">
      <input type="hidden" name="request_id" value="{request_id}">
      <input type="password" name="password" placeholder="Owner password"
             autocomplete="current-password" autofocus required>
      <button type="submit">Allow access</button>
    </form>
  </div>
  <p class="muted">If you didn't initiate this request, close this tab.</p>
</body>
</html>"""


def _escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&#39;")
    )


def make_consent_routes(
    provider: TigerOAuthProvider,
) -> tuple[Callable[[Request], Awaitable[Response]], Callable[[Request], Awaitable[Response]]]:
    """Return (GET handler, POST handler) for the /consent endpoint.

    The owner password is read from MCP_OAUTH_OWNER_PASSWORD env var at
    request time so that updates land without a process restart.
    """

    async def _render_form(request_id: str, client_name: str, error_html: str = "",
                           status_code: int = 200) -> HTMLResponse:
        html = _CONSENT_HTML.format(
            client_name=_escape(client_name),
            request_id=_escape(request_id),
            error=error_html,
        )
        return HTMLResponse(html, status_code=status_code)

    async def render(request: Request) -> Response:
        request_id = request.query_params.get("request_id", "")
        if not request_id:
            return HTMLResponse("Missing request_id", status_code=400)
        pending = await provider.get_pending_request(request_id)
        if pending is None:
            return HTMLResponse(
                "This authorization request is invalid or has expired.",
                status_code=400,
            )
        client = await provider.get_client(pending.client_id)
        client_name = "An MCP client"
        if client is not None and getattr(client, "client_name", None):
            client_name = client.client_name  # type: ignore[assignment]
        return await _render_form(request_id, client_name)

    async def handle(request: Request) -> Response:
        form = await request.form()
        request_id = str(form.get("request_id", ""))
        password = str(form.get("password", ""))

        expected = os.environ.get("MCP_OAUTH_OWNER_PASSWORD", "").strip()
        if not expected:
            logger.error("MCP_OAUTH_OWNER_PASSWORD not set on the server")
            return HTMLResponse(
                "Server is not configured for OAuth — owner password missing.",
                status_code=500,
            )

        pending = await provider.get_pending_request(request_id)
        if pending is None:
            return HTMLResponse(
                "This authorization request is invalid or has expired.",
                status_code=400,
            )
        client = await provider.get_client(pending.client_id)
        client_name = "An MCP client"
        if client is not None and getattr(client, "client_name", None):
            client_name = client.client_name  # type: ignore[assignment]

        if not hmac.compare_digest(password, expected):
            logger.warning("OAuth consent: wrong password for request_id=%s", request_id[:8])
            return await _render_form(
                request_id,
                client_name,
                error_html='<div class="err">Wrong password. Try again.</div>',
                status_code=401,
            )

        try:
            code, state, redirect_uri = await provider.finalize_authorization(request_id)
        except ValueError as e:
            return HTMLResponse(str(e), status_code=400)

        qs = {"code": code}
        if state:
            qs["state"] = state
        parsed = urlparse(redirect_uri)
        existing = dict(parse_qsl(parsed.query))
        existing.update(qs)
        target = urlunparse(parsed._replace(query=urlencode(existing)))
        logger.info("OAuth consent: granted code for client=%s", pending.client_id)
        return RedirectResponse(url=target, status_code=302)

    return render, handle
