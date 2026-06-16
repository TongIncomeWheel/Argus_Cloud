"""OAuth 2.1 + PKCE + Dynamic Client Registration implementation for the
Tiger MCP server.

Single-user design: anyone can attempt the OAuth flow, but only someone
holding `MCP_OAUTH_OWNER_PASSWORD` can complete the consent step. Tokens
live in process memory; Cloud Run cold starts will force a re-auth from
the Claude consumer app, which is acceptable for one-user deployments.
"""
