# Tiger MCP server — Fly.io deploy

Hosts `mcp_servers/tiger/server.py` as a long-lived SSE-transport MCP server
with bearer-token auth.

## What this is (and isn't) — Phase 2a

- **Is:** a public HTTPS MCP server you can hit from anywhere that knows the
  bearer token. Works with `curl`, Claude Code's `.mcp.json` (via a custom
  headers entry), or any HTTP MCP client.
- **Isn't yet:** registrable as a Claude.ai consumer Custom Connector — that
  requires OAuth 2.1 + PKCE per the MCP spec. Phase 2b will add the OAuth
  flow on top of the same server.

## One-time deploy

Prereqs: a Fly.io account with payment method on file (free hobby allowance
covers this workload at $0 expected).

```bash
# From the repo root.
cd mcp_servers/tiger/deploy

# 1. Pick a unique app name — edit fly.toml's `app = "..."` line.

# 2. Create the app on Fly. This reads fly.toml and creates the app,
#    builder, certs. Don't accept the "auto-detect" prompts; we already
#    have a Dockerfile and fly.toml.
fly launch --copy-config --no-deploy

# 3. Set the secrets. The TIGER_* values come from
#    developer.itigerup.com/profile after you rotate the leaked key.
#    MCP_BEARER_TOKEN: generate a long random string and keep it secret.
fly secrets set \
  TIGER_PRIVATE_KEY_PK8="..." \
  TIGER_ID="..." \
  TIGER_ACCOUNT="..." \
  TIGER_LICENSE="TBSG" \
  TIGER_ENV="PROD" \
  MCP_BEARER_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(48))')"

# 4. First deploy.
fly deploy

# 5. Sanity check from your laptop.
curl -i \
  -H "Authorization: Bearer $(fly secrets list | awk '/MCP_BEARER_TOKEN/ {print $2}')" \
  https://argus-tiger-mcp.fly.dev/sse
```

A 401 means the bearer is wrong; a 200 with an SSE stream means it's up.

## Wiring it into Claude Code

Add an `httpServers` entry to your local `~/.claude/settings.json` (NOT the
repo-level `.mcp.json` — that one stays the stdio dev convenience):

```json
{
  "mcpServers": {
    "tiger-hosted": {
      "url": "https://argus-tiger-mcp.fly.dev/sse",
      "headers": {
        "Authorization": "Bearer <your-MCP_BEARER_TOKEN>"
      }
    }
  }
}
```

## Updating secrets

```bash
# Rotating a Tiger key, for example:
fly secrets set TIGER_PRIVATE_KEY_PK8="<new key>"
# Triggers a re-deploy automatically.
```

## Cost expectations

- Compute: shared-cpu-1x @ 256 MB, always-on ≈ $1.94/mo
- Outbound bandwidth: trivial for this workload (Tiger API + occasional client calls)
- Storage: none (server is stateless in Phase 2a)
- Net: $0 expected within Fly hobby allowance (~$5/mo)

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| 401 on every request | Bearer mismatch — verify `MCP_BEARER_TOKEN` |
| 500 on first Tiger tool call | TIGER_* secrets missing or malformed — `fly logs` |
| Cold start lag > 5s | First request after a deploy; subsequent calls warm |
| Tools list shows 0 entries | Process crashed during boot — check `fly logs` |
