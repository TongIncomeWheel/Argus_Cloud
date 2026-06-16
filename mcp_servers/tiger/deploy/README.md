# Tiger MCP server — Google Cloud Run deploy

Hosts `mcp_servers/tiger/server.py` as an SSE-transport MCP server on
Google Cloud Run, with bearer-token auth and Tiger credentials stored in
Google Secret Manager.

## What this is (and isn't) — Phase 2a

- **Is:** a public HTTPS MCP server you can hit from anywhere that knows
  the bearer token. Works with `curl`, Claude Code's `~/.claude/settings.json`
  (via a custom headers entry), or any HTTP MCP client.
- **Isn't yet:** registrable as a Claude.ai consumer Custom Connector —
  that needs OAuth 2.1 + PKCE per the MCP spec. Phase 2b adds the OAuth
  flow on top of the same server.

## Why Cloud Run

| | Why it fits |
|---|---|
| **Free tier is real** | 2M requests/mo, 360K GB-seconds, 180K vCPU-seconds — far above this workload |
| **Scales to zero** | $0 idle cost; ~1-3s cold start on first call after quiet period |
| **Secret Manager** | Tiger key + bearer never appear in env-var dumps, are versioned + audited |
| **HTTPS automatic** | Free TLS cert on `*.run.app` subdomain |
| **Region near Tiger** | `asia-southeast1` (Singapore) for TBSG-license proximity |

A credit card is required during GCP account setup but you won't be
billed unless you exceed the free quota — which this workload won't.

## One-time deploy

Prereqs:
- `gcloud` CLI installed (`brew install --cask google-cloud-sdk` or
  https://cloud.google.com/sdk/docs/install).
- A GCP project with billing enabled (free tier still requires this).

```bash
# === 0. Set context ===
export PROJECT_ID="<your-gcp-project-id>"
export REGION="asia-southeast1"
export SERVICE="argus-tiger-mcp"

gcloud config set project "$PROJECT_ID"
gcloud config set run/region "$REGION"

# Enable the APIs we use.
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  artifactregistry.googleapis.com

# === 1. Create the secrets ===
# Values come from developer.itigerup.com/profile (after rotating the
# leaked key) and a freshly generated bearer.

printf '%s' "<TIGER_PRIVATE_KEY_PK8 value>" \
  | gcloud secrets create tiger-private-key-pk8 --data-file=- --replication-policy=automatic

printf '%s' "<TIGER_ID value>" \
  | gcloud secrets create tiger-id --data-file=- --replication-policy=automatic

printf '%s' "50179929" \
  | gcloud secrets create tiger-account --data-file=- --replication-policy=automatic

python3 -c 'import secrets; print(secrets.token_urlsafe(48), end="")' \
  | gcloud secrets create mcp-bearer-token --data-file=- --replication-policy=automatic

# Grant Cloud Run's runtime service account read on the secrets.
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')
RUN_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

for secret in tiger-private-key-pk8 tiger-id tiger-account mcp-bearer-token; do
  gcloud secrets add-iam-policy-binding "$secret" \
    --member="serviceAccount:${RUN_SA}" \
    --role="roles/secretmanager.secretAccessor"
done

# === 2. Build + deploy from source ===
# Run this from the repo root so the build context includes tiger_api/.
cd "$(git rev-parse --show-toplevel)"

gcloud run deploy "$SERVICE" \
  --source . \
  --dockerfile mcp_servers/tiger/deploy/Dockerfile \
  --region "$REGION" \
  --allow-unauthenticated \
  --port 8080 \
  --memory 256Mi \
  --cpu 1 \
  --min-instances 0 \
  --max-instances 3 \
  --concurrency 50 \
  --timeout 3600 \
  --no-cpu-throttling \
  --set-env-vars MCP_TRANSPORT=sse,MCP_HOST=0.0.0.0,TIGER_LICENSE=TBSG,TIGER_ENV=PROD \
  --set-secrets TIGER_PRIVATE_KEY_PK8=tiger-private-key-pk8:latest,TIGER_ID=tiger-id:latest,TIGER_ACCOUNT=tiger-account:latest,MCP_BEARER_TOKEN=mcp-bearer-token:latest

# === 3. Patch MCP_BASE_URL with the URL Cloud Run just printed ===
URL=$(gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.url)')
gcloud run services update "$SERVICE" --region "$REGION" \
  --update-env-vars "MCP_BASE_URL=${URL}"

echo "Deployed: $URL"
```

`--allow-unauthenticated` is correct here: the bearer token is the auth
gate. Cloud Run's IAM-based auth is a *second* layer you could add but
it requires the calling client to sign with a Google identity, which
Claude Code and Claude.ai do not.

## Sanity check

```bash
BEARER=$(gcloud secrets versions access latest --secret=mcp-bearer-token)

# Without bearer → 401
curl -i "$URL/sse" | head -5

# With bearer → SSE stream
curl -i -H "Authorization: Bearer $BEARER" "$URL/sse" | head -20
```

## Wiring into Claude Code

Add to `~/.claude/settings.json` (NOT the repo-level `.mcp.json` — that
stays the stdio dev convenience):

```json
{
  "mcpServers": {
    "tiger-hosted": {
      "url": "https://argus-tiger-mcp-<hash>-as.a.run.app/sse",
      "headers": {
        "Authorization": "Bearer <your-MCP_BEARER_TOKEN>"
      }
    }
  }
}
```

## Updating secrets / rotating Tiger key

```bash
# Append a new version to a secret; Cloud Run picks it up on next request
# because we pinned ":latest" in --set-secrets.
printf '%s' "<new key>" | gcloud secrets versions add tiger-private-key-pk8 --data-file=-

# Force a re-deploy if you want the new value picked up immediately:
gcloud run services update "$SERVICE" --region "$REGION" --update-env-vars REDEPLOY=$(date +%s)
```

## Cost expectations

For one user querying their Tiger account a few times a day:
- Requests: tens per day → far below the 2M/mo free quota
- vCPU time: ~minutes per day → far below 180K vCPU-seconds/mo free
- Memory time: same scale
- Egress: trivial

**Net: $0/mo expected, with a credit card on file as the safety net.**

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| 401 on every request | Bearer mismatch — `gcloud secrets versions access latest --secret=mcp-bearer-token` to check |
| 500 on first Tiger tool call | Secret missing/malformed — `gcloud run services logs read $SERVICE --limit 50` |
| Cold start lag ~3s | First request after idle period; subsequent calls are warm |
| Container fails to start | Check Cloud Build logs: `gcloud builds list --limit 5` |
| Tools list shows 0 entries | Process crashed during boot — check Cloud Run logs |

## Declarative alternative

`cloud-run.yaml` in this directory has the same configuration as the
`gcloud run deploy` walkthrough above. Apply with
`gcloud run services replace mcp_servers/tiger/deploy/cloud-run.yaml`
after editing `PROJECT_ID` and `PROJECT_HASH` placeholders.
