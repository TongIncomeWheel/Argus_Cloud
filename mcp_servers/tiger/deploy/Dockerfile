# Tiger MCP server image — runs the SSE transport for Fly.io / Cloud Run.
#
# Build context expects the repo root as build context (so tiger_api/ and
# requirements.txt are accessible). From the repo root:
#   docker build -f mcp_servers/tiger/deploy/Dockerfile -t tiger-mcp .

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install runtime deps. We deliberately do NOT install streamlit/plotly etc.
# from the full requirements.txt — the MCP server only needs the Tiger SDK,
# the MCP SDK, and pandas (used by funding history).
COPY mcp_servers/tiger/deploy/requirements-mcp.txt /tmp/req.txt
RUN pip install -r /tmp/req.txt

# Copy only what the MCP server needs to keep the image small.
COPY tiger_api ./tiger_api
COPY mcp_servers ./mcp_servers

ENV MCP_TRANSPORT=sse \
    MCP_HOST=0.0.0.0 \
    MCP_PORT=8000

EXPOSE 8000

# Fly.io healthchecks hit "/sse" on FastMCP's SSE transport. No special
# entrypoint needed — the server logs its bind address on startup.
CMD ["python", "-m", "mcp_servers.tiger.server", "--transport", "sse"]
