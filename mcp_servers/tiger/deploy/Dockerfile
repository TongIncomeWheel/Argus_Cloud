# Tiger MCP server image — runs the SSE transport for Google Cloud Run.
#
# Build context is the repo root (so tiger_api/ is accessible). From the
# repo root:
#   docker build -f mcp_servers/tiger/deploy/Dockerfile -t tiger-mcp .
#
# Cloud Run sets the PORT env var to 8080 by default; the server reads
# either MCP_PORT or PORT.

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install only what the MCP server needs — not the full Argus
# requirements.txt (no streamlit/plotly/gspread/alpaca-py/yfinance).
COPY mcp_servers/tiger/deploy/requirements-mcp.txt /tmp/req.txt
RUN pip install -r /tmp/req.txt

# Copy the bits we actually run.
COPY tiger_api ./tiger_api
COPY mcp_servers ./mcp_servers

ENV MCP_TRANSPORT=streamable-http \
    MCP_HOST=0.0.0.0 \
    PORT=8080

EXPOSE 8080

# Cloud Run probes the service URL by default. Streamable-HTTP transport
# responds on /mcp. Each tool call is its own short-lived HTTP request,
# so there's no persistent SSE stream to be killed by Cloud Run's
# 60-minute request timeout. Earlier deploys used --transport sse and
# saw connections die after the 1h SSE max.
CMD ["python", "-m", "mcp_servers.tiger.server", "--transport", "streamable-http"]
