FROM python:3.12-slim-bullseye

WORKDIR /app

# Pin to a known release; bump when upgrading.
# Asset naming: github-mcp-server_Linux_{x86_64|arm64}.tar.gz (no version in filename).
ARG GITHUB_MCP_SERVER_VERSION=1.0.5

RUN set -eux && \
    apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    DPKG_ARCH=$(dpkg --print-architecture) && \
    case "$DPKG_ARCH" in \
        amd64)  GH_ARCH="x86_64" ;; \
        arm64)  GH_ARCH="arm64"  ;; \
        *)      echo "Unsupported arch: $DPKG_ARCH" && exit 1 ;; \
    esac && \
    curl -fL \
        "https://github.com/github/github-mcp-server/releases/download/v${GITHUB_MCP_SERVER_VERSION}/github-mcp-server_Linux_${GH_ARCH}.tar.gz" \
        -o /tmp/github-mcp-server.tar.gz && \
    tar -xzf /tmp/github-mcp-server.tar.gz -C /usr/local/bin github-mcp-server && \
    rm /tmp/github-mcp-server.tar.gz && \
    chmod +x /usr/local/bin/github-mcp-server && \
    apt-get purge -y curl && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "poetry>=2.1.0,<3.0.0" && \
    poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock* ./
RUN poetry install --only main --no-interaction --no-ansi --no-root

COPY src/ ./src/

EXPOSE 8001

CMD ["ddtrace-run", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8001"]
