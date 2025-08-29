# Build stage
FROM python:3.13-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

# Copy project files
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Build wheel
RUN pip install --upgrade pip build && \
    python -m build --wheel --outdir /dist

# Runtime stage
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Create non-root user
RUN useradd -m -u 1001 appuser

WORKDIR /app

# Copy wheel from builder
COPY --from=builder /dist/*.whl /tmp/

# Install the wheel and dependencies
RUN pip install --no-cache-dir /tmp/*.whl && \
    rm /tmp/*.whl

ARG DREMIOAI_DREMIO__URI
ARG DREMIOAI_DREMIO__PAT

ENV DREMIOAI_TOOLS__SERVER_MODE=FOR_DATA_PATTERNS
ENV DREMIOAI_DREMIO__URI=$DREMIOAI_DREMIO__URI
ENV DREMIOAI_DREMIO__PAT=$DREMIOAI_DREMIO__PAT
ENV DREMIOAI_DREMIO__OAUTH_SUPPORTED=false

# Expose port 80
EXPOSE 80

# Console script is now properly installed
CMD ["dremio-mcp-server", "run", "--port", "80", "--disable-auth", "--enable-streaming-http", "--no-log-to-file", "--enable-json-logging"]
