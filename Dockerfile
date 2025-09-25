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

# Set default environment variables for the MCP server
ENV DREMIOAI_TOOLS__SERVER_MODE=FOR_DATA_PATTERNS
ENV DREMIOAI_DREMIO__OAUTH_SUPPORTED=false

# Expose port 80
EXPOSE 80

# Use the Cumulocity startup script that will retrieve configuration from tenant options
CMD ["cumulocity-mcp-server"]
