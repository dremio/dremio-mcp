# Build stage
FROM python:3.13-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /build

# Copy project files
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
RUN chmod +x /bin/uv /bin/uvx

COPY pyproject.toml uv.lock README.md src/ ./

RUN uv sync --no-dev --frozen
RUN mkdir /dist
RUN uv export --frozen --no-editable --format requirements.txt | grep -v -e '^[[:space:]]*\.[[:space:]]*$' > /dist/requirements.txt
RUN uv build --wheel -o /dist


# Runtime stage
FROM python:3.13-slim

# Create non-root user
RUN useradd -m -u 1001 appuser

WORKDIR /app

# Copy wheel from builder
COPY --from=builder /dist/*.whl /dist/requirements.txt /tmp/

# Install the wheel and dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN pip install --no-cache-dir /tmp/dremioai*.whl
RUN rm /tmp/*.whl /tmp/requirements.txt

USER 1001

# Console script is now properly installed
CMD ["dremio-mcp-server", "run"]