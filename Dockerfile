# Build stage
FROM python:3.13-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && . ~/.cargo/env
ENV PATH="/root/.cargo/bin:${PATH}"

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN pip install --upgrade pip build && \
    python -m build --wheel --outdir /dist && \
    pip install --prefix=/install /dist/*.whl

# Runtime stage
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Copy the complete Python installation from builder (this includes all compiled dependencies)
COPY --from=builder /install /usr/local

ENV DREMIOAI_TOOLS__SERVER_MODE=FOR_DATA_PATTERNS
ENV DREMIOAI_DREMIO__OAUTH_SUPPORTED=false

EXPOSE 80

CMD ["cumulocity-mcp-server"]
