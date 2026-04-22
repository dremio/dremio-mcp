# Dremio MCP Server

## Project Overview

An MCP (Model Context Protocol) server that enables LLM integration with Dremio. It allows LLMs like Claude to query and interact with Dremio data sources via the MCP protocol. Supports local (stdio) and remote (streaming HTTP) deployment modes.

## Tech Stack

- **Language**: Python 3.11+
- **Package Manager**: `uv` (not pip)
- **Build System**: Hatchling
- **Framework**: FastMCP / FastAPI / Starlette
- **Key Libraries**: mcp, pydantic, structlog, typer, PyJWT, LaunchDarkly SDK
- **Testing**: pytest with pytest-asyncio (strict mode)

## Project Structure

```
src/dremioai/
├── api/              # API clients (Dremio REST, Prometheus, CLI)
│   ├── dremio/       # Dremio API client
│   ├── prometheus/   # Prometheus API client
│   └── cli/          # CLI helpers
├── config/           # Configuration management (YAML-based)
├── servers/          # MCP server implementation
│   ├── mcp.py        # Main MCP server entry point (CLI via typer)
│   ├── jwks_verifier.py  # JWT/JWKS auth verification
│   └── frameworks/   # Framework integrations (langchain, beeai)
├── tools/            # MCP tool definitions
│   └── tools.py      # Base Tools class
├── metrics/          # Prometheus metrics
└── resources/        # MCP resources
```

## Common Commands

```bash
# Install dependencies
uv sync

# Run the MCP server
uv run dremio-mcp-server run

# Run with custom config
uv run dremio-mcp-server run --config-file <path>

# Run all tests
uv run pytest tests

# Run a specific test file
uv run pytest tests/test_chart.py

# Manage config
uv run dremio-mcp-server config create dremioai --uri <uri> --pat <pat>
uv run dremio-mcp-server config list --type dremioai

# Build Docker image
docker build -t dremio-mcp:0.1.0 .
```

## Development Guidelines

- Follow PEP 8 style guidelines
- Use type hints for function arguments and return values
- Async-first: tools and server handlers are async (`asyncio_mode = strict`)
- New tools must inherit from the `Tools` base class in `dremioai.tools.tools`
- Tools are categorized by `ToolType`: `FOR_DATA_PATTERNS`, `FOR_SELF`, `FOR_PROMETHEUS`
- Config is YAML-based, located at `~/.config/dremioai/config.yaml` by default
- Commit messages start with a JIRA ticket ID (e.g., `DX-XXXXX: description`)
- Branch from `main` for all changes

## Testing

- Test files live in `tests/` mirroring the `src/` structure
- pytest config is in `pytest.ini` with `-v --showlocals -x` defaults
- Tests use strict asyncio mode — use `@pytest.mark.asyncio` for async tests
- E2E tests are in `tests/e2e/`

## Deployment

- **Local**: stdio mode via `uv run dremio-mcp-server run`
- **Remote/K8s**: Helm chart in `helm/dremio-mcp/` with streaming HTTP mode
- Auth: PAT (dev/local) or OAuth + External Token Provider (production)
