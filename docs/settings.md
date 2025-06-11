# Dremio AI Settings Configuration

## Default Configuration

The default configuration file is located at:

-   `$HOME/.config/dremioai/config.yaml` (Unix/macOS)
-   `%XDG_CONFIG_HOME%/dremioai/config.yaml` (if XDG_CONFIG_HOME is set)

You can view the default configuration (if it exists) and its location using the `dremio-mcp-server config list` command:

```bash
$ uv run dremio-mcp-server config list
Default config file: /home/user/.config/dremioai/config.yaml (exists = True)
dremio:
  uri: https://api.dremio.cloud
  pat: your-pat-here
  project_id: project123
  experimental_enabled: false
  allow_dml: false
tools:
  server_mode: FOR_SELF
```

To only show the configuration file path without displaying its contents:

```bash
$ dremio-mcp-server config list --show-filename
Default config file: /home/user/.config/dremioai/config.yaml (exists = True)
```

## Overview

The `dremioai.config.settings` module provides a comprehensive configuration system for managing various aspects of the Dremio AI tools and servers. It uses Pydantic for robust validation and type checking of configuration values.

## Configuration Structure

### Base Settings

| Section      | Description                     |
| ------------ | ------------------------------- |
| `dremio`     | Dremio connection settings      |
| `tools`      | Tool-specific configurations    |
| `prometheus` | Prometheus integration settings |
| `langchain`  | LangChain framework settings    |
| `beeai`      | BeeAI framework settings        |

## Configuration Sections

### Dremio Settings

```yaml
dremio:
  uri: <string|DremioCloudUri> # Dremio instance URI
  pat: <string> # Personal Access Token
  project_id: <string> # Optional: Project ID for Dremio Cloud
  enable_experimental: <bool> # Optional: Enable experimental features
  allow_dml: <bool> # Optional: Allow MCP Server to create views in Dremio
```

URI can be specified as:

-   Direct URL: `https://your-dremio-instance`
-   Predefined for Dremio cloud: `PROD` (https://api.dremio.cloud) or `PRODEMEA` (https://api.eu.dremio.cloud)

PAT can be provided:

-   Directly as a string
-   As a file path prefixed with '@' (e.g., "@~/tokens/dremio.token")

### Tools Settings

```yaml
tools:
  server_mode: <string|ToolType|int> # Tool types to enable
```

Server modes:

-   `FOR_SELF`: Dremio cluster introspection
-   `FOR_PROMETHEUS`: Prometheus integration
-   `FOR_DATA_PATTERNS`: Data pattern analysis
-   `EXPERIMENTAL`: Experimental features

NOTE: if you intend to use experimental features you must also set experimental_enabled: true as shown above. 

Multiple modes can be combined using comma separation: `FOR_SELF,FOR_PROMETHEUS`

### Prometheus Settings

```yaml
prometheus:
  uri: <string|HttpUrl> # Prometheus server URI
  token: <string> # Authentication token
```

### LangChain Settings (Experimental)

The Tools defined in the `tools` section can be used with LangChain if required as well. The LangChain integration depends on `openai` or `ollama` for LLM. The [src/dremoiai/servers/frameworks/langchain/server.py] supplies a command line interface showing the integration.

```yaml
langchain:
  llm: <Model> # LLM type (ollama/openai)
  openai:
    api_key: <string> # OpenAI API key
    model: <string> # Model name (default: gpt-4)
    org: <string> # Optional: Organization ID
  ollama:
    model: <string> # Model name (default: llama3.1)
```

### BeeAI Settings (Experimental)

[BeeAI](https://github.com/i-am-bee/beeai-framework) framework supports MCP as well as multiple LLM providers. The [src/dremoiai/servers/frameworks/beeai/server.py] supplies a command line interface showing this integration.

```yaml
beeai:
  mcp_server:
    command: "uv"
    args:
    - "run"
    - "--directory"
    - "<toplevel git directory>"
    - "dremio-mcp-server"
    - "run"
  sliding_memory_size: <int> # Memory window size
  anthropic:
    api_key: <string> # Anthropic API key
    chat_model: <string> # Chat model name
  openai: <OpenAI> # OpenAI settings (same as LangChain)
  ollama: <Ollama> # Ollama settings (same as LangChain)
```
 

## Configuration Methods

### File-based Configuration

Default configuration location: `~/.config/dremioai/config.yaml`

Example:

```yaml
dremio:
  uri: "https://api.dremio.cloud"
  pat: "@~/tokens/dremio.pat"
  project_id: "project123" # required only for DC
  enable_experimental: <bool> # Optional: Enable experimental features
  allow_dml: <bool> # Optional: Allow MCP Server to create views in Dremio
tools:
  server_mode: "FOR_SELF,FOR_DATA_PATTERNS"

```

### Environment Variables

Settings can be configured using environment variables with nested delimiter '\_':

```bash
DREMIO_URI="https://api.dremio.cloud"
DREMIO_PAT="your-pat-here"
TOOLS_SERVER_MODE="FOR_SELF"
```

### Programmatic Configuration

```python
from dremioai.config import settings

# Load from file
settings.configure("path/to/config.yaml")

# Get current settings
current = settings.instance()

# Override settings
current.with_overrides({
    "dremio.uri": "https://new-uri.com",
    "tools.server_mode": "FOR_SELF"
})
```

## Validation

-   All settings are validated using Pydantic models
-   Type checking and conversion is performed automatically
-   Custom validators are implemented for special types (URIs, file paths, etc.)
-   Invalid configurations will raise appropriate validation errors

## Security Notes

-   Token files should have appropriate permissions
-   Environment files (.env) should be properly secured
-   Avoid committing sensitive configuration to version control
