# Cumulocity Microservice Integration

This directory contains scripts for running the Dremio MCP server as a Cumulocity microservice.

## Overview

The Dremio MCP server can be deployed as a Cumulocity microservice that retrieves its configuration (Dremio URL and credentials) from Cumulocity tenant options. This approach keeps sensitive information secure and allows for easy configuration management.

## Architecture

1. **Cumulocity Microservice Environment**: Provides `C8Y_TENANT`, `C8Y_USER`, and `C8Y_PASSWORD` environment variables
2. **Startup Script**: Retrieves configuration from tenant options and launches the MCP server
3. **Tenant Options**: Store Dremio URL and Personal Access Token
4. **MCP Server**: Runs with the retrieved configuration

## Files

- `cumulocity_startup.py`: Main startup script that retrieves config and launches MCP server
- `setup_tenant_options.py`: Helper script to set up tenant options (run once)
- `test_cumulocity.py`: Testing utilities for local development

## Setup Instructions

### 1. Set up Tenant Options

Before deploying the microservice, you need to configure the tenant options with your Dremio connection details:

```bash
python -m dremioai.scripts.setup_tenant_options \
  --tenant YOUR_TENANT_ID \
  --user YOUR_CUMULOCITY_USER \
  --password YOUR_CUMULOCITY_PASSWORD \
  --dremio-url https://your-dremio-server.com:9047 \
  --dremio-pat YOUR_DREMIO_PERSONAL_ACCESS_TOKEN
```

This creates two tenant options:
- Category: `dremio-mcp`, Key: `url`
- Category: `dremio-mcp`, Key: `pat`

### 2. Deploy the Microservice

Build and deploy your Docker image to Cumulocity. The microservice will:

1. Start with the `cumulocity-mcp-server` command
2. Retrieve Dremio configuration from tenant options
3. Set `DREMIOAI_DREMIO__URI` and `DREMIOAI_DREMIO__PAT` environment variables
4. Launch the MCP server with SSE transport on port 80
5. Expose the SSE endpoint at `/service/dremio-mcp/`

### 3. Access the Service

Once deployed, the MCP server will be available at:
- SSE endpoint: `https://your-tenant.cumulocity.com/service/dremio-mcp/sse`
- Messages endpoint: `https://your-tenant.cumulocity.com/service/dremio-mcp/messages/`
- Health check: `https://your-tenant.cumulocity.com/service/dremio-mcp/healthz`

## Local Testing

For local development and testing:

```bash
# Set up test environment (modify the script with your credentials)
python -m dremioai.scripts.test_cumulocity

# Run the startup script locally
python -m dremioai.scripts.test_cumulocity run
```

## Environment Variables

### Required (provided by Cumulocity):
- `C8Y_TENANT`: Your Cumulocity tenant ID
- `C8Y_USER`: Microservice username
- `C8Y_PASSWORD`: Microservice password

### Optional:
- `C8Y_BASEURL`: Custom Cumulocity base URL (defaults to `https://{tenant}.cumulocity.com`)

### Set by the startup script:
- `DREMIOAI_DREMIO__URI`: Retrieved from tenant option `datahub.dremio-url`
- `DREMIOAI_DREMIO__PAT`: Retrieved from tenant option `datahub.credentials.dremio-pat`

## Security

- Dremio credentials are stored securely in Cumulocity tenant options
- The microservice uses Cumulocity's authentication system
- Personal Access Tokens are never logged or exposed in container images
- All communication uses HTTPS

## Troubleshooting

### Startup Failures

Check the microservice logs for:
1. Missing Cumulocity environment variables
2. Authentication failures
3. Missing or empty tenant options
4. Network connectivity issues

### Common Issues

1. **Missing tenant options**: Run the setup script to create them
2. **Authentication failures**: Verify C8Y credentials and permissions
3. **Dremio connection issues**: Verify the Dremio URL and PAT are correct
4. **Network issues**: Ensure the microservice can reach the Dremio server

## Customization

You can customize the MCP server arguments by modifying the `cumulocity_startup.py` script or by passing arguments to the startup script:

```python
# Custom arguments in the startup script
mcp_args = [
    "serve",
    "--port", "80",
    "--disable-auth",
    "--enable-sse",
    "--no-log-to-file",
    "--enable-json-logging",
    "--root-path", "/service/dremio-mcp",
    "--log-level", "DEBUG"  # Add custom arguments here
]
```