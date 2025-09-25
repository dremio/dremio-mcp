#!/usr/bin/env python3
"""
Cumulocity Microservice Startup Script

This script retrieves Dremio configuration from Cumulocity tenant options
and launches the MCP server with the appropriate environment variables.

Required Environment Variables (injected by Cumulocity):
- C8Y_TENANT: Cumulocity tenant ID
- C8Y_USER: Cumulocity username
- C8Y_PASSWORD: Cumulocity password

Tenant Options Retrieved:
- Category: "datahub"
- Keys:
  - dremio-url: The Dremio URI
  - credentials.dremio-pat: The Dremio Personal Access Token
"""

import os
import sys
import subprocess
import requests
import json
import logging
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CumulocityClient:
    """Simple Cumulocity client for tenant options API"""

    def __init__(self, tenant: str, username: str, password: str, base_url: Optional[str] = None):
        self.tenant = tenant
        self.username = username
        self.password = password

        # Use C8Y_BASEURL if available, otherwise construct from tenant
        if base_url:
            self.base_url = base_url.rstrip('/')
        else:
            # Assume standard Cumulocity cloud URL pattern
            self.base_url = f"https://{tenant}.cumulocity.com"

        self.auth = (f"{tenant}/{username}", password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

    def get_tenant_option(self, category: str, key: str) -> Optional[str]:
        """Retrieve a specific tenant option value"""
        try:
            url = f"{self.base_url}/tenant/options/{category}/{key}"
            logger.info(f"Retrieving tenant option: {category}.{key}")

            response = self.session.get(url)
            response.raise_for_status()

            data = response.json()
            value = data.get('value')

            if value:
                logger.info(f"Successfully retrieved tenant option: {category}.{key}")
                return value
            else:
                logger.warning(f"Tenant option {category}.{key} is empty")
                return None

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.error(f"Tenant option not found: {category}.{key}")
            else:
                logger.error(f"HTTP error retrieving tenant option {category}.{key}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving tenant option {category}.{key}: {e}")
            return None

    def get_datahub_config(self) -> Dict[str, Optional[str]]:
        """Retrieve Dremio configuration from datahub tenant options"""
        config = {
            'dremio_uri': self.get_tenant_option('datahub', 'dremio-url'),
            'dremio_pat': self.get_tenant_option('datahub', 'credentials.dremio-pat')
        }

        return config


def validate_environment() -> Dict[str, str]:
    """Validate required Cumulocity environment variables"""
    required_vars = ['C8Y_TENANT', 'C8Y_USER', 'C8Y_PASSWORD']
    env_vars = {}

    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        else:
            env_vars[var] = value

    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    logger.info("All required Cumulocity environment variables are present")
    return env_vars


def setup_dremio_environment(config: Dict[str, Optional[str]]) -> None:
    """Set up Dremio environment variables from tenant options"""
    dremio_uri = config.get('dremio_uri')
    dremio_pat = config.get('dremio_pat')

    if not dremio_uri:
        logger.error("Dremio URI not found in tenant options (datahub.dremio-url)")
        sys.exit(1)

    if not dremio_pat:
        logger.error("Dremio PAT not found in tenant options (datahub.credentials.dremio-pat)")
        sys.exit(1)

    # Set environment variables for the MCP server
    os.environ['DREMIOAI_DREMIO__URI'] = dremio_uri
    os.environ['DREMIOAI_DREMIO__PAT'] = dremio_pat

    logger.info(f"Set DREMIOAI_DREMIO__URI to: {dremio_uri}")
    logger.info("Set DREMIOAI_DREMIO__PAT (value hidden for security)")


def launch_mcp_server() -> None:
    """Launch the MCP server with the configured environment"""
    # Get the command line arguments (excluding the script name)
    mcp_args = sys.argv[1:] if len(sys.argv) > 1 else [
        "serve",
        "--port", "80",
        "--disable-auth",
        "--enable-sse",
        "--no-log-to-file",
        "--enable-json-logging",
        "--root-path", "/service/dremio-mcp"
    ]

    # Launch the MCP server
    cmd = ["dremio-mcp-server"] + mcp_args
    logger.info(f"Launching MCP server with command: {' '.join(cmd)}")

    try:
        # Replace current process with MCP server
        os.execvp("dremio-mcp-server", cmd)
    except Exception as e:
        logger.error(f"Failed to launch MCP server: {e}")
        sys.exit(1)


def main():
    """Main startup function"""
    logger.info("Starting Cumulocity MCP Server...")

    # Validate environment variables
    env_vars = validate_environment()

    # Get optional base URL
    base_url = os.getenv('C8Y_BASEURL')

    # Initialize Cumulocity client
    client = CumulocityClient(
        tenant=env_vars['C8Y_TENANT'],
        username=env_vars['C8Y_USER'],
        password=env_vars['C8Y_PASSWORD'],
        base_url=base_url
    )

    # Retrieve Dremio configuration
    logger.info("Retrieving Dremio configuration from tenant options...")
    config = client.get_datahub_config()

    # Setup environment variables
    setup_dremio_environment(config)

    # Launch MCP server
    launch_mcp_server()


if __name__ == "__main__":
    main()