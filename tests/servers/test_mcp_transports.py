#
#  Copyright (C) 2017-2025 Dremio Corporation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import pytest
from unittest.mock import Mock, patch
from dremioai.config import settings
from dremioai.servers import mcp
from dremioai.servers.oauth_middleware import create_oauth_middleware


class TestMCPTransportConfiguration:
    """Test MCP server transport configuration"""

    def test_mcp_transport_enum(self):
        """Test MCPTransport enum values"""
        assert settings.MCPTransport.stdio == "stdio"
        assert settings.MCPTransport.streamable_http == "streamable-http"
        assert settings.MCPTransport.sse == "sse"

    def test_mcp_server_config_defaults(self):
        """Test MCPServerConfig default values"""
        config = settings.MCPServerConfig()
        assert config.transport == settings.MCPTransport.stdio
        assert config.host == "127.0.0.1"
        assert config.port == 8000
        assert config.path == "/mcp"
        assert config.oauth_enabled is False
        assert config.oauth_client_id is None
        assert config.oauth_client_secret is None
        assert config.oauth_scopes == ["mcp.access"]

    def test_mcp_server_config_http_transport_detection(self):
        """Test HTTP transport detection"""
        stdio_config = settings.MCPServerConfig(transport=settings.MCPTransport.stdio)
        assert not stdio_config.is_http_transport

        http_config = settings.MCPServerConfig(transport=settings.MCPTransport.streamable_http)
        assert http_config.is_http_transport

        sse_config = settings.MCPServerConfig(transport=settings.MCPTransport.sse)
        assert sse_config.is_http_transport

    def test_mcp_server_config_server_url(self):
        """Test server URL generation"""
        stdio_config = settings.MCPServerConfig(transport=settings.MCPTransport.stdio)
        assert stdio_config.server_url is None

        http_config = settings.MCPServerConfig(
            transport=settings.MCPTransport.streamable_http,
            host="localhost",
            port=9000,
            path="/api/mcp"
        )
        assert http_config.server_url == "http://localhost:9000/api/mcp"

    def test_settings_includes_mcp_server_config(self):
        """Test that Settings includes mcp_server configuration"""
        settings_instance = settings.Settings()
        assert hasattr(settings_instance, 'mcp_server')
        assert isinstance(settings_instance.mcp_server, settings.MCPServerConfig)

    def test_transport_choices(self):
        """Test transport choices function"""
        choices = mcp._transport()
        assert "stdio" in choices
        assert "streamable-http" in choices
        assert "sse" in choices

    @patch('dremioai.servers.mcp.FastMCP')
    def test_run_with_transport_stdio(self, mock_fastmcp):
        """Test running with STDIO transport"""
        mock_mcp_instance = Mock()
        mock_fastmcp.return_value = mock_mcp_instance

        config = settings.MCPServerConfig(transport=settings.MCPTransport.stdio)
        mcp.run_with_transport(mock_mcp_instance, config)

        mock_mcp_instance.run.assert_called_once_with()

    @patch('dremioai.servers.mcp.FastMCP')
    @patch('dremioai.servers.mcp.create_oauth_middleware')
    def test_run_with_transport_streamable_http(self, mock_create_oauth, mock_fastmcp):
        """Test running with streamable-http transport"""
        mock_mcp_instance = Mock()
        mock_fastmcp.return_value = mock_mcp_instance
        mock_create_oauth.return_value = None

        config = settings.MCPServerConfig(
            transport=settings.MCPTransport.streamable_http,
            host="localhost",
            port=9000,
            path="/api/mcp"
        )
        mcp.run_with_transport(mock_mcp_instance, config)

        mock_mcp_instance.run.assert_called_once_with(
            transport="streamable-http",
            host="localhost",
            port=9000,
            path="/api/mcp"
        )

    @patch('dremioai.servers.mcp.FastMCP')
    @patch('dremioai.servers.mcp.create_oauth_middleware')
    def test_run_with_transport_sse(self, mock_create_oauth, mock_fastmcp):
        """Test running with SSE transport"""
        mock_mcp_instance = Mock()
        mock_fastmcp.return_value = mock_mcp_instance
        mock_create_oauth.return_value = None

        config = settings.MCPServerConfig(
            transport=settings.MCPTransport.sse,
            host="localhost",
            port=9000,
            path="/sse"
        )
        mcp.run_with_transport(mock_mcp_instance, config)

        mock_mcp_instance.run.assert_called_once_with(
            transport="sse",
            host="localhost",
            port=9000,
            path="/sse"
        )

    def test_run_with_transport_invalid(self):
        """Test running with invalid transport raises error"""
        mock_mcp_instance = Mock()
        config = Mock()
        config.transport = "invalid"

        with pytest.raises(ValueError, match="Unsupported transport: invalid"):
            mcp.run_with_transport(mock_mcp_instance, config)


class TestOAuthMiddleware:
    """Test OAuth middleware functionality"""

    def test_create_oauth_middleware_disabled(self):
        """Test OAuth middleware creation when disabled"""
        config = settings.MCPServerConfig(oauth_enabled=False)
        middleware = create_oauth_middleware(config)
        assert middleware is None

    def test_create_oauth_middleware_no_client_id(self):
        """Test OAuth middleware creation without client ID"""
        config = settings.MCPServerConfig(oauth_enabled=True, oauth_client_id=None)
        middleware = create_oauth_middleware(config)
        assert middleware is None

    @patch('dremioai.config.settings.instance')
    def test_create_oauth_middleware_enabled(self, mock_settings_instance):
        """Test OAuth middleware creation when enabled"""
        # Mock settings
        mock_dremio = Mock()
        mock_dremio.uri = "https://api.dremio.cloud"
        mock_settings_instance.return_value.dremio = mock_dremio

        config = settings.MCPServerConfig(
            oauth_enabled=True,
            oauth_client_id="test-client-id",
            oauth_client_secret="test-secret",
            oauth_scopes=["mcp.access", "mcp.tools"]
        )
        
        middleware = create_oauth_middleware(config)
        assert middleware is not None
        assert middleware.client_id == "test-client-id"
        assert middleware.client_secret == "test-secret"
        assert middleware.scopes == ["mcp.access", "mcp.tools"]
        assert middleware.userinfo_url == "https://api.dremio.cloud/oauth/userinfo"
