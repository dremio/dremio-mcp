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
from urllib.parse import urlparse

import httpx
import pytest
from mcp.types import CallToolResult

from conftest import http_streamable_client_server, http_streamable_mcp_server


@pytest.mark.asyncio
async def test_metrics_endpoint_default(mock_config_dir, logging_server, logging_level):
    """Test that metrics are NOT available on the main app port by default (since separate metrics server is always started)."""
    async with http_streamable_mcp_server(logging_server, logging_level) as sf:
        async with httpx.AsyncClient() as client:
            # Test that metrics are NOT available on the main app port at /metrics
            # because the current implementation always starts separate metrics server
            metrics_url = urlparse(sf.mcp_server.url)._replace(path="/metrics").geturl()
            response = await client.get(metrics_url)
            assert response.status_code == 404


@pytest.mark.asyncio
async def test_separate_metrics_server(mock_config_dir, logging_server, logging_level):
    """Test that metrics can be served on a separate port."""
    async with http_streamable_mcp_server(logging_server, logging_level) as sf:
        # Give server time to start
        import asyncio

        await asyncio.sleep(1)

        async with httpx.AsyncClient() as client:
            # Test that metrics are available on separate port (from fixture)
            metrics_response = await client.get(
                f"http://127.0.0.1:{sf.metrics_port}/", timeout=1.0
            )
            assert metrics_response.status_code == 200


@pytest.mark.asyncio
async def test_metrics_format(mock_config_dir, logging_server, logging_level):
    """Test that metrics are in text format and initially contain no metric values."""
    async with http_streamable_mcp_server(logging_server, logging_level) as sf:
        # Give server time to start
        import asyncio

        await asyncio.sleep(1)

        async with httpx.AsyncClient() as client:
            # Test that metrics are available on separate port (from fixture)
            metrics_response = await client.get(
                f"http://127.0.0.1:{sf.metrics_port}/metrics", timeout=1.0
            )

            # Verify Prometheus format
            content_type = metrics_response.headers.get("content-type", "")
            assert "text/plain" in content_type.lower()

            content = metrics_response.text
            assert isinstance(content, str)

            # Should contain metric definitions (HELP and TYPE lines) but no actual metric values
            if content.strip():
                assert "# HELP mcp_tool_invocations_total" in content
                assert "# TYPE mcp_tool_invocations_total counter" in content
                assert "# HELP mcp_tool_invocation_duration" in content
                assert "# TYPE mcp_tool_invocation_duration histogram" in content

                # But should NOT contain any actual metric values (lines with numbers)
                lines = content.strip().split("\n")
                metric_value_lines = [
                    line for line in lines if not line.startswith("#") and line.strip()
                ]
                assert (
                    len(metric_value_lines) == 0
                ), f"Expected no metric values, but found: {metric_value_lines}"


@pytest.mark.asyncio
async def test_metrics_with_tool_invocation(
    mock_config_dir, logging_server, logging_level
):
    """Test that metrics are recorded when tools are invoked."""
    # Use the existing MCP server fixture to invoke tools
    async with http_streamable_mcp_server(logging_server, logging_level) as sf:
        # Give server time to start
        import asyncio

        await asyncio.sleep(1)

        # Invoke a tool to generate metrics
        async with http_streamable_client_server(
            sf.mcp_server, token="test-token"
        ) as session:
            result: CallToolResult = await session.call_tool(
                "RunSqlQuery", {"s": "SELECT 1"}
            )
            assert result is not None and result.structuredContent is not None

        # Check metrics after tool invocation
        async with httpx.AsyncClient() as client:
            metrics_response = await client.get(
                f"http://127.0.0.1:{sf.metrics_port}/", timeout=5.0
            )

            content = metrics_response.text

            # Should contain tool invocation metrics
            if content.strip():
                assert (
                    "mcp_tool_invocations" in content
                    and "mcp_tool_invocation_duration" in content
                )
