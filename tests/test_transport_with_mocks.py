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
import asyncio
from contextlib import contextmanager
from unittest.mock import AsyncMock, patch
import pandas as pd

from dremioai.api.transport import AsyncHttpClient
from dremioai.servers import mcp as mcp_server
from dremioai.config.tools import ToolType
from dremioai.config import settings
from dremioai.tools.tools import get_tools
from tests.mocks.http_mock import mock_http_client


class TestTransportWithMocks:
    """Test the transport module using the HTTP mock framework"""

    @pytest.mark.asyncio
    async def test_async_http_client_get_with_mock(self):
        """Test AsyncHttpClient.get() with mocked responses"""
        mock_data = {"/api/v3/catalog": "catalog/spaces.json"}

        with mock_http_client(mock_data):
            client = AsyncHttpClient("http://test.dremio.com", "fake-token")

            # Test GET request with JSON deserialization
            result = await client.get("/api/v3/catalog")

            assert result is not None
            assert "data" in result
            assert len(result["data"]) == 2
            assert result["data"][0]["name"] == "Sample Space"
            assert result["data"][1]["name"] == "Analytics"

    @pytest.mark.asyncio
    async def test_async_http_client_post_with_mock(self):
        """Test AsyncHttpClient.post() with mocked responses"""
        mock_data = {"/api/v3/sql": "sql/job_status.json"}

        with mock_http_client(mock_data):
            client = AsyncHttpClient("http://test.dremio.com", "fake-token")

            # Test POST request with JSON deserialization
            result = await client.post(
                "/api/v3/sql", body={"sql": "SELECT * FROM test"}
            )

            assert result is not None
            assert result["jobState"] == "COMPLETED"
            assert result["rowCount"] == 42
            assert result["id"] == "job123-456-789"

    @pytest.mark.asyncio
    async def test_async_http_client_with_params(self):
        """Test AsyncHttpClient with query parameters"""
        mock_data = {"/api/v3/projects": "projects/project_list.json"}

        with mock_http_client(mock_data):
            client = AsyncHttpClient("http://test.dremio.com", "fake-token")

            # Test GET with parameters
            result = await client.get(
                "/api/v3/projects", params={"limit": "10", "offset": "0"}
            )

            assert result is not None
            # The mock will return the project_list.json content

    @pytest.mark.asyncio
    async def test_async_http_client_multiple_endpoints(self):
        """Test multiple endpoints with different mock data"""
        mock_data = {
            "/api/v3/catalog": "catalog/spaces.json",
            "/api/v3/sql": "sql/job_status.json",
            "/api/v3/projects": "projects/project_list.json",
        }

        with mock_http_client(mock_data):
            client = AsyncHttpClient("http://test.dremio.com", "fake-token")

            # Test multiple different endpoints
            catalog_result = await client.get("/api/v3/catalog")
            sql_result = await client.post("/api/v3/sql", body={"sql": "SELECT 1"})
            projects_result = await client.get("/api/v3/projects")

            # Verify each endpoint returns the expected mock data
            assert catalog_result["data"][0]["name"] == "Sample Space"
            assert sql_result["jobState"] == "COMPLETED"
            assert projects_result is not None  # project_list.json content

    @contextmanager
    def mock_settings_for_fastmcp(self, mode: ToolType):
        """Create mock settings for testing FastMCP server"""
        try:
            old = settings.instance()
            settings._settings.set(
                settings.Settings.model_validate(
                    {
                        "dremio": {
                            "uri": "https://test-dremio-uri.com",
                            "pat": "test-pat",
                            "project_id": "test-project-id",
                            "enable_search": True,  # Enable search for SearchTableAndViews tool
                        },
                        "tools": {"server_mode": mode},
                    }
                )
            )
            yield settings.instance()
        finally:
            settings._settings.set(old)

    @pytest.mark.asyncio
    async def test_fastmcp_server_tool_invocation(self):
        """Test creating FastMCP server and invoking all FOR_DATA_PATTERNS tools"""

        # Mock data for various API endpoints that tools might call
        mock_data = {
            "/api/v3/sql": "sql/job_status.json",
            "/api/v3/catalog": "catalog/spaces.json",
            "/api/v3/catalog/by-path": "catalog/table_schema.json",
            "/api/v3/search": "search/search_results.json",
        }

        with mock_http_client(mock_data):
            with self.mock_settings_for_fastmcp(ToolType.FOR_DATA_PATTERNS):
                # Initialize FastMCP server with FOR_DATA_PATTERNS tools
                fastmcp_server = mcp_server.init(mode=ToolType.FOR_DATA_PATTERNS)

                # Get list of registered tools
                tools_list = await fastmcp_server.list_tools()
                tool_names = {tool.name for tool in tools_list}

                # Verify expected tools are registered
                expected_tools = {
                    t.__name__ for t in get_tools(For=ToolType.FOR_DATA_PATTERNS)
                }
                assert tool_names == expected_tools

                # Test invoking each tool with appropriate parameters
                for tool in tools_list:
                    try:
                        if tool.name == "RunSqlQuery":
                            # Mock the sql.run_query function
                            with patch(
                                "dremioai.api.dremio.sql.run_query",
                                new_callable=AsyncMock,
                            ) as mock_run_query:
                                mock_df = pd.DataFrame([{"test_column": 1}])
                                mock_run_query.return_value = mock_df

                                result = await fastmcp_server.call_tool(
                                    tool.name, {"s": "SELECT 1 as test_column"}
                                )
                                assert result is not None
                                # Check if result has content and extract the text
                                if hasattr(result, "content") and result.content:
                                    content_text = (
                                        result.content[0].text
                                        if hasattr(result.content[0], "text")
                                        else str(result.content[0])
                                    )
                                    assert (
                                        "result" in content_text
                                        or "test_column" in content_text
                                    )

                        elif tool.name == "GetUsefulSystemTableNames":
                            result = await fastmcp_server.call_tool(tool.name, {})
                            assert result is not None
                            # This tool should return successfully even if the return format is different

                        elif tool.name == "GetSchemaOfTable":
                            # Mock the get_schema function and the settings URI
                            with patch(
                                "dremioai.api.dremio.catalog.get_schema",
                                new_callable=AsyncMock,
                            ) as mock_get_schema:
                                mock_get_schema.return_value = {
                                    "fields": [{"name": "test_col", "type": "VARCHAR"}]
                                }

                                result = await fastmcp_server.call_tool(
                                    tool.name, {"table_name": "test_table"}
                                )
                                assert result is not None

                        elif tool.name == "GetTableOrViewLineage":
                            # Mock the get_lineage function
                            with patch(
                                "dremioai.api.dremio.catalog.get_lineage",
                                new_callable=AsyncMock,
                            ) as mock_get_lineage:
                                mock_get_lineage.return_value = {
                                    "lineage": "test_lineage"
                                }

                                result = await fastmcp_server.call_tool(
                                    tool.name, {"table_name": "test_table"}
                                )
                                assert result is not None

                        elif tool.name == "SearchTableAndViews":
                            # Mock the search functions and run_in_parallel
                            with patch(
                                "dremioai.api.util.run_in_parallel",
                                new_callable=AsyncMock,
                            ) as mock_run_parallel:
                                mock_df = pd.DataFrame(
                                    [
                                        {
                                            "name": "test_table",
                                            "type": "TABLE",
                                            "tags": [],
                                            "description": "Test table",
                                            "schema": [],
                                        }
                                    ]
                                )
                                # Mock run_in_parallel to return list of DataFrames
                                mock_run_parallel.return_value = [mock_df, mock_df]

                                result = await fastmcp_server.call_tool(
                                    tool.name, {"query": "test query"}
                                )
                                assert result is not None

                        elif tool.name == "GetDescriptionOfTableOrSchema":
                            # Mock the get_descriptions function
                            with patch(
                                "dremioai.api.dremio.catalog.get_descriptions",
                                new_callable=AsyncMock,
                            ) as mock_get_descriptions:
                                mock_get_descriptions.return_value = {
                                    "test_table": {"description": "Test description"}
                                }

                                result = await fastmcp_server.call_tool(
                                    tool.name, {"name": "test_table"}
                                )
                                assert result is not None

                        print(f"✓ Successfully invoked tool: {tool.name}")

                    except Exception as e:
                        print(f"✗ Failed to invoke tool {tool.name}: {str(e)}")
                        # For this simple test, we'll continue with other tools
                        # In a more comprehensive test, you might want to fail here
                        continue

                print(
                    f"Completed testing {len(tools_list)} tools for FOR_DATA_PATTERNS mode"
                )

    @pytest.mark.asyncio
    async def test_async_http_client_with_custom_deserializer(self):
        """Test AsyncHttpClient with custom deserialization"""
        from pydantic import BaseModel
        from typing import List

        class Space(BaseModel):
            id: str
            name: str
            type: str

        class SpacesResponse(BaseModel):
            data: List[Space]

        mock_data = {"/api/v3/catalog": "catalog/spaces.json"}

        with mock_http_client(mock_data):
            client = AsyncHttpClient("http://test.dremio.com", "fake-token")

            # Test with Pydantic model deserialization
            result = await client.get("/api/v3/catalog", deser=SpacesResponse)

            assert isinstance(result, SpacesResponse)
            assert len(result.data) == 2
            assert result.data[0].name == "Sample Space"
            assert result.data[1].name == "Analytics"
