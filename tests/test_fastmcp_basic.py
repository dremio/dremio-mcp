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
import uuid

import pytest
from contextlib import contextmanager
from collections import OrderedDict

from dremioai.servers import mcp as mcp_server
from dremioai.config.tools import ToolType
from dremioai.config import settings
from dremioai.tools.tools import get_tools

# Import mock_http_client - handle both pytest and standalone execution
try:
    from tests.mocks.http_mock import mock_http_client
except ImportError:
    # For standalone execution, add project root to path
    import sys
    from pathlib import Path

    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))
    from tests.mocks.http_mock import mock_http_client


@contextmanager
def mock_settings_for_test(mode: ToolType):
    """Create mock settings for testing FastMCP server"""
    try:
        old = settings.instance()
        settings._settings.set(
            settings.Settings.model_validate(
                {
                    "dremio": {
                        "uri": "https://test-dremio-uri.com",
                        "pat": "test-pat",
                        "project_id": uuid.uuid4(),
                        "enable_search": True,
                    },
                    "tools": {"server_mode": mode},
                }
            )
        )
        yield settings.instance()
    finally:
        settings._settings.set(old)


@pytest.mark.asyncio
async def test_create_fastmcp_server_and_register_tools():
    """
    Simple test that creates a FastMCP server, registers all tools for FOR_DATA_PATTERNS mode,
    and performs basic invocation of each tool using transport mocks.
    """

    # Mock data for HTTP endpoints that tools will call
    mock_data = OrderedDict(
        [
            (r"/sql", "sql/job_submission.json"),  # SQL query submission
            (r"/job/test-job-12345$", "sql/job_status.json"),  # Job status check
            (r"/job/test-job-12345/results$", "sql/job_results.json"),  # Job results
            (r"/search", "search/search_results.json"),  # Search endpoints
            (r"/catalog/.*/wiki", "catalog/wiki.json"),  # Wiki endpoints
            (r"/catalog/.*/tags", "catalog/tags.json"),  # Tags endpoints
            (r"/catalog/.*/graph", "catalog/lineage.json"),  # Lineage endpoints
            (r"/catalog(/by-path)?", "catalog/table_schema.json"),  # Schema endpoints
        ]
    )

    with mock_http_client(mock_data):
        with mock_settings_for_test(ToolType.FOR_DATA_PATTERNS):
            # Create FastMCP server with FOR_DATA_PATTERNS tools
            fastmcp_server = mcp_server.init(mode=ToolType.FOR_DATA_PATTERNS)

            # Verify server was created successfully
            assert fastmcp_server is not None

            # Get list of registered tools
            tools_list = await fastmcp_server.list_tools()
            tool_names = {tool.name for tool in tools_list}

            # Verify expected tools are registered
            expected_tools = {
                t.__name__ for t in get_tools(For=ToolType.FOR_DATA_PATTERNS)
            }
            assert tool_names == expected_tools

            # Test basic invocation of each tool
            successful_invocations = 0
            args = {
                "RunSqlQuery": {"s": "SELECT 1"},
                "SearchTableAndViews": {"query": "test query"},
                "GetSchemaOfTable": {"table_name": "test_table"},
                "GetUsefulSystemTableNames": {},
                "GetTableOrViewLineage": {"table_name": "test_table"},
                "GetDescriptionOfTableOrSchema": {"name": "test_table"},
            }
            for tool in tools_list:
                if (
                    result := await fastmcp_server.call_tool(tool.name, args[tool.name])
                ) is not None:
                    successful_invocations += 1

            assert successful_invocations == len(tools_list)


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_create_fastmcp_server_and_register_tools())
