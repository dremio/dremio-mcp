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
from mcp.types import CallToolResult
from conftest import http_streamable_client_server, http_streamable_mcp_server


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "project_id",
    [
        pytest.param(None, id="no_project_id"),
        pytest.param("my-project-id", id="project_id"),
    ],
)
async def test_tool_pat(mock_config_dir, logging_server, logging_level, project_id):
    async with http_streamable_mcp_server(
        logging_server, logging_level, project_id
    ) as sf:
        async with http_streamable_client_server(
            sf.mcp_server, token="my-token"
        ) as session:
            result: CallToolResult = await session.call_tool(
                "RunSqlQuery", {"s": "SELECT 1"}
            )
            assert (
                result is not None and result.structuredContent is not None
            ), f"Error running tool {result}"
            assert result.structuredContent["result"]["result"][0]["test_column"] == 1
            from rich import print as pp

            pp(logging_server.logs())
            for le in logging_server.logs():
                assert (
                    le.headers.get("authorization") == "Bearer my-token"
                ), f"{le} does not have the right auth header"

                if project_id:
                    assert (
                        project_id in le.path
                    ), f"{le} does not have the right project id"
