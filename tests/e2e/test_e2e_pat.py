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

import httpx
import pytest
from mcp.types import CallToolResult
from conftest import http_streamable_client_server, http_streamable_mcp_server
from urllib.parse import urlparse
from dremioai.config import settings

from dremioai.metrics.tool_metrics import invocation_counter


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "project_id",
    [
        pytest.param(None, id="no_project_id"),
        pytest.param(uuid.uuid4(), id="project_id"),
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

            for le in logging_server.logs():
                assert (
                    le.headers.get("authorization") == "Bearer my-token"
                ), f"{le} does not have the right auth header"

                if project_id:
                    assert (
                        str(project_id) in le.path
                    ), f"{le} does not have the right project id"

        async with httpx.AsyncClient() as client:
            u = urlparse(sf.mcp_server.url)._replace(path="/metrics/").geturl()
            r = await client.get(u, headers={"Authorization": "Bearer my-token"})
            assert (
                r.status_code == 200
            ), f"Error getting metrics: {r.text} status={r.status_code}"
            if project_id is None:
                project_id = settings.instance().dremio.project_id
            for line in r.text.splitlines():
                if (
                    line.startswith(f"{invocation_counter._name}_total{{")
                    and f'project_id="{project_id}"' in line
                ):
                    assert (
                        float(line.split()[-1]) == 1.0
                    ), f"Invocation count not 1: {line}"
                    break
            else:
                assert False, f"Invocation count for {project_id} not found in {r.text}"
