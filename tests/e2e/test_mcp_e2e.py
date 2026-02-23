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
from conftest import http_streamable_client_server, http_streamable_mcp_server
from mcp.types import CallToolResult
from rich import print as pp

from dremioai.tools.tools import get_tools
from dremioai.config import settings
from urllib.parse import urlparse
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_basic(mock_config_dir, logging_server, logging_level):
    async with http_streamable_mcp_server(logging_server, logging_level) as sf:
        async with http_streamable_client_server(
            sf.mcp_server, token="my-token"
        ) as session:
            lts = await session.list_tools()
            tr = {t.name for t in lts.tools}
            assert tr == {
                t.__name__ for t in get_tools(For=settings.instance().tools.server_mode)
            }


@pytest.mark.asyncio
async def test_healthz(mock_config_dir, logging_server, logging_level):
    async with http_streamable_mcp_server(logging_server, logging_level) as sf:
        async with AsyncClient() as client:
            r = await client.get(
                urlparse(sf.mcp_server.url)._replace(path="/healthz").geturl()
            )
            assert (
                r.status_code == 200
            ), f"/healthz failed with {r.text}, {r.status_code}"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "engine_name",
    [
        pytest.param(None, id="no_engine_name"),
        pytest.param("test-engine"),
        pytest.param("test-engine-2"),
    ],
)
async def test_wlm_engine_name(
    mock_config_dir, logging_server, logging_level, engine_name
):
    async with http_streamable_mcp_server(
        logging_server, logging_level, wlm_engine=engine_name
    ) as sf:
        async with http_streamable_client_server(
            sf.mcp_server, token="my-token"
        ) as session:
            result: CallToolResult = await session.call_tool(
                "RunSqlQuery", {"query": "SELECT 1"}
            )
            assert (
                result is not None
                and result.structuredContent is not None
                and result.structuredContent["result"]["result"][0]["test_column"] == 1
            ), f"Error running tool {result}"

            for le in logging_server.logs():
                if le.path.endswith("/sql") and le.method == "POST":
                    if engine_name is None:
                        assert (
                            le.json.get("engineName") is None
                        ), f"{le.json} has engineName"
                    else:
                        assert (
                            le.json.get("engineName") == engine_name
                        ), f"{le.json} does not have the right engineName"
