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
async def test_oauth_discovery_rfc8414_compliance(mock_config_dir, logging_server, logging_level):
    """Test that OAuth discovery fails with trailing slash (reproduces DX-114676).

    This test reproduces the issue that started happening around Feb 12, 2026 when
    Claude Desktop clients were updated to strictly validate RFC 8414 Section 3.2,
    which requires the issuer field in OAuth metadata to exactly match the discovery URL.

    The bug: AnyHttpUrl adds a trailing slash to the issuer URL, causing strict clients
    to reject the OAuth metadata because the issuer doesn't match the discovery URL.
    """
    async with http_streamable_mcp_server(logging_server, logging_level) as sf:
        async with AsyncClient() as client:
            oauth_url = urlparse(sf.mcp_server.url)._replace(
                path="/.well-known/oauth-authorization-server"
            ).geturl()

            r = await client.get(oauth_url)

            if r.status_code == 404:
                pytest.skip("OAuth not configured for this test environment")

            assert r.status_code == 200, f"OAuth metadata endpoint failed: {r.text}"

            # Check the raw JSON response (what clients actually receive)
            data = r.json()
            issuer_from_json = data["issuer"]

            if issuer_from_json.endswith('/'):
                pytest.fail(
                    f"RFC 8414 Section 3.2 violation: issuer has trailing slash.\n"
                    f"Got: {issuer_from_json}\n"
                    f"This causes OAuth discovery to fail with strict clients (Claude Desktop after Feb 12, 2026).\n"
                    f"The issuer field MUST exactly match the discovery URL without trailing slash."
                )


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
