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
from mocks.http_mock import LARGE_SQL_MARKER, LARGE_SQL_JOB_ID


@pytest.mark.asyncio
async def test_run_sql_query_large_mock_fetches_multiple_pages(
    mock_config_dir, logging_server, logging_level
):
    async with http_streamable_mcp_server(
        logging_server,
        logging_level,
        dremio_overrides={"max_result_rows": 1200, "max_result_bytes": 0},
    ) as sf:
        async with http_streamable_client_server(
            sf.mcp_server, token="my-token"
        ) as session:
            result: CallToolResult = await session.call_tool(
                "RunSqlQuery",
                {"query": f"SELECT 1 /* {LARGE_SQL_MARKER} */"},
            )

        assert result is not None and result.structuredContent is not None
        assert not result.isError
        payload = result.structuredContent["result"]
        assert payload["result"][0]["row_id"] == 0
        assert payload["result"][-1]["row_id"] == 1199
        assert len(payload["result"]) == 1200

        result_fetches = [
            le
            for le in logging_server.logs()
            if le.path.endswith(f"/job/{LARGE_SQL_JOB_ID}/results") and le.method == "GET"
        ]
        assert len(result_fetches) == 3
        assert result_fetches[0].query_params == {"offset": "0", "limit": "500"}
        assert result_fetches[1].query_params == {"offset": "500", "limit": "500"}
        assert result_fetches[2].query_params == {"offset": "1000", "limit": "200"}


@pytest.mark.asyncio
async def test_run_sql_query_large_mock_truncation_sets_tool_error(
    mock_config_dir, logging_server, logging_level
):
    async with http_streamable_mcp_server(
        logging_server,
        logging_level,
        dremio_overrides={"max_result_rows": 1000, "max_result_bytes": 0},
    ) as sf:
        async with http_streamable_client_server(
            sf.mcp_server, token="my-token"
        ) as session:
            result: CallToolResult = await session.call_tool(
                "RunSqlQuery",
                {"query": f"SELECT 1 /* {LARGE_SQL_MARKER} */"},
            )

        assert result is not None and result.structuredContent is not None
        assert result.isError
        payload = result.structuredContent["result"]
        assert payload["truncated"] is True
        assert payload["truncation_reason"] == "row_limit"
        assert payload["returned_rows"] == 1000
        assert payload["total_rows"] == 1200
        assert "returned too much data" in result.content[0].text
