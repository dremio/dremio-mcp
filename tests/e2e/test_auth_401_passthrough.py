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
"""
Test: Dremio API 401 is swallowed by the MCP SDK and returned as a 200 tool
result.  The MCP client never sees HTTP 401, so its OAuth refresh flow is
never triggered.
"""

import io
import json
import random

import pytest

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.middleware import Middleware

from mcp.types import CallToolResult

from conftest import (
    http_streamable_mcp_server,
    http_streamable_client_server,
)
from mocks.http_mock import (
    LoggingMiddleware,
    LoggingServerFixture,
    start_server_with_app,
)

# Mock Dremio responses matching the Pydantic models in sql.py
_JOB_SUBMISSION = {"id": "test-job-12345"}
_JOB_STATUS = {
    "jobState": "COMPLETED",
    "id": "test-job-12345",
    "rowCount": 1,
    "errorMessage": "",
    "startedAt": "2024-01-01T10:00:00.000Z",
    "endedAt": "2024-01-01T10:00:05.123Z",
    "queryType": "REST",
    "queueName": "SMALL",
    "queueId": "SMALL",
    "resourceSchedulingStartedAt": "2024-01-01T10:00:00.500Z",
    "resourceSchedulingEndedAt": "2024-01-01T10:00:01.000Z",
    "cancellationReason": "",
}
_JOB_RESULTS = {
    "rowCount": 1,
    "rows": [{"test_column": 1}],
    "schema": [{"name": "test_column", "type": {"name": "INTEGER"}}],
}


def _create_401_after_first_server(log_level="warning") -> LoggingServerFixture:
    """Mock Dremio backend: 200 on first SQL submission, 401 on subsequent."""
    log_file = io.StringIO()
    call_count = {"sql": 0}

    async def handler(request: Request):
        path = request.url.path
        if path.endswith("/sql"):
            call_count["sql"] += 1
            if call_count["sql"] <= 1:
                return JSONResponse(_JOB_SUBMISSION)
            return Response(
                content=json.dumps({"errorMessage": "Unauthorized"}),
                status_code=401,
                media_type="application/json",
            )
        if "/job/test-job-12345" in path and "results" not in path:
            return JSONResponse(_JOB_STATUS)
        if "/job/test-job-12345/results" in path:
            return JSONResponse(_JOB_RESULTS)
        return JSONResponse({"error": f"No mock for {path}"}, status_code=404)

    app = Starlette(
        debug=True,
        routes=[Route("/{path:path}", handler, methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])],
        middleware=[Middleware(LoggingMiddleware, log_file=log_file)],
    )
    port = random.randrange(12000, 15000)
    thread, stop_event = start_server_with_app(
        app, host="127.0.0.1", port=port, log_level=log_level, name="dremio-401-mock",
    )
    return LoggingServerFixture(
        url=f"http://127.0.0.1:{port}", log=log_file,
        stop_event=stop_event, server_thread=thread,
    )


@pytest.mark.asyncio
async def test_dremio_401_is_swallowed_as_200_tool_result(
    mock_config_dir, logging_level
):
    """
    1. First tool call succeeds (Dremio returns 200)
    2. Second tool call: Dremio returns 401 (expired token)
    3. MCP SDK wraps the 401 error inside a 200 HTTP response
    4. MCP client sees 200 → never triggers token refresh

    The test passes under the current (broken) behavior — it documents
    the SDK limitation.
    """
    dremio_mock = _create_401_after_first_server(logging_level)
    try:
        async with http_streamable_mcp_server(dremio_mock, logging_level) as sf:
            async with http_streamable_client_server(
                sf.mcp_server, token="valid-but-will-expire-token"
            ) as session:
                # First query succeeds
                result1: CallToolResult = await session.call_tool(
                    "RunSqlQuery", {"query": "SELECT 1"}
                )
                assert result1.structuredContent is not None
                assert result1.structuredContent["result"]["result"][0]["test_column"] == 1

                # Second query: Dremio returns 401, but MCP client sees 200
                result2: CallToolResult = await session.call_tool(
                    "RunSqlQuery", {"query": "SELECT 2"}
                )
                assert result2 is not None
                assert result2.isError or (
                    result2.content and any(
                        "401" in str(c) or "unauthorized" in str(c).lower()
                        for c in result2.content
                    )
                )

        # Verify Dremio mock saw both requests with the same (unrefreshed) token
        sql_requests = [l for l in dremio_mock.logs() if l.path.endswith("/sql")]
        assert len(sql_requests) == 2
        assert sql_requests[0].response_status == 200
        assert sql_requests[1].response_status == 401
        for req in sql_requests:
            assert req.headers.get("authorization") == "Bearer valid-but-will-expire-token"
    finally:
        dremio_mock.close()
