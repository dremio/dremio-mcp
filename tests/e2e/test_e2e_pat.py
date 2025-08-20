import pytest
from mcp.types import CallToolResult
from conftest import http_streamable_client_server


@pytest.mark.asyncio
async def test_tool_pat(http_streamable_mcp_server):
    async with http_streamable_client_server(
        http_streamable_mcp_server.mcp_server,
        token="my-token",
    ) as session:
        result: CallToolResult = await session.call_tool(
            "RunSqlQuery", {"s": "SELECT 1"}
        )
        assert result.structuredContent["result"]["result"][0]["test_column"] == 1
        for le in http_streamable_mcp_server.logging_server.logs():
            assert (
                le.headers.get("authorization") == "Bearer my-token"
            ), f"{le} does not have the right auth header"
