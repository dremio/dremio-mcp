import pytest
from rich import print as pp
from mcp import ClientSession, types
from mcp.client.streamable_http import streamablehttp_client


@pytest.mark.asyncio
async def test_basic(http_streamable_mcp_server):
    mcp_server, logging_server = http_streamable_mcp_server
    # r = await mcp_server.call_tool("RunSqlQuery", {"s": "SELECT 1"})
    async with streamablehttp_client(url=mcp_server.url) as (
        read_stream,
        write_stream,
        gid,
    ):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            for t in await session.list_tools():
                pp(t)
