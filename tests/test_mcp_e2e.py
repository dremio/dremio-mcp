import pytest
from rich import print as pp
from mcp import ClientSession, types
from mcp.client.streamable_http import streamablehttp_client
from dremioai.tools.tools import get_tools
from dremioai.config import settings


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
            lts = await session.list_tools()
            tr = {t.name for t in lts.tools}
            pp(tr)
            pp(settings.instance())
            assert tr == {
                t.__name__ for t in get_tools(For=settings.instance().tools.server_mode)
            }
