import pytest
from conftest import http_streamable_client_server

from dremioai.tools.tools import get_tools
from dremioai.config import settings


@pytest.mark.asyncio
async def test_basic(http_streamable_mcp_server):
    async with http_streamable_client_server(
        http_streamable_mcp_server.mcp_server
    ) as session:
        lts = await session.list_tools()
        tr = {t.name for t in lts.tools}
        assert tr == {
            t.__name__ for t in get_tools(For=settings.instance().tools.server_mode)
        }
