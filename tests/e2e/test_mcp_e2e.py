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

from dremioai.tools.tools import get_tools
from dremioai.config import settings


@pytest.mark.asyncio
async def test_basic(mock_config_dir, logging_server, logging_level):
    async with http_streamable_mcp_server(logging_server, logging_level) as sf:
        async with http_streamable_client_server(sf.mcp_server) as session:
            lts = await session.list_tools()
            tr = {t.name for t in lts.tools}
            assert tr == {
                t.__name__ for t in get_tools(For=settings.instance().tools.server_mode)
            }
