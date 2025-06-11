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
from unittest.mock import patch, MagicMock

from dremioai.config.tools import ToolType
from dremioai.servers import mcp as mcp_server
from dremioai.tools.tools import get_tools
from dremioai.config import settings

from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters
from contextlib import asynccontextmanager, contextmanager
from rich import print as pp
from tempfile import TemporaryDirectory
from pathlib import Path
import json


@contextmanager
def mock_settings(mode: ToolType):
    """Create mock settings for testing MCP server"""
    # Create a mock settings instance
    try:
        old = settings.instance()
        with TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            settings._settings.set(
                settings.Settings.model_validate(
                    {
                        "dremio": {
                            "uri": "https://test-dremio-uri.com",
                            "pat": "test-pat",
                        },
                        "tools": {"server_mode": mode},
                    }
                )
            )
            cfg = temp_dir / "config.yaml"
            settings.write_settings(cfg=cfg, inst=settings.instance())
            yield settings.instance(), cfg
    finally:
        settings._settings.set(old)


@asynccontextmanager
async def mcp_server_session(cfg: Path):
    """Create an MCP server instance with mock settings"""
    params = mcp_server.create_default_mcpserver_config()
    params["args"].extend(["--cfg", str(cfg)])
    params = StdioServerParameters(command=params["command"], args=params["args"])
    async with (
        stdio_client(params) as (read, write),
        ClientSession(read, write) as session,
    ):
        await session.initialize()
        yield session


@pytest.mark.parametrize(
    "mode",
    [
        pytest.param(ToolType.FOR_SELF, id="FOR_SELF"),
        pytest.param(ToolType.FOR_DATA_PATTERNS, id="FOR_DATA_PATTERNS"),
        pytest.param(
            ToolType.FOR_SELF | ToolType.FOR_DATA_PATTERNS,
            id="FOR_SELF|FOR_DATA_PATTERNS",
        ),
    ],
)
@pytest.mark.asyncio
async def test_mcp_server_initialization(mode: ToolType):
    with mock_settings(mode) as (_, cfg):
        async with mcp_server_session(cfg) as session:
            tools = await session.list_tools()
            assert len(tools.tools) > 0
            names = {tool.name for tool in tools.tools}
            exp = {t.__name__ for t in get_tools(For=mode)}
            assert names == exp


@pytest.fixture(
    params=[pytest.param(True, id="exists"), pytest.param(False, id="not_exists")]
)
def claude_config_path(request):
    with TemporaryDirectory() as temp_dir:
        p = Path(temp_dir) / "claude_desktop_config.json"
        if request.param:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("{}")
        with patch("dremioai.servers.mcp.get_claude_config_path") as mock_update:
            mock_update.return_value = p
            yield p


def test_claude_config_creation(claude_config_path):
    dcmp = {"Dremio": mcp_server.create_default_mcpserver_config()}
    mcp_server.create_default_config_helper(False)

    assert claude_config_path.exists()
    d = json.load(claude_config_path.open())
    assert d["mcpServers"] == dcmp
