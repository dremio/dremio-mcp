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

"""Tests for FastMCPServerWithAuthToken remote tool listing and invocation."""

import pytest
from unittest.mock import AsyncMock, patch

from dremioai.api.dremio.ai_tools import AiTool, InvokeToolResponse, ListToolsResponse
from dremioai.config import settings
from dremioai.config.tools import ToolType
from dremioai.servers.mcp import FastMCPServerWithAuthToken


def _make_server() -> FastMCPServerWithAuthToken:
    return FastMCPServerWithAuthToken("TestServer", log_level="DEBUG", debug=True)


def _make_settings(enable_remote_tools: bool = True):
    return settings.Settings.model_validate(
        {
            "dremio": {
                "uri": "https://dremio.example.com",
                "pat": "test-pat",
                "enable_remote_tools": enable_remote_tools,
            },
            "tools": {"server_mode": ToolType.DYNAMIC_REMOTE_TOOLS.name},
        }
    )


@pytest.fixture(autouse=True)
def _base_settings(reset_settings_state):
    settings.set_base_settings(_make_settings(enable_remote_tools=True))
    yield
    settings.reset_state_for_tests()


@pytest.mark.asyncio
async def test_list_tools_includes_remote_tools():
    """Merged static + remote returned when enable_remote_tools=True."""
    server = _make_server()

    async def my_static(x: str) -> str:
        """A static tool."""
        return x

    server.add_tool(my_static, name="my_static", description="static")

    remote = [AiTool(name="remote_tool_1", description="A remote tool", inputSchema={"type": "object"})]
    list_response = ListToolsResponse(tools=remote)

    with patch.object(server, "_list_remote_tools", new=AsyncMock(return_value=list_response)):
        result = await server.list_tools()

    names = [t.name for t in result]
    assert "my_static" in names
    assert "remote_tool_1" in names


@pytest.mark.asyncio
async def test_list_tools_excludes_remote_when_disabled():
    """enable_remote_tools=False → static tools only; backend not called."""
    settings.set_base_settings(_make_settings(enable_remote_tools=False))
    server = _make_server()

    with patch.object(server, "_list_remote_tools", new=AsyncMock()) as mock_list:
        result = await server.list_tools()
        mock_list.assert_not_called()

    assert all(t.name != "remote_tool_1" for t in result)


@pytest.mark.asyncio
async def test_call_remote_tool_directly():
    """call_tool() routes to _invoke_remote_tool for remote tools; returns dict result."""
    server = _make_server()

    from dremioai.api.dremio.ai_tools import InvokeToolResponseResult
    invoke_response = InvokeToolResponse(result=InvokeToolResponseResult.model_validate({"message": "hello from remote"}))

    with patch.object(server, "_invoke_remote_tool", new=AsyncMock(return_value=invoke_response)):
        result = await server.call_tool("my_remote", {})

    assert isinstance(result, dict)
    assert result.get("result", {}).get("message") == "hello from remote"


@pytest.mark.asyncio
async def test_call_static_tool_unchanged():
    """Static tools route through super().call_tool(); _invoke_remote_tool is not called."""
    server = _make_server()

    async def my_static_tool(x: str) -> str:
        """A static tool for testing."""
        return f"static:{x}"

    server.add_tool(my_static_tool, name="my_static_tool", description="test static tool")

    with patch.object(server, "_invoke_remote_tool", new=AsyncMock()) as mock_invoke:
        result = await server.call_tool("my_static_tool", {"x": "hello"})
        mock_invoke.assert_not_called()

    assert result is not None


@pytest.mark.asyncio
async def test_different_project_ids_different_tools():
    """Each list_tools() call hits backend independently — no shared cache."""
    server = _make_server()

    call_count = 0

    async def fake_list_remote(self_=None):
        nonlocal call_count
        call_count += 1
        return ListToolsResponse(tools=[])

    with patch.object(server, "_list_remote_tools", side_effect=fake_list_remote):
        await server.list_tools()
        await server.list_tools()

    assert call_count == 2


@pytest.mark.asyncio
async def test_static_tool_wins_name_collision():
    """If a remote tool has the same name as a static tool, static wins and warning is logged."""
    server = _make_server()

    async def colliding_tool() -> str:
        """A static tool that collides with a remote tool name."""
        return "static"

    server.add_tool(colliding_tool, name="colliding_tool", description="static")

    remote = [AiTool(name="colliding_tool", description="remote", inputSchema={"type": "object"})]
    list_response = ListToolsResponse(tools=remote)

    with patch.object(server, "_list_remote_tools", new=AsyncMock(return_value=list_response)):
        with patch.object(server._logger, "warning") as mock_warn:
            result = await server.list_tools()
            assert mock_warn.called

    collision_tools = [t for t in result if t.name == "colliding_tool"]
    assert len(collision_tools) == 1


@pytest.mark.asyncio
async def test_call_unknown_tool_returns_error():
    """Unknown remote tool → invoke_tool returns error dict."""
    server = _make_server()

    invoke_response = InvokeToolResponse(error="tool 'nonexistent_tool' not found")

    with patch.object(server, "_invoke_remote_tool", new=AsyncMock(return_value=invoke_response)):
        result = await server.call_tool("nonexistent_tool", {})

    assert isinstance(result, dict)
    assert "error" in result


@pytest.mark.asyncio
async def test_remote_tool_api_error_handled():
    """If _list_remote_tools() response has .error, degrade to static tools only."""
    server = _make_server()

    error_response = ListToolsResponse(error="service unavailable")

    with patch.object(server, "_list_remote_tools", new=AsyncMock(return_value=error_response)):
        result = await server.list_tools()

    remote_names = [t.name for t in result if t.name == "remote_tool_1"]
    assert len(remote_names) == 0


@pytest.mark.asyncio
async def test_remote_tools_disabled_call_returns_error_dict():
    """call_tool for non-static tool when enable_remote_tools=False returns an error dict."""
    settings.set_base_settings(_make_settings(enable_remote_tools=False))
    server = _make_server()

    result = await server.call_tool("any_remote_tool", {})

    assert isinstance(result, dict)
    assert "error" in result
