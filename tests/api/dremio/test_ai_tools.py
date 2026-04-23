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
from dremioai.api.dremio.ai_tools import (
    AiTool,
    InvokeToolResponse,
    list_tools,
    invoke_tool,
)
from mocks.http_mock import HttpMockFramework


# --- list_tools tests ---

@pytest.mark.asyncio
async def test_list_tools_returns_tools(mock_settings_instance):
    with HttpMockFramework() as mock:
        mock.load_mock_data(r"/api/v4/ai/tools$", "ai_tools/list_tools.json")
        result = await list_tools()
    assert bool(result)
    assert len(result.tools) == 3
    names = [t.name for t in result.tools]
    assert "runSql" in names
    assert "getTableOrViewSchema" in names
    assert "listEngines" in names


@pytest.mark.asyncio
async def test_list_tools_returns_input_schema(mock_settings_instance):
    with HttpMockFramework() as mock:
        mock.load_mock_data(r"/api/v4/ai/tools$", "ai_tools/list_tools.json")
        result = await list_tools()
    run_sql = next(t for t in result.tools if t.name == "runSql")
    assert run_sql.input_schema["type"] == "object"
    assert "sqlText" in run_sql.input_schema["properties"]


@pytest.mark.asyncio
async def test_list_tools_empty_registry(mock_settings_instance):
    with HttpMockFramework() as mock:
        mock.add_mock_response(r"/api/v4/ai/tools$", {"tools": []})
        result = await list_tools()
    assert result.tools == []
    assert bool(result)


# --- invoke_tool tests ---

@pytest.mark.asyncio
async def test_invoke_tool_success(mock_settings_instance):
    with HttpMockFramework() as mock:
        mock.load_mock_data(r"/api/v4/ai/tools/runSql:invoke$", "ai_tools/invoke_result.json")
        result = await invoke_tool("runSql", {"sqlText": "SELECT 1"})
    assert bool(result)
    assert result.result["columns"] == ["id", "name"]
    assert result.error is None


@pytest.mark.asyncio
async def test_invoke_tool_error_response(mock_settings_instance):
    with HttpMockFramework() as mock:
        mock.load_mock_data(r"/api/v4/ai/tools/unknownTool:invoke$", "ai_tools/invoke_error.json")
        result = await invoke_tool("unknownTool", {})
    assert result.error is not None
    assert "not found" in result.error
    assert result.result is None


# --- Pydantic model unit tests (no HTTP) ---

def test_ai_tool_model_validation():
    raw = {
        "name": "runSql",
        "description": "runSql",
        "inputSchema": {
            "type": "object",
            "properties": {"sqlText": {"type": "string"}},
            "required": ["sqlText"],
        },
    }
    tool = AiTool.model_validate(raw)
    assert tool.name == "runSql"
    assert tool.input_schema["type"] == "object"
    assert tool.input_schema["required"] == ["sqlText"]


def test_ai_tool_model_minimal_schema():
    """Tools with an empty inputSchema (e.g. listEngines) should deserialize cleanly."""
    raw = {"name": "listEngines", "description": "listEngines", "inputSchema": {"type": "object"}}
    tool = AiTool.model_validate(raw)
    assert tool.name == "listEngines"
    assert tool.input_schema == {"type": "object"}


def test_invoke_tool_response_succeeded():
    resp = InvokeToolResponse.model_validate({"result": {"sql": "SELECT 1"}})
    assert bool(resp) is True
    assert resp.result == {"sql": "SELECT 1"}
    assert resp.error is None


def test_invoke_tool_response_failed():
    resp = InvokeToolResponse.model_validate({"error": "Tool not found"})
    assert bool(resp) is False
    assert resp.result is None
    assert resp.error == "Tool not found"


def test_invoke_tool_response_empty():
    resp = InvokeToolResponse.model_validate({})
    assert bool(resp) is True
    assert resp.result is None
    assert resp.error is None
    assert resp.is_empty is True


def test_invoke_tool_response_is_empty_false_when_result():
    """is_empty should be False when a result is present."""
    resp = InvokeToolResponse.model_validate({"result": {"sql": "SELECT 1"}})
    assert resp.is_empty is False


def test_invoke_tool_response_is_empty_false_when_error():
    """is_empty should be False when an error is present."""
    resp = InvokeToolResponse.model_validate({"error": "Tool not found"})
    assert resp.is_empty is False


# --- HTTP error scenario tests ---

@pytest.mark.asyncio
async def test_list_tools_http_error(mock_settings_instance):
    """list_tools should return a response with error set on HTTP 4xx/5xx."""
    with HttpMockFramework() as mock:
        mock.add_mock_response(r"/api/v4/ai/tools$", {"error": "Unauthorized"}, status=401)
        result = await list_tools()
    assert not bool(result)
    assert result.error is not None
    assert "401" in result.error


@pytest.mark.asyncio
async def test_invoke_tool_http_error(mock_settings_instance):
    """invoke_tool should return a response with error set on HTTP 500."""
    with HttpMockFramework() as mock:
        mock.add_mock_response(r"/api/v4/ai/tools/runSql:invoke$", {"error": "Internal Server Error"}, status=500)
        result = await invoke_tool("runSql", {"sqlText": "SELECT 1"})
    assert not bool(result)
    assert result.error is not None
    assert "500" in result.error


@pytest.mark.asyncio
async def test_invoke_tool_url_encodes_name(mock_settings_instance):
    """tool_name with special characters should be URL-encoded."""
    with HttpMockFramework() as mock:
        # The encoded name "my%2Ftool" should appear in the URL
        mock.add_mock_response(r"/api/v4/ai/tools/my%2Ftool:invoke$", {"result": "ok", "error": None})
        result = await invoke_tool("my/tool", {})
    assert result.result == "ok"
