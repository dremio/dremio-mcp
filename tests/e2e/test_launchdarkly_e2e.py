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
from conftest import http_streamable_client_server, http_streamable_mcp_server
from mcp.types import CallToolResult

from dremioai.config import settings
from dremioai.config.feature_flags import FeatureFlagManager


def _make_mock_ld_client(flag_values: dict):
    mock_client = MagicMock()
    mock_client.is_initialized.return_value = True
    mock_client.variation.side_effect = lambda key, ctx, default: flag_values.get(key, default)
    return mock_client


@pytest.fixture(autouse=True)
def reset_feature_flag_manager():
    FeatureFlagManager.reset()
    yield
    FeatureFlagManager.reset()


@pytest.mark.asyncio
async def test_allow_dml_without_ld(mock_config_dir, logging_server, logging_level):
    """DML is rejected by default when LD is not configured (allow_dml=False)."""
    async with http_streamable_mcp_server(logging_server, logging_level) as sf:
        async with http_streamable_client_server(sf.mcp_server, token="my-token") as session:
            result: CallToolResult = await session.call_tool(
                "RunSqlQuery", {"query": "DROP TABLE foo"}
            )
            assert "error" in result.structuredContent.get("result", {}), \
                "DML should be rejected when allow_dml is False"


def _enable_ld_mock(mock_ldclient, flag_values: dict):
    """Set up LD mock by directly injecting a configured FeatureFlagManager.

    We bypass lazy init because the MCP server runs in a separate thread
    with its own ContextVar copy — settings changes in the test thread
    aren't visible there. But FeatureFlagManager._instance is a class var
    shared across threads.
    """
    mock_ldclient.get.return_value = _make_mock_ld_client(flag_values)
    FeatureFlagManager.reset()
    mgr = FeatureFlagManager("test-sdk-key")
    FeatureFlagManager._instance = mgr


@pytest.mark.asyncio
@patch("dremioai.config.feature_flags.ldclient")
async def test_allow_dml_enabled_by_ld(mock_ldclient, mock_config_dir, logging_server, logging_level):
    """LD override sets allow_dml=True, so DML queries pass validation."""
    async with http_streamable_mcp_server(logging_server, logging_level) as sf:
        # Enable LD *after* server configures settings (FeatureFlagManager is lazy)
        _enable_ld_mock(mock_ldclient, {"dremio.allow_dml": True})

        async with http_streamable_client_server(sf.mcp_server, token="my-token") as session:
            result: CallToolResult = await session.call_tool(
                "RunSqlQuery", {"query": "DROP TABLE foo"}
            )
            err = result.structuredContent.get("result", {}).get("error", "")
            assert "DML" not in err and "not permitted" not in err, \
                f"DML should pass validation when LD sets allow_dml=True: {err}"


@pytest.mark.asyncio
@patch("dremioai.config.feature_flags.ldclient")
async def test_log_level_overridden_by_ld(mock_ldclient, mock_config_dir, logging_server, logging_level):
    """LD override changes log_level; verify via .get() during server lifecycle."""
    async with http_streamable_mcp_server(logging_server, logging_level) as sf:
        _enable_ld_mock(mock_ldclient, {"log_level": "DEBUG"})

        # Direct access returns config value (INFO), .get() returns LD value (DEBUG)
        assert settings.instance().log_level == "INFO"
        assert settings.instance().get("log_level") == "DEBUG"


@pytest.mark.asyncio
@patch("dremioai.config.feature_flags.ldclient")
async def test_ld_flags_do_not_affect_config_values(mock_ldclient, mock_config_dir, logging_server, logging_level):
    """LD overrides only affect .get(), not direct access or model_dump()."""
    async with http_streamable_mcp_server(logging_server, logging_level) as sf:
        _enable_ld_mock(mock_ldclient, {"dremio.allow_dml": True, "log_level": "TRACE"})

        # .get() returns LD values
        assert settings.instance().dremio.get("allow_dml") is True
        assert settings.instance().get("log_level") == "TRACE"

        # Direct access returns config values
        assert settings.instance().dremio.allow_dml is False
        assert settings.instance().log_level == "INFO"

        # model_dump returns config values
        assert settings.instance().dremio.model_dump()["allow_dml"] is False
