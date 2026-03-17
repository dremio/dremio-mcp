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

import base64
import json
import uuid

import pytest
from unittest.mock import patch, MagicMock
from conftest import http_streamable_client_server, http_streamable_mcp_server
from mcp.types import CallToolResult

from dremioai.config import settings
from dremioai.config.feature_flags import FeatureFlagManager, LDContextKind


_ORG_ID_OVERRIDES = {"extract_org_id_from_jwt": True}


def _find_context_after_org_set(captured: list) -> object:
    """Return the first captured LD context that was evaluated AFTER set_org_id.

    The extract_org_id_from_jwt flag check itself fires *before* org_id is
    set on FeatureFlagManager, so we skip it and look for a subsequent call
    (e.g. dremio.allow_dml) where org_id should already be in the context.
    """
    for key, ctx in captured:
        if key != "dremio.extract_org_id_from_jwt":
            return ctx
    return None


def _make_mock_ld_client(flag_values: dict):
    mock_client = MagicMock()
    mock_client.is_initialized.return_value = True
    mock_client.variation.side_effect = lambda key, ctx, default: flag_values.get(key, default)
    return mock_client


def _build_jwt(aud: str) -> str:
    """Build a minimal JWT with the given ``aud`` claim.

    No real signature — ``_extract_jwt_aud`` only base64-decodes the payload.
    """
    header = base64.urlsafe_b64encode(json.dumps({"alg": "none"}).encode()).rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(json.dumps({"aud": aud}).encode()).rstrip(b"=").decode()
    return f"{header}.{payload}.nosig"


def _inject_capturing_ld_client() -> list:
    """Reset FeatureFlagManager and inject a mock client that captures contexts.

    Returns a list that ``variation()`` appends ``(flag_key, context)`` tuples to.
    """
    FeatureFlagManager.reset()
    captured: list[tuple[str, object]] = []

    mock_client = MagicMock()
    mock_client.is_initialized.return_value = True

    def _capturing_variation(key, ctx, default):
        captured.append((key, ctx))
        return default

    mock_client.variation.side_effect = _capturing_variation

    # Manually construct the singleton and inject the mock client
    instance = FeatureFlagManager.__new__(FeatureFlagManager)
    instance._client = mock_client
    FeatureFlagManager._instance = instance

    return captured


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
    """Set up LD mock by initializing FeatureFlagManager directly.

    We call initialize() rather than relying on Settings.model_post_init
    because the MCP server runs in a separate thread with its own ContextVar
    copy. FeatureFlagManager._instance is a class var shared across threads.
    """
    mock_ldclient.get.return_value = _make_mock_ld_client(flag_values)
    FeatureFlagManager.initialize("test-sdk-key")


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


@pytest.mark.asyncio
async def test_org_id_and_project_id_in_ld_context(mock_config_dir, logging_server, logging_level):
    """When a JWT with ``aud`` is used and a project_id endpoint is active,
    the LD context should contain all three kinds: application, projectId, orgId."""
    project_id = str(uuid.uuid4())
    async with http_streamable_mcp_server(
        logging_server, logging_level, project_id=project_id,
        dremio_overrides=_ORG_ID_OVERRIDES,
    ) as sf:
        captured = _inject_capturing_ld_client()
        token = _build_jwt(aud="test-org-e2e")
        async with http_streamable_client_server(sf.mcp_server, token=token) as session:
            # call_tool triggers get_flag("dremio.allow_dml", ...) via RunSqlQuery
            await session.call_tool("RunSqlQuery", {"query": "SELECT 1"})

        # Find a context captured after org_id was set (skip the
        # extract_org_id_from_jwt check which fires before set_org_id)
        ctx = _find_context_after_org_set(captured)
        assert ctx is not None, "Expected at least one LD variation() call after org_id set"

        # Multi-context: get_individual_context(kind) returns the sub-context
        app_ctx = ctx.get_individual_context(LDContextKind.APPLICATION)
        proj_ctx = ctx.get_individual_context(LDContextKind.PROJECT)
        org_ctx = ctx.get_individual_context(LDContextKind.ORGANIZATION)

        assert app_ctx is not None, "Missing 'application' kind in LD context"
        assert app_ctx.key == "mcp-server"
        assert proj_ctx is not None, f"Missing '{LDContextKind.PROJECT}' kind in LD context"
        assert proj_ctx.key == project_id
        assert org_ctx is not None, f"Missing '{LDContextKind.ORGANIZATION}' kind in LD context"
        assert org_ctx.key == "test-org-e2e"


@pytest.mark.asyncio
async def test_org_id_without_project_id(mock_config_dir, logging_server, logging_level):
    """When no project_id endpoint is used but a JWT has ``aud``,
    the LD context should have application + orgId but NOT projectId."""
    async with http_streamable_mcp_server(
        logging_server, logging_level, dremio_overrides=_ORG_ID_OVERRIDES,
    ) as sf:
        captured = _inject_capturing_ld_client()
        token = _build_jwt(aud="test-org-only")
        async with http_streamable_client_server(sf.mcp_server, token=token) as session:
            await session.call_tool("RunSqlQuery", {"query": "SELECT 1"})

        ctx = _find_context_after_org_set(captured)
        assert ctx is not None, "Expected at least one LD variation() call after org_id set"

        org_ctx = ctx.get_individual_context(LDContextKind.ORGANIZATION)
        assert org_ctx is not None, f"Missing '{LDContextKind.ORGANIZATION}' kind in LD context"
        assert org_ctx.key == "test-org-only"

        # application should still be present
        app_ctx = ctx.get_individual_context(LDContextKind.APPLICATION)
        assert app_ctx is not None, "Missing 'application' kind in LD context"

        # projectId should NOT be present (no project_id in URL path)
        proj_ctx = ctx.get_individual_context(LDContextKind.PROJECT)
        assert proj_ctx is None, f"projectId should be absent but found: {proj_ctx}"


@pytest.mark.asyncio
async def test_opaque_token_no_org_id(mock_config_dir, logging_server, logging_level):
    """When extract_org_id_from_jwt is enabled but an opaque (non-JWT) token
    is used, _extract_jwt_aud returns None and the LD context should be a
    simple single-kind context."""
    async with http_streamable_mcp_server(
        logging_server, logging_level, dremio_overrides=_ORG_ID_OVERRIDES,
    ) as sf:
        captured = _inject_capturing_ld_client()
        async with http_streamable_client_server(sf.mcp_server, token="opaque-abc") as session:
            await session.call_tool("RunSqlQuery", {"query": "SELECT 1"})

        ctx = _find_context_after_org_set(captured)
        assert ctx is not None, "Expected at least one LD variation() call"

        # Single-kind context: the context itself is just "mcp-server"
        assert ctx.key == "mcp-server", f"Expected single 'mcp-server' context, got key={ctx.key}"
        # Should NOT have projectId or orgId sub-contexts
        assert ctx.get_individual_context(LDContextKind.PROJECT) is None
        assert ctx.get_individual_context(LDContextKind.ORGANIZATION) is None
