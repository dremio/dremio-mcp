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

import json
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest
from unittest.mock import AsyncMock, patch
from mcp.types import CallToolResult
from mcp.server.fastmcp.utilities.func_metadata import func_metadata
from dremioai.api.dremio.sql import QueryResult
from dremioai.config import settings
from dremioai.tools.tools import (
    GetUsefulSystemTableNames,
    GetSchemaOfTable,
    RunSqlQuery,
)


async def mock_mcp_validate_tool_output(tool, *args, **kwargs):
    """
    Use FastMCP's actual validation method instead of mimicking it.

    This uses FastMCP's convert_result method which performs the exact same
    validation that FastMCP does internally when processing tool outputs.
    """

    # Get function metadata like FastMCP does
    metadata = func_metadata(tool.invoke, structured_output=True)
    actual_output = await tool.invoke(*args, **kwargs)

    # Use FastMCP's actual convert_result method - this performs validation!
    # If validation fails, this will raise an exception
    metadata.convert_result(actual_output)

    # If we reach here, validation passed
    return True


@pytest.mark.asyncio
async def test_get_useful_system_table_names_validation():
    tool = GetUsefulSystemTableNames()
    await mock_mcp_validate_tool_output(tool)


@pytest.mark.asyncio
async def test_get_schema_of_table_validation():
    tool = GetSchemaOfTable()
    mock_schema_result = {
        "fields": [
            {"name": "job_id", "type": "VARCHAR"},
            {"name": "user_name", "type": "VARCHAR"},
        ],
        "text": "System jobs table",
    }

    with patch("dremioai.tools.tools.get_schema", return_value=mock_schema_result):
        await mock_mcp_validate_tool_output(tool, "sys.jobs")


@pytest.mark.asyncio
async def test_run_sql_query_json_safe_output():
    tool = RunSqlQuery()
    qr = QueryResult(
        rows=[
            {
                "ts": pd.Timestamp("2024-01-02T03:04:05"),
                "latency_ms": np.int64(150),
                "ratio": np.float64(0.75),
                "amount": Decimal("10.25"),
                "maybe_null": pd.NA,
            }
        ],
        total_rows=1,
        returned_rows=1,
        pages_fetched=1,
        result_schema=None,
    )

    with patch(
        "dremioai.tools.tools.sql.run_query", new_callable=AsyncMock
    ) as mock_run_query:
        mock_run_query.return_value = qr
        token = settings.push_settings_override(
            settings.Settings.model_validate({"dremio": {"uri": "https://test"}})
        )
        try:
            result = await tool.invoke("SELECT 1")
        finally:
            settings.pop_settings_override(token)

    assert isinstance(result, CallToolResult)
    assert not result.isError
    assert result.structuredContent is None
    payload = result.content[0].text
    assert "2024-01-02T03:04:05" in payload


@pytest.mark.asyncio
async def test_run_sql_query_byte_limit_returns_partial_rows():
    tool = RunSqlQuery()
    qr = QueryResult(
        rows=[{"value": str(i) * 80} for i in range(10)],
        total_rows=10,
        returned_rows=10,
        pages_fetched=1,
        result_schema=None,
    )

    with patch(
        "dremioai.tools.tools.sql.run_query", new_callable=AsyncMock
    ) as mock_run_query:
        mock_run_query.return_value = qr
        token = settings.push_settings_override(
            settings.Settings.model_validate(
                {
                    "dremio": {
                        "uri": "https://test",
                        "max_result_bytes": 600,
                    }
                }
            )
        )
        try:
            result = await tool.invoke("SELECT 1")
        finally:
            settings.pop_settings_override(token)

    assert isinstance(result, CallToolResult)
    assert not result.isError
    assert result.structuredContent is None
    text = result.content[0].text
    assert len(text.encode("utf-8")) <= 600
    payload = json.loads(text)
    assert payload["truncated"] is True
    assert payload["truncation_reason"] == "byte_limit"
    assert payload["total_rows"] == 10
    returned = payload["returned_rows"]
    assert 0 < returned < 10
    assert payload["result"] == [{"value": str(i) * 80} for i in range(returned)]
    assert (
        f"Partial result: showing the first {returned} of 10 rows" in payload["message"]
    )
    assert "600-byte response size limit" in payload["message"]
    assert mock_run_query.await_args.kwargs["max_bytes"] == 600


@pytest.mark.asyncio
async def test_run_sql_query_many_small_rows_fit_serialized_budget():
    tool = RunSqlQuery()
    rows = [{"a": i, "b": i, "c": i} for i in range(500)]
    qr = QueryResult(
        rows=rows,
        total_rows=500,
        returned_rows=500,
        pages_fetched=1,
        result_schema=None,
    )

    with patch(
        "dremioai.tools.tools.sql.run_query", new_callable=AsyncMock
    ) as mock_run_query:
        mock_run_query.return_value = qr
        token = settings.push_settings_override(
            settings.Settings.model_validate(
                {"dremio": {"uri": "https://test", "max_result_bytes": 2000}}
            )
        )
        try:
            result = await tool.invoke("SELECT 1")
        finally:
            settings.pop_settings_override(token)

    text = result.content[0].text
    assert len(text.encode("utf-8")) <= 2000
    payload = json.loads(text)
    assert payload["truncated"] is True
    assert 0 < payload["returned_rows"] < 500


@pytest.mark.asyncio
async def test_run_sql_query_rows_left_unread_are_reported_as_partial():
    tool = RunSqlQuery()
    qr = QueryResult(
        rows=[{"value": 1}],
        total_rows=1000,
        returned_rows=1,
        pages_fetched=1,
        result_schema=None,
    )

    with patch(
        "dremioai.tools.tools.sql.run_query", new_callable=AsyncMock
    ) as mock_run_query:
        mock_run_query.return_value = qr
        token = settings.push_settings_override(
            settings.Settings.model_validate({"dremio": {"uri": "https://test"}})
        )
        try:
            result = await tool.invoke("SELECT 1")
        finally:
            settings.pop_settings_override(token)

    assert not result.isError
    payload = json.loads(result.content[0].text)
    assert payload["truncated"] is True
    assert payload["returned_rows"] == 1
    assert payload["total_rows"] == 1000


@pytest.mark.asyncio
async def test_run_sql_query_single_oversized_row_explains_no_rows_shown():
    tool = RunSqlQuery()
    qr = QueryResult(
        rows=[{"value": "x" * 500}],
        total_rows=3,
        returned_rows=1,
        pages_fetched=1,
        result_schema=None,
    )

    with patch(
        "dremioai.tools.tools.sql.run_query", new_callable=AsyncMock
    ) as mock_run_query:
        mock_run_query.return_value = qr
        token = settings.push_settings_override(
            settings.Settings.model_validate(
                {"dremio": {"uri": "https://test", "max_result_bytes": 100}}
            )
        )
        try:
            result = await tool.invoke("SELECT 1")
        finally:
            settings.pop_settings_override(token)

    assert not result.isError
    payload = json.loads(result.content[0].text)
    assert payload["result"] == []
    assert "even a single row exceeds" in payload["message"]
