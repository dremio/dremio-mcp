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
from mcp.server.fastmcp.utilities.func_metadata import func_metadata
from dremioai.config import settings
from dremioai.tools.tools import GetUsefulSystemTableNames, GetSchemaOfTable, RunSqlQuery


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
    df = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp("2024-01-02T03:04:05"),
                "latency_ms": np.int64(150),
                "ratio": np.float64(0.75),
                "amount": Decimal("10.25"),
                "maybe_null": pd.NA,
            }
        ]
    )

    with patch("dremioai.tools.tools.sql.run_query", new_callable=AsyncMock) as mock_run_query:
        mock_run_query.return_value = df
        token = settings._settings.set(
            settings.Settings.model_validate({"dremio": {"uri": "https://test"}})
        )
        try:
            result = await tool.invoke("SELECT 1")
        finally:
            settings._settings.reset(token)

    assert isinstance(result, dict)
    assert "result" in result
    payload = json.dumps(result)
    assert "2024-01-02T03:04:05" in payload
