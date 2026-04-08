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
from unittest.mock import AsyncMock, patch, MagicMock
import pandas as pd
from dremioai.api.dremio.sql import (
    Job,
    JobResults,
    QueryResult,
    QuerySubmission,
    run_query_capped,
)


@pytest.mark.parametrize(
    "js",
    [
        pytest.param(
            """
    {
        "jobState": "COMPLETED",
        "rowCount": 1,
        "errorMessage": "",
        "startedAt": "2025-06-11T15:35:11.636Z",
        "endedAt": "2025-06-11T15:35:15.949Z",
        "queryType": "REST",
        "queueName": "SMALL",
        "queueId": "SMALL",
        "resourceSchedulingStartedAt": "2025-06-11T15:35:12.435Z",
        "resourceSchedulingEndedAt": "2025-06-11T15:35:12.503Z",
        "cancellationReason": ""
    }""",
            id="with rows",
        ),
        pytest.param(
            """{
        "jobState": "METADATA_RETRIEVAL",
        "errorMessage": "",
        "startedAt": "2025-06-11T15:35:11.565Z",
        "queryType": "REST",
        "cancellationReason": ""
    }""",
            id="without rows",
        ),
    ],
)
def test_basic_job(js: str):
    j = Job.model_validate_json(js)


# -- helpers for run_query_capped tests ----------------------------------------

def _make_completed_job(row_count: int) -> Job:
    return Job.model_validate(
        {"jobState": "COMPLETED", "rowCount": row_count, "queryType": "REST"}
    )


def _make_job_results(rows, schema_names=None):
    if schema_names is None:
        schema_names = list(rows[0].keys()) if rows else []
    return JobResults.model_validate(
        {
            "rowCount": len(rows),
            "schema": [{"name": n, "type": {"name": "VARCHAR"}} for n in schema_names],
            "rows": rows,
        }
    )


def _mock_settings(project_id=None, engine_name=None, polling_interval=0):
    s = MagicMock()
    s.dremio.project_id = project_id
    s.dremio.wlm = None
    s.dremio.api.polling_interval = polling_interval
    return s


@pytest.mark.asyncio
async def test_run_query_capped_under_limit():
    """Rows below max_rows => truncated=False, all rows returned."""
    rows = [{"a": str(i)} for i in range(3)]
    job = _make_completed_job(3)
    jr = _make_job_results(rows)

    with (
        patch("dremioai.api.dremio.sql.AsyncHttpClient") as MockClient,
        patch("dremioai.api.dremio.sql.settings") as mock_settings_mod,
    ):
        mock_settings_mod.instance.return_value = _mock_settings()
        client = MockClient.return_value
        client.post = AsyncMock(return_value=QuerySubmission(id="j1"))
        client.get = AsyncMock(return_value=job)
        with patch("dremioai.api.dremio.sql._fetch_results", AsyncMock(return_value=jr)):
            qr = await run_query_capped("SELECT 1", max_rows=10)

    assert qr.truncated is False
    assert qr.returned_rows == 3
    assert qr.total_rows == 3
    assert len(qr.df) == 3


@pytest.mark.asyncio
async def test_run_query_capped_row_limit_hit():
    """Row limit fires => truncated=True."""
    job = _make_completed_job(100)
    rows = [{"a": str(i)} for i in range(10)]
    jr = _make_job_results(rows)

    with (
        patch("dremioai.api.dremio.sql.AsyncHttpClient") as MockClient,
        patch("dremioai.api.dremio.sql.settings") as mock_settings_mod,
    ):
        mock_settings_mod.instance.return_value = _mock_settings()
        client = MockClient.return_value
        client.post = AsyncMock(return_value=QuerySubmission(id="j1"))
        client.get = AsyncMock(return_value=job)
        with patch("dremioai.api.dremio.sql._fetch_results", AsyncMock(return_value=jr)):
            qr = await run_query_capped("SELECT 1", max_rows=10)

    assert qr.truncated is True
    assert qr.returned_rows == 10
    assert qr.total_rows == 100


@pytest.mark.asyncio
async def test_run_query_capped_unlimited():
    """max_rows=0 fetches all rows."""
    job = _make_completed_job(5)
    rows = [{"a": str(i)} for i in range(5)]
    jr = _make_job_results(rows)

    with (
        patch("dremioai.api.dremio.sql.AsyncHttpClient") as MockClient,
        patch("dremioai.api.dremio.sql.settings") as mock_settings_mod,
    ):
        mock_settings_mod.instance.return_value = _mock_settings()
        client = MockClient.return_value
        client.post = AsyncMock(return_value=QuerySubmission(id="j1"))
        client.get = AsyncMock(return_value=job)
        with patch("dremioai.api.dremio.sql._fetch_results", AsyncMock(return_value=jr)):
            qr = await run_query_capped("SELECT 1", max_rows=0)

    assert qr.truncated is False
    assert qr.returned_rows == 5
    assert qr.total_rows == 5


@pytest.mark.asyncio
async def test_run_query_capped_empty_result():
    """row_count=0 => empty DataFrame, truncated=False."""
    job = _make_completed_job(0)

    with (
        patch("dremioai.api.dremio.sql.AsyncHttpClient") as MockClient,
        patch("dremioai.api.dremio.sql.settings") as mock_settings_mod,
    ):
        mock_settings_mod.instance.return_value = _mock_settings()
        client = MockClient.return_value
        client.post = AsyncMock(return_value=QuerySubmission(id="j1"))
        client.get = AsyncMock(return_value=job)

        qr = await run_query_capped("SELECT 1", max_rows=10)

    assert qr.truncated is False
    assert qr.returned_rows == 0
    assert qr.total_rows == 0
    assert qr.df.empty


@pytest.mark.asyncio
async def test_run_query_capped_exact_boundary():
    """row_count == max_rows => truncated=False."""
    job = _make_completed_job(5)
    rows = [{"a": str(i)} for i in range(5)]
    jr = _make_job_results(rows)

    with (
        patch("dremioai.api.dremio.sql.AsyncHttpClient") as MockClient,
        patch("dremioai.api.dremio.sql.settings") as mock_settings_mod,
    ):
        mock_settings_mod.instance.return_value = _mock_settings()
        client = MockClient.return_value
        client.post = AsyncMock(return_value=QuerySubmission(id="j1"))
        client.get = AsyncMock(return_value=job)
        with patch("dremioai.api.dremio.sql._fetch_results", AsyncMock(return_value=jr)):
            qr = await run_query_capped("SELECT 1", max_rows=5)

    assert qr.truncated is False
    assert qr.returned_rows == 5
    assert qr.total_rows == 5
