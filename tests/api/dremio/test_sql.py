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

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dremioai.api.dremio.sql import (
    Job,
    JobResults,
    JobResultsWrapper,
    JobState,
    QuerySubmission,
    QueryType,
    get_results,
    run_query_capped,
)
from dremioai.config import settings


@pytest.mark.asyncio
@patch("dremioai.config.feature_flags.ldclient")
async def test_get_results_uses_polling_interval_get_for_ld_precedence(mock_ldclient):
    mock_client = MagicMock()
    mock_client.is_initialized.return_value = True
    mock_client.variation.side_effect = lambda key, ctx, default: (
        0.25 if key == "dremio.api.polling_interval" else default
    )
    mock_ldclient.get.return_value = mock_client

    settings.set_base_settings(
        settings.Settings.model_validate(
            {
                "launchdarkly": {"sdk_key": "test-key"},
                "dremio": {
                    "uri": "https://test.dremio.cloud",
                    "pat": "test-pat",
                    "api": {"polling_interval": 3.0},
                },
            }
        )
    )

    running = Job(jobState=JobState.RUNNING, queryType=QueryType.REST)
    complete = Job(
        jobState=JobState.COMPLETED,
        rowCount=1,
        queryType=QueryType.REST,
    )
    results = JobResults(rowCount=1, schema=[], rows=[{"value": 1}])
    client = AsyncMock()
    client.get.side_effect = [running, complete]

    with (
        patch(
            "dremioai.api.dremio.sql._fetch_results",
            new=AsyncMock(return_value=results),
        ),
        patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        response = await get_results(
            project_id=None,
            qs=QuerySubmission(id="job-1"),
            client=client,
        )

    assert isinstance(response, JobResultsWrapper)
    assert mock_sleep.await_args_list[0].args[0] == 0.25


def _make_completed_job(row_count: int) -> Job:
    return Job(jobState=JobState.COMPLETED, rowCount=row_count, queryType=QueryType.REST)


def _make_job_results(rows):
    return JobResults(
        rowCount=len(rows),
        schema=[{"name": name, "type": {"name": "VARCHAR"}} for name in rows[0].keys()]
        if rows
        else [],
        rows=rows,
    )


@pytest.mark.asyncio
async def test_run_query_capped_under_limit():
    rows = [{"a": str(i)} for i in range(3)]
    job = _make_completed_job(3)
    client = AsyncMock()
    client.post.return_value = QuerySubmission(id="job-1")

    with (
        patch("dremioai.api.dremio.sql.AsyncHttpClient", return_value=client),
        patch("dremioai.api.dremio.sql._wait_for_job", new=AsyncMock(return_value=job)),
        patch(
            "dremioai.api.dremio.sql._fetch_results",
            new=AsyncMock(return_value=_make_job_results(rows)),
        ) as mock_fetch,
    ):
        qr = await run_query_capped("SELECT 1", max_rows=10)

    assert qr.rows == rows
    assert qr.total_rows == 3
    assert qr.returned_rows == 3
    assert qr.pages_fetched == 1
    assert mock_fetch.await_count == 1
    assert mock_fetch.await_args.args[-2:] == (0, 3)


@pytest.mark.asyncio
async def test_run_query_capped_row_limit_hit():
    rows = [{"a": str(i)} for i in range(10)]
    job = _make_completed_job(100)
    client = AsyncMock()
    client.post.return_value = QuerySubmission(id="job-1")

    with (
        patch("dremioai.api.dremio.sql.AsyncHttpClient", return_value=client),
        patch("dremioai.api.dremio.sql._wait_for_job", new=AsyncMock(return_value=job)),
        patch(
            "dremioai.api.dremio.sql._fetch_results",
            new=AsyncMock(return_value=_make_job_results(rows)),
        ) as mock_fetch,
    ):
        qr = await run_query_capped("SELECT 1", max_rows=10)

    assert qr.truncated is True
    assert qr.total_rows == 100
    assert qr.returned_rows == 10
    assert qr.pages_fetched == 1
    assert len(qr.rows) == 10
    assert mock_fetch.await_count == 1


@pytest.mark.asyncio
async def test_run_query_capped_fetches_sequential_pages():
    job = _make_completed_job(700)
    page1 = _make_job_results([{"a": str(i)} for i in range(500)])
    page2 = _make_job_results([{"a": str(i)} for i in range(500, 700)])
    client = AsyncMock()
    client.post.return_value = QuerySubmission(id="job-1")

    with (
        patch("dremioai.api.dremio.sql.AsyncHttpClient", return_value=client),
        patch("dremioai.api.dremio.sql._wait_for_job", new=AsyncMock(return_value=job)),
        patch(
            "dremioai.api.dremio.sql._fetch_results",
            new=AsyncMock(side_effect=[page1, page2]),
        ) as mock_fetch,
    ):
        qr = await run_query_capped("SELECT 1", max_rows=700)

    assert qr.truncated is False
    assert qr.returned_rows == 700
    assert qr.pages_fetched == 2
    assert mock_fetch.await_count == 2
    assert mock_fetch.await_args_list[0].args[-2:] == (0, 500)
    assert mock_fetch.await_args_list[1].args[-2:] == (500, 200)
