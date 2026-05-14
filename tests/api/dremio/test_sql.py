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

    settings._set_base_settings(
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
