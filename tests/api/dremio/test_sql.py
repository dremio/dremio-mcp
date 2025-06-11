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
from dremioai.api.dremio.sql import Job


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
