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

from unittest.mock import patch

import pytest
from pydantic import ValidationError

from dremioai.api.dremio import search


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        pytest.param(
            {"query": "sales"},
            {"maxResults": 50, "filter": "", "queries": ["sales"], "query": "sales"},
            id="scalar_query",
        ),
        pytest.param(
            {"queries": ["sales", "orders"]},
            {
                "maxResults": 50,
                "filter": "",
                "queries": ["sales", "orders"],
                "query": "sales",
            },
            id="queries_list",
        ),
        pytest.param(
            {},
            {"maxResults": 50, "filter": "", "queries": [""], "query": ""},
            id="match_all",
        ),
        pytest.param(
            {
                "query": "sales",
                "filter": [search.Category.TABLE],
                "maxResults": 5,
            },
            {
                "maxResults": 5,
                "filter": 'category in ["TABLE"]',
                "queries": ["sales"],
                "query": "sales",
            },
            id="with_filter",
        ),
    ],
)
def test_search_body_sends_queries_and_query(kwargs, expected):
    assert search.Search(**kwargs).model_dump(exclude_none=True) == expected


@pytest.mark.parametrize("queries", [[], ["sales", None]])
def test_search_rejects_invalid_queries(queries):
    with pytest.raises(ValidationError):
        search.Search(queries=queries)


class _RecordingPost:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def __call__(self, endpoint, body=None, deser=None, params=None, **_kw):
        self.calls.append({"endpoint": endpoint, "body": body, "params": params})
        return search.EnterpriseSearchResults.model_validate(self.responses.pop(0))


def _catalog_result(name):
    return {
        "category": "TABLE",
        "catalogObject": {"path": ["space", name], "type": "TABLE", "labels": []},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("cloud", [True, False], ids=["cloud", "software"])
async def test_get_search_results_posts_queries(mock_settings_instance, cloud):
    if not cloud:
        mock_settings_instance.dremio.raw_project_id = None
    project_id = mock_settings_instance.dremio.project_id
    endpoint = f"/v0/projects/{project_id}/search" if cloud else "/api/v3/search"
    post = _RecordingPost(
        [
            {"results": [_catalog_result("a")], "nextPageToken": "page-2"},
            {"results": [_catalog_result("b")]},
        ]
    )

    async def no_schema(*_a, **_kw):
        return None

    with (
        patch.object(search.AsyncHttpClient, "post", new=post),
        patch.object(search, "get_schema", side_effect=no_schema),
    ):
        await search.get_search_results("sales")

    params = {"removeCatalogName": "true"}
    assert post.calls == [
        {
            "endpoint": endpoint,
            "body": {
                "maxResults": 50,
                "filter": "",
                "queries": ["sales"],
                "query": "sales",
            },
            "params": params,
        },
        {
            "endpoint": endpoint,
            "body": {
                "maxResults": 50,
                "pageToken": "page-2",
                "filter": "",
                "queries": ["sales"],
                "query": "sales",
            },
            "params": params,
        },
    ]
