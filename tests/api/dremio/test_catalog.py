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
from types import SimpleNamespace

import pandas as pd
import pytest
from aiohttp import ClientResponseError

from dremioai.api.dremio import catalog
from dremioai.tools import tools as tools_mod


def _client_response_error(status: int, message: str) -> ClientResponseError:
    request_info = SimpleNamespace(
        real_url="http://test/catalog/by-path/x", method="GET", headers={}, url="http://test"
    )
    return ClientResponseError(
        request_info=request_info, history=(), status=status, message=message
    )


@pytest.mark.asyncio
async def test_get_schemas_all_success():
    async def fake_get_schema(p, *_a, **_kw):
        return {"schema": {"col": "VARCHAR"}, "path": p}

    with patch.object(catalog, "get_schema", side_effect=fake_get_schema):
        result = await catalog.get_schemas([["a", "b"], ["c"]])

    assert len(result) == 2
    assert all(r.error is None for r in result)
    assert result[0].data["path"] == ["a", "b"]
    assert result[1].data["path"] == ["c"]


@pytest.mark.asyncio
async def test_get_schemas_one_failure_does_not_break_others():
    """A single broken catalog entry must not fail the whole batch (DX-118395)."""

    async def fake_get_schema(p, *_a, **_kw):
        if p == ["bad", "view"]:
            raise _client_response_error(400, "Bad Request")
        return {"schema": {"col": "VARCHAR"}, "path": p}

    paths = [["ok", "one"], ["bad", "view"], ["ok", "two"]]
    with patch.object(catalog, "get_schema", side_effect=fake_get_schema):
        result = await catalog.get_schemas(paths)

    assert len(result) == 3
    assert result[0].error is None
    assert result[0].data["path"] == ["ok", "one"]
    # Failed entry: no data + HTTP error surfaced + original exception preserved.
    assert result[1].data is None
    assert "HTTP 400" in result[1].error
    assert "Bad Request" in result[1].error
    assert result[2].error is None
    assert result[2].data["path"] == ["ok", "two"]


@pytest.mark.asyncio
async def test_get_schemas_non_http_exception_is_captured():
    async def fake_get_schema(p, *_a, **_kw):
        if p == ["boom"]:
            raise ValueError("kapow")
        return {"path": p}

    with patch.object(catalog, "get_schema", side_effect=fake_get_schema):
        result = await catalog.get_schemas([["ok"], ["boom"]])

    assert result[0].error is None
    assert result[1].data is None
    assert "ValueError" in result[1].error
    assert "kapow" in result[1].error


@pytest.mark.asyncio
async def test_search_table_and_views_surfaces_skipped_paths():
    """Tool response should include a `skipped` list when schema fetches fail."""

    ok_df = pd.DataFrame([{"path": ["ok", "tbl"], "name": "ok.tbl", "schema": {"a": "INT"}}])

    bad_df = pd.DataFrame(
        [{"path": ["bad", "view"], "name": "bad.view", "schema": None}]
    )
    bad_df.attrs["skipped"] = [
        {"path": ["bad", "view"], "reason": "HTTP 400: Bad Request"}
    ]

    async def fake_search(search_obj, use_df=False):
        if search_obj.filter == 'category in ["TABLE"]':
            return ok_df
        return bad_df

    with patch.object(tools_mod.search, "get_search_results", side_effect=fake_search):
        result = await tools_mod.SearchTableAndViews().invoke("NYC bike trips")

    assert "results" in result
    assert len(result["results"]) == 2
    assert "skipped" in result
    assert result["skipped"] == [
        {"path": ["bad", "view"], "reason": "HTTP 400: Bad Request"}
    ]
    assert "skipped_note" in result
    assert "1 table(s)/view(s)" in result["skipped_note"]


@pytest.mark.asyncio
async def test_search_table_and_views_no_skipped_key_when_all_succeed():
    df = pd.DataFrame([{"path": ["ok"], "name": "ok", "schema": {}}])

    async def fake_search(search_obj, use_df=False):
        return df

    with patch.object(tools_mod.search, "get_search_results", side_effect=fake_search):
        result = await tools_mod.SearchTableAndViews().invoke("q")

    assert "results" in result
    assert "skipped" not in result
    assert "skipped_note" not in result


@pytest.mark.asyncio
async def test_get_schemas_all_failing_returns_empty_dicts_with_errors():
    async def fake_get_schema(*_a, **_kw):
        raise _client_response_error(404, "Not Found")

    with patch.object(catalog, "get_schema", side_effect=fake_get_schema):
        result = await catalog.get_schemas([["a"], ["b"]])

    assert len(result) == 2
    for r in result:
        assert r.data is None
        assert "HTTP 404" in r.error