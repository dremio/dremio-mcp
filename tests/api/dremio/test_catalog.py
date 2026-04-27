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

from dremioai.api.dremio import catalog, search
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
    assert result[0]["path"] == ["a", "b"]
    assert result[1]["path"] == ["c"]


@pytest.mark.asyncio
async def test_get_schemas_propagates_http_exception():
    """get_schemas does not swallow errors — exceptions bubble up to the caller (DX-118395)."""

    async def fake_get_schema(*_a, **_kw):
        raise _client_response_error(400, "Bad Request")

    with patch.object(catalog, "get_schema", side_effect=fake_get_schema):
        with pytest.raises(ClientResponseError):
            await catalog.get_schemas([["ok", "one"], ["bad", "view"]])


@pytest.mark.asyncio
async def test_get_schemas_propagates_non_http_exception():
    async def fake_get_schema(*_a, **_kw):
        raise ValueError("kapow")

    with patch.object(catalog, "get_schema", side_effect=fake_get_schema):
        with pytest.raises(ValueError, match="kapow"):
            await catalog.get_schemas([["boom"]])


@pytest.mark.asyncio
async def test_populate_schemas_marks_not_found_on_failure():
    """One broken catalog entry must not fail the whole search (DX-118395).
    Error is embedded per-row via schema_not_found on EnterpriseSearchCatalogObject."""

    async def fake_get_schema(dataset_path_or_id, *_a, **_kw):
        if dataset_path_or_id == ["bad", "view"]:
            raise _client_response_error(400, "Bad Request")
        return {"schema": {"col": "VARCHAR"}}

    ok = search.EnterpriseSearchCatalogObject(path=["ok", "one"], labels=[])
    bad = search.EnterpriseSearchCatalogObject(path=["bad", "view"], labels=[])

    with patch.object(search, "get_schema", side_effect=fake_get_schema):
        await ok.populate_schemas()
        await bad.populate_schemas()

    assert ok.schema == {"col": "VARCHAR"}
    assert bad.schema is None


@pytest.mark.asyncio
async def test_search_table_and_views_drops_broken_entries_and_returns_healthy_ones():
    """DX-118395: one broken catalog entry must not fail the whole tool call.

    The tool silently drops entries whose schema could not be fetched and
    returns the healthy ones.
    """

    ok_df = pd.DataFrame(
        [{"path": ["ok", "tbl"], "name": "ok.tbl", "schema": {"a": "INT"}}]
    )
    bad_df = pd.DataFrame(
        [{"path": ["bad", "view"], "name": "bad.view", "schema": None}]
    )

    async def fake_search(search_obj, use_df=False):
        if search_obj.filter == 'category in ["TABLE"]':
            return ok_df
        return bad_df

    with patch.object(tools_mod.search, "get_search_results", side_effect=fake_search):
        result = await tools_mod.SearchTableAndViews().invoke("NYC bike trips")

    assert set(result.keys()) == {"results"}
    names = {row["name"] for row in result["results"]}
    assert "ok.tbl" in names


@pytest.mark.asyncio
async def test_get_descriptions_raises_on_schema_fetch_error():
    """get_descriptions must remain fail-fast so GetDescriptionOfTableOrSchema
    surfaces an error instead of silently returning partial data."""

    async def fake_get_schema(p, *_a, **_kw):
        raise _client_response_error(400, "Bad Request")

    with patch.object(catalog, "get_schema", side_effect=fake_get_schema):
        with pytest.raises(ClientResponseError):
            await catalog.get_descriptions([["a", "b"]])

