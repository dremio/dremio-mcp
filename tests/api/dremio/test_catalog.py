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

import inspect
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

    assert ok.dremio_schema == {"col": "VARCHAR"}
    assert bad.dremio_schema is None


@pytest.mark.asyncio
async def test_search_table_and_views_drops_broken_entries_and_returns_healthy_ones(
    mock_settings_instance,
):
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
    names = {row["name"] for row in result["results"]["tables_and_views"]}
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



class _FakeLineageClient:
    """Stands in for AsyncHttpClient: records the request, validates the payload like the real one."""

    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    async def get(self, url, params=None, deser=None):
        self.calls.append((url, params))
        return deser.model_validate(self.payload)


def _settings_without_project():
    return SimpleNamespace(dremio=SimpleNamespace(project_id=None))


async def _get_lineage(payload, dataset="11111111-1111-1111-1111-111111111111"):
    client = _FakeLineageClient(payload)
    with patch.object(catalog, "AsyncHttpClient", return_value=client), patch.object(
        catalog.settings, "instance", return_value=_settings_without_project()
    ):
        result = await catalog.get_lineage(dataset)
    return result, client


@pytest.mark.asyncio
async def test_get_lineage_uses_lineage_endpoint():
    result, client = await _get_lineage({"entities": []})

    url, params = client.calls[0]
    assert url == "/api/v3/catalog/11111111-1111-1111-1111-111111111111/lineage"
    assert "graph" not in url
    assert params == {"removeCatalogName": "true"}
    assert result["entities"] == []


@pytest.mark.asyncio
async def test_get_lineage_parses_entities_and_edges():
    payload = {
        "entities": [
            {"id": "a", "name": "t", "path": ["Samples", "t"], "type": "TABLE"},
            {"id": "b", "name": "v", "path": ["space", "v"], "type": "VIEW"},
        ],
        "sourceEdges": [],
        "datasetEdges": [
            {"upstream": {"entityId": "a"}, "downstream": {"entityId": "b"}}
        ],
    }
    result, _ = await _get_lineage(payload)

    assert [e["id"] for e in result["entities"]] == ["a", "b"]
    assert result["datasetEdges"][0]["upstream"]["entityId"] == "a"
    assert result["datasetEdges"][0]["downstream"]["entityId"] == "b"
    assert result["sourceEdges"] == []


@pytest.mark.asyncio
async def test_get_lineage_accepts_view_over_space_dataset():
    """DX-110949: a VDS built on a dataset that lives in a Space must not fail to parse.
    /lineage reports the Space as an entity; the old /graph models rejected containerType SPACE."""
    payload = {
        "entities": [
            {"id": "s", "name": "samples", "path": ["samples"], "type": "SOURCE"},
            {"id": "t", "name": "t", "path": ["samples", "t"], "type": "TABLE"},
            {"id": "v", "name": "v", "path": ["my_space", "v"], "type": "VIEW"},
        ],
        "sourceEdges": [
            {"upstream": {"entityId": "s"}, "downstream": {"entityId": "t"}}
        ],
        "datasetEdges": [
            {"upstream": {"entityId": "t"}, "downstream": {"entityId": "v"}}
        ],
    }
    result, _ = await _get_lineage(payload)

    assert len(result["entities"]) == 3


@pytest.mark.asyncio
async def test_get_lineage_tolerates_unknown_entity_type():
    result, _ = await _get_lineage(
        {"entities": [{"id": "x", "path": ["x"], "type": "SOMETHING_NEW"}]}
    )

    assert result["entities"][0]["type"] == "SOMETHING_NEW"


@pytest.mark.asyncio
async def test_get_lineage_view_without_dependencies():
    result, _ = await _get_lineage(
        {
            "entities": [{"id": "v", "name": "v", "path": ["s", "v"], "type": "VIEW"}],
            "sourceEdges": [],
            "datasetEdges": [],
        }
    )

    assert len(result["entities"]) == 1
    assert result["sourceEdges"] == [] and result["datasetEdges"] == []


async def _lineage_tool_result(exc: Exception):
    with patch.object(tools_mod, "get_lineage", side_effect=exc):
        invoke = inspect.unwrap(tools_mod.GetTableOrViewLineage.invoke)
        return await invoke(tools_mod.GetTableOrViewLineage(), "some.table")


@pytest.mark.parametrize(
    "status, expected",
    [
        (404, "not found"),
        (403, "was denied"),
        (401, "was denied"),
        (500, "lineage lookup failed"),
    ],
)
@pytest.mark.asyncio
async def test_lineage_tool_error_message_depends_on_status(status, expected):
    result = await _lineage_tool_result(_client_response_error(status, "boom"))

    assert result["error"] == "Unable to retrieve lineage for the specified table or view."
    assert expected in result["message"]
    assert "http" not in result["message"].lower()


@pytest.mark.asyncio
async def test_lineage_tool_reports_unexpected_response_format():
    from pydantic import TypeAdapter

    try:
        TypeAdapter(int).validate_python("x")
    except Exception as validation_error:
        result = await _lineage_tool_result(validation_error)

    assert "unexpected format" in result["message"]
