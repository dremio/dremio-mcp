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

from pydantic import BaseModel, Field, ConfigDict
from typing import Dict, List, Optional, Any, TypeVar
from urllib.parse import quote

from aiohttp import ClientResponseError
from dremioai.api.transport import DremioAsyncHttpClient as AsyncHttpClient
from dremioai.config import settings, feature_flags
from dremioai.log import logger

log = logger(__name__)


class AiTool(BaseModel):

    name: str
    description: Optional[str] = None
    input_schema: Dict[str, Any] = Field(
        default_factory=lambda: {"type": "object"}, alias="inputSchema"
    )
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class ListToolsResponse(BaseModel):
    tools: List[AiTool] = Field(default_factory=list)
    error: Optional[str] = None

    def __bool__(self):
        return self.error is None


def is_semantic_layer_enabled() -> bool:
    return feature_flags.FeatureFlagManager.instance().get_flag(
        "ai_semantic_layer.enabled", False
    ) or settings.instance().dremio.get("enable_semantic_layer")


class InvokeToolResponseResult(BaseModel):
    """Wraps the ``result`` object returned by an AI-tool invocation.

    Extra fields from the server (e.g. ``columns``, ``rows``) are stored in
    ``model_extra`` and exposed via dict-style access so callers can write
    ``response.result["columns"]`` without changing their code.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    # Inner-result level error (Java uses "errorMessage" here)
    error: Optional[str] = Field(default=None, alias="errorMessage")

    def __bool__(self) -> bool:
        return self.error is None

    def __getitem__(self, key: str) -> Any:
        """Allow dict-style read access to extra fields: ``result["columns"]``."""
        if key in self.__class__.model_fields:
            return getattr(self, key)
        return self.model_extra[key]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, dict):
            return self.model_extra == other
        return super().__eq__(other)


class InvokeToolResponse(BaseModel):

    result: Optional[InvokeToolResponseResult] = None
    # Top-level error key from Dremio is "error" (not "errorMessage")
    error: Optional[str] = None

    def __bool__(self):
        return self.error is None

    @property
    def is_empty(self) -> bool:
        """True when the response carries neither a result nor an error.

        This can happen when Dremio returns a 200 with an empty body for a
        void tool.  Callers may choose to treat this as a successful no-op.
        """
        return self.result is None and self.error is None


# ---------------------------------------------------------------------------
# Semantic-layer response models
# ---------------------------------------------------------------------------


class RelationshipUsage(BaseModel):
    """Historical usage count for a table relationship in a given month."""

    year: str
    month: str
    count: int


class TableRelationship(BaseModel):
    """A join relationship between two tables, as returned by ``getTableRelationShips``."""

    source_table_id: str = Field(alias="sourceTableId")
    source_table_name: str = Field(alias="sourceTableName")
    source_table_alias: Optional[str] = Field(None, alias="sourceTableAlias")
    source_column_name: str = Field(alias="sourceColumnName")
    target_table_id: str = Field(alias="targetTableId")
    target_table_name: str = Field(alias="targetTableName")
    target_table_alias: Optional[str] = Field(None, alias="targetTableAlias")
    target_column_name: str = Field(alias="targetColumnName")
    join_cardinality: Optional[str] = Field(None, alias="joinCardinality")
    description: Optional[str] = None
    usage_metrics: List[RelationshipUsage] = Field(
        default_factory=list, alias="usageMetrics"
    )

    model_config = ConfigDict(populate_by_name=True)


class TableRelationshipsResponse(BaseModel):
    relationships: Optional[List[TableRelationship]] = Field(default_factory=list)
    error: Optional[str] = Field(None, alias="errorMessage")

    def __bool__(self):
        return self.error is None

    @property
    def is_empty(self) -> bool:
        return self.error is None and (
            self.relationships is None or len(self.relationships) == 0
        )


class MetricUsage(BaseModel):
    """Historical usage count for a metric in a given month."""

    year: str
    month: str
    count: int


class MetricColumn(BaseModel):
    """A column referenced by a metric's SQL formula."""

    table_name: Optional[str] = Field(None, alias="tableName")
    column_name: str = Field(alias="columnName")

    model_config = ConfigDict(populate_by_name=True)


class Metric(BaseModel):
    """A semantic-layer metric returned by ``searchMetrics``."""

    name: str
    description: Optional[str] = None
    sql_formula: str = Field(alias="sqlFormula")
    columns: List[MetricColumn] = Field(default_factory=list)
    synonyms: List[str] = Field(default_factory=list)
    usage_metrics: List[MetricUsage] = Field(default_factory=list, alias="usageMetrics")

    model_config = ConfigDict(populate_by_name=True)


class MetricsSearchResult(BaseModel):
    """Top-level payload inside ``InvokeToolResponse.result`` for ``searchMetrics``."""

    error: Optional[str] = Field(None, alias="errorMessage")
    metrics: List[Metric] = Field(default_factory=list)

    def __bool__(self):
        return self.error is None

    @property
    def is_empty(self) -> bool:
        return self.error is None and (self.metrics is None or len(self.metrics) == 0)

    model_config = ConfigDict(populate_by_name=True)


async def list_tools() -> ListToolsResponse:
    try:
        client = AsyncHttpClient()
        project_id = settings.instance().dremio.project_id
        endpoint = f"/v1/projects/{project_id}" if project_id else "/api/v4"
        return await client.get(f"{endpoint}/ai/tools", deser=ListToolsResponse)
    except ClientResponseError as e:
        log.exception("Failed to list AI tools")
        return ListToolsResponse(error=f"HTTP {e.status} {e.message}")
    except Exception:
        log.exception("Failed to list AI tools")
        return ListToolsResponse(error="Unexpected error listing AI tools")


async def invoke_tool(tool_name: str, args: Dict[str, Any]) -> InvokeToolResponse:
    safe_name = quote(tool_name, safe="")
    try:
        client = AsyncHttpClient()
        project_id = settings.instance().dremio.project_id
        endpoint = f"/v1/projects/{project_id}" if project_id else "/api/v4"
        return await client.post(
            f"{endpoint}/ai/tools/{safe_name}:invoke",
            body={"args": args},
            deser=InvokeToolResponse,
        )
    except ClientResponseError as e:
        log.exception("Failed to invoke AI tool '%s'", tool_name)
        return InvokeToolResponse(error=f"HTTP {e.status} {e.message}")
    except Exception:
        log.exception("Failed to invoke AI tool '%s'", tool_name)
        return InvokeToolResponse(error=f"Unexpected error invoking tool '{tool_name}'")


DeserT = TypeVar("DeserT", bound=BaseModel)


async def invoke_tool_with_deser(
    tool_name: str, args: Dict[str, Any], deser: DeserT
) -> Optional[List[DeserT]]:
    if resp := await invoke_tool(tool_name, args):
        if resp.is_empty:
            return []
        try:
            return deser.model_validate(resp.result.model_extra)
        except:
            log.exception(f"Failed to process {tool_name} ({args})")
    else:
        log.error(f"Failed {tool_name} for {args}: {resp}")
    return None


async def get_relationships(path: List[str]) -> Optional[TableRelationshipsResponse]:
    return await invoke_tool_with_deser(
        "getTableRelationships", {"path": path}, TableRelationshipsResponse
    )


async def get_metrics(query: str) -> Optional[List[Metric]]:
    return await invoke_tool_with_deser(
        "searchMetrics", {"query": query}, MetricsSearchResult
    )


_SEMANTIC_TOOL_NAMES = frozenset({"searchMetrics", "getTableRelationships"})


async def get_semantic_layer_tool_descriptions() -> dict[str, str]:
    """Return the descriptions for ``searchMetrics`` and ``getTableRelationships``
    as advertised by Dremio's AI tool registry.

    Used by :class:`SearchTableAndViews` to enrich its own tool description with
    live, server-supplied text when ``enable_semantic_layer`` is configured.
    Returns an empty dict when the registry is unavailable or returns an error.
    """
    response = await list_tools()
    if response.error or not response.tools:
        return {}
    return {
        t.name: (t.description or "")
        for t in response.tools
        if t.name in _SEMANTIC_TOOL_NAMES
    }
