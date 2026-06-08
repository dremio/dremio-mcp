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
from prometheus_client import Counter, Histogram
from dremioai.metrics.registry import _registry

invocation_counter = Counter(
    "mcp_tool_invocations",
    "Number of times a tool is invoked",
    ["tool", "project_id"],
    registry=_registry,
)
invocation_duration = Histogram(
    "mcp_tool_invocation_duration",
    "Time taken to invoke a tool",
    ["tool", "project_id"],
    registry=_registry,
)
sql_result_total_rows = Histogram(
    "mcp_runsql_total_rows",
    "Total row count reported by Dremio jobs invoked via RunSqlQuery",
    ["project_id"],
    registry=_registry,
)
sql_result_returned_rows = Histogram(
    "mcp_runsql_returned_rows",
    "Number of rows returned to the MCP client by RunSqlQuery after truncation",
    ["project_id"],
    registry=_registry,
)
sql_result_response_bytes = Histogram(
    "mcp_runsql_response_bytes",
    "UTF-8 JSON response payload bytes returned by RunSqlQuery",
    ["project_id"],
    registry=_registry,
)
sql_result_pages_fetched = Histogram(
    "mcp_runsql_pages_fetched",
    "Number of result pages fetched from Dremio for each RunSqlQuery invocation",
    ["project_id"],
    registry=_registry,
)
sql_result_truncations = Counter(
    "mcp_runsql_truncations",
    "Number of RunSqlQuery responses truncated by server-side guards",
    ["project_id", "reason"],
    registry=_registry,
)
