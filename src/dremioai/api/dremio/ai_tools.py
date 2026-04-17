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
from typing import Dict, List, Optional, Any
from urllib.parse import quote

from aiohttp import ClientResponseError
from dremioai.api.transport import DremioAsyncHttpClient as AsyncHttpClient
from dremioai.log import logger

log = logger(__name__)


class AiToolError(Exception):
    """Domain-specific error for AI tool API failures."""

    def __init__(self, message: str, status: Optional[int] = None):
        self.status = status
        super().__init__(message)


class AiTool(BaseModel):

    name: str
    description: Optional[str] = None
    input_schema: Dict[str, Any] = Field(
        default_factory=lambda: {"type": "object"}, alias="inputSchema"
    )
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class ListToolsResponse(BaseModel):
    tools: List[AiTool]


class InvokeToolResponse(BaseModel):

    result: Optional[Any] = None
    error: Optional[str] = None

    @property
    def succeeded(self) -> bool:
        """True when there is a result and no error.

        Note: a Dremio 200 with an empty body (e.g. a void tool) will yield
        ``result=None, error=None``.  Use :pyattr:`is_empty` to distinguish
        this from a real failure.
        """
        return self.error is None and self.result is not None

    @property
    def is_empty(self) -> bool:
        """True when the response carries neither a result nor an error.

        This can happen when Dremio returns a 200 with an empty body for a
        void tool.  Callers may choose to treat this as a successful no-op.
        """
        return self.result is None and self.error is None


async def list_tools() -> List[Dict[str, Any]]:
    try:
        client = AsyncHttpClient()
        response: ListToolsResponse = await client.get(
            "/api/v4/ai/tools",
            deser=ListToolsResponse,
        )
        return [t.model_dump(by_alias=False) for t in response.tools]
    except ClientResponseError as e:
        log.error(f"Failed to list AI tools: HTTP {e.status} {e.message}")
        raise AiToolError(
            f"Failed to list AI tools: HTTP {e.status} {e.message}",
            status=e.status,
        ) from e
    except Exception as e:
        log.error(f"Failed to list AI tools: {e}")
        raise AiToolError(f"Failed to list AI tools: {e}") from e


async def invoke_tool(tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    safe_name = quote(tool_name, safe="")
    try:
        client = AsyncHttpClient()
        response: InvokeToolResponse = await client.post(
            f"/api/v4/ai/tools/{safe_name}:invoke",
            body={"args": args},
            deser=InvokeToolResponse,
        )
        return response.model_dump(exclude_none=True)
    except ClientResponseError as e:
        log.error(
            f"Failed to invoke AI tool '{tool_name}': HTTP {e.status} {e.message}"
        )
        raise AiToolError(
            f"Failed to invoke AI tool '{tool_name}': HTTP {e.status} {e.message}",
            status=e.status,
        ) from e
    except Exception as e:
        log.error(f"Failed to invoke AI tool '{tool_name}': {e}")
        raise AiToolError(f"Failed to invoke AI tool '{tool_name}': {e}") from e
