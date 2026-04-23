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


class InvokeToolResponse(BaseModel):

    result: Optional[Any] = None
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


async def list_tools() -> ListToolsResponse:
    try:
        client = AsyncHttpClient()
        return await client.get(
            "/api/v4/ai/tools",
            deser=ListToolsResponse,
        )
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
        return await client.post(
            f"/api/v4/ai/tools/{safe_name}:invoke",
            body={"args": args},
            deser=InvokeToolResponse,
        )
    except ClientResponseError as e:
        log.exception("Failed to invoke AI tool '%s'", tool_name)
        return InvokeToolResponse(error=f"HTTP {e.status} {e.message}")
    except Exception:
        log.exception("Failed to invoke AI tool '%s'", tool_name)
        return InvokeToolResponse(error=f"Unexpected error invoking tool '{tool_name}'")
