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

from dremioai.api.transport import DremioAsyncHttpClient as AsyncHttpClient


class AiTool(BaseModel):

    name: str
    description: Optional[str] = None
    input_schema: Dict[str, Any] = Field(alias="inputSchema")
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class ListToolsResponse(BaseModel):
    tools: List[AiTool]


class InvokeToolResponse(BaseModel):

    result: Optional[Any] = None
    error: Optional[str] = None

    @property
    def succeeded(self) -> bool:
        return self.error is None


async def list_tools() -> List[Dict[str, Any]]:
    client = AsyncHttpClient()
    response: ListToolsResponse = await client.get(
        "/api/v4/ai/tools",
        deser=ListToolsResponse,
    )
    return [t.model_dump(by_alias=False) for t in response.tools]


async def invoke_tool(tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    client = AsyncHttpClient()
    response: InvokeToolResponse = await client.post(
        f"/api/v4/ai/tools/{tool_name}:invoke",
        body={"args": args},
        deser=InvokeToolResponse,
    )
    return response.model_dump(exclude_none=True)
