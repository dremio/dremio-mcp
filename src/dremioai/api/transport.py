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

from aiohttp import ClientSession, ClientResponse, ClientResponseError
from pathlib import Path
from typing import AnyStr, Callable, Optional, Dict, TypeAlias, Union, TextIO
from dremioai.log import logger
from json import loads
from pydantic import BaseModel, ValidationError

from dremioai.config import settings
from dremioai.api.oauth2 import get_oauth2_tokens

DeserializationStrategy: TypeAlias = Union[Callable, BaseModel]


class AsyncHttpClient:
    def __init__(self, uri: AnyStr, token: AnyStr):
        self.uri = uri
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "content-type": "application/json",
        }
        self.update_headers()

    def update_headers(self):
        pass

    async def download(self, response: ClientResponse, file: TextIO):
        while chunk := await response.content.read(1024):
            file.write(chunk)
        file.flush()

    async def deserialize(
        self,
        response: ClientResponse,
        deser: DeserializationStrategy,
        top_level_list: bool = False,
    ):
        js = await response.text()
        try:
            if deser is not None and issubclass(deser, BaseModel):
                if top_level_list:
                    return [deser.model_validate(o) for o in loads(js)]
                return deser.model_validate_json(js)
            return loads(js, object_hook=deser)
        except ValidationError as e:
            logger().error(
                f"in {response.request_info.method} {response.request_info.url}: {e.errors()}\ndata = {js}"
            )
            raise RuntimeError(f"Unable to parse {e}, deser={deser}\n{e.errors()}")
        except Exception as e:
            logger().error(
                f"in {response.request_info.method} {response.request_info.url} deser={deser}: unable to parse {js}: {e}"
            )
            raise

    async def handle_response(
        self,
        response: ClientResponse,
        deser: DeserializationStrategy,
        file: TextIO,
        top_level_list: bool = False,
    ):
        response.raise_for_status()
        if file is None:
            return await self.deserialize(
                response, deser, top_level_list=top_level_list
            )
        await self.download(response, file)

    async def get(
        self,
        endpoint: AnyStr,
        params: Dict[AnyStr, AnyStr] = None,
        deser: Optional[DeserializationStrategy] = None,
        body: Dict[AnyStr, AnyStr] = None,
        file: Optional[TextIO] = None,
        top_level_list: bool = False,
    ):
        async with ClientSession() as session:
            logger().info(
                f"{self.uri}{endpoint}', headers={self.headers}, params={params}"
            )
            async with session.get(
                f"{self.uri}{endpoint}",
                headers=self.headers,
                json=body,
                params=params,
                ssl=False,
            ) as response:
                return await self.handle_response(
                    response, deser, file, top_level_list=top_level_list
                )

    async def post(
        self,
        endpoint: AnyStr,
        body: Optional[AnyStr] = None,
        deser: Optional[DeserializationStrategy] = None,
        file: Optional[TextIO] = None,
        top_level_list: bool = False,
    ):
        async with ClientSession() as session:
            async with session.post(
                f"{self.uri}{endpoint}", headers=self.headers, json=body, ssl=False
            ) as response:
                return await self.handle_response(
                    response, deser, file, top_level_list=top_level_list
                )


class DremioAsyncHttpClient(AsyncHttpClient):
    def __init__(self):
        dremio = settings.instance().dremio
        if (
            dremio.oauth_supported
            and dremio.oauth_configured
            and (dremio.oauth2.has_expired or dremio.pat is None)
        ):
            oauth = get_oauth2_tokens()
            oauth.update_settings()

        uri = dremio.uri
        pat = dremio.pat

        if uri is None or pat is None:
            raise RuntimeError(f"uri={uri} pat={pat} are required")
        super().__init__(uri, pat)
