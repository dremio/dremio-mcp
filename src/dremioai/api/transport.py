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
import logging
import asyncio

from aiohttp import ClientSession, ClientResponse, ClientResponseError
from typing import (
    AnyStr,
    Callable,
    Optional,
    Dict,
    TypeAlias,
    Union,
    TextIO,
    Awaitable,
    Any,
)
from pathlib import Path
from dremioai.log import logger
from json import loads
from pydantic import BaseModel, ValidationError
from http import HTTPStatus

from dremioai.config import settings
from dremioai.api.oauth2 import get_oauth2_tokens

DeserializationStrategy: TypeAlias = Union[Callable, BaseModel]


class RetryConfig:
    def __init__(self):
        if settings.instance() and settings.instance().dremio:
            self.config = settings.instance().dremio.http_retry
        else:
            self.config = settings.HttpRetry()

    @property
    def max_retries(self) -> int:
        """Expose max_retries from config for convenience"""
        return self.config.max_retries

    def get_config_delay(self, attempt_number: int = 0) -> float:
        return self.config.initial_delay * (
            self.config.backoff_multiplier**attempt_number
        )

    def get_delay(
        self,
        response: ClientResponse,
        attempt_number: int,
    ) -> float:
        retry_after = response.headers.get("Retry-After")
        delay = self.get_config_delay(attempt_number=attempt_number)
        if retry_after is not None:
            try:
                delay = min(delay, int(retry_after))
            except (ValueError, TypeError) as e:
                logger().debug(
                    f"Invalid Retry-After header, using exponential backoff - {e}"
                )

        return min(delay, self.config.max_delay)


async def retry_middleware(
    req, handler: Callable[[any], Awaitable[ClientResponse]]
) -> ClientResponse:
    """
    Middleware that automatically retries requests on 429 (rate limit) errors.
    Uses exponential backoff with configurable parameters from settings.
    """
    retry_config = RetryConfig()
    for attempt in range(retry_config.max_retries + 1):
        response = await handler(req)
        if response.status != HTTPStatus.TOO_MANY_REQUESTS:
            break

        delay = retry_config.get_delay(response, attempt)
        logger(f"{__name__}.retry").warning(
            f"Rate limited (429) on {req.method} {req.url.path}. "
            f"Retry {attempt + 1}/{retry_config.max_retries} after {delay:.2f}s"
        )
        await asyncio.sleep(delay)

    return response


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

    def log_request(
        self, method: str, endpoint: str, params: Optional[Dict[AnyStr, Any]] = None
    ):
        if logger().isEnabledFor(logging.DEBUG):
            sanitized_headers = {
                k: (v if k != "Authorization" else "Bearer <redacted>")
                for k, v in self.headers.items()
            }
            logger().debug(
                f"{method} {self.uri}{endpoint}', headers={sanitized_headers}, params={params}"
            )

    async def get(
        self,
        endpoint: AnyStr,
        params: Dict[AnyStr, AnyStr] = None,
        deser: Optional[DeserializationStrategy] = None,
        body: Dict[AnyStr, AnyStr] = None,
        file: Optional[TextIO] = None,
        top_level_list: bool = False,
    ):
        async with ClientSession(middlewares=(retry_middleware,)) as session:
            self.log_request("GET", endpoint, params)
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
        async with ClientSession(middlewares=(retry_middleware,)) as session:
            self.log_request("POST", endpoint)
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
