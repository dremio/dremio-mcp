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

from mcp.server.auth.provider import (
    OAuthAuthorizationServerProvider,
    OAuthClientInformationFull,
    AuthorizationParams,
)


class DremioCloudOAuthProvider(OAuthAuthorizationServerProvider):
    # redirect to https://login.[eu.]dremio.cloud/oauth/authorize
    # the callbacks are limited to 'http://localhost' and therefore
    # we need to implement a local callback on that (with random port)
    # to get the authorization code and then call the actual callback

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        # without support for DCR, we just return client information with the same
        # client id as sent in. The actual validation will occur after authorize
        # redirection
        return OAuthClientInformationFull.model_validate(
            {"client_id": client_id, "redirect_uris": ["http://localhost"]}
        )

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        raise NotImplementedError(
            "Dynamic client registration is not supported for Dremio Cloud"
        )

    async def authorize(
        self, client: OAuthClientInformationFull, params: AuthorizationParams
    ) -> str:
        print(client)
        print(params)
        raise NotImplementedError("Authorization is not supported for Dremio Cloud")

    async def load_access_token(self, token):
        return await super().load_access_token(token)
