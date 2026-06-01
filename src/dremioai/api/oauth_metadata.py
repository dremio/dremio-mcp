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
from mcp.shared.auth import OAuthMetadata
from pydantic import AnyHttpUrl, BaseModel, field_serializer


class OAuthMetadataRFC8414(OAuthMetadata):
    """RFC 8414 compliant OAuth metadata that strips trailing slash from issuer URL.

    The MCP SDK's OAuthMetadata uses AnyHttpUrl for the issuer field, which adds
    a trailing slash during serialization. RFC 8414 Section 3.2 requires the issuer
    to exactly match the discovery URL without trailing slash.
    """

    @field_serializer("issuer")
    def serialize_issuer(self, value: AnyHttpUrl) -> str:
        return str(value).rstrip("/")


class OAuthProtectedResourceMetadata(BaseModel):
    """RFC 9728 protected resource metadata."""

    resource: AnyHttpUrl
    authorization_servers: list[AnyHttpUrl] | None = None

    @field_serializer("authorization_servers")
    def serialize_authorization_servers(
        self, value: list[AnyHttpUrl] | None
    ) -> list[str] | None:
        return [str(url).rstrip("/") for url in value] if value is not None else None
