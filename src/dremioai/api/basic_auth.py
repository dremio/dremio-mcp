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
"""Session-token login for Dremio Software deployments.

Dremio Software Community edition cannot issue PATs, so the only credential it
offers is username/password. This module exchanges those credentials for a
session token via ``POST /apiv2/login`` and installs it as the effective token
for API calls (the REST API accepts session tokens as ``Bearer`` tokens).

Mirrors the shape of :mod:`dremioai.api.oauth2`: ``get_session_token()``
returns an object whose ``update_settings()`` mutates the live settings.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from json import dumps, loads
from typing import Optional
from urllib.request import Request, urlopen

from dremioai.config import settings
from dremioai.log import logger

# Re-login this long before the server-reported expiry, so an in-flight
# request never straddles the expiration boundary.
_EXPIRY_SAFETY_MARGIN = timedelta(minutes=5)


@dataclass
class SessionToken:
    token: str
    expiry: Optional[datetime]

    def update_settings(self):
        dremio = settings.instance().dremio
        dremio.pat = self.token
        dremio.basic_auth.expiry = self.expiry


def get_session_token(timeout: float = 30.0) -> SessionToken:
    """Exchange the configured username/password for a Dremio session token."""
    dremio = settings.instance().dremio
    basic_auth = dremio.basic_auth
    body = dumps(
        {"userName": basic_auth.username, "password": basic_auth.password}
    ).encode("utf-8")
    request = Request(
        f"{dremio.uri}/apiv2/login",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=timeout) as response:
        payload = loads(response.read().decode("utf-8"))

    expiry = None
    if expires_ms := payload.get("expires"):
        expiry = datetime.fromtimestamp(expires_ms / 1000) - _EXPIRY_SAFETY_MARGIN

    logger().info(f"Obtained Dremio session token (expires {expiry})")
    return SessionToken(token=payload["token"], expiry=expiry)
