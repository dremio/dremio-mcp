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
"""
Mock OAuth2 provider for ``--mock`` mode.

Provides a self-contained OAuth2 flow (RFC 8414 metadata, RFC 7636 PKCE,
Dynamic Client Registration) backed entirely by in-memory state so the
MCP server can be exercised without a real Dremio instance or external
identity provider.
"""

import hashlib
import secrets
import time
from base64 import urlsafe_b64encode
from typing import Optional
from urllib.parse import urlencode
from uuid import uuid4

import jwt as pyjwt
from mcp.server.auth.json_response import PydanticJSONResponse
from mcp.server.auth.provider import AccessToken, TokenVerifier
from pydantic import AnyHttpUrl
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response

from dremioai import log
from dremioai.api.oauth_metadata import OAuthMetadataRFC8414

logger = log.logger(__name__)


class MockJWTIssuer:
    """Self-contained HS256 JWT issuer for mock mode."""

    def __init__(
        self,
        issuer_url: str,
        default_expiry: int = 3600,
        refresh_token_expiry: int = 86400,
    ):
        self._secret = secrets.token_hex(32)
        self._issuer_url = issuer_url.rstrip("/")
        self._default_expiry = default_expiry
        self._refresh_token_expiry = refresh_token_expiry
        # code -> {client_id, redirect_uri, code_challenge, code_challenge_method, sub, aud}
        self._pending_codes: dict[str, dict] = {}
        # refresh_token -> {sub, aud, client_id, issued_at}
        self._refresh_tokens: dict[str, dict] = {}
        logger.info(
            f"MockJWTIssuer initialised: issuer={self._issuer_url}, "
            f"token_expiry={default_expiry}s, refresh_token_expiry={refresh_token_expiry}s"
        )

    def issue_token(
        self,
        sub: str = "mock-user",
        aud: str = "mock-audience",
        scopes: Optional[list[str]] = None,
    ) -> str:
        now = int(time.time())
        payload = {
            "iss": self._issuer_url,
            "sub": sub,
            "aud": aud,
            "exp": now + self._default_expiry,
            "iat": now,
            "scopes": scopes or ["read"],
        }
        token = pyjwt.encode(payload, self._secret, algorithm="HS256")
        logger.info(f"Issued mock JWT: sub={sub}, aud={aud}, expires_in={self._default_expiry}s")
        return token

    def verify_token(self, token: str) -> Optional[dict]:
        try:
            claims = pyjwt.decode(
                token,
                self._secret,
                algorithms=["HS256"],
                options={"verify_aud": False},
            )
            logger.info(f"Mock JWT verified: sub={claims.get('sub')}")
            return claims
        except pyjwt.PyJWTError:
            logger.info("Mock JWT verification failed")
            return None

    def issue_authorization_code(
        self,
        client_id: str,
        redirect_uri: str,
        code_challenge: str,
        code_challenge_method: str = "S256",
    ) -> str:
        code = str(uuid4())
        self._pending_codes[code] = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "code_challenge": code_challenge,
            "code_challenge_method": code_challenge_method,
            "sub": "mock-user",
            "aud": "mock-audience",
        }
        logger.info(f"Issued authorization code for client_id={client_id}, redirect_uri={redirect_uri}")
        return code

    def exchange_code(self, code: str, code_verifier: str) -> Optional[dict]:
        params = self._pending_codes.pop(code, None)
        if params is None:
            logger.info(f"Code exchange failed: unknown code")
            return None

        # Validate PKCE S256
        if params.get("code_challenge_method") == "S256":
            digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
            expected = urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
            if expected != params["code_challenge"]:
                logger.info(f"Code exchange failed: PKCE verification failed for client_id={params['client_id']}")
                return None

        logger.info(f"Exchanging authorization code for client_id={params['client_id']}")
        access_token = self.issue_token(sub=params["sub"], aud=params["aud"])
        refresh_token = secrets.token_hex(32)
        self._refresh_tokens[refresh_token] = {
            "sub": params["sub"],
            "aud": params["aud"],
            "client_id": params["client_id"],
            "issued_at": int(time.time()),
        }
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_in": self._default_expiry,
            "token_type": "Bearer",
        }

    def refresh(self, refresh_token: str) -> Optional[dict]:
        params = self._refresh_tokens.get(refresh_token)
        if params is None:
            logger.info("Refresh token exchange failed: unknown token")
            return None

        issued_at = params.get("issued_at", 0)
        if int(time.time()) - issued_at > self._refresh_token_expiry:
            logger.info(f"Refresh token expired for client_id={params['client_id']}")
            del self._refresh_tokens[refresh_token]
            return None

        logger.info(f"Refreshing token for client_id={params['client_id']}")
        access_token = self.issue_token(sub=params["sub"], aud=params["aud"])
        new_refresh = secrets.token_hex(32)
        self._refresh_tokens[new_refresh] = {
            **params,
            "issued_at": int(time.time()),
        }
        del self._refresh_tokens[refresh_token]
        return {
            "access_token": access_token,
            "refresh_token": new_refresh,
            "expires_in": self._default_expiry,
            "token_type": "Bearer",
        }


class MockTokenVerifier(TokenVerifier):
    """Token verifier that delegates to a ``MockJWTIssuer``."""

    def __init__(self, issuer: MockJWTIssuer):
        self._issuer = issuer
        logger.info("MockTokenVerifier initialised")

    async def verify_token(self, token: str) -> Optional[AccessToken]:
        if not token:
            logger.info("MockTokenVerifier: empty token")
            return None
        claims = self._issuer.verify_token(token)
        if claims is None:
            logger.info("MockTokenVerifier: token rejected")
            return None
        logger.info(f"MockTokenVerifier: token accepted for sub={claims.get('sub')}")
        return AccessToken(
            token=token,
            client_id="mock-client",
            scopes=["read", "jwt_verified"],
            expires_at=claims.get("exp"),
        )


# ---------------------------------------------------------------------------
# Mock OAuth route handlers
# ---------------------------------------------------------------------------


async def mock_register(request: Request) -> Response:
    """POST /oauth/register — Dynamic Client Registration (RFC 7591)."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    client_id = f"mock-{uuid4()}"
    client_name = body.get("client_name", "mock-client")
    logger.info(f"mock_register: client_name={client_name}, client_id={client_id}")
    return JSONResponse(
        {
            "client_id": client_id,
            "client_name": client_name,
            "redirect_uris": body.get("redirect_uris", []),
            "grant_types": body.get("grant_types", ["authorization_code"]),
            "response_types": body.get("response_types", ["code"]),
            "token_endpoint_auth_method": "none",
        }
    )


async def mock_authorize(request: Request, issuer: MockJWTIssuer) -> Response:
    """GET /oauth/authorize — auto-approves and redirects with auth code."""
    params = dict(request.query_params)
    client_id = params.get("client_id", "unknown")
    redirect_uri = params.get("redirect_uri", "")
    state = params.get("state", "")
    code_challenge = params.get("code_challenge", "")
    code_challenge_method = params.get("code_challenge_method", "S256")

    logger.info(f"mock_authorize: client_id={client_id}, redirect_uri={redirect_uri}")

    code = issuer.issue_authorization_code(
        client_id=client_id,
        redirect_uri=redirect_uri,
        code_challenge=code_challenge,
        code_challenge_method=code_challenge_method,
    )

    sep = "&" if "?" in redirect_uri else "?"
    location = f"{redirect_uri}{sep}{urlencode({'code': code, 'state': state})}"
    logger.info(f"mock_authorize: redirecting to {redirect_uri}")
    return RedirectResponse(url=location, status_code=302)


async def mock_token(request: Request, issuer: MockJWTIssuer) -> Response:
    """POST /oauth/token — exchanges codes and refresh tokens."""
    try:
        body = dict(await request.form())
    except Exception:
        body = {}

    grant_type = body.get("grant_type")
    logger.info(f"mock_token: grant_type={grant_type}")

    if grant_type == "authorization_code":
        code = body.get("code", "")
        code_verifier = body.get("code_verifier", "")
        result = issuer.exchange_code(code, code_verifier)
        if result is None:
            logger.info("mock_token: authorization_code exchange failed")
            return JSONResponse({"error": "invalid_grant"}, status_code=400)
        logger.info("mock_token: authorization_code exchange succeeded")
        return JSONResponse(result)

    if grant_type == "refresh_token":
        rt = body.get("refresh_token", "")
        result = issuer.refresh(rt)
        if result is None:
            logger.info("mock_token: refresh_token exchange failed")
            return JSONResponse({"error": "invalid_grant"}, status_code=400)
        logger.info("mock_token: refresh_token exchange succeeded")
        return JSONResponse(result)

    logger.info(f"mock_token: unsupported grant_type={grant_type}")
    return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)


def register_mock_routes(mcp, issuer: MockJWTIssuer) -> None:
    """Register mock OAuth endpoints on the FastMCP instance."""
    base_url = issuer._issuer_url
    logger.info(f"Registering mock OAuth routes at {base_url}")

    @mcp.custom_route("/oauth/register", methods=["POST"])
    async def _register(request: Request) -> Response:
        return await mock_register(request)

    @mcp.custom_route("/oauth/authorize", methods=["GET"])
    async def _authorize(request: Request) -> Response:
        return await mock_authorize(request, issuer)

    @mcp.custom_route("/oauth/token", methods=["POST"])
    async def _token(request: Request) -> Response:
        return await mock_token(request, issuer)

    @mcp.custom_route("/.well-known/oauth-authorization-server", methods=["GET"])
    @mcp.custom_route(
        "/mcp/{project_id}/.well-known/oauth-authorization-server", methods=["GET"]
    )
    async def _metadata(request: Request) -> Response:
        # Derive base URL from the incoming request so metadata works
        # behind ngrok, tunnels, and reverse proxies.
        scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
        host = request.headers.get("x-forwarded-host", request.headers.get("host", ""))
        request_base = f"{scheme}://{host}".rstrip("/")
        logger.info(f"mock_metadata: using base_url={request_base}")
        md = OAuthMetadataRFC8414(
            issuer=AnyHttpUrl(request_base),
            authorization_endpoint=f"{request_base}/oauth/authorize",
            token_endpoint=f"{request_base}/oauth/token",
            registration_endpoint=AnyHttpUrl(f"{request_base}/oauth/register"),
            scopes_supported=["read", "offline_access"],
            response_types_supported=["code"],
            grant_types_supported=["authorization_code", "refresh_token"],
            code_challenge_methods_supported=["S256"],
            token_endpoint_auth_methods_supported=["none"],
        )
        return PydanticJSONResponse(md)
