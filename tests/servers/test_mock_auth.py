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

import hashlib
import time
from base64 import urlsafe_b64encode
from urllib.parse import parse_qs, urlparse

import pytest

from dremioai.servers.mock_auth import (
    MockJWTIssuer,
    MockTokenVerifier,
    mock_authorize,
    mock_register,
    mock_token,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pkce_pair():
    """Return a (code_verifier, code_challenge) tuple for S256."""
    verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return verifier, challenge


class _FakeRequest:
    """Minimal stand-in for starlette.requests.Request."""

    def __init__(self, query_params=None, body=None, form_data=None):
        self.query_params = query_params or {}
        self._body = body or {}
        self._form_data = form_data or {}

    async def json(self):
        return self._body

    async def form(self):
        return self._form_data


# ---------------------------------------------------------------------------
# MockJWTIssuer
# ---------------------------------------------------------------------------


class TestMockJWTIssuer:
    def test_issue_token_returns_valid_jwt(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        token = issuer.issue_token(sub="alice", aud="my-aud", scopes=["read", "write"])

        claims = issuer.verify_token(token)
        assert claims is not None
        assert claims["sub"] == "alice"
        assert claims["aud"] == "my-aud"
        assert claims["iss"] == "http://localhost:8080"
        assert claims["scopes"] == ["read", "write"]
        assert claims["exp"] > time.time()
        assert claims["iat"] <= time.time()

    def test_issue_token_defaults(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        token = issuer.issue_token()
        claims = issuer.verify_token(token)
        assert claims is not None
        assert claims["sub"] == "mock-user"
        assert claims["aud"] == "mock-audience"
        assert claims["scopes"] == ["read"]

    def test_verify_token_rejects_garbage(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        assert issuer.verify_token("not-a-jwt") is None

    def test_verify_token_rejects_wrong_secret(self):
        issuer_a = MockJWTIssuer("http://localhost:8080")
        issuer_b = MockJWTIssuer("http://localhost:8080")
        token = issuer_a.issue_token()
        # Different issuer has a different random secret
        assert issuer_b.verify_token(token) is None

    def test_verify_token_rejects_expired(self):
        issuer = MockJWTIssuer("http://localhost:8080", default_expiry=-1)
        token = issuer.issue_token()
        assert issuer.verify_token(token) is None

    def test_issuer_url_trailing_slash_stripped(self):
        issuer = MockJWTIssuer("http://localhost:8080/")
        token = issuer.issue_token()
        claims = issuer.verify_token(token)
        assert claims is not None
        assert claims["iss"] == "http://localhost:8080"

    def test_full_pkce_flow(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        verifier, challenge = _pkce_pair()

        code = issuer.issue_authorization_code(
            client_id="client-1",
            redirect_uri="http://localhost/callback",
            code_challenge=challenge,
            code_challenge_method="S256",
        )
        assert code is not None

        result = issuer.exchange_code(code, verifier)
        assert result is not None
        assert "access_token" in result
        assert "refresh_token" in result
        assert result["token_type"] == "Bearer"
        assert result["expires_in"] == 3600

        # Verify the issued access token is valid
        claims = issuer.verify_token(result["access_token"])
        assert claims is not None

    def test_exchange_code_rejects_wrong_verifier(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        _, challenge = _pkce_pair()

        code = issuer.issue_authorization_code(
            client_id="client-1",
            redirect_uri="http://localhost/callback",
            code_challenge=challenge,
        )
        assert issuer.exchange_code(code, "wrong-verifier") is None

    def test_exchange_code_rejects_reused_code(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        verifier, challenge = _pkce_pair()

        code = issuer.issue_authorization_code(
            client_id="client-1",
            redirect_uri="http://localhost/callback",
            code_challenge=challenge,
        )
        issuer.exchange_code(code, verifier)
        # Second use of same code should fail
        assert issuer.exchange_code(code, verifier) is None

    def test_exchange_code_invalid_code(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        assert issuer.exchange_code("nonexistent-code", "verifier") is None

    def test_refresh_token(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        verifier, challenge = _pkce_pair()

        code = issuer.issue_authorization_code(
            client_id="client-1",
            redirect_uri="http://localhost/callback",
            code_challenge=challenge,
        )
        tokens = issuer.exchange_code(code, verifier)
        assert tokens is not None
        refresh_token = tokens["refresh_token"]

        result = issuer.refresh(refresh_token)
        assert result is not None
        assert "access_token" in result
        assert "refresh_token" in result
        assert result["refresh_token"] != refresh_token  # rotated

        # Old refresh token should be invalid now
        assert issuer.refresh(refresh_token) is None

        # New refresh token should work
        result2 = issuer.refresh(result["refresh_token"])
        assert result2 is not None

    def test_refresh_invalid_token(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        assert issuer.refresh("nonexistent") is None


# ---------------------------------------------------------------------------
# MockTokenVerifier
# ---------------------------------------------------------------------------


class TestMockTokenVerifier:
    @pytest.mark.asyncio
    async def test_verify_valid_token(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        verifier = MockTokenVerifier(issuer)
        token = issuer.issue_token()

        result = await verifier.verify_token(token)
        assert result is not None
        assert result.token == token
        assert result.client_id == "mock-client"
        assert "read" in result.scopes
        assert "jwt_verified" in result.scopes

    @pytest.mark.asyncio
    async def test_verify_invalid_token(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        verifier = MockTokenVerifier(issuer)
        assert await verifier.verify_token("garbage") is None

    @pytest.mark.asyncio
    async def test_verify_empty_token(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        verifier = MockTokenVerifier(issuer)
        assert await verifier.verify_token("") is None


# ---------------------------------------------------------------------------
# Mock route handlers
# ---------------------------------------------------------------------------


class TestMockRouteHandlers:
    @pytest.mark.asyncio
    async def test_register(self):
        request = _FakeRequest(
            body={
                "client_name": "test-app",
                "redirect_uris": ["http://localhost/callback"],
            }
        )
        response = await mock_register(request)
        assert response.status_code == 200
        body = response.body.decode()
        import json

        data = json.loads(body)
        assert data["client_name"] == "test-app"
        assert data["client_id"].startswith("mock-")
        assert data["token_endpoint_auth_method"] == "none"

    @pytest.mark.asyncio
    async def test_register_empty_body(self):
        request = _FakeRequest(body={})
        response = await mock_register(request)
        assert response.status_code == 200
        import json

        data = json.loads(response.body.decode())
        assert data["client_name"] == "mock-client"

    @pytest.mark.asyncio
    async def test_authorize_redirects(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        verifier, challenge = _pkce_pair()

        request = _FakeRequest(
            query_params={
                "client_id": "client-1",
                "redirect_uri": "http://localhost/callback",
                "state": "my-state",
                "code_challenge": challenge,
                "code_challenge_method": "S256",
                "response_type": "code",
                "scope": "read",
            }
        )
        response = await mock_authorize(request, issuer)
        assert response.status_code == 302

        location = response.headers["location"]
        parsed = urlparse(location)
        params = parse_qs(parsed.query)
        assert params["state"] == ["my-state"]
        assert "code" in params

        # The code should be exchangeable
        code = params["code"][0]
        result = issuer.exchange_code(code, verifier)
        assert result is not None

    @pytest.mark.asyncio
    async def test_token_exchange(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        verifier, challenge = _pkce_pair()

        code = issuer.issue_authorization_code(
            client_id="client-1",
            redirect_uri="http://localhost/callback",
            code_challenge=challenge,
        )

        request = _FakeRequest(
            form_data={
                "grant_type": "authorization_code",
                "code": code,
                "code_verifier": verifier,
            }
        )
        response = await mock_token(request, issuer)
        assert response.status_code == 200

        import json

        data = json.loads(response.body.decode())
        assert "access_token" in data
        assert "refresh_token" in data
        assert data["token_type"] == "Bearer"

    @pytest.mark.asyncio
    async def test_token_refresh(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        verifier, challenge = _pkce_pair()

        code = issuer.issue_authorization_code(
            client_id="client-1",
            redirect_uri="http://localhost/callback",
            code_challenge=challenge,
        )
        tokens = issuer.exchange_code(code, verifier)
        assert tokens is not None

        request = _FakeRequest(
            form_data={
                "grant_type": "refresh_token",
                "refresh_token": tokens["refresh_token"],
            }
        )
        response = await mock_token(request, issuer)
        assert response.status_code == 200

        import json

        data = json.loads(response.body.decode())
        assert "access_token" in data

    @pytest.mark.asyncio
    async def test_token_invalid_grant(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        request = _FakeRequest(
            form_data={
                "grant_type": "authorization_code",
                "code": "bad-code",
                "code_verifier": "whatever",
            }
        )
        response = await mock_token(request, issuer)
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_token_unsupported_grant_type(self):
        issuer = MockJWTIssuer("http://localhost:8080")
        request = _FakeRequest(
            form_data={"grant_type": "client_credentials"}
        )
        response = await mock_token(request, issuer)
        assert response.status_code == 400

        import json

        data = json.loads(response.body.decode())
        assert data["error"] == "unsupported_grant_type"
