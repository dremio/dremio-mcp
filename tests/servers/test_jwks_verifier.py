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
Tests for JWKSVerifier — JWKS-based JWT verification and claims extraction.
"""

import logging
import time
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

import jwt as pyjwt
from jwt import PyJWKClient, PyJWKClientError, ExpiredSignatureError

from dremioai.servers.jwks_verifier import JWKSVerifier, VerifiedClaims, TokenExpiredError
from dremioai.servers.mcp import make_logged_invoke, RequireAuthWithWWWAuthenticateMiddleware

JWKS_DECODE = "dremioai.servers.jwks_verifier.pyjwt.decode"


@pytest.fixture
def verifier():
    with patch.object(PyJWKClient, "__init__", return_value=None):
        return JWKSVerifier("https://example.com/.well-known/jwks.json")


@pytest.fixture
def mock_key():
    return MagicMock()


def _claims(exp=None, aud=None):
    """Build a claims dict for mocking pyjwt.decode."""
    c = {"sub": "test-user"}
    if exp is not None:
        c["exp"] = exp
    if aud is not None:
        c["aud"] = aud
    return c


class TestJWKSVerifier:

    @pytest.mark.asyncio
    async def test_valid_token_returns_claims(self, verifier, mock_key):
        future_exp = int(time.time()) + 3600
        with patch.object(PyJWKClient, "get_signing_key_from_jwt", return_value=mock_key), \
             patch(JWKS_DECODE, return_value=_claims(exp=future_exp, aud="org-123")):
            result = await verifier.verify("t")
        assert result == VerifiedClaims(exp=future_exp, org_id="org-123", user_id="test-user")

    @pytest.mark.asyncio
    async def test_user_id_extracted_from_sub(self, verifier, mock_key):
        future_exp = int(time.time()) + 3600
        with patch.object(PyJWKClient, "get_signing_key_from_jwt", return_value=mock_key), \
             patch(JWKS_DECODE, return_value=_claims(exp=future_exp, aud="org-123")):
            result = await verifier.verify("t")
        assert result.user_id == "test-user"

    @pytest.mark.asyncio
    async def test_aud_list_extracts_first(self, verifier, mock_key):
        with patch.object(PyJWKClient, "get_signing_key_from_jwt", return_value=mock_key), \
             patch(JWKS_DECODE, return_value=_claims(exp=9999999999, aud=["org-a", "org-b"])):
            result = await verifier.verify("t")
        assert result.org_id == "org-a"

    @pytest.mark.asyncio
    async def test_sub_list_extracts_first(self, verifier, mock_key):
        claims = {"sub": ["user-1", "user-2"], "exp": 9999999999}
        with patch.object(PyJWKClient, "get_signing_key_from_jwt", return_value=mock_key), \
             patch(JWKS_DECODE, return_value=claims):
            result = await verifier.verify("t")
        assert result.user_id == "user-1"

    @pytest.mark.asyncio
    async def test_expired_token_raises_token_expired_error(self, verifier, mock_key):
        with patch.object(PyJWKClient, "get_signing_key_from_jwt", return_value=mock_key), \
             patch(JWKS_DECODE, side_effect=ExpiredSignatureError("expired")):
            with pytest.raises(TokenExpiredError):
                await verifier.verify("t")

    @pytest.mark.asyncio
    async def test_jwks_fetch_error_returns_none(self, verifier):
        with patch.object(PyJWKClient, "get_signing_key_from_jwt",
                          side_effect=PyJWKClientError("connection refused")):
            assert await verifier.verify("t") is None

    @pytest.mark.asyncio
    async def test_invalid_token_returns_none(self, verifier):
        with patch.object(PyJWKClient, "get_signing_key_from_jwt",
                          side_effect=pyjwt.DecodeError("bad")):
            assert await verifier.verify("t") is None

    @pytest.mark.asyncio
    async def test_key_rotation_triggers_cache_refresh(self, verifier, mock_key):
        future_exp = int(time.time()) + 3600
        with patch.object(PyJWKClient, "get_signing_key_from_jwt",
                          side_effect=[pyjwt.InvalidKeyError("kid not found"), mock_key]), \
             patch(JWKS_DECODE, return_value=_claims(exp=future_exp, aud="org-456")):
            result = await verifier.verify("t")
        assert result == VerifiedClaims(exp=future_exp, org_id="org-456", user_id="test-user")

    @pytest.mark.asyncio
    async def test_no_exp_claim(self, verifier, mock_key):
        with patch.object(PyJWKClient, "get_signing_key_from_jwt", return_value=mock_key), \
             patch(JWKS_DECODE, return_value=_claims(aud="org-789")):
            result = await verifier.verify("t")
        assert result.exp is None
        assert result.org_id == "org-789"

    def test_custom_lifespan(self):
        with patch.object(PyJWKClient, "__init__", return_value=None):
            v = JWKSVerifier("https://example.com/jwks", lifespan=7200)
        assert v._lifespan == 7200


class TestTokenExpiryBuffer:
    """Tests for the 60-second expiry buffer in DelegatingTokenVerifier.verify_token()."""

    @pytest.fixture(autouse=True)
    def _patch_settings(self):
        mock_dremio = MagicMock()
        mock_dremio.get.return_value = None  # jwks_uri returns None by default
        mock_inst = MagicMock()
        mock_inst.dremio = mock_dremio
        with patch("dremioai.servers.mcp.settings") as mock_settings:
            mock_settings.instance.return_value = mock_inst
            self._mock_dremio = mock_dremio
            self._mock_settings = mock_settings
            yield

    def _make_verifier_with_jwks(self, verified_claims):
        """Create a DelegatingTokenVerifier with a mocked JWKSVerifier."""
        from dremioai.servers.mcp import FastMCPServerWithAuthToken

        # Make jwks_uri return a value so __init__ creates a JWKSVerifier
        self._mock_dremio.get.side_effect = lambda k: {
            "jwks_uri": "https://example.com/.well-known/jwks.json",
            "jwks_cache_lifespan": 3600,
            "jwks_token_expiry_buffer_secs": 60,
        }.get(k)

        with patch.object(PyJWKClient, "__init__", return_value=None):
            verifier = FastMCPServerWithAuthToken.DelegatingTokenVerifier()

        # Mock the JWKSVerifier.verify method
        verifier._jwks_verifier.verify = AsyncMock(return_value=verified_claims)
        return verifier

    @pytest.mark.asyncio
    async def test_buffer_token_at_exp_minus_59_is_rejected(self):
        """Token with exp=now+59: after -60 buffer expires_at is in the past, rejected."""
        now = int(time.time())
        claims = VerifiedClaims(exp=now + 59, org_id="org-1")
        verifier = self._make_verifier_with_jwks(claims)
        result = await verifier.verify_token("test-token")
        assert result is None

    @pytest.mark.asyncio
    async def test_buffer_token_at_exp_plus_61_passes(self):
        """Token with exp=now+121: after -60 buffer expires_at is in the future, accepted."""
        now = int(time.time())
        claims = VerifiedClaims(exp=now + 121, org_id="org-2")
        verifier = self._make_verifier_with_jwks(claims)
        result = await verifier.verify_token("test-token")
        assert result is not None
        assert result.expires_at > int(time.time())

    @pytest.mark.asyncio
    async def test_buffer_token_with_none_exp_passes_through(self):
        """Token with exp=None should pass through without buffer adjustment."""
        claims = VerifiedClaims(exp=None, org_id="org-3")
        verifier = self._make_verifier_with_jwks(claims)
        result = await verifier.verify_token("test-token")
        assert result is not None
        assert result.expires_at is None

    @pytest.mark.asyncio
    async def test_expired_token_causes_verify_token_to_return_none(self):
        """When JWKSVerifier.verify() raises TokenExpiredError, verify_token() returns None."""
        verifier = self._make_verifier_with_jwks(VerifiedClaims(exp=9999, org_id="org-4"))
        verifier._jwks_verifier.verify = AsyncMock(side_effect=TokenExpiredError())
        result = await verifier.verify_token("test-token")
        assert result is None

    @pytest.mark.asyncio
    async def test_buffer_early_return_logs_warning(self, caplog):
        """Token past expiry buffer window: verify_token() returns None and logs WARNING."""
        now = int(time.time())
        # exp=now+59 → expires_at=now-1 → past, early return
        claims = VerifiedClaims(exp=now + 59, org_id="org-5", user_id="user-x")
        verifier = self._make_verifier_with_jwks(claims)
        with caplog.at_level(logging.WARNING):
            result = await verifier.verify_token("test-token")
        assert result is None
        warning_messages = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("expiry buffer" in msg for msg in warning_messages)

    @pytest.mark.asyncio
    async def test_no_warning_on_happy_path(self, caplog):
        """Valid future token should not produce WARNING logs."""
        now = int(time.time())
        claims = VerifiedClaims(exp=now + 3600, org_id="org-6")
        verifier = self._make_verifier_with_jwks(claims)
        with caplog.at_level(logging.WARNING):
            await verifier.verify_token("test-token")
        warning_messages = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warning_messages) == 0

    @pytest.mark.asyncio
    async def test_degradation_returns_none_when_jwks_returns_none(self, caplog):
        """When JWKSVerifier.verify() returns None, verify_token() returns None and logs WARNING."""
        verifier = self._make_verifier_with_jwks(None)
        with caplog.at_level(logging.WARNING):
            result = await verifier.verify_token("test-token")
        assert result is None
        warning_messages = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("JWKS verify" in msg for msg in warning_messages)


class TestDispatchWarning:
    """Tests for RequireAuthWithWWWAuthenticateMiddleware.dispatch() WARNING logging."""

    @pytest.mark.asyncio
    async def test_dispatch_logs_warning_on_401(self, caplog):
        middleware = RequireAuthWithWWWAuthenticateMiddleware(app=MagicMock())

        mock_user = MagicMock()
        mock_user.is_authenticated = False

        mock_client = MagicMock()
        mock_client.host = "192.168.1.1"

        mock_request = MagicMock(spec=["user", "url", "client"])
        mock_request.user = mock_user
        mock_request.url.path = "/mcp/tools"
        mock_request.client = mock_client

        call_next = AsyncMock()

        with caplog.at_level(logging.WARNING):
            response = await middleware.dispatch(mock_request, call_next)

        assert response.status_code == 401
        warning_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warning_records) >= 1
        # structlog renders bound kwargs into the message string
        assert any(
            "/mcp/tools" in str(r.message) or "192.168.1.1" in str(r.message)
            for r in warning_records
        )


class TestMakeLoggedInvoke:
    """Tests for make_logged_invoke WARNING logging on tool exceptions."""

    @pytest.mark.asyncio
    async def test_logs_warning_on_exception(self, caplog):
        async def failing_fn():
            raise ValueError("something went wrong")

        wrapped = make_logged_invoke("my_tool", failing_fn)
        with caplog.at_level(logging.WARNING):
            with pytest.raises(ValueError):
                await wrapped()
        warning_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warning_records) >= 1
        assert any(
            "my_tool" in str(r.message) for r in warning_records
        )

    @pytest.mark.asyncio
    async def test_no_log_on_success(self, caplog):
        async def ok_fn():
            return "ok"

        wrapped = make_logged_invoke("good_tool", ok_fn)
        with caplog.at_level(logging.WARNING):
            result = await wrapped()
        assert result == "ok"
        warning_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warning_records) == 0
