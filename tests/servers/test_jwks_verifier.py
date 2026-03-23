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

import time
import pytest
from unittest.mock import patch, MagicMock

import jwt as pyjwt
from jwt import PyJWKClient, PyJWKClientError, ExpiredSignatureError

from dremioai.servers.jwks_verifier import JWKSVerifier, VerifiedClaims

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
        assert result == VerifiedClaims(exp=future_exp, aud="org-123")

    @pytest.mark.asyncio
    async def test_aud_list_extracts_first(self, verifier, mock_key):
        with patch.object(PyJWKClient, "get_signing_key_from_jwt", return_value=mock_key), \
             patch(JWKS_DECODE, return_value=_claims(exp=9999999999, aud=["org-a", "org-b"])):
            result = await verifier.verify("t")
        assert result.aud == "org-a"

    @pytest.mark.asyncio
    async def test_expired_token_returns_exp_zero(self, verifier, mock_key):
        with patch.object(PyJWKClient, "get_signing_key_from_jwt", return_value=mock_key), \
             patch(JWKS_DECODE, side_effect=ExpiredSignatureError("expired")):
            result = await verifier.verify("t")
        assert result == VerifiedClaims(exp=0)

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
        assert result == VerifiedClaims(exp=future_exp, aud="org-456")

    @pytest.mark.asyncio
    async def test_no_exp_claim(self, verifier, mock_key):
        with patch.object(PyJWKClient, "get_signing_key_from_jwt", return_value=mock_key), \
             patch(JWKS_DECODE, return_value=_claims(aud="org-789")):
            result = await verifier.verify("t")
        assert result.exp is None
        assert result.aud == "org-789"

    def test_custom_lifespan(self):
        with patch.object(PyJWKClient, "__init__", return_value=None):
            v = JWKSVerifier("https://example.com/jwks", lifespan=7200)
        assert v._lifespan == 7200
