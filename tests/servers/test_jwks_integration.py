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
Integration test for JWKSVerifier against a real JWKS endpoint and token.

Fill in JWKS_URL and TOKEN below before running:

    uv run pytest tests/servers/test_jwks_integration.py -v -s
"""

import pytest

from dremioai.servers.jwks_verifier import JWKSVerifier, TokenExpiredError

# ── Fill these in ──────────────────────────────────────────────────────────────
JWKS_URL = ""   # e.g. "https://your-idp.example.com/.well-known/jwks.json"
TOKEN    = ""   # a valid (non-expired) JWT signed by the above JWKS endpoint
# ──────────────────────────────────────────────────────────────────────────────

pytestmark = pytest.mark.skipif(
    not JWKS_URL or not TOKEN,
    reason="JWKS_URL and TOKEN must be set in the test file before running",
)


@pytest.mark.asyncio
async def test_valid_token_verifies_and_returns_claims():
    verifier = JWKSVerifier(JWKS_URL)
    claims = await verifier.verify(TOKEN)
    assert claims is not None, "Expected valid claims but got None"
    print(f"\nclaims: {claims}")


@pytest.mark.asyncio
async def test_claims_contain_expected_fields():
    verifier = JWKSVerifier(JWKS_URL)
    claims = await verifier.verify(TOKEN)
    assert claims is not None
    # At least one of exp / org_id / user_id should be populated
    assert any(
        v is not None for v in (claims.exp, claims.org_id, claims.user_id)
    ), f"All claims fields are None: {claims}"


@pytest.mark.asyncio
async def test_tampered_token_returns_none():
    verifier = JWKSVerifier(JWKS_URL)
    tampered = TOKEN[:-10] + "AAAAAAAAAA"
    result = await verifier.verify(tampered)
    assert result is None, "Expected None for a tampered token"


@pytest.mark.asyncio
async def test_garbage_token_returns_none():
    verifier = JWKSVerifier(JWKS_URL)
    result = await verifier.verify("not.a.jwt")
    assert result is None, "Expected None for a completely invalid token"
