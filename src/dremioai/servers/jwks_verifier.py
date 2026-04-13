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
JWKS-based JWT token verifier for the MCP server.

Verifies JWT signatures and extracts claims (``exp``, ``aud``, etc.)
so that expired tokens are rejected with HTTP 401 *before* any tool
execution, triggering the MCP client's OAuth token refresh flow.

The JWKS keyset is cached and refreshed automatically on cache miss
or verification error (e.g. key rotation).
"""

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import jwt as pyjwt
from jwt import ExpiredSignatureError, PyJWKClient, PyJWKClientError
from jwt.exceptions import MissingCryptographyError

from dremioai import log

logger = log.logger(__name__)

_DEFAULT_JWKS_CACHE_LIFESPAN = 3600  # 1 hour in seconds


class TokenExpiredError(Exception):
    """Raised by JWKSVerifier.verify() when the JWT token has expired.

    Callers should catch this to return an unauthenticated response (HTTP 401)
    rather than forwarding the expired token to downstream services.
    """

    pass


@dataclass
class VerifiedClaims:
    """Subset of JWT claims extracted after signature verification."""

    exp: Optional[int] = None
    org_id: Optional[str] = None
    user_id: Optional[str] = None


class JWKSVerifier:
    """Verify JWT tokens using a remote JWKS endpoint.

    Uses ``PyJWKClient`` with built-in caching (``lifespan`` seconds).
    On verification failure due to a key-related error the cache is
    invalidated and verification is retried once with fresh keys.
    """

    def __init__(self, jwks_uri: str, lifespan: int = _DEFAULT_JWKS_CACHE_LIFESPAN):
        self._jwks_uri = jwks_uri
        self._lifespan = lifespan
        self._client = PyJWKClient(
            jwks_uri,
            cache_jwk_set=True,
            lifespan=lifespan,
        )
        logger.info(f"JWKS verifier initialised with uri={jwks_uri}, cache={lifespan}s")

    async def verify(self, token: str) -> Optional[VerifiedClaims]:
        """Verify *token* and return its claims.

        Returns ``None`` when verification cannot be performed — the
        token will still be forwarded to Dremio for real validation on
        the tool call.

        Returns ``VerifiedClaims(exp=0)`` for expired tokens so that
        ``BearerAuthBackend`` rejects them with HTTP 401.

        PyJWKClient.get_signing_key_from_jwt() makes blocking HTTP calls
        to fetch JWKS on cache miss, so we run it in a thread pool to
        avoid blocking the event loop.
        """
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, self._verify, token)
        except (pyjwt.InvalidKeyError, PyJWKClientError, KeyError):
            logger.info("JWKS cache miss or fetch error, refreshing and retrying")
            try:
                self._client = PyJWKClient(
                    self._jwks_uri,
                    cache_jwk_set=True,
                    lifespan=self._lifespan,
                )
                return await loop.run_in_executor(None, self._verify, token)
            except Exception:
                logger.warning(
                    "JWKS verification failed after cache refresh",
                    jwks_uri=self._jwks_uri,
                    exc_info=True,
                )
                return None
        except ExpiredSignatureError:
            logger.warning("Token expired", jwks_uri=self._jwks_uri)
            raise TokenExpiredError()
        except MissingCryptographyError:
            logger.error(
                "JWT verification requires cryptography support (install PyJWT[crypto] / cryptography)"
            )
            return None
        except Exception:
            logger.debug("JWT verification failed, skipping enforcement", exc_info=True)
            return None

    def _verify(self, token: str) -> VerifiedClaims:
        signing_key = self._client.get_signing_key_from_jwt(token)
        claims = pyjwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            options={
                "verify_aud": False,
                "verify_iss": False,
                "verify_exp": True,
            },
        )

        def flatten_get(claims: Dict[str, Any], key: str) -> Any:
            if (v := claims.get(key)) is not None and isinstance(v, List):
                return v[0] if v else None
            else:
                return v

        return VerifiedClaims(
            exp=flatten_get(claims, "exp"),
            org_id=flatten_get(claims, "aud"),
            user_id=flatten_get(claims, "sub"),
        )
