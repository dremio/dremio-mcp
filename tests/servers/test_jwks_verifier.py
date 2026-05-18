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
import structlog
from unittest.mock import patch, MagicMock, AsyncMock

import httpx
import jwt as pyjwt
from jwt import PyJWKClient, PyJWKClientError, ExpiredSignatureError
from mcp.server.lowlevel.server import request_ctx
from mcp.server.streamable_http import (
    MCP_PROTOCOL_VERSION_HEADER,
    MCP_SESSION_ID_HEADER,
)
from mcp.shared.context import RequestContext
from starlette.requests import Request
from starlette.responses import Response

from dremioai import log
from dremioai.config import settings
from dremioai.servers.jwks_verifier import JWKSVerifier, VerifiedClaims, TokenExpiredError
from dremioai.servers.mcp import (
    FastMCPServerWithAuthToken,
    MCPTransportLoggingMiddleware,
    Transports,
    init,
    make_logged_invoke,
    RequireAuthWithWWWAuthenticateMiddleware,
)

JWKS_DECODE = "dremioai.servers.jwks_verifier.pyjwt.decode"


@pytest.fixture
def verifier():
    with patch.object(PyJWKClient, "__init__", return_value=None):
        return JWKSVerifier("https://example.com/.well-known/jwks.json")


@pytest.fixture(autouse=True)
def configure_logging():
    structlog.reset_defaults()
    log._level = None
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    log.configure(enable_json_logging=False, to_file=False)
    yield
    structlog.reset_defaults()
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)


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


class TestStreamableHttpLogging:
    @pytest.mark.asyncio
    async def test_run_streamable_http_disables_uvicorn_access_logs(self):
        server = FastMCPServerWithAuthToken(
            "test-server",
            host="127.0.0.1",
            port=8765,
            log_level="INFO",
            stateless_http=True,
        )
        server._mock_token_verifier = MagicMock()

        mock_uvicorn_server = MagicMock()
        mock_uvicorn_server.serve = AsyncMock()

        with patch("dremioai.servers.mcp.uvicorn.Config") as mock_config, patch(
            "dremioai.servers.mcp.uvicorn.Server", return_value=mock_uvicorn_server
        ):
            await server.run_streamable_http_async()

        assert mock_config.call_args.kwargs["access_log"] is False
        mock_uvicorn_server.serve.assert_awaited_once()


class TestDispatchWarning:
    """Tests for RequireAuthWithWWWAuthenticateMiddleware.dispatch() WARNING logging."""

    @pytest.mark.asyncio
    async def test_dispatch_logs_warning_on_401(self, caplog):
        settings.set_base_settings(settings.Settings())
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


class TestMCPTransportLoggingMiddleware:
    @pytest.mark.asyncio
    async def test_logs_transport_error_context(self, caplog):
        async def app(scope, receive, send):
            response = Response(
                "Bad Request: Unsupported protocol version: 2025-11-25",
                status_code=400,
                headers={MCP_SESSION_ID_HEADER: "response-session"},
            )
            await response(scope, receive, send)

        scope = {
            "type": "http",
            "method": "POST",
            "path": "/mcp/project-123",
            "headers": [
                (MCP_SESSION_ID_HEADER.encode(), b"request-session"),
                (MCP_PROTOCOL_VERSION_HEADER.encode(), b"2025-11-25"),
                (b"accept", b"application/json, text/event-stream"),
                (b"content-type", b"application/json"),
                (b"authorization", b"Bearer secret-token"),
            ],
            "client": ("203.0.113.10", 4321),
            "server": ("testserver", 80),
            "scheme": "http",
            "query_string": b"",
        }

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        sent_messages = []

        async def send(message):
            sent_messages.append(message)

        middleware = MCPTransportLoggingMiddleware(app)
        with caplog.at_level(logging.WARNING):
            await middleware(scope, receive, send)

        warning_messages = [
            str(r.message) for r in caplog.records if r.levelno >= logging.WARNING
        ]
        assert any("MCP transport request failed" in msg for msg in warning_messages)
        logged = "\n".join(warning_messages)
        assert "request-session" in logged
        assert "response-session" in logged
        assert "2025-11-25" in logged
        assert "Unsupported protocol version" in logged
        assert "203.0.113.10" in logged
        assert "secret-token" not in logged

    @pytest.mark.asyncio
    async def test_logs_redirects(self, caplog):
        async def app(scope, receive, send):
            response = Response(status_code=307, headers={"location": "/mcp/new"})
            await response(scope, receive, send)

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/mcp/project-123",
            "headers": [],
            "client": ("203.0.113.10", 4321),
            "server": ("testserver", 80),
            "scheme": "http",
            "query_string": b"",
        }

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(_message):
            pass

        middleware = MCPTransportLoggingMiddleware(app)
        with caplog.at_level(logging.WARNING):
            await middleware(scope, receive, send)

        warning_messages = [
            str(r.message) for r in caplog.records if r.levelno >= logging.WARNING
        ]
        assert any(
            "MCP transport request failed" in msg and "307" in msg
            for msg in warning_messages
        )


class TestStreamableHttpInit:
    def test_streamable_http_transport_is_stateless(self):
        with patch("dremioai.servers.mcp.tools.get_tools", return_value=[]), patch(
            "dremioai.servers.mcp.tools.get_resources", return_value=[]
        ):
            mcp = init(
                mode=None,
                transport=Transports.streamable_http,
                host="127.0.0.1",
                port=8000,
                mock=True,
            )

        assert mcp.settings.stateless_http is True

    @pytest.mark.asyncio
    async def test_transport_logging_wraps_unauthorized_responses(self, caplog):
        with patch("dremioai.servers.mcp.tools.get_tools", return_value=[]), patch(
            "dremioai.servers.mcp.tools.get_resources", return_value=[]
        ):
            mcp = init(
                mode=None,
                transport=Transports.streamable_http,
                host="127.0.0.1",
                port=8000,
                mock=True,
            )

        app = mcp.streamable_http_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            with caplog.at_level(logging.INFO):
                response = await client.post("/mcp")

        assert response.status_code == 401
        messages = [str(r.message) for r in caplog.records if r.levelno >= logging.INFO]
        assert any("Unauthorized request rejected" in msg for msg in messages)
        assert any(
            "MCP transport request failed" in msg and "401" in msg for msg in messages
        )


class TestMakeLoggedInvoke:
    """Tests for make_logged_invoke WARNING logging on tool exceptions."""

    @pytest.fixture
    def info_logging(self):
        previous_level = log.level()
        log.set_level(logging.INFO)
        try:
            yield
        finally:
            log.set_level(previous_level)

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

    @pytest.mark.asyncio
    async def test_logs_info_on_success(self, caplog, info_logging):
        async def ok_fn():
            return "ok"

        wrapped = make_logged_invoke("good_tool", ok_fn)
        with caplog.at_level(logging.INFO):
            result = await wrapped()
        assert result == "ok"
        info_records = [r for r in caplog.records if r.levelno == logging.INFO]
        assert any(
            "Tool invocation completed" in str(r.message)
            and "good_tool" in str(r.message)
            and "success" in str(r.message)
            for r in info_records
        )

    @pytest.mark.asyncio
    async def test_logs_mcp_request_context(self, caplog, info_logging):
        async def ok_fn():
            return "ok"

        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/mcp/project-123",
                "headers": [
                    (MCP_SESSION_ID_HEADER.encode(), b"session-abc"),
                    (MCP_PROTOCOL_VERSION_HEADER.encode(), b"2025-06-18"),
                ],
                "client": ("192.0.2.10", 4321),
                "server": ("testserver", 80),
                "scheme": "http",
            }
        )
        token = request_ctx.set(
            RequestContext(
                request_id="rpc-123",
                meta=None,
                session=None,
                lifespan_context=None,
                request=request,
            )
        )
        try:
            wrapped = make_logged_invoke("context_tool", ok_fn)
            with caplog.at_level(logging.INFO):
                result = await wrapped()
        finally:
            request_ctx.reset(token)

        assert result == "ok"
        info_records = [r for r in caplog.records if r.levelno == logging.INFO]
        assert any(
            "session-abc" in str(r.message)
            and "rpc-123" in str(r.message)
            and "192.0.2.10" in str(r.message)
            and "context_tool" in str(r.message)
            for r in info_records
        )

    @pytest.mark.asyncio
    async def test_propagates_mcp_request_context_to_nested_logs(
        self, caplog, info_logging
    ):
        nested_logger = log.logger("nested_tool_log")

        async def ok_fn():
            nested_logger.info("Nested tool log")
            return "ok"

        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/mcp/project-123",
                "headers": [
                    (MCP_SESSION_ID_HEADER.encode(), b"session-abc"),
                    (MCP_PROTOCOL_VERSION_HEADER.encode(), b"2025-06-18"),
                ],
                "client": ("192.0.2.10", 4321),
                "server": ("testserver", 80),
                "scheme": "http",
            }
        )
        token = request_ctx.set(
            RequestContext(
                request_id="rpc-123",
                meta=None,
                session=None,
                lifespan_context=None,
                request=request,
            )
        )
        try:
            wrapped = make_logged_invoke("context_tool", ok_fn)
            with caplog.at_level(logging.INFO):
                result = await wrapped()
                nested_logger.info("Outside invocation")
        finally:
            request_ctx.reset(token)

        assert result == "ok"
        nested_records = [
            r for r in caplog.records if "Nested tool log" in str(r.message)
        ]
        assert len(nested_records) == 1
        nested_message = str(nested_records[0].message)
        assert "session-abc" in nested_message
        assert "rpc-123" in nested_message
        assert "context_tool" in nested_message
        assert "tool_invocation_id" in nested_message

        outside_records = [
            r for r in caplog.records if "Outside invocation" in str(r.message)
        ]
        assert len(outside_records) == 1
        outside_message = str(outside_records[0].message)
        assert "session-abc" not in outside_message
        assert "tool_invocation_id" not in outside_message
