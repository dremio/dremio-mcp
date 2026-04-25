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
import asyncio
import contextlib
import logging
import os
import sys
import threading
import time
from enum import StrEnum, auto
from functools import reduce, wraps
from json import dump as jdump
from json import load
from operator import ior
from pathlib import Path
from shutil import which
from typing import Annotated, Any, Dict, List, Optional, Tuple, Union

import jwt
import uvicorn
from click import Choice
from mcp.cli.claude import get_claude_config_path
from mcp.server.auth.json_response import PydanticJSONResponse
from mcp.server.auth.middleware.auth_context import AuthContextMiddleware
from mcp.server.auth.middleware.bearer_auth import BearerAuthBackend
from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.prompts import Prompt
from mcp.server.fastmcp.resources import FunctionResource
from mcp.types import ToolAnnotations
from pydantic import AnyHttpUrl
from pydantic.networks import AnyUrl
from rich import console, table
from rich import print as pp
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.responses import Response as StarletteResponse
from typer import Argument, BadParameter, Option, Typer
from yaml import dump

from dremioai import log
from dremioai.api.oauth2 import get_oauth2_tokens
from dremioai.api.oauth_metadata import OAuthMetadataRFC8414
from dremioai.config import settings
from dremioai.config.feature_flags import FeatureFlagManager
from dremioai.metrics.registry import get_metrics_app
from dremioai.servers.jwks_verifier import JWKSVerifier, TokenExpiredError
from dremioai.tools import tools
from dremioai.tools.tools import ProjectIdMiddleware


class RequireAuthWithWWWAuthenticateMiddleware(BaseHTTPMiddleware):
    """
    Custom middleware that requires authentication and returns WWW-Authenticate header
    for unauthorized requests. This middleware should be placed AFTER AuthenticationMiddleware
    so that request.user is available.
    """

    logger = log.logger("RequireAuthWithWWWAuthenticateMiddleware")

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint):
        # Check if user is authenticated (request.user is available after AuthenticationMiddleware)
        if (
            not hasattr(request, "user")
            or not request.user.is_authenticated
            and request.url.path.startswith("/mcp")
        ):
            client_host = request.client.host if request.client else "unknown"
            self.logger.warning(
                "Unauthorized request rejected",
                path=request.url.path,
                client=client_host,
                project_id=ProjectIdMiddleware.get_project_id(),
                endpoint=str(settings.instance().dremio.uri),
            )
            # Return 401 with WWW-Authenticate header
            return StarletteResponse(
                content="Unauthorized",
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )

        # User is authenticated, proceed with the request
        return await call_next(request)


class Transports(StrEnum):
    stdio = auto()
    streamable_http = "streamable-http"


class FastMCPServerWithAuthToken(FastMCP):
    class DelegatingTokenVerifier(TokenVerifier):
        logger = log.logger("DelegatingTokenVerifier")

        def __init__(self):
            self._jwks_verifier = None
            dremio = settings.instance().dremio
            if jwks_uri := dremio.get("jwks_uri"):
                lifespan = dremio.get("jwks_cache_lifespan") or 3600
                self._jwks_verifier = JWKSVerifier(jwks_uri, lifespan=lifespan)

        @staticmethod
        def extract_jwt_aud(token: str) -> str | None:
            """Extract aud from a JWT without signature verification.

            Used only when ``jwks_uri`` is not configured but
            ``extract_org_id_from_jwt`` is enabled.
            """
            try:
                claims = jwt.decode(token, options={"verify_signature": False})
                aud = claims.get("aud")
                return aud[0] if isinstance(aud, list) else aud
            except:
                FastMCPServerWithAuthToken.DelegatingTokenVerifier.logger.exception(
                    f"Failed to extract org_id from JWT: token={len(token)} bytes"
                )
                return None

        async def verify_token(self, token: str) -> AccessToken | None:
            if not token:
                self.logger.info("Token not provided")
                return None

            expires_at = org_id = user_id = None
            if isinstance(self._jwks_verifier, JWKSVerifier):
                try:
                    verified = await self._jwks_verifier.verify(token)
                except TokenExpiredError:
                    self.logger.warning(
                        "Token rejected — JWT has expired",
                        project_id=ProjectIdMiddleware.get_project_id(),
                    )
                    return None
                if verified:
                    buffer = settings.instance().dremio.get(
                        "jwks_token_expiry_buffer_secs"
                    )
                    # Subtract the buffer so BearerAuthBackend's
                    # `if auth_info.expires_at and expires_at < time.time()` guard
                    # fires this many seconds before Auth0 considers the token expired,
                    # giving the client's OAuth refresh flow a clean window.
                    expires_at = (
                        (verified.exp - buffer) if verified.exp is not None else None
                    )
                    org_id = verified.org_id
                    user_id = verified.user_id
                    # The actual expiry guard runs in BearerAuthBackend, but we
                    # return None early here so we can log the rejection with
                    # context (project_id, user_id) before it disappears silently.
                    if expires_at is not None and expires_at < int(time.time()):
                        self.logger.warning(
                            "Token rejected — past expiry buffer window",
                            project_id=ProjectIdMiddleware.get_project_id(),
                            user_id=user_id,
                        )
                        return None
                else:
                    self.logger.warning(
                        "JWKS verify() returned None — rejecting token to force reauth",
                        project_id=ProjectIdMiddleware.get_project_id(),
                    )
                    return None
            elif settings.instance().dremio.get("extract_org_id_from_jwt"):
                org_id = self.extract_jwt_aud(token)

            if org_id is not None and settings.instance().dremio.get(
                "extract_org_id_from_jwt"
            ):
                FeatureFlagManager.set_org_id(org_id)

            return AccessToken(
                token=token,
                client_id=user_id or "unknown",
                scopes=["read"],
                expires_at=expires_at,
            )

    def streamable_http_app(self):
        if self._mock_token_verifier is not None:
            token_verifier = self._mock_token_verifier
        else:
            token_verifier = FastMCPServerWithAuthToken.DelegatingTokenVerifier()
        app = super().streamable_http_app()
        app.add_middleware(RequireAuthWithWWWAuthenticateMiddleware)
        app.add_middleware(AuthContextMiddleware)
        app.add_middleware(
            AuthenticationMiddleware, backend=BearerAuthBackend(token_verifier)
        )
        # Add middleware in reverse order (last added = first executed)
        if self.support_project_id_endpoints:
            # this means, dynamically allow endpoints
            # like ../mcp/{project_id}/..  and extract that project id as
            # context var
            app.add_middleware(ProjectIdMiddleware)

        # Metrics are now served on a separate port, not mounted here
        return app

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.support_project_id_endpoints = False
        self._mock_token_verifier = None


def _make_mock_invoke(tool_class_name: str, original_doc: str):
    """Create a mock invoke function that returns a canned response."""

    async def mock_invoke(**kwargs):
        return {"mock": True, "tool": tool_class_name, "result": []}

    mock_invoke.__doc__ = original_doc
    return mock_invoke


def make_logged_invoke(tool_name: str, fn):
    _log = log.logger("tool_invoke")

    @wraps(fn)
    async def _wrapper(*args, **kwargs):
        try:
            return await fn(*args, **kwargs)
        except Exception as exc:
            _log.warning(
                "Tool invocation raised an exception",
                tool=tool_name,
                error=str(exc),
            )
            raise

    return _wrapper


def init(
    mode: Union[tools.ToolType, List[tools.ToolType]] = None,
    transport: Transports = Transports.stdio,
    port: int = None,
    host: str = "127.0.0.1",
    support_project_id_endpoints: bool = False,
    mock: bool = False,
    mock_token_expiry: int = 3600,
    mock_refresh_token_expiry: int = 86400,
) -> FastMCP:
    mcp_cls = FastMCP if transport == Transports.stdio else FastMCPServerWithAuthToken
    log.logger("init").info(
        f"Initializing MCP server with mode={mode}, mock={mock}, class={mcp_cls.__name__}"
    )
    opts = {"log_level": "DEBUG", "debug": True, "lifespan": _server_lifespan}
    if port is not None:
        opts["port"] = port
    if host is not None:
        opts["host"] = host

    mcp = mcp_cls("Dremio", **opts)
    if transport == Transports.streamable_http and support_project_id_endpoints:
        mcp.support_project_id_endpoints = support_project_id_endpoints

    # In mock mode, set up mock OAuth issuer and token verifier
    if mock:
        from dremioai.servers.mock_auth import (
            MockJWTIssuer,
            MockTokenVerifier,
            register_mock_routes,
        )

        issuer_url = f"http://{host}:{port}" if port else f"http://{host}"
        mock_issuer = MockJWTIssuer(
            issuer_url=issuer_url,
            default_expiry=mock_token_expiry,
            refresh_token_expiry=mock_refresh_token_expiry,
        )
        if isinstance(mcp, FastMCPServerWithAuthToken):
            mcp._mock_token_verifier = MockTokenVerifier(mock_issuer)
        register_mock_routes(mcp, mock_issuer)

    mode = reduce(ior, mode) if mode is not None else None
    allow_dml = settings.instance().dremio.get("allow_dml") if not mock else False
    for tool in tools.get_tools(For=mode):
        tool_instance = tool()
        is_sql_tool = tool is tools.RunSqlQuery
        if mock:
            invoke_fn = _make_mock_invoke(
                tool.__name__, tool_instance.invoke.__doc__
            )
        else:
            invoke_fn = tool_instance.invoke
        mcp.add_tool(
            invoke_fn if mock else make_logged_invoke(tool.__name__, tool_instance.invoke),
            name=tool.__name__,
            description=tool_instance.invoke.__doc__,
            annotations=ToolAnnotations(
                readOnlyHint=not (is_sql_tool and allow_dml),
                destructiveHint=bool(is_sql_tool and allow_dml),
            ),
        )

    for resource in tools.get_resources(For=mode):
        resource_instance = resource()
        mcp.add_resource(
            FunctionResource(
                uri=AnyUrl(resource_instance.resource_path),
                name=resource.__name__,
                description=resource.__doc__,
                mime_type="application/json",
                fn=resource_instance.invoke,
            )
        )
    # if mode is None or (mode & tools.ToolType.FOR_SELF) != 0:
    mcp.add_prompt(
        Prompt.from_function(tools.system_prompt, "System Prompt", "System Prompt")
    )

    if not mock:

        @mcp.custom_route("/.well-known/oauth-authorization-server", methods=["GET"])
        @mcp.custom_route(
            "/mcp/{project_id}/.well-known/oauth-authorization-server", methods=["GET"]
        )
        async def authorization_server_metadata(request: Request) -> Response:
            if issuer := settings.instance().dremio.auth_issuer_uri:
                auth, tok, reg = settings.instance().dremio.auth_endpoints
                md = OAuthMetadataRFC8414(
                    issuer=AnyHttpUrl(issuer),
                    authorization_endpoint=auth,
                    token_endpoint=tok,
                    registration_endpoint=AnyHttpUrl(reg),
                    scopes_supported=["dremio.all", "offline_access"],
                    response_types_supported=["code"],
                    grant_types_supported=["authorization_code", "refresh_token"],
                    code_challenge_methods_supported=["S256"],
                    token_endpoint_auth_methods_supported=["none"],
                )
                return PydanticJSONResponse(md)
            return Response(status_code=404)

    @mcp.custom_route("/healthz", methods=["GET"])
    async def health_check(_request: Request) -> Response:
        """Kubernetes-style health check endpoint"""
        return Response(content="OK", status_code=200, media_type="text/plain")

    return mcp


app = None


def create_metrics_server(host: str, port: int, log_level: str) -> uvicorn.Server:
    # Create a separate uvicorn server for Prometheus metrics.
    metrics_app = get_metrics_app()
    config = uvicorn.Config(
        app=metrics_app,
        host=host,
        port=port,
        log_level=log_level.lower(),
        access_log=False,
    )
    server = uvicorn.Server(config)

    log.logger("metrics_server").info(
        f"Created metrics server config for {host}:{port}"
    )
    return server


_LOG_LEVEL_REFRESH_INTERVAL = 60  # seconds


async def _log_level_refresh_loop():
    """Periodically sync log level from LD flags."""
    _log = log.logger("log_level_refresh")
    while True:
        await asyncio.sleep(_LOG_LEVEL_REFRESH_INTERVAL)
        try:
            s = settings.instance()
            if s is None:
                continue
            level_name = s.get("log_level")
            level = getattr(logging, level_name.upper(), None)
            if level is not None and level != log.level():
                _log.info(f"Updating log level to {level_name}")
                log.set_level(level)
        except Exception as e:
            _log.debug(f"Log level refresh failed: {e}")


@contextlib.asynccontextmanager
async def _server_lifespan(app: FastMCP):
    """Lifespan context manager that runs background tasks alongside the server."""
    task = asyncio.create_task(_log_level_refresh_loop())
    try:
        yield
    finally:
        task.cancel()


def run_with_metrics_server(
    app: FastMCP, transport: Transports, metrics_server: uvicorn.Server | None = None
):
    """
    Run the main MCP server alongside the metrics server using asyncio.

    Args:
        app: The FastMCP server instance
        transport: Transport type
        metrics_server: Optional metrics server to run concurrently
    """
    if metrics_server:
        # Start metrics server as background task for all transports
        log.logger("server_startup").info("Starting metrics server as background task")

        threading.Thread(
            target=lambda: asyncio.run(metrics_server.serve()), daemon=True
        ).start()

    app.run(transport=transport.value)


def _mode() -> List[str]:
    return [tt.name for tt in tools.ToolType]


ty = Typer(context_settings=dict(help_option_names=["-h", "--help"]))


@ty.command(name="run", help="Run the DremioAI MCP server")
def main(
    config_file: Annotated[
        Optional[Path],
        Option("-c", "--cfg", help="The config yaml for various options"),
    ] = None,
    log_to_file: Annotated[Optional[bool], Option(help="Log to file")] = True,
    enable_json_logging: Annotated[
        Optional[bool], Option(help="Enable JSON logs")
    ] = False,
    enable_streaming_http: Annotated[
        Optional[bool], Option(help="Run MCP as streaming HTTP")
    ] = False,
    log_level: Annotated[
        Optional[str],
        Option(
            help="The log level", click_type=Choice(list(logging._nameToLevel.keys()))
        ),
    ] = "INFO",
    port: Annotated[Optional[int], Option(help="The port to listen on")] = None,
    host: Annotated[
        Optional[str],
        Option(help="Where uvicorn listens for requests"),
    ] = "127.0.0.1",
    mock: Annotated[
        Optional[bool],
        Option(help="Run in mock mode for client sanity testing"),
    ] = False,
    mock_token_expiry: Annotated[
        Optional[int],
        Option(help="Mock mode: access token expiry in seconds"),
    ] = 3600,
    mock_refresh_token_expiry: Annotated[
        Optional[int],
        Option(help="Mock mode: refresh token expiry in seconds"),
    ] = 86400,
):
    log.configure(enable_json_logging=enable_json_logging, to_file=log_to_file)
    log.set_level(log_level)

    if mock:
        transport = Transports.streamable_http
        # In mock mode, create a minimal settings instance — no Dremio config needed
        settings._settings.set(
            settings.Settings.model_validate(
                {
                    "dremio": {
                        "uri": "http://localhost:9047",
                        "pat": "mock-pat",
                    }
                }
            )
        )
    else:
        if enable_streaming_http:
            transport = Transports.streamable_http
        else:
            transport = Transports.stdio
        cfg = settings.configure(config_file).get()
        dremio = settings.instance().dremio
        if (
            dremio.oauth_supported
            and dremio.oauth_configured
            and (dremio.oauth2.has_expired or dremio.pat is None)
        ):
            oauth = get_oauth2_tokens()
            oauth.update_settings()

    app = init(
        mode=settings.instance().tools.server_mode,
        transport=transport,
        port=port,
        host=host,
        support_project_id_endpoints=True,
        mock=mock,
        mock_token_expiry=mock_token_expiry,
        mock_refresh_token_expiry=mock_refresh_token_expiry,
    )

    # Create metrics server based on configuration
    metrics_server = None
    if (
        not mock
        and settings.instance().dremio.prometheus_metrics_enabled
        and settings.instance().dremio.prometheus_metrics_port is not None
    ):
        metrics_server = create_metrics_server(
            host=host,
            port=settings.instance().dremio.prometheus_metrics_port,
            log_level=log_level,
        )

    # Run the servers
    run_with_metrics_server(app, transport, metrics_server)


tc = Typer(
    context_settings=dict(help_option_names=["-h", "--help"]),
    name="config",
    help="Configuration management",
)


class ConfigTypes(StrEnum):
    dremioai = auto()
    claude = auto()


def get_claude_config_path() -> Path:
    # copy of the function from mcp sdk, but returns the path whether or not
    # it exists
    dir = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"), "Claude")
    match sys.platform:
        case "win32":
            dir = Path(Path.home(), "AppData", "Roaming", "Claude")
        case "darwin":
            dir = Path(Path.home(), "Library", "Application Support", "Claude")
    return dir / "claude_desktop_config.json"


@tc.command("list", help="Show default configuration, if it exists")
def show_default_config(
    show_filename: Annotated[
        bool, Option(help="Show the filename for default config file")
    ] = False,
    type: Annotated[
        Optional[ConfigTypes],
        Option(help="The type of configuration to show", show_default=True),
    ] = ConfigTypes.dremioai,
):

    match type:
        case ConfigTypes.dremioai:
            dc = settings.default_config()
            pp(f"Default config file: {dc!s} (exists = {dc.exists()!s})")
            if not show_filename:
                settings.configure(dc)
                pp(
                    dump(
                        settings.instance().model_dump(
                            exclude_none=True,
                            mode="json",
                            exclude_unset=True,
                            by_alias=True,
                        )
                    )
                )
            pp(f"Default log file: {log.get_log_file()!s}")
        case ConfigTypes.claude:
            cc = get_claude_config_path()
            pp(f"Default config file: '{cc!s}' (exists = {cc.exists()!s})")
            if not show_filename:
                jdump(load(cc.open()), sys.stdout, indent=2)


cc = Typer(
    context_settings=dict(help_option_names=["-h", "--help"]),
    name="create",
    help="Create DremioAI or LLM configuration files",
)
tc.add_typer(cc)


def create_default_mcpserver_config() -> Dict[str, Any]:
    if (uv := which("uv")) is not None:
        uv = Path(uv).resolve()
        dir = str(Path(os.getcwd()).resolve())
        return {
            "command": str(uv),
            "args": ["run", "--directory", dir, "dremio-mcp-server", "run"],
        }
    else:
        raise FileNotFoundError("uv command not found. Please install uv")


def create_default_config_helper(dry_run: bool):
    cc = get_claude_config_path()
    dcmp = {"Dremio": create_default_mcpserver_config()}
    c = load(cc.open()) if cc.exists() else {"mcpServers": {}}
    c.setdefault("mcpServers", {}).update(dcmp)
    if dry_run:
        pp(c)
        return

    if not cc.exists():
        cc.parent.mkdir(parents=True, exist_ok=True)

    with cc.open("w") as f:
        jdump(c, f)
        pp(f"Created default config file: {cc!s}")


@cc.command("claude", help="Create a default configuration file for Claude")
def create_default_config(
    dry_run: Annotated[
        bool, Option(help="Dry run, do not overwrite the config file. Just print it")
    ] = False,
):
    create_default_config_helper(dry_run)


@cc.command("dremioai", help="Create a default configuration file")
def create_default_config(
    uri: Annotated[
        str,
        Option(
            help=f"The Dremio URL or shorthand for Dremio Cloud regions ({','.join(settings.DremioCloudUri)})"
        ),
    ],
    pat: Annotated[
        str,
        Option(
            help="The Dremio PAT. If it starts with @ then treat the rest is treated as a filename"
        ),
    ],
    project_id: Annotated[
        Optional[str],
        Option(help="The Dremio project id, only if connecting to Dremio Cloud"),
    ] = None,
    mode: Annotated[
        Optional[List[str]],
        Option("-m", "--mode", help="MCP server mode", click_type=Choice(_mode())),
    ] = [tools.ToolType.FOR_DATA_PATTERNS.name],
    enable_search: Annotated[bool, Option(help="Enable semantic search")] = False,
    oauth_client_id: Annotated[
        Optional[str],
        Option(help="The ID of OAuth application, for OAuth2 logon support"),
    ] = None,
    dry_run: Annotated[
        bool, Option(help="Dry run, do not overwrite the config file. Just print it")
    ] = False,
):
    mode = "|".join([tools.ToolType[m.upper()].name for m in mode])
    dremio = settings.Dremio.model_validate(
        {
            "uri": uri,
            "pat": pat,
            "project_id": project_id,
            "enable_search": enable_search,
            "oauth": (
                settings.OAuth2.model_validate({"client_id": oauth_client_id})
                if oauth_client_id
                else None
            ),
        }
    )
    ts = settings.Tools.model_validate({"server_mode": mode})
    settings.configure(settings.default_config(), force=True)
    settings.instance().dremio = dremio
    settings.instance().tools = ts
    if (d := settings.write_settings(dry_run=dry_run)) is not None and dry_run:
        pp(d)
    elif not dry_run:
        pp(f"Created default config file: {settings.default_config()!s}")


# --------------------------------------------------------------------------------
# testing support

tl = Typer(
    context_settings=dict(help_option_names=["-h", "--help"]),
    name="tools",
    help="Support for testing tools directly",
)

# tl.add_typer(call)


@tl.command(
    name="list",
    help="List the available tools",
    context_settings=dict(help_option_names=["-h", "--help"]),
)
def tools_list(
    mode: Annotated[
        Optional[List[str]],
        Option("-m", "--mode", help="MCP server mode", click_type=Choice(_mode())),
    ] = [tools.ToolType.FOR_SELF.name],
):
    mode = reduce(ior, [tools.ToolType[m.upper()] for m in mode])
    tab = table.Table(
        table.Column("Tool", justify="left", style="cyan"),
        "Description",
        "For",
        title="Tools list",
        show_lines=True,
    )

    for tool in tools.get_tools(For=mode):
        For = tools.get_for(tool)
        try:
            tab.add_row(tool.__name__, tool.invoke.__doc__.strip(), For.name)
        except Exception as e:
            tab.add_row(tool.__name__, "No Description", For.name)
    console.Console().print(tab)


@tl.command(
    name="invoke",
    help="Execute an available tools",
    context_settings=dict(help_option_names=["-h", "--help"]),
)
def tools_exec(
    tool: Annotated[str, Option("-t", "--tool", help="The tool to execute")],
    config_file: Annotated[
        Optional[Path],
        Option("-c", "--cfg", help="The config yaml for various options"),
    ] = None,
    args: Annotated[
        Optional[List[str]],
        Argument(help="The arguments to pass to the tool (arg=value ...)"),
    ] = None,
):
    def _to_kw(arg: str) -> Tuple[str, str]:
        if "=" not in arg:
            raise BadParameter(f"Argument {arg} is not in the form arg=value")
        return tuple(arg.split("=", 1))

    settings.configure(config_file)

    if args is None:
        args = {}
    elif type(args) == str:
        args = [args]
    args = dict(map(_to_kw, args))
    for_all = reduce(ior, tools.ToolType.__members__.values())
    all_tools = {t.__name__: t for t in tools.get_tools(for_all)}

    if selected := all_tools.get(tool):
        tool_instance = selected()  # get arguments from settings
        result = asyncio.run(tool_instance.invoke(**args))
        pp(result)
    else:
        raise BadParameter(f"Tool {tool} not found")


ty.add_typer(tl)
ty.add_typer(tc)


def cli():
    ty()


if __name__ == "__main__":
    cli()
