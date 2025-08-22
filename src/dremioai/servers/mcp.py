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

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.prompts import Prompt
from mcp.server.fastmcp.resources import FunctionResource
from mcp.cli.claude import get_claude_config_path
from pydantic.networks import AnyUrl
from dremioai.tools import tools
import os
from typing import List, Union, Annotated, Optional, Tuple, Dict, Any
from functools import reduce
from operator import ior
from pathlib import Path
from dremioai import log
from typer import Typer, Option, Argument, BadParameter
from rich import console, table, print as pp
from click import Choice
import logging
from dremioai.config import settings
from dremioai.api.oauth2 import get_oauth2_tokens
from enum import StrEnum, auto
from json import load, dump as jdump
from shutil import which
import asyncio
from yaml import dump
import sys

from mcp.server.auth.middleware.auth_context import (
    AuthContextMiddleware,
)
from mcp.server.auth.middleware.bearer_auth import BearerAuthBackend
from mcp.server.auth.provider import AccessToken, TokenVerifier
from starlette.middleware.authentication import AuthenticationMiddleware

from dremioai.tools.tools import ProjectIdMiddleware


class Transports(StrEnum):
    stdio = auto()
    streamable_http = "streamable-http"


class FastMCPServerWithAuthToken(FastMCP):
    class DelegatingTokenVerifier(TokenVerifier):
        async def verify_token(self, token: str) -> AccessToken | None:
            log.logger("verify_token").info(f"Verifying token: {token}")
            return (
                AccessToken(
                    token=token,  # Include the token itself
                    client_id="unused-client",
                    scopes=["read"],
                )
                if token
                else None
            )

    def streamable_http_app(self):
        token_verifier = FastMCPServerWithAuthToken.DelegatingTokenVerifier()
        app = super().streamable_http_app()
        app.add_middleware(AuthContextMiddleware)
        app.add_middleware(
            AuthenticationMiddleware, backend=BearerAuthBackend(token_verifier)
        )
        if self.support_project_id_endpoints:
            # this means, dynamically allow endpoints
            # like ../mcp/{project_id}/..  and extract that project id as
            # context var
            app.add_middleware(ProjectIdMiddleware)
        return app

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.support_project_id_endpoints = False


def init(
    mode: Union[tools.ToolType, List[tools.ToolType]] = None,
    transport: Transports = Transports.stdio,
    port: int = None,
    support_project_id_endpoints: bool = False,
) -> FastMCP:
    mcp_cls = FastMCP if transport == Transports.stdio else FastMCPServerWithAuthToken
    log.logger("init").info(
        f"Initializing MCP server with mode={mode}, class={mcp_cls.__name__}"
    )
    opts = {"log_level": "DEBUG"}
    if port is not None:
        opts["port"] = port
    mcp = mcp_cls("Dremio", **opts)
    if transport == Transports.streamable_http and support_project_id_endpoints:
        mcp.support_project_id_endpoints = support_project_id_endpoints
    mode = reduce(ior, mode) if mode is not None else None
    for tool in tools.get_tools(For=mode):
        tool_instance = tool()
        mcp.add_tool(
            tool_instance.invoke,
            name=tool.__name__,
            description=tool_instance.invoke.__doc__,
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
    return mcp


app = None


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
):
    log.configure(enable_json_logging=enable_json_logging, to_file=log_to_file)
    log.set_level(log_level)
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
        mode=cfg.tools.server_mode,
        transport=transport,
        port=port,
    )
    app.run(transport=transport.value)


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
            help=f"The Dremio URL or shorthand for Dremio Cloud regions ({ ','.join(settings.DremioCloudUri)})"
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
