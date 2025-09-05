"""
MCP HTTP Streamable Client Example using Python SDK

This example demonstrates how to create an MCP client that connects to a server
using the Streamable HTTP transport protocol.
"""

import asyncio
import functools
import random
import time
from contextlib import asynccontextmanager
from typing import Annotated, Optional, AsyncGenerator, Callable, Any
from urllib.parse import urlparse

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.shared.auth import OAuthMetadata
from typer import Typer, Option
from rich import print as pp
import requests

from dremioai import log
from dremioai.api.oauth2 import get_oauth2_tokens, OAuth2Redirect


def async_command(func: Callable) -> Callable:
    """Decorator to run async functions in Typer commands."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return asyncio.run(func(*args, **kwargs))

    return wrapper


app = Typer(
    no_args_is_help=True,
    name="mcp-client",
    help="Run simple mcp client",
    context_settings=dict(help_option_names=["-h", "--help"]),
)

auth = Typer(
    no_args_is_help=True,
    name="auth",
    help="Auth related sub commands",
    context_settings=dict(help_option_names=["-h", "--help"]),
)


def get_oauth_config(url: str) -> OAuthMetadata:
    u = urlparse(url)
    u = u._replace(path="/.well-known/oauth-authorization-server")
    log.logger("auth").info(f"Checking auth for {u.geturl()}")
    r = requests.get(u.geturl())
    if r.status_code != 200:
        pp(f"Cannot get oauth config: {u.geturl()}")
        r.raise_for_status()
    return OAuthMetadata.model_validate(r.json())


@auth.command("list")
def list_auth(
    url: Annotated[
        Optional[str], Option(help="The URL of the MCP server")
    ] = "http://127.0.0.1:8000/mcp",
):
    pp(get_oauth_config(url))


@auth.command("check")
def check_auth(
    client_id: Annotated[str, Option(help="The client id to check")],
    url: Annotated[
        Optional[str], Option(help="The URL of the MCP server")
    ] = "http://127.0.0.1:8000/mcp",
) -> OAuth2Redirect:
    md = get_oauth_config(url)
    oauth = get_oauth2_tokens(
        client_id, str(md.authorization_endpoint), str(md.token_endpoint)
    )
    pp(oauth.access_token)
    return oauth


cli = Typer(
    no_args_is_help=True,
    name="cli",
    help="MCP client session related sub commands",
    context_settings=dict(help_option_names=["-h", "--help"]),
)


@asynccontextmanager
async def mcp_client_session(
    url: str, token: Optional[str] = None
) -> AsyncGenerator[ClientSession, None]:
    headers = {"Authorization": f"Bearer {token}"} if token is not None else None
    async with streamablehttp_client(url=url, headers=headers) as (
        read_stream,
        write_stream,
        gid,
    ):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            yield session


@cli.command("list-tools")
@async_command
async def list_tools(
    url: Annotated[
        Optional[str], Option(help="The URL of the MCP server")
    ] = "http://127.0.0.1:8000/mcp",
    token: Annotated[
        Optional[str], Option(help="The authorization token to use")
    ] = None,
):
    async with mcp_client_session(url, token) as session:
        tools = await session.list_tools()
        for tool in tools:
            pp(tool)


@app.command("test", help="Run a quick smoketest for a deployed MCP server")
@async_command
async def run_test(
    client_id: Annotated[str, Option(help="The OAuth client id")],
    url: Annotated[
        Optional[str], Option(help="The URL of the MCP server")
    ] = "http://127.0.0.1:8000/mcp",
):
    pp("Checking auth..", end=" ")
    a = check_auth(client_id, url)
    pp("[green]OK[/green]\nConnecting to server..")
    async with mcp_client_session(url, a.access_token) as session:
        tools = await session.list_tools()
        pp([i.name for i in tools.tools])

        pp("[green]OK[/green]\nCalling tool..")
        n = int(time.time())
        query = f"SELECT {n} as n"
        result = await session.call_tool("RunSqlQuery", {"s": query})
        result = result.structuredContent["result"]["result"]
        pp(result)

        query2 = f"""
        SELECT query
        FROM   sys.project.jobs_recent
        WHERE query_type = 'REST' and  submitted_ts > CURRENT_TIMESTAMP() - INTERVAL '1' minute
        and query like '/* dremioai: submitter=RunS%' and query like '%SELECT {n} as n';
        """
        result = await session.call_tool("RunSqlQuery", {"s": query2})
        result = result.structuredContent["result"]["result"]
        pp(result)

        if len(result) != 1:
            pp("[red]FAIL[/red]")
    pp("[green]OK[/green]")


# Add the CLI subcommand to the main app
app.add_typer(cli)
app.add_typer(auth)


if __name__ == "__main__":
    log.configure(enable_json_logging=False, to_file=False)
    app()
