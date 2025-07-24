"""
MCP HTTP Streamable Client Example using Python SDK

This example demonstrates how to create an MCP client that connects to a server
using the Streamable HTTP transport protocol.
"""

import asyncio

from mcp import ClientSession, types
from mcp.client.streamable_http import streamablehttp_client
from typer import Typer, Option
from rich import print as pp
from typing import Annotated, Optional

app = Typer(
    no_args_is_help=True,
    name="mcp-client",
    help="Run simple mcp client",
    context_settings=dict(help_option_names=["-h", "--help"]),
)


# Example usage and demonstration
async def cli(url, token):
    async with streamablehttp_client(
        url=url, headers={"Authorization": f"Bearer {token}"}
    ) as (read_stream, write_stream, gid):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            for t in await session.list_tools():
                pp(t)
            pp(await session.call_tool("RunSqlQuery", {"s": "SELECT 1"}))


@app.command()
def main(
    token: Annotated[Optional[str], Option(help="The authorization token to use")],
    url: Annotated[
        Optional[str], Option(help="The URL of the MCP server")
    ] = "http://127.0.0.1:8000/mcp",
):
    asyncio.run(cli(url, token))


if __name__ == "__main__":
    app()
