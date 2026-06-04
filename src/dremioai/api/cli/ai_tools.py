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

"""CLI commands for Dremio remote AI tools."""

import asyncio
import json
from typing import Annotated, Optional

from rich import print as pp
from typer import Argument, Option, Typer

from dremioai.api.dremio import ai_tools

app = Typer(
    name="ai-tools",
    help="Interact with remote Dremio AI tools",
    context_settings=dict(help_option_names=["-h", "--help"]),
    no_args_is_help=True,
)


@app.command("list")
def list_tools():
    """List available remote AI tools from Dremio."""
    response = asyncio.run(ai_tools.list_tools())
    if response.error:
        pp(f"[red]Error:[/red] {response.error}")
        raise SystemExit(1)
    if not response.tools:
        pp("[yellow]No remote tools available.[/yellow]")
        return
    for tool in response.tools:
        pp(f"[bold]{tool.name}[/bold]  {tool.description or ''}")


@app.command("call")
def call_tool(
    tool_name: Annotated[str, Argument(help="Name of the tool to invoke")],
    args: Annotated[
        Optional[str],
        Option("--args", "-a", help="Tool arguments as a JSON object string"),
    ] = None,
):
    """Invoke a remote AI tool by name."""
    parsed_args: dict = {}
    if args:
        try:
            parsed_args = json.loads(args)
        except json.JSONDecodeError as exc:
            pp(f"[red]Invalid JSON in --args:[/red] {exc}")
            raise SystemExit(1)

    response = asyncio.run(ai_tools.invoke_tool(tool_name, parsed_args))
    if response.error:
        pp(f"[red]Error:[/red] {response.error}")
        raise SystemExit(1)
    pp(response.result)
