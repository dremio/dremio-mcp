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


from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import ToolMessage, AIMessage
from langchain_core.language_models import LanguageModelLike
from langchain_mcp_adapters.client import MultiServerMCPClient

from prompt_toolkit import PromptSession, print_formatted_text
from prompt_toolkit.history import FileHistory
from prompt_toolkit.styles import Style
from prompt_toolkit.formatted_text import HTML

from dremioai.log import logger
from pathlib import Path
from typing import Dict
import sys
import asyncio

from typing import List, Optional, Annotated
from dremioai import log
from dremioai.config import settings
from typer import Typer, Option
from rich.console import Console
from rich.markdown import Markdown
from dremioai.servers.frameworks.langchain.tools import (
    discover_tools,
    discover_prompt,
    StructuredTool,
    ChatPromptTemplate,
)

tl = Typer(
    context_settings=dict(help_option_names=["-h", "--help"]),
    name="langchain-server",
    help="Support for testing tools directly",
)


async def user_input(
    tools: List[StructuredTool],
    prompt: ChatPromptTemplate,
    llm: LanguageModelLike,
    debug: bool = False,
):
    custom_style = Style.from_dict(
        {
            "prompt": "ansicyan",  # Input prompt color
            "error": "ansired",  # Error messages color
        }
    )
    session = PromptSession(
        history=FileHistory(Path.home() / ".mcp.history"), style=custom_style
    )
    executor = create_react_agent(
        model=llm,
        tools=discover_tools(settings.instance().tools.server_mode),
        prompt=discover_prompt(),
        debug=debug,
    )

    chat_history = []
    console = Console()

    while True:
        try:
            user_input = await session.prompt_async(HTML("<prompt>>> </prompt>"))
            if user_input.lower() == "q":
                break
        except EOFError:
            break
        args = {"messages": [("human", user_input)]}
        if chat_history:
            args["chat_history"] = chat_history
        response: Dict[str, List[ToolMessage, AIMessage]] = await executor.ainvoke(args)

        console.print(Markdown(str(response["messages"][-1].content)))
        chat_history.extend(
            [("human", user_input), ("system", response["messages"][-1].content)]
        )


async def using_mcp(llm: LanguageModelLike, config_file: Path, debug: bool = False):
    client = MultiServerMCPClient(
        {
            "dremioai": {
                "command": sys.executable,
                "args": [
                    "-m",
                    "dremioai.servers.mcp",
                    "run",
                    "-c",
                    str(config_file),
                ],
                "transport": "stdio",
            }
        }
    )
    async with client.session("dremioai") as session:
        tools = await session.list_tools()
        # prompt = await session.list_prompts()
        # logger().info(f"Found prompt={prompt}")
        prompt = await session.get_prompt("System Prompt")
        prompt = discover_prompt(prompt.messages[0].content.text)
        logger().info(f"Found {len(tools.tools)} tools and prompt={prompt}")
        await user_input(tools, prompt, llm, debug)


@tl.command()
def main(
    config_file: Annotated[
        Optional[Path],
        Option(
            "-c",
            "--cfg",
            help="The config yaml for various options",
            show_default=True,
        ),
    ] = settings.default_config(),
    debug: Annotated[bool, Option(help="Enable debug logging")] = False,
    use_as_mcp: Annotated[
        bool,
        Option(help="Use the server as a MCP server instead of using tools directly"),
    ] = True,
):
    log.configure(to_file=True)

    settings.configure(config_file)

    if settings.instance().langchain.ollama is not None:
        llm = ChatOllama(
            model=settings.instance().langchain.ollama.model,
            temperature=0,
            verbose=True,
        )
    else:
        llm = ChatOpenAI(
            model=settings.instance().langchain.openai.model,
            temperature=0,
            verbose=True,
            api_key=settings.instance().langchain.openai.api_key,
        )

    if use_as_mcp:
        asyncio.run(using_mcp(llm, config_file, debug))
    else:
        tools = discover_tools(settings.instance().tools.server_mode)
        prompt = discover_prompt()
        logger().info(f"[non mcp] Found {len(tools.tools)} tools and prompt={prompt}")
        asyncio.run(user_input(tools, prompt, llm, debug))


def cli():
    tl()


if __name__ == "__main__":
    cli()
