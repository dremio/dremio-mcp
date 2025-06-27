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

from prompt_toolkit import PromptSession, print_formatted_text
from prompt_toolkit.history import FileHistory
from prompt_toolkit.styles import Style
from prompt_toolkit.formatted_text import HTML

from dremioai.tools.tools import get_tools, get_resources
from pathlib import Path
from typing import Dict

from typing import List, Optional, Annotated
from dremioai import log
from dremioai.config import settings
from typer import Typer, Option
from rich.console import Console
from rich.markdown import Markdown
from dremioai.servers.frameworks.langchain.tools import discover_tools, discover_prompt

# def system_prompt():
#     return """
#     You are helpful AI bot with access to several tools for analyzing Dremio cluster and jobs.
#     The information about jobs is stored in a table called sys.project.jobs_recent.
#     There is a tool avaialble for each of the following operations:
#     """ + "\n".join(
#         f"{t.__name__}: {'\n\t'.join(t.invoke.__doc__.splitlines())}"
#         for t in (get_tools() + get_resources())
#     )


tl = Typer(
    context_settings=dict(help_option_names=["-h", "--help"]),
    name="langchain-server",
    help="Support for testing tools directly",
)


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
):

    settings.configure(config_file)

    custom_style = Style.from_dict(
        {
            "prompt": "ansicyan",  # Input prompt color
            "error": "ansired",  # Error messages color
        }
    )
    session = PromptSession(
        history=FileHistory(Path.home() / ".mcp.history"), style=custom_style
    )

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

    executor = create_react_agent(
        model=llm,
        tools=discover_tools(settings.instance().tools.server_mode),
        prompt=discover_prompt(),
        debug=debug,
    )

    # agent = create_openai_tools_agent(
    #     llm=llm, tools=registered_tools, prompt=chat_prompt
    # )

    # agent_executor = AgentExecutor(agent=agent, tools=registered_tools, verbose=True)
    # chain = chat_prompt | llm | StrOutputParser()
    chat_history = []
    console = Console()

    while True:
        try:
            user_input = session.prompt(HTML("<prompt>>> </prompt>"))
            if user_input.lower() == "q":
                break
        except EOFError:
            break
        args = {"messages": [("human", user_input)]}
        if chat_history:
            args["chat_history"] = chat_history
        response: Dict[str, List[ToolMessage, AIMessage]] = executor.invoke(args)
        # for message in response:
        #     if isinstance(message, ToolMessage):
        #         message.pretty_print()

        console.print(Markdown(str(response["messages"][-1].content)))
        chat_history.extend(
            [("human", user_input), ("system", response["messages"][-1].content)]
        )
        # response = agent_executor.invoke({"input": user_input})
        # if output := response.get("output"):
        # pp(output)
        # else:
        # pp(response)


def cli():
    tl()


if __name__ == "__main__":
    cli()
