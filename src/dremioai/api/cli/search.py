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

from typing import Annotated, Optional, List
from typer import Option, Argument, Typer
import asyncio
from rich import print as pp
from pathlib import Path
from dremioai.api.dremio import search
from dremioai.config import settings
from dremioai.log import logger

app = Typer(
    no_args_is_help=True,
    name="search",
    help="Run semantic search apis",
    context_settings=dict(help_option_names=["-h", "--help"]),
)


@app.command("run")
def do_search(
    query: Annotated[str, Argument(...)],
    category: Annotated[List[search.Category], Option(help="Search category")] = None,
    use_df: Annotated[
        Optional[bool], Option(help="Convert results to pandas dataframe")
    ] = False,
):
    args = {"query": query}
    if category:
        args["filter"] = category
    result = asyncio.run(
        search.get_search_results(search=search.Search(**args), use_df=use_df)
    )
    pp(result)
