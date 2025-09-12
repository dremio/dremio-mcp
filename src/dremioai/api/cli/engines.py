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
from dremioai.api.dremio import engines
from dremioai.config import settings

app = Typer(
    no_args_is_help=True,
    name="engines",
    help="Run commands related to engines",
    context_settings=dict(help_option_names=["-h", "--help"]),
)


@app.command("list")
def elist(
    use_df: Annotated[
        Optional[bool], Option(help="Convert results to pandas dataframe")
    ] = False,
):
    result = asyncio.run(
        engines.get_engines(
            settings.instance().dremio.uri,
            settings.instance().dremio.pat,
            settings.instance().dremio.project_id,
            use_df=use_df,
        )
    )
    pp(result)


@app.command("get")
def eget(
    engine_ids: Annotated[
        List[str], Argument(help="Engine IDs to retrieve details for")
    ],
    use_df: Annotated[
        Optional[bool], Option(help="Convert results to pandas dataframe")
    ] = False,
):
    result = asyncio.run(
        engines.get_engines(
            settings.instance().dremio.uri,
            settings.instance().dremio.pat,
            settings.instance().dremio.project_id,
            engine_ids=engine_ids,
            use_df=use_df,
        )
    )
    pp(result)
