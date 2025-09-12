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

from typer import Typer, Option, BadParameter, Argument
from rich import print_json as pj, print as pp
from typing import Annotated, Optional, List
from pathlib import Path
from dremioai.api.dremio import catalog, sql
import asyncio
from dremioai.log import configure, set_level
from dremioai.config import settings

from dremioai.api.cli.engines import app as engines_app
from dremioai.api.cli.prometheus import app as prometheus_app
from dremioai.api.cli.search import app as search_app
from dremioai.api.cli.oauth import app as oauth_app


def common_args(
    config_file: Annotated[
        Optional[Path],
        Option("-c", "--config", help="The config file with Dremio connection details"),
    ] = None,
):
    settings.configure(config_file, force=True)


app = Typer(context_settings=dict(help_option_names=["-h", "--help"]))

catalog_app = Typer(
    context_settings=dict(help_option_names=["-h", "--help"]), callback=common_args
)
sql_app = Typer(context_settings=dict(help_option_names=["-h", "--help"]))

app.add_typer(catalog_app, name="catalog", help="Run catalog oriented commands")
app.add_typer(sql_app, name="sql", help="Run SQL commands", callback=common_args)
app.add_typer(engines_app, callback=common_args)
app.add_typer(prometheus_app, callback=common_args)
app.add_typer(search_app, callback=common_args)
app.add_typer(oauth_app, callback=common_args)


@catalog_app.command(name="lineage")
def run_catalog(dataset_id: Annotated[str, Option(...)]):
    lineage = asyncio.run(catalog.get_lineage(dataset_id))
    pp(lineage)


@catalog_app.command(name="schema")
def run_catalog(
    id_or_path: Annotated[str, Option(help="Dataset ID or path")],
    by_id: Annotated[bool, Option(help="Whether the dataset id is an id")] = False,
):
    schema = asyncio.run(catalog.get_schema(id_or_path, by_id=by_id))
    pp(schema)


# _qg = "Query / Job ID "
@sql_app.command("run")
def run_sql(
    query: Annotated[
        Optional[str],
        Option(
            help="SQL query to run. If it startswith'@' then treat the rest as a filename"
        ),
    ] = None,
    job_id: Annotated[
        Optional[str], Option(help="Retrieve results of existing job")
    ] = None,
    use_df: Annotated[
        Optional[bool], Option(help="Convert results to pandas dataframe")
    ] = False,
    engine_name: Annotated[
        Optional[str], Option(help="The engine name to run the query on")
    ] = None,
):
    if query is None and job_id is None:
        raise BadParameter("Either query or job_id must be provided")

    if query is not None:
        query = Path(query[1:]).read_text().strip() if query.startswith("@") else query
        query = f"/* dremioai: submitter=cli {'engine=' + engine_name if engine_name is not None else ''} */\n{query}"
        if engine_name is not None:
            query = sql.Query(sql=query, engineName=engine_name)
        result = asyncio.run(sql.run_query(query, use_df=use_df))
    else:
        result = asyncio.run(
            sql.get_results(
                settings.instance().dremio.project_id, job_id, use_df=use_df
            )
        )

    pp(result if use_df else [r for jr in result for r in jr.rows])


if __name__ == "__main__":
    configure()
    set_level("DEBUG")
    app()
