"""
MCP HTTP Streamable Client Example using Python SDK

This example demonstrates how to create an MCP client that connects to a server
using the Streamable HTTP transport protocol.
"""

import asyncio
import contextlib
import functools
import json
import random
import time
from contextlib import asynccontextmanager
from typing import Annotated, Optional, AsyncGenerator, Callable, Any, Dict
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


@cli.command("call-tool")
@async_command
async def call_tool(
    tool: Annotated[str, Option(help="The tool to call")],
    url: Annotated[
        Optional[str], Option(help="The URL of the MCP server")
    ] = "http://127.0.0.1:8000/mcp",
    token: Annotated[
        Optional[str], Option(help="The authorization token to use")
    ] = None,
    args: Annotated[
        Optional[str], Option(help="The arguments to pass to the tool as a JSON")
    ] = None,
):
    async with mcp_client_session(url, token) as session:
        result = await session.call_tool(tool, json.loads(args) if args else None)
        if result.isError:
            pp("[red]Error[/red]")
            pp(result.content)
            return
        pp(result.structuredContent["result"])


def _assert(condition: bool, msg: str):
    if not condition:
        pp(f"[red]FAIL[/red] {msg}")
        raise SystemExit(1)


def _derive_dremio_api_uri(mcp_url: str) -> str:
    """Derive the Dremio API URI from an MCP server URL.

    Replaces 'mcp.' with 'api.' in the hostname and returns the base URL.
    E.g. https://mcp.dremio.cloud/mcp/proj-id -> https://api.dremio.cloud
         https://mcp.eu.dremio.cloud/... -> https://api.eu.dremio.cloud
    """
    parsed = urlparse(mcp_url)
    hostname = parsed.hostname or ""
    if hostname.startswith("mcp."):
        api_host = "api." + hostname[4:]
        scheme = parsed.scheme
        port_str = (
            f":{parsed.port}" if parsed.port and parsed.port not in (80, 443) else ""
        )
        return f"{scheme}://{api_host}{port_str}"
    return hostname


def _extract_project_id(mcp_url: str) -> Optional[str]:
    """Extract project ID from MCP URL path (e.g. /mcp/<project-id>)."""
    parsed = urlparse(mcp_url)
    path_parts = [p for p in parsed.path.split("/") if p and p != "mcp"]
    return path_parts[0] if path_parts else None


@contextlib.contextmanager
def _local_mcp_server(dremio_uri: str, port: int = 8989):
    """Start a local MCP server configured to proxy to the given Dremio URI."""
    import threading
    from dremioai.config import settings
    from dremioai.config.tools import ToolType
    from dremioai.servers.mcp import init, Transports

    old = settings.instance()
    try:
        configured_settings = old.model_copy(deep=True).with_overrides(
            {
                "dremio.uri": dremio_uri,
                "dremio.raw_project_id": "DREMIO_DYNAMIC",
                "dremio.enable_search": True,
                "tools.server_mode": ToolType.FOR_DATA_PATTERNS.name,
            }
        )
        settings._settings.set(configured_settings)
        mcp_server = init(
            transport=Transports.streamable_http,
            port=port,
            host="127.0.0.1",
            mode=configured_settings.tools.server_mode,
            support_project_id_endpoints=True,
        )

        def _run():
            import uvicorn

            # Propagate settings to server thread — ContextVar doesn't
            # automatically transfer across threads, so without this the
            # server would fall back to the default config file.
            settings._settings.set(configured_settings)

            a = mcp_server.streamable_http_app()
            c = uvicorn.Config(app=a, host="127.0.0.1", port=port, log_level="warning")
            s = uvicorn.Server(c)
            asyncio.new_event_loop().run_until_complete(s.serve())

        t = threading.Thread(target=_run, daemon=True)
        t.start()

        # Wait for server to be ready
        import time as _time

        for _ in range(30):
            try:
                requests.get(f"http://127.0.0.1:{port}/healthz", timeout=1)
                break
            except Exception:
                _time.sleep(0.5)
        else:
            raise RuntimeError("Local MCP server did not start in time")

        yield port
    finally:
        settings._settings.set(old)


@app.command("test", help="Run a quick smoketest for a deployed MCP server")
@async_command
async def run_test(
    client_id: Annotated[
        Optional[str],
        Option(help="The OAuth client id (skipped when --token is provided)"),
    ] = None,
    token: Annotated[
        Optional[str], Option(help="Bearer token to use directly (skips OAuth)")
    ] = None,
    url: Annotated[
        Optional[str],
        Option(
            help="The URL of the MCP server (e.g. https://mcp.dremio.cloud/mcp/<project-id>)"
        ),
    ] = "http://127.0.0.1:8000/mcp",
    check_annotations: Annotated[
        bool, Option(help="Check MCP tool annotations")
    ] = True,
    check_new_contract: Annotated[
        bool,
        Option(
            help="Check new-contract changes (param rename, input validation, etc.)"
        ),
    ] = True,
    local: Annotated[
        bool, Option(help="Start a local MCP server for running the tests")
    ] = False,
):
    if not local:
        # Remote mode: connect directly to the URL
        if token is None and client_id is None:
            pp(
                "[red]FAIL[/red] Provide either --client-id (for OAuth) or --token (for direct auth)"
            )
            raise SystemExit(1)
        if token is None:
            pp("Checking auth..", end=" ")
            a = check_auth(client_id, url)
            token = a.access_token
            pp("[green]OK[/green]")
        else:
            pp("Using provided token, skipping OAuth..")
        await _run_smoketests(url, token, check_annotations, check_new_contract)
        return

    # Local mode: start a local MCP server and test against it
    dremio_api_uri = _derive_dremio_api_uri(url)
    project_id = _extract_project_id(url)

    if token is None:
        # No token — do OAuth against the local server (after it starts)
        if client_id is None:
            pp("[red]FAIL[/red] --local without --token requires --client-id for OAuth")
            raise SystemExit(1)

    pp(
        f"Starting local MCP server (dremio.uri={dremio_api_uri}, project_id=DREMIO_DYNAMIC).."
    )
    local_port = random.randrange(9000, 12000)
    with _local_mcp_server(dremio_api_uri, port=local_port):
        local_url = (
            f"http://127.0.0.1:{local_port}/mcp/{project_id}/"
            if project_id
            else f"http://127.0.0.1:{local_port}/mcp/"
        )
        pp(f"Local server ready at {local_url}")

        if token is None:
            pp("Checking auth against local server..", end=" ")
            a = check_auth(client_id, local_url)
            token = a.access_token
            pp("[green]OK[/green]")

        await _run_smoketests(local_url, token, check_annotations, check_new_contract)


async def _run_smoketests(
    url: str,
    token: str,
    check_annotations: bool,
    check_new_contract: bool,
):
    # Determine the SQL query parameter name based on contract version
    sql_param = "query" if check_new_contract else "s"

    pp("Connecting to server..")
    async with mcp_client_session(url, token) as session:
        tools_result = await session.list_tools()
        tool_names = [t.name for t in tools_result.tools]
        pp(tool_names)

        # ------------------------------------------------------------------
        # 1. MCP tool annotations: every tool must have annotations;
        #    RunSqlQuery may have destructiveHint=True if allow_dml is on
        # ------------------------------------------------------------------
        if check_annotations:
            pp("Checking tool annotations..", end=" ")
            for t in tools_result.tools:
                _assert(
                    t.annotations is not None,
                    f"Tool {t.name} is missing annotations",
                )
                if t.name == "RunSqlQuery":
                    # RunSqlQuery: readOnlyHint and destructiveHint depend on
                    # the server's allow_dml setting — just verify they are
                    # consistent (mutually exclusive).
                    _assert(
                        t.annotations.readOnlyHint != t.annotations.destructiveHint,
                        f"RunSqlQuery readOnlyHint={t.annotations.readOnlyHint} and "
                        f"destructiveHint={t.annotations.destructiveHint} should be opposites",
                    )
                    pp(
                        f"\n  RunSqlQuery: readOnly={t.annotations.readOnlyHint}, "
                        f"destructive={t.annotations.destructiveHint}"
                    )
                else:
                    _assert(
                        t.annotations.readOnlyHint is True,
                        f"Tool {t.name} does not have readOnlyHint=True",
                    )
                    _assert(
                        t.annotations.destructiveHint is False,
                        f"Tool {t.name} does not have destructiveHint=False",
                    )
            pp("[green]OK[/green]")
        else:
            pp("Skipping tool annotations check")

        # ------------------------------------------------------------------
        # 2. RunSqlQuery: parameter renamed from 's' to 'query'
        # ------------------------------------------------------------------
        if check_new_contract:
            pp("Checking RunSqlQuery parameter name..", end=" ")
            sql_tool = next(t for t in tools_result.tools if t.name == "RunSqlQuery")
            params = sql_tool.inputSchema.get("properties", {})
            _assert("query" in params, "RunSqlQuery should have a 'query' parameter")
            _assert("s" not in params, "RunSqlQuery should NOT have an 's' parameter")
            pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 3. RunSqlQuery: basic SELECT works
        # ------------------------------------------------------------------
        pp("Checking RunSqlQuery SELECT..", end=" ")
        n = int(time.time())
        query = f"SELECT {n} as n"
        result = await session.call_tool("RunSqlQuery", {sql_param: query})
        _assert(not result.isError, f"RunSqlQuery SELECT failed: {result.content}")
        _assert(
            result.structuredContent is not None,
            "RunSqlQuery returned no structured content",
        )
        pp(result.structuredContent["result"]["result"])
        pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 4. RunSqlQuery: DML is rejected with clean error
        # ------------------------------------------------------------------
        if check_new_contract:
            pp("Checking RunSqlQuery DML rejection..", end=" ")
            result = await session.call_tool(
                "RunSqlQuery", {sql_param: "DROP TABLE foo"}
            )
            _assert(
                result.structuredContent is not None
                and "error" in result.structuredContent["result"],
                "RunSqlQuery should return an error dict for DML",
            )
            _assert(
                "SELECT" in result.structuredContent["result"]["error"],
                "DML error should mention that only SELECT is allowed",
            )
            pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 5. RunSqlQuery: verify job tracking via jobs_recent
        # ------------------------------------------------------------------
        pp("Checking RunSqlQuery job tracking..", end=" ")
        query2 = f"""
        SELECT query
        FROM   sys.project.jobs_recent
        WHERE query_type = 'REST' and  submitted_ts > CURRENT_TIMESTAMP() - INTERVAL '1' minute
        and query like '/* dremioai: submitter=RunS%' and query like '%SELECT {n} as n';
        """
        result = await session.call_tool("RunSqlQuery", {sql_param: query2})
        _assert(not result.isError, f"Job tracking query failed: {result.content}")
        rows = result.structuredContent["result"]["result"]
        pp(rows)
        _assert(len(rows) == 1, f"Expected 1 job tracking row, got {len(rows)}")
        pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 6. GetUsefulSystemTableNames: should return multiple entries
        # ------------------------------------------------------------------
        if check_new_contract:
            pp("Checking GetUsefulSystemTableNames..", end=" ")
            result = await session.call_tool("GetUsefulSystemTableNames", {})
            _assert(
                not result.isError,
                f"GetUsefulSystemTableNames failed: {result.content}",
            )
            table_names = result.structuredContent["result"]
            _assert(
                len(table_names) > 1,
                f"Expected multiple system tables, got {len(table_names)}",
            )
            expected_tables = [
                'INFORMATION_SCHEMA."TABLES"',
                "sys.project.jobs_recent",
                'INFORMATION_SCHEMA."COLUMNS"',
            ]
            for et in expected_tables:
                _assert(et in table_names, f"Missing expected system table: {et}")
            pp(f"{len(table_names)} tables returned")
            pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 7. GetSchemaOfTable: empty string returns validation error
        # ------------------------------------------------------------------
        if check_new_contract:
            pp("Checking GetSchemaOfTable empty input validation..", end=" ")
            result = await session.call_tool("GetSchemaOfTable", {"table_name": ""})
            _assert(
                not result.isError,
                f"GetSchemaOfTable empty input crashed: {result.content}",
            )
            schema_result = result.structuredContent["result"]
            _assert(
                "error" in schema_result,
                "GetSchemaOfTable('') should return an error dict",
            )
            _assert(
                "empty" in schema_result["error"].lower(),
                f"Error should mention 'empty', got: {schema_result['error']}",
            )
            pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 8. GetSchemaOfTable: valid table returns schema
        # ------------------------------------------------------------------
        pp("Checking GetSchemaOfTable with valid table..", end=" ")
        result = await session.call_tool(
            "GetSchemaOfTable",
            {"table_name": 'INFORMATION_SCHEMA."TABLES"'},
        )
        _assert(not result.isError, f"GetSchemaOfTable failed: {result.content}")
        schema_result = result.structuredContent["result"]
        _assert(
            "fields" in schema_result,
            f"Expected 'fields' in schema result, got keys: {list(schema_result.keys())}",
        )
        pp(f"{len(schema_result['fields'])} fields returned")
        pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 9. GetSchemaOfTable: parameter docstring includes format examples
        # ------------------------------------------------------------------
        if check_new_contract:
            pp("Checking GetSchemaOfTable parameter docs..", end=" ")
            schema_tool = next(
                t for t in tools_result.tools if t.name == "GetSchemaOfTable"
            )
            desc = schema_tool.description or ""
            _assert(
                "dot-separated" in desc or "list of path" in desc,
                "GetSchemaOfTable description should document input formats",
            )
            pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 10. GetDescriptionOfTableOrSchema: should work (auth fix)
        # ------------------------------------------------------------------
        if check_new_contract:
            pp("Checking GetDescriptionOfTableOrSchema auth..", end=" ")
            result = await session.call_tool(
                "GetDescriptionOfTableOrSchema",
                {"name": 'INFORMATION_SCHEMA."TABLES"'},
            )
            _assert(
                not result.isError,
                f"GetDescriptionOfTableOrSchema failed (auth issue?): {result.content}",
            )
            _assert(
                result.structuredContent is not None,
                "GetDescriptionOfTableOrSchema returned no structured content",
            )
            pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 11. GetTableOrViewLineage: bad table returns sanitized error
        # ------------------------------------------------------------------
        if check_new_contract:
            pp("Checking GetTableOrViewLineage error sanitization..", end=" ")
            result = await session.call_tool(
                "GetTableOrViewLineage",
                {"table_name": "nonexistent.table.name"},
            )
            _assert(
                not result.isError,
                f"GetTableOrViewLineage crashed instead of returning error dict: {result.content}",
            )
            lineage_result = result.structuredContent["result"]
            if "error" in lineage_result:
                _assert(
                    "api.dremio.cloud" not in lineage_result["error"],
                    "Lineage error should not expose internal API URLs",
                )
                _assert(
                    "project" not in lineage_result["error"].lower()
                    or "project id" not in lineage_result["error"].lower(),
                    "Lineage error should not expose project IDs",
                )
            pp("[green]OK[/green]")

    pp("\n[green]All smoketests passed![/green]")


# Add the CLI subcommand to the main app
app.add_typer(cli)
app.add_typer(auth)


if __name__ == "__main__":
    log.configure(enable_json_logging=False, to_file=False)
    app()
