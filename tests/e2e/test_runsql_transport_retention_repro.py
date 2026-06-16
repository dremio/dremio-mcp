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

import gc
import os
import socket
import subprocess
import uuid
from collections import Counter
from contextlib import asynccontextmanager

import pytest
from mcp import types
from mcp.types import CallToolResult, TextContent

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from dremioai.config import settings
from dremioai.config.tools import ToolType
from dremioai.log import set_level
from dremioai.servers.mcp import Transports, init
from mocks.http_mock import ServerFixture, start_server_with_app
from mocks.http_mock import LARGE_SQL_MARKER


def _current_rss_mb() -> float:
    out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(os.getpid())], text=True)
    return int(out.strip()) / 1024.0


def _reserve_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return sock.getsockname()[1]


@asynccontextmanager
async def _local_streamable_mcp_server(logging_server, logging_level):
    old = settings.instance()
    sf = None
    try:
        settings.configure(force=True)
        host = "127.0.0.1"
        port = _reserve_local_port()
        config = {
            "dremio": {
                "uri": logging_server.url,
                "project_id": uuid.uuid4(),
                "pat": "test-pat",
                "enable_search": True,
                "metrics_enabled": False,
                "max_result_rows": 1200,
                "max_result_bytes": 0,
            },
            "tools": {"server_mode": ToolType.FOR_DATA_PATTERNS.name},
        }
        settings.set_base_settings(settings.Settings.model_validate(config))
        settings.write_settings()
        set_level(logging_level.upper())

        mcp = init(
            transport=Transports.streamable_http,
            port=port,
            mode=settings.instance().tools.server_mode,
        )
        app = mcp.streamable_http_app()
        server, stop_event = start_server_with_app(
            app,
            host=host,
            port=port,
            log_level=logging_level,
        )
        sf = ServerFixture(f"http://{host}:{port}/mcp/", stop_event, server)
        yield sf, mcp
    finally:
        if sf is not None:
            sf.close()
        settings.set_base_settings(old)


@asynccontextmanager
async def _local_streamable_client(server_fixture: ServerFixture, token="my-token"):
    headers = {"Authorization": f"Bearer {token}"}
    async with streamablehttp_client(url=server_fixture.url, headers=headers) as (
        read_stream,
        write_stream,
        _gid,
    ):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            yield session


def _with_compact_content(mcp_server):
    original = mcp_server._mcp_server.request_handlers[types.CallToolRequest]

    async def wrapped(req):
        result = await original(req)
        if not isinstance(result, types.ServerResult):
            return result

        root = result.root
        if not isinstance(root, types.CallToolResult) or root.structuredContent is None:
            return result

        return types.ServerResult(
            types.CallToolResult(
                content=[TextContent(type="text", text="structured response omitted")],
                structuredContent=root.structuredContent,
                isError=root.isError,
                meta=root.meta,
            )
        )

    mcp_server._mcp_server.request_handlers[types.CallToolRequest] = wrapped
    return original


def _task_group_counts(mcp) -> tuple[int, dict[str, int]]:
    tasks = list(mcp.session_manager._task_group._tasks)
    counts = Counter()
    for task in tasks:
        coro = task.get_coro()
        code = getattr(coro, "cr_code", None)
        name = code.co_name if code is not None else type(coro).__name__
        counts[name] += 1
    return len(tasks), dict(sorted(counts.items()))


async def _run_soak(session, mcp, iterations: int, sample_every: int, label: str):
    samples = []

    for i in range(1, iterations + 1):
        result: CallToolResult = await session.call_tool(
            "RunSqlQuery",
            {"query": f"SELECT 1 /* {LARGE_SQL_MARKER} */"},
        )
        assert result is not None and result.structuredContent is not None
        payload = result.structuredContent["result"]
        assert len(payload["result"]) == 1200
        del payload
        del result

        if i % sample_every == 0:
            rss_before_gc = _current_rss_mb()
            collected = gc.collect()
            rss_after_gc = _current_rss_mb()
            task_count, task_types = _task_group_counts(mcp)
            sample = {
                "iter": i,
                "rss_before_gc_mb": round(rss_before_gc, 1),
                "rss_after_gc_mb": round(rss_after_gc, 1),
                "gc_collected": collected,
                "task_count": task_count,
                "task_types": task_types,
            }
            samples.append(sample)
            print(f"{label} sample={sample}")

    return samples


@pytest.mark.asyncio
async def test_run_sql_query_transport_retention_repro(
    mock_config_dir, logging_server, logging_level
):
    iterations = int(os.getenv("RUNSQL_TRANSPORT_REPRO_ITERS", "0"))
    if iterations <= 0:
        pytest.skip("Set RUNSQL_TRANSPORT_REPRO_ITERS to run the transport retention repro")

    sample_every = int(os.getenv("RUNSQL_TRANSPORT_REPRO_SAMPLE_EVERY", "10"))
    mode = os.getenv("RUNSQL_TRANSPORT_REPRO_MODE", "both")
    assert mode in {"baseline", "compact", "both"}

    baseline = []
    compact = []

    if mode in {"baseline", "both"}:
        async with _local_streamable_mcp_server(logging_server, logging_level) as (sf, mcp):
            async with _local_streamable_client(sf, token="my-token") as session:
                baseline = await _run_soak(
                    session, mcp, iterations, sample_every, "TRANSPORT BASELINE"
                )

    if mode in {"compact", "both"}:
        async with _local_streamable_mcp_server(logging_server, logging_level) as (sf, mcp):
            original = _with_compact_content(mcp)
            try:
                async with _local_streamable_client(sf, token="my-token") as session:
                    compact = await _run_soak(
                        session, mcp, iterations, sample_every, "TRANSPORT COMPACT"
                    )
            finally:
                mcp._mcp_server.request_handlers[types.CallToolRequest] = original

    if baseline:
        print(f"TRANSPORT BASELINE samples={baseline}")
    if compact:
        print(f"TRANSPORT COMPACT samples={compact}")

    if mode == "both" and baseline and compact:
        baseline_delta = baseline[-1]["rss_after_gc_mb"] - baseline[0]["rss_after_gc_mb"]
        compact_delta = compact[-1]["rss_after_gc_mb"] - compact[0]["rss_after_gc_mb"]
        baseline_task_delta = baseline[-1]["task_count"] - baseline[0]["task_count"]
        compact_task_delta = compact[-1]["task_count"] - compact[0]["task_count"]
        print(
            "TRANSPORT RETENTION summary "
            f"baseline_delta_mb={baseline_delta:.1f} compact_delta_mb={compact_delta:.1f} "
            f"baseline_task_delta={baseline_task_delta} compact_task_delta={compact_task_delta}"
        )
        assert baseline_delta > compact_delta
        assert baseline_task_delta == 0
        assert compact_task_delta == 0
