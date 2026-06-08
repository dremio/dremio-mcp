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
import json
import os
import socket
import subprocess
from collections import Counter
from contextlib import asynccontextmanager

import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.server.fastmcp import FastMCP

from mocks.http_mock import ServerFixture, start_server_with_app


ROW_COUNT = 1200
PAYLOAD_WIDTH = 256


def _current_rss_mb() -> float:
    out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(os.getpid())], text=True)
    return int(out.strip()) / 1024.0


def _reserve_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return sock.getsockname()[1]


def _large_payload() -> dict:
    return {
        "result": [
            {"row_id": idx, "payload": f"row-{idx}-" + ("x" * PAYLOAD_WIDTH)}
            for idx in range(ROW_COUNT)
        ]
    }


def _task_group_counts(mcp) -> tuple[int, dict[str, int]]:
    tasks = list(mcp.session_manager._task_group._tasks)
    counts = Counter()
    for task in tasks:
        coro = task.get_coro()
        code = getattr(coro, "cr_code", None)
        name = code.co_name if code is not None else type(coro).__name__
        counts[name] += 1
    return len(tasks), dict(sorted(counts.items()))


def _build_server(mode: str) -> FastMCP:
    mcp = FastMCP(
        "StandaloneLeakRepro",
        stateless_http=True,
        debug=True,
        log_level="DEBUG",
    )

    if mode == "structured":
        async def large_payload_tool():
            return _large_payload()
    elif mode == "text":
        async def large_payload_tool():
            return json.dumps(_large_payload())
    else:
        raise ValueError(f"Unsupported mode: {mode}")

    mcp.add_tool(
        large_payload_tool,
        name="LargePayload",
        description="Return a large payload for stateless transport leak repro",
    )
    return mcp


@asynccontextmanager
async def _local_streamable_server(mode: str):
    sf = None
    host = "127.0.0.1"
    port = _reserve_local_port()
    mcp = _build_server(mode)
    app = mcp.streamable_http_app()
    server, stop_event = start_server_with_app(
        app,
        host=host,
        port=port,
        log_level="warning",
        name=f"standalone-{mode}-mcp-server",
    )
    try:
        sf = ServerFixture(f"http://{host}:{port}/mcp/", stop_event, server)
        yield sf, mcp
    finally:
        if sf is not None:
            sf.close()


@asynccontextmanager
async def _local_streamable_client(server_fixture: ServerFixture):
    async with streamablehttp_client(url=server_fixture.url) as (
        read_stream,
        write_stream,
        _gid,
    ):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            yield session


async def _run_soak(session, mcp, iterations: int, sample_every: int, label: str):
    samples = []

    for i in range(1, iterations + 1):
        result = await session.call_tool("LargePayload", {})
        if result.structuredContent is not None:
            assert len(result.structuredContent["result"]) == ROW_COUNT
        else:
            assert result.content and ROW_COUNT > 0
        del result

        if i % sample_every == 0:
            gc_collected = gc.collect()
            rss_after_gc = _current_rss_mb()
            task_count, task_types = _task_group_counts(mcp)
            sample = {
                "iter": i,
                "rss_after_gc_mb": round(rss_after_gc, 1),
                "gc_collected": gc_collected,
                "task_count": task_count,
                "task_types": task_types,
            }
            samples.append(sample)
            print(f"{label} sample={sample}")

    return samples


@pytest.mark.asyncio
async def test_stateless_streamable_http_standalone_repro():
    iterations = int(os.getenv("MCP_STATELESS_STANDALONE_ITERS", "0"))
    if iterations <= 0:
        pytest.skip("Set MCP_STATELESS_STANDALONE_ITERS to run the standalone stateless repro")

    sample_every = int(os.getenv("MCP_STATELESS_STANDALONE_SAMPLE_EVERY", "10"))
    mode = os.getenv("MCP_STATELESS_STANDALONE_MODE", "both")
    assert mode in {"structured", "text", "both"}

    structured = []
    text = []

    if mode in {"structured", "both"}:
        async with _local_streamable_server("structured") as (sf, mcp):
            async with _local_streamable_client(sf) as session:
                structured = await _run_soak(
                    session, mcp, iterations, sample_every, "STANDALONE STRUCTURED"
                )

    if mode in {"text", "both"}:
        async with _local_streamable_server("text") as (sf, mcp):
            async with _local_streamable_client(sf) as session:
                text = await _run_soak(session, mcp, iterations, sample_every, "STANDALONE TEXT")

    if structured:
        print(f"STANDALONE STRUCTURED samples={structured}")
    if text:
        print(f"STANDALONE TEXT samples={text}")

    if mode == "both" and structured and text:
        structured_rss_delta = (
            structured[-1]["rss_after_gc_mb"] - structured[0]["rss_after_gc_mb"]
        )
        text_rss_delta = text[-1]["rss_after_gc_mb"] - text[0]["rss_after_gc_mb"]
        structured_task_delta = structured[-1]["task_count"] - structured[0]["task_count"]
        text_task_delta = text[-1]["task_count"] - text[0]["task_count"]
        print(
            "STANDALONE summary "
            f"structured_rss_delta_mb={structured_rss_delta:.1f} "
            f"text_rss_delta_mb={text_rss_delta:.1f} "
            f"structured_task_delta={structured_task_delta} "
            f"text_task_delta={text_task_delta}"
        )
        assert structured_task_delta == 0
        assert text_task_delta == 0
