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
import inspect
import os
import socket
import subprocess
import asyncio
import uuid
import weakref
from collections import Counter
from contextlib import asynccontextmanager

import pytest
from mcp import ClientSession, types
from mcp.client.streamable_http import streamablehttp_client
from mcp.shared.message import SessionMessage
from mcp.types import CallToolResult, JSONRPCMessage, JSONRPCResponse, TextContent

from dremioai.config import settings
from dremioai.config.tools import ToolType
from dremioai.log import set_level
from dremioai.servers.mcp import Transports, init
from mocks.http_mock import LARGE_SQL_MARKER, ServerFixture, start_server_with_app


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


def _alive(refs):
    return [r() for r in refs if r() is not None]


def _heap_type_counts(text_threshold: int):
    counts = Counter()
    samples = {}
    for obj in gc.get_objects():
        if isinstance(obj, TextContent) and len(obj.text) >= text_threshold:
            counts["TextContent.large"] += 1
            samples.setdefault("TextContent.large", obj)
        elif isinstance(obj, CallToolResult):
            counts["CallToolResult"] += 1
            samples.setdefault("CallToolResult", obj)
        elif isinstance(obj, SessionMessage):
            counts["SessionMessage"] += 1
            samples.setdefault("SessionMessage", obj)
        elif isinstance(obj, JSONRPCResponse):
            counts["JSONRPCResponse"] += 1
            samples.setdefault("JSONRPCResponse", obj)
        elif isinstance(obj, JSONRPCMessage):
            counts["JSONRPCMessage"] += 1
            samples.setdefault("JSONRPCMessage", obj)
        elif isinstance(obj, asyncio.Task):
            name = getattr(obj.get_coro(), "__name__", type(obj.get_coro()).__name__)
            counts[f"Task.{name}"] += 1
            samples.setdefault(f"Task.{name}", obj)
    return counts, samples


def _track_server_tool_results(mcp_server, calltool_refs, text_refs):
    original = mcp_server._mcp_server.request_handlers[types.CallToolRequest]

    async def wrapped(req):
        result = await original(req)
        if isinstance(result, types.ServerResult) and isinstance(result.root, types.CallToolResult):
            root = result.root
            calltool_refs.append(weakref.ref(root))
            for content in root.content:
                if isinstance(content, TextContent):
                    text_refs.append(weakref.ref(content))
        return result

    mcp_server._mcp_server.request_handlers[types.CallToolRequest] = wrapped
    return original


def _describe_object(obj):
    t = type(obj)
    if isinstance(obj, TextContent):
        return f"TextContent(len={len(obj.text)})"
    if isinstance(obj, CallToolResult):
        content_len = len(obj.content)
        return f"CallToolResult(content={content_len}, structured={obj.structuredContent is not None})"
    if isinstance(obj, JSONRPCResponse):
        return f"JSONRPCResponse(id={obj.id})"
    if isinstance(obj, JSONRPCMessage):
        return f"JSONRPCMessage(root={type(obj.root).__name__})"
    if isinstance(obj, SessionMessage):
        return f"SessionMessage(message={type(obj.message.root).__name__})"
    if isinstance(obj, dict):
        keys = list(obj.keys())[:5]
        return f"dict(keys={keys})"
    if isinstance(obj, list):
        return f"list(len={len(obj)})"
    if isinstance(obj, tuple):
        return f"tuple(len={len(obj)})"
    if inspect.isframe(obj):
        return f"frame({obj.f_code.co_name} {obj.f_code.co_filename}:{obj.f_lineno})"
    if inspect.iscoroutine(obj):
        return f"coroutine({obj.cr_code.co_name} {obj.cr_code.co_filename}:{obj.cr_code.co_firstlineno})"
    if inspect.isgenerator(obj):
        return f"generator({obj.gi_code.co_name})"
    if isinstance(obj, asyncio.Task):
        coro = obj.get_coro()
        code = getattr(coro, "cr_code", None)
        if code is not None:
            return f"Task({code.co_name} {code.co_filename}:{code.co_firstlineno})"
        return "Task"
    return t.__name__


def _owner_summary(container, exclude_ids):
    owners = [
        r
        for r in gc.get_referrers(container)
        if id(r) not in exclude_ids and not inspect.isframe(r) and not inspect.ismodule(r)
    ]
    counts = Counter(type(r).__name__ for r in owners)
    samples = [_describe_object(r) for r in owners[:5]]
    return counts, samples


def _collect_excluded_ids():
    excluded = set()
    frame = inspect.currentframe()
    depth = 0
    while frame is not None and depth < 12:
        excluded.add(id(frame))
        excluded.add(id(frame.f_locals))
        for value in frame.f_locals.values():
            excluded.add(id(value))
            if isinstance(value, (list, tuple, dict, set)):
                excluded.add(id(value))
        frame = frame.f_back
        depth += 1
    return excluded


def _print_referrer_summary(obj, label):
    exclude_ids = _collect_excluded_ids()
    refs = [
        r
        for r in gc.get_referrers(obj)
        if id(r) not in exclude_ids
        and not inspect.isframe(r)
        and not inspect.ismodule(r)
        and not inspect.iscoroutine(r)
    ]
    print(f"{label} object={_describe_object(obj)}")
    print(f"{label} direct_referrer_counts={dict(Counter(type(r).__name__ for r in refs).most_common(10))}")
    for idx, ref in enumerate(refs[:8], start=1):
        print(f"{label} ref[{idx}]={_describe_object(ref)}")
        if isinstance(ref, (list, dict, tuple)):
            counts, samples = _owner_summary(ref, exclude_ids | {id(obj), id(ref)})
            print(f"{label} ref[{idx}] owners={dict(counts.most_common(8))}")
            for sample in samples[:4]:
                print(f"{label} ref[{idx}] owner_sample={sample}")


@pytest.mark.asyncio
async def test_run_sql_query_transport_referrers(
    mock_config_dir, logging_server, logging_level
):
    iterations = int(os.getenv("RUNSQL_TRANSPORT_REF_ITERS", "0"))
    if iterations <= 0:
        pytest.skip("Set RUNSQL_TRANSPORT_REF_ITERS to run the transport referrer diagnostic")

    sample_every = int(os.getenv("RUNSQL_TRANSPORT_REF_SAMPLE_EVERY", "10"))
    text_threshold = int(os.getenv("RUNSQL_TRANSPORT_REF_TEXT_THRESHOLD", "10000"))

    server_calltool_refs = []
    server_text_refs = []
    client_calltool_refs = []
    client_text_refs = []

    async with _local_streamable_mcp_server(logging_server, logging_level) as (sf, mcp):
        original = _track_server_tool_results(mcp, server_calltool_refs, server_text_refs)
        try:
            async with _local_streamable_client(sf, token="my-token") as session:
                for i in range(1, iterations + 1):
                    result = await session.call_tool(
                        "RunSqlQuery",
                        {"query": f"SELECT 1 /* {LARGE_SQL_MARKER} */"},
                    )
                    client_calltool_refs.append(weakref.ref(result))
                    for content in result.content:
                        if isinstance(content, TextContent):
                            client_text_refs.append(weakref.ref(content))

                    payload = result.structuredContent["result"]
                    assert len(payload["result"]) == 1200
                    del payload
                    del result

                    if i % sample_every == 0:
                        rss_before_gc = _current_rss_mb()
                        collected = gc.collect()
                        rss_after_gc = _current_rss_mb()
                        alive_server_calltool = _alive(server_calltool_refs)
                        alive_server_text = [
                            obj
                            for obj in _alive(server_text_refs)
                            if isinstance(obj, TextContent) and len(obj.text) >= text_threshold
                        ]
                        alive_client_calltool = _alive(client_calltool_refs)
                        alive_client_text = [
                            obj
                            for obj in _alive(client_text_refs)
                            if isinstance(obj, TextContent) and len(obj.text) >= text_threshold
                        ]

                        sample = {
                            "iter": i,
                            "rss_before_gc_mb": round(rss_before_gc, 1),
                            "rss_after_gc_mb": round(rss_after_gc, 1),
                            "gc_collected": collected,
                            "alive_server_calltool": len(alive_server_calltool),
                            "alive_server_large_text": len(alive_server_text),
                            "alive_client_calltool": len(alive_client_calltool),
                            "alive_client_large_text": len(alive_client_text),
                        }
                        heap_counts, heap_samples = _heap_type_counts(text_threshold)
                        print(f"REF sample={sample}")
                        print(f"REF heap_counts={dict(heap_counts)}")

                        if alive_server_calltool:
                            _print_referrer_summary(alive_server_calltool[0], "REF server_calltool")
                        if alive_server_text:
                            _print_referrer_summary(alive_server_text[0], "REF server_text")
                        if alive_client_calltool:
                            _print_referrer_summary(alive_client_calltool[0], "REF client_calltool")
                        if alive_client_text:
                            _print_referrer_summary(alive_client_text[0], "REF client_text")
                        if "JSONRPCResponse" in heap_samples:
                            _print_referrer_summary(heap_samples["JSONRPCResponse"], "REF jsonrpc_response")
                        if "SessionMessage" in heap_samples:
                            _print_referrer_summary(heap_samples["SessionMessage"], "REF session_message")
                        interesting_task_keys = [
                            "Task._receive_loop",
                            "Task._settings_refresh_loop",
                            "Task.run_stateless_server",
                            "Task.message_router",
                            "Task.handle_request_async",
                            "Task._shutdown_watcher",
                        ]
                        for task_key in interesting_task_keys:
                            if task_key not in heap_samples:
                                continue
                            _print_referrer_summary(heap_samples[task_key], f"REF {task_key}")
        finally:
            mcp._mcp_server.request_handlers[types.CallToolRequest] = original
