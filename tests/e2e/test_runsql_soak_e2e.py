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

import os
import subprocess
import gc
import tracemalloc

import pytest
from mcp.types import CallToolResult

from conftest import http_streamable_client_server, http_streamable_mcp_server
from mocks.http_mock import LARGE_SQL_MARKER


def _current_rss_mb() -> float:
    out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(os.getpid())], text=True)
    return int(out.strip()) / 1024.0


@pytest.mark.asyncio
async def test_run_sql_query_large_mock_soak(
    mock_config_dir, logging_server, logging_level
):
    iterations = int(os.getenv("RUNSQL_SOAK_ITERS", "0"))
    if iterations <= 0:
        pytest.skip("Set RUNSQL_SOAK_ITERS to run the RunSqlQuery soak test")

    sample_every = int(os.getenv("RUNSQL_SOAK_SAMPLE_EVERY", "10"))
    enable_profile = os.getenv("RUNSQL_SOAK_PROFILE", "0") == "1"
    rss_samples = []
    snapshot_start = None
    snapshot_end = None

    if enable_profile:
        tracemalloc.start(25)

    try:
        async with http_streamable_mcp_server(
            logging_server,
            logging_level,
            dremio_overrides={"max_result_rows": 1200, "max_result_bytes": 0},
        ) as sf:
            async with http_streamable_client_server(sf.mcp_server, token="my-token") as session:
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
                        if enable_profile:
                            current, peak = tracemalloc.get_traced_memory()
                            sample = {
                                "iter": i,
                                "rss_before_gc_mb": round(rss_before_gc, 1),
                                "rss_after_gc_mb": round(rss_after_gc, 1),
                                "py_current_mb": round(current / 1024 / 1024, 1),
                                "py_peak_mb": round(peak / 1024 / 1024, 1),
                                "gc_collected": collected,
                            }
                            if snapshot_start is None:
                                snapshot_start = tracemalloc.take_snapshot()
                            snapshot_end = tracemalloc.take_snapshot()
                        else:
                            sample = {
                                "iter": i,
                                "rss_before_gc_mb": round(rss_before_gc, 1),
                                "rss_after_gc_mb": round(rss_after_gc, 1),
                                "gc_collected": collected,
                            }
                        rss_samples.append(sample)
                        print(f"SOAK sample={sample}")
    finally:
        if enable_profile:
            if snapshot_start is not None and snapshot_end is not None:
                print("SOAK tracemalloc_top_diffs")
                for stat in snapshot_end.compare_to(snapshot_start, "lineno")[:15]:
                    print(stat)
            tracemalloc.stop()

    print(f"SOAK samples={rss_samples}")
    if rss_samples:
        start = rss_samples[0]["rss_after_gc_mb"]
        end = rss_samples[-1]["rss_after_gc_mb"]
        peak = max(v["rss_after_gc_mb"] for v in rss_samples)
        print(
            "SOAK summary "
            f"start_mb={start:.1f} end_mb={end:.1f} peak_mb={peak:.1f} delta_mb={end-start:.1f}"
        )
