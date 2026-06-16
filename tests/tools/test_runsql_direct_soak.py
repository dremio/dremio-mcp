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
import subprocess
import tracemalloc
import uuid

import pytest

from dremioai.config import settings
from dremioai.tools.tools import RunSqlQuery
from mocks.http_mock import LARGE_SQL_MARKER


def _current_rss_mb() -> float:
    out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(os.getpid())], text=True)
    return int(out.strip()) / 1024.0


@pytest.mark.asyncio
async def test_run_sql_query_direct_soak(mock_config_dir, logging_server):
    iterations = int(os.getenv("RUNSQL_DIRECT_SOAK_ITERS", "0"))
    if iterations <= 0:
        pytest.skip("Set RUNSQL_DIRECT_SOAK_ITERS to run the direct RunSqlQuery soak test")

    sample_every = int(os.getenv("RUNSQL_DIRECT_SOAK_SAMPLE_EVERY", "10"))
    enable_profile = os.getenv("RUNSQL_DIRECT_SOAK_PROFILE", "0") == "1"
    samples = []
    snapshot_start = None
    snapshot_end = None

    settings.set_base_settings(
        settings.Settings.model_validate(
            {
                "dremio": {
                    "uri": logging_server.url,
                    "project_id": uuid.uuid4(),
                    "pat": "test-pat",
                    "enable_search": True,
                    "max_result_rows": 1200,
                    "max_result_bytes": 0,
                },
                "tools": {"server_mode": "FOR_DATA_PATTERNS"},
            }
        )
    )

    if enable_profile:
        tracemalloc.start(25)

    try:
        tool = RunSqlQuery()
        for i in range(1, iterations + 1):
            result = await tool.invoke(f"SELECT 1 /* {LARGE_SQL_MARKER} */")
            assert len(result["result"]) == 1200
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
                samples.append(sample)
                print(f"DIRECT SOAK sample={sample}")
    finally:
        if enable_profile:
            if snapshot_start is not None and snapshot_end is not None:
                print("DIRECT SOAK tracemalloc_top_diffs")
                for stat in snapshot_end.compare_to(snapshot_start, "lineno")[:15]:
                    print(stat)
            tracemalloc.stop()

    print(f"DIRECT SOAK samples={samples}")
    if samples:
        start = samples[0]["rss_after_gc_mb"]
        end = samples[-1]["rss_after_gc_mb"]
        peak = max(v["rss_after_gc_mb"] for v in samples)
        print(
            "DIRECT SOAK summary "
            f"start_mb={start:.1f} end_mb={end:.1f} peak_mb={peak:.1f} delta_mb={end-start:.1f}"
        )
