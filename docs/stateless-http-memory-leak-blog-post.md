# Diagnosing a Stateless Streamable HTTP Memory Leak

## Overview

This document captures how we diagnosed a memory leak in stateless MCP Streamable
HTTP, starting from production-ish memory graphs and ending with a minimal upstream
patch.

The short version:

- large `RunSqlQuery` responses first exposed the problem
- fixing our own SQL result handling reduced the symptom but did not eliminate it
- the real leak turned out to be in the MCP SDK's stateless transport task lifecycle
- structured tool responses amplified the memory cost, but were not the root cause

## The Initial Symptom

We started with Kubernetes memory graphs showing MCP pods climb from a few hundred MiB
to multiple GiB and then eventually OOM or get replaced.

At first glance, the obvious suspect was `RunSqlQuery` because it can return large
payloads and historically built several in-memory copies of the result.

That led to the first round of fixes:

- cap result rows and bytes
- fetch result pages sequentially
- remove pandas from the default `RunSqlQuery` MCP response path
- add metrics for rows, pages, truncation, and response size

Those changes were real improvements, but memory still ratcheted upward under repeated
large responses.

## First Key Split: Tool Logic vs Transport

We then asked a sharper question:

Is the leak in the tool implementation, or in the MCP transport/serialization layer?

To answer that, we built two soak paths:

1. call `RunSqlQuery.invoke()` directly in-process
2. call the same tool through MCP Streamable HTTP

The direct invoke path stayed flat.
The Streamable HTTP path grew.

That separated the problem cleanly:

- tool code was no longer the main leak
- transport/session/serialization was the real suspect

The RSS-based soak tests were the first hard split:

```text
direct RunSqlQuery.invoke() soak, 30 iterations
- rss after gc: 153.7 -> 154.6 -> 154.9 MiB
- delta: +1.2 MiB

transport soak, 30 iterations
- rss after gc: 195.7 -> 244.1 -> 273.4 MiB
- delta: +77.7 MiB from iter 10 to 30
```

That told us the SQL tool path itself was now basically stable, while the full MCP
transport path was still retaining memory between calls.

## Structured Content Was an Amplifier

Next we tested whether MCP's result conversion behavior was making the leak more
expensive.

We ran the same transport soak twice:

- baseline: full `structuredContent` and normal text `content`
- compact: keep full `structuredContent`, replace text `content` with a tiny summary

The compact variant reduced retained memory growth sharply.

That told us:

- large mirrored text payloads were materially increasing memory cost
- but the transport path was still holding onto per-request state too long

In other words:

- payload duplication explained "why each leaked request was expensive"
- it did not explain "why requests were leaking at all"

The compact-content experiment made that visible in one run:

```text
baseline transport soak, 30 calls
- rss after gc: 180.4 -> 277.6 MiB
- delta: +97.2 MiB

compact-content transport soak, 30 calls
- rss after gc: 242.1 -> 259.4 MiB
- delta: +17.3 MiB
```

So reducing the mirrored text payload cut most of the retained memory growth, but it
still did not eliminate the underlying request-to-request accumulation.

## Memray and Tracemalloc Narrowed the Area

`memray` and `tracemalloc` showed hot spots in:

- pydantic validation/serialization
- transport buffering
- SSE-related paths
- MCP result conversion

That was useful, but allocator hot spots alone do not prove object retention.

So we added GC/task diagnostics and looked for live-object growth instead.

### Tracemalloc Excerpts

`tracemalloc` helped answer an important question: "is this just RSS not returning to
the OS, or is Python-tracked memory growing too?"

During the leaking transport soak:

```text
py_current_mb: 14.0 -> 25.4 -> 36.6
py_peak_mb:    18.1 -> 29.4 -> 40.7
```

That ruled out a pure allocator-retention explanation. Live Python allocations were
also increasing.

The top `tracemalloc` diffs between iterations were especially revealing:

```text
+7.6 MiB  pydantic/main.py:782          validate_json
+7.3 MiB  mcp/server/fastmcp/utilities/func_metadata.py:492
+6.4 MiB  pydantic/main.py:475          to_python
```

That pushed the investigation toward MCP result conversion and pydantic model
serialization instead of back toward SQL fetching.

### Memray Excerpts

`memray` gave a better view of total allocation pressure and where bytes were being
spent across the transport path.

Baseline profile:

```text
total allocated: 1.969 GB
peak memory:     143.960 MB

top allocators:
- pydantic/_internal/_repr.py:62   287.988 MB
- pydantic/main.py:782             250.199 MB
- asyncio/selector_events.py:1005  237.480 MB
- sse_starlette/event.py:59         72.617 MB
- json/encoder.py:261               69.719 MB
```

Compact-content profile:

```text
total allocated: 1.416 GB
peak memory:     129.037 MB

notable drops vs baseline:
- pydantic/main.py:782     down ~131 MB
- pydantic/_internal/_repr.py:62 down ~128 MB
- sse_starlette/event.py:59 disappeared from the top list
```

That strongly supported the conclusion that duplicated content/serialization was a
major memory amplifier in the transport path.

## The Strongest Leak Signal: Task Count

The breakthrough came from counting tasks in the session manager's AnyIO task group.

Before the fix, repeated stateless requests caused:

- `mcp.session_manager._task_group._tasks` to grow roughly linearly
- retained task families to include request-scoped server/session/router tasks

That is a much stronger leak signal than RSS alone, because it identifies a live
object family whose population grows with requests.

The unit tests made that concrete:

```text
before fix, stateless standalone repro
- iter 10: task_count=13
- iter 20: task_count=23
- iter 30: task_count=33

before fix, Dremio-backed transport repro
- iter 10: task_count=13
- iter 20: task_count=23
```

And after the request-scoped task-group fix:

```text
after fix, standalone repro
- iter 10: task_count=0
- iter 20: task_count=0

after fix, Dremio-backed transport repro
- iter 10: task_count=0
- iter 20: task_count=0
```

That was the decisive proof that the root issue was task ownership and teardown, not
just expensive response payloads.

At that point the hypothesis became:

- stateless per-request tasks are being started in the wrong task group
- the long-lived manager task group is retaining request-scoped work

## Standalone Repro

To prove this was not Dremio-specific, we built a plain FastMCP repro:

- [tests/e2e/test_stateless_mcp_standalone_repro.py](/Users/aniket.kulkarni/ws/github/aniket-s-kulkarni/dremio-mcp/tests/e2e/test_stateless_mcp_standalone_repro.py:1)

That repro:

- uses plain `FastMCP`
- enables `stateless_http=True`
- defines one tool returning a large payload
- runs repeated tool calls through Streamable HTTP
- samples:
  - RSS after `gc.collect()`
  - task count in `session_manager._task_group`
  - task coroutine type counts

It reproduced the same task growth without any Dremio tooling.

That made it suitable for an upstream bug report and regression test.

## Understanding the Broken Ownership Model

The stateless handler was effectively doing this:

1. create a per-request transport
2. start `run_stateless_server`
3. attach it to the session manager's long-lived task group
4. handle one request
5. return

This is a structured concurrency bug.

In AnyIO, a task group owns its child tasks until they exit. If per-request tasks are
started in a process-lifetime task group, then request lifetimes are no longer the
owner boundary. That makes it easy for request objects to survive too long.

For stateless HTTP, the correct owner boundary is the request itself.

## The Fix

The fix is small and local:

- create a request-scoped AnyIO task group inside `_handle_stateless_request()`
- start `run_stateless_server` there
- handle the request
- cancel and exit the request-scoped group immediately after the response completes

That change is captured in:

- [patches/0001-fix-stateless-streamable-http-task-lifecycle.patch](/Users/aniket.kulkarni/ws/github/aniket-s-kulkarni/dremio-mcp/patches/0001-fix-stateless-streamable-http-task-lifecycle.patch:1)

## Validation

After the fix:

- standalone repro task delta dropped to `0`
- Dremio-backed repro task delta dropped to `0`
- RSS remained effectively flat in both cases

A representative after-fix run looked like this:

```text
standalone repro
- structured_rss_delta_mb=0.1
- text_rss_delta_mb=0.1
- structured_task_delta=0
- text_task_delta=0

Dremio-backed repro
- baseline_delta_mb=0.1
- compact_delta_mb=0.0
- baseline_task_delta=0
- compact_task_delta=0
```

That is the strongest evidence that the root leak mechanism was fixed.

## Separate Local App Issue

We also found a local app-level issue:

- our normal server lifespan started a `_settings_refresh_loop()`
- in stateless HTTP mode, the SDK creates a fresh `app.run()` task per request
- so using the normal lifespan created one refresh task per request

We fixed that separately with a no-op stateless lifespan in:

- [mcp.py](/Users/aniket.kulkarni/ws/github/aniket-s-kulkarni/dremio-mcp/src/dremioai/servers/mcp.py:624)

That was worth fixing, but it was not the root SDK leak because the standalone repro
proved the SDK issue existed without Dremio code.

## What Remains

There are still occasional `sse_starlette` `_shutdown_watcher()` pending-task warnings
at teardown. Those warnings did not accumulate after the task-group fix, so they look
like shutdown cleanup noise rather than the main leak.

They are worth a follow-up, but they should be tracked separately from the stateless
task accumulation bug.

## Lessons

- Fixing big payloads can reduce symptoms without fixing the actual leak.
- Direct-invoke vs transport-level testing is a powerful split for narrowing blame.
- Task population is often a better leak indicator than RSS alone.
- Structured concurrency bugs are really ownership bugs: the wrong parent scope keeps
  the wrong children alive.
