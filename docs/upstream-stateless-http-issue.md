# Upstream Issue Draft: Stateless Streamable HTTP Task Accumulation Causes Memory Growth

## Title

Stateless Streamable HTTP starts per-request server tasks in the manager task group, causing task accumulation and memory growth

## Summary

In stateless Streamable HTTP mode, the MCP Python SDK appears to start a fresh
per-request `run_stateless_server` task inside the session manager's long-lived AnyIO
task group. Repeated requests cause task objects to accumulate, and retained
request/response state drives memory growth.

This reproduces without application-specific code using plain `FastMCP`.

## Affected Area

- `mcp/server/streamable_http_manager.py`
- stateless Streamable HTTP request handling

## Observed Behavior

Under repeated stateless HTTP tool calls:

- `session_manager._task_group._tasks` grows with requests
- RSS ratchets upward
- large structured tool results amplify the memory cost

Before patching, the leak signal was strongest in task count, not just RSS.

## Expected Behavior

In stateless mode, per-request server/transport tasks should be torn down when the
request completes. Task count should stay flat after warmup.

## Standalone Repro

A standalone repro exists here:

- [tests/e2e/test_stateless_mcp_standalone_repro.py](/Users/aniket.kulkarni/ws/github/aniket-s-kulkarni/dremio-mcp/tests/e2e/test_stateless_mcp_standalone_repro.py:1)

Characteristics:

- plain `FastMCP`
- `stateless_http=True`
- one large tool response
- two modes:
  - `structured`: returns a dict
  - `text`: returns a JSON string
- samples:
  - RSS after `gc.collect()`
  - `len(mcp.session_manager._task_group._tasks)`
  - task coroutine name counts

## Repro Command

```bash
MCP_STATELESS_STANDALONE_ITERS=20 \
MCP_STATELESS_STANDALONE_SAMPLE_EVERY=10 \
MCP_STATELESS_STANDALONE_MODE=both \
uv run pytest tests/e2e/test_stateless_mcp_standalone_repro.py -s
```

## Root Cause

The stateless handler starts `run_stateless_server` under the manager-wide task group.
That task group lives for the session manager lifetime, not the request lifetime.

Stateless request tasks therefore have the wrong owner.

If request teardown is delayed or incomplete, the group retains the tasks and the
objects reachable from them, including request/response/session state.

## Proposed Fix

Use a request-scoped AnyIO task group in `_handle_stateless_request()`:

```python
async with anyio.create_task_group() as request_tg:
    await request_tg.start(run_stateless_server)
    try:
        await http_transport.handle_request(scope, receive, send)
    finally:
        request_tg.cancel_scope.cancel()
```

This keeps the manager-wide task group for manager lifecycle only and makes
stateless per-request tasks die with the request.

## Validation After Patch

With the request-scoped task group fix applied locally:

- standalone repro:
  - `structured_task_delta=0`
  - `text_task_delta=0`
  - RSS delta approximately flat
- Dremio-backed repro:
  - `baseline_task_delta=0`
  - `compact_task_delta=0`
  - RSS delta approximately flat

## Notes on Structured Content

This investigation also found that structured tool responses can be materially more
expensive than text-only responses because of mirrored content/serialization behavior.
That increases per-request memory cost, but it is not the root leak mechanism.

Even text-only responses showed the stateless task accumulation before the lifecycle
fix.

## Additional Observation

There are still occasional `sse_starlette` `_shutdown_watcher()` pending-task warnings
at teardown after the lifecycle fix. Those warnings do not appear to accumulate with
requests and may be a separate shutdown cleanup issue.
