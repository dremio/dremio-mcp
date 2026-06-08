# Upstream Patch Summary: Stateless Streamable HTTP Task Lifecycle

## Summary

This patch fixes a memory leak in the MCP Python SDK stateless Streamable HTTP path.

The leak is caused by starting a per-request `run_stateless_server` task inside the
session manager's long-lived AnyIO task group. In stateless mode, request-scoped
server tasks should die with the request. Instead, they were being parented by the
manager-wide task group, which caused task objects and request-linked transport state
to accumulate across calls.

The fix is small:

- keep the session manager's top-level task group for manager lifecycle only
- create a fresh request-scoped task group inside `_handle_stateless_request()`
- start `run_stateless_server` inside that local task group
- cancel that local group as soon as `http_transport.handle_request(...)` completes

## Root Cause

The original stateless handler did this conceptually:

1. create a new `StreamableHTTPServerTransport`
2. start `run_stateless_server`
3. attach that task to `self._task_group`
4. handle one HTTP request
5. return

That is the wrong ownership model for stateless requests.

`self._task_group` lives for the entire lifetime of the session manager. Any task
started there remains reachable through the group's internal task set until the task
fully exits and the group cleans it up. If teardown is delayed or incomplete, those
tasks retain request-linked objects, which makes memory usage ratchet upward.

## Fix

The patched code changes `_handle_stateless_request()` to:

```python
async with anyio.create_task_group() as request_tg:
    await request_tg.start(run_stateless_server)
    try:
        await http_transport.handle_request(scope, receive, send)
    finally:
        request_tg.cancel_scope.cancel()
```

This matches stateless semantics:

- one request
- one transport
- one server task
- one request-scoped task group

When the request finishes, the task group exits and AnyIO guarantees cancellation and
cleanup of its child tasks.

## Why This Fix Is Correct

- Stateless HTTP should not preserve per-request server tasks across requests.
- A request-scoped task group gives the correct lifetime boundary.
- Stateful mode is unchanged because stateful sessions intentionally survive across
  multiple requests and still belong in the manager-level task group.

## AnyIO / Transport / App Lifecycle Notes

### What is AnyIO?

AnyIO is an async concurrency library that provides structured concurrency on top of
`asyncio` and `trio`-style concepts. Its task groups are similar to nursery scopes:
child tasks belong to a parent scope and are cancelled/awaited as that scope exits.

### What does `http_transport.connect()` do?

`http_transport.connect()` opens the internal stream pair used to bridge the HTTP
transport layer with the MCP server runtime. It yields the `read_stream` and
`write_stream` consumed by `app.run(...)`.

### What does `app.run(...)` do?

`app.run(...)` is the server's request-processing loop for the connected transport
streams. In stateless mode, it should live only as long as the single request-bound
transport connection.

## Validation

Two repros validate the fix:

- standalone FastMCP repro in
  [tests/e2e/test_stateless_mcp_standalone_repro.py](/Users/aniket.kulkarni/ws/github/aniket-s-kulkarni/dremio-mcp/tests/e2e/test_stateless_mcp_standalone_repro.py:1)
- Dremio-backed repro in
  [tests/e2e/test_runsql_transport_retention_repro.py](/Users/aniket.kulkarni/ws/github/aniket-s-kulkarni/dremio-mcp/tests/e2e/test_runsql_transport_retention_repro.py:1)

With the patch:

- task-count growth drops to `0`
- RSS stays effectively flat across repeated requests

## Remaining Noise

There are still occasional `sse_starlette` `_shutdown_watcher()` pending-task warnings
at teardown. Those do not accumulate with requests after this fix and appear to be a
separate shutdown-cleanup concern, not the original leak.
