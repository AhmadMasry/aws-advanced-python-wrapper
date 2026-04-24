# Phase C — Async EFM Standing Watchdog

> **For agentic workers:** Use superpowers:subagent-driven-development.

**Goal:** Close audit P0 #5 + associated HIGH/MED findings on `AsyncHostMonitoringPlugin` by converting the short-lived "watchdog task per execute call" into a standing background task that runs for the connection's lifetime, tracking failure counts across intervals and marking hosts UNAVAILABLE when the threshold is hit.

**Architecture:** One `asyncio.Task` per plugin instance (= per connection). Started lazily on first `execute`. Runs until plugin teardown. Probes `driver_dialect.ping(conn)` on `interval_ms` cadence after the grace period. Maintains a persistent `_consecutive_failures` counter that survives between executes. On threshold, marks all aliases UNAVAILABLE via `plugin_service.set_availability`, aborts the connection, raises `AwsWrapperError` on the next call. Hooks `notify_connection_changed` to restart the monitor when the connection swaps (e.g., post-failover).

Simpler than sync v1 (which shares one monitor across connections to the same host). Parity at the **behavior-level** — each connection gets its own standing watchdog — is what the audit requires. Cross-connection sharing can follow as a later refinement if needed.

**Tech Stack:** Python 3.10–3.14. Reuses `plugin_service.set_availability` (A.3), `AsyncCanReleaseResources` (A.7 follow-up), `register_shutdown_hook` (cleanup.py). `HostAvailability`, `HostInfo.as_aliases()`.

---

## File structure

**Modify:**
- `aws_advanced_python_wrapper/aio/host_monitoring_plugin.py` — lifecycle rewrite
- `tests/unit/test_aio_host_monitoring_plugin.py` — extend with lifecycle + failure-threshold + notify-hook tests

**No new files.**

---

## Task C.1: Standing monitor task + host aliases

Replace the short-lived per-execute task with a lazy-started standing task. The task starts on the first `execute` call after the plugin is enabled, and runs until `release_resources` (implemented in C.3). Maintains failure count across probes.

Use `HostInfo.as_aliases()` to know which aliases belong to this host — sync v1 uses these for host-level monitoring keyed by alias set.

### Files
- Modify: `aws_advanced_python_wrapper/aio/host_monitoring_plugin.py`
- Test: `tests/unit/test_aio_host_monitoring_plugin.py`

### TDD

- [ ] **C.1.1: Inspect existing tests** — read `tests/unit/test_aio_host_monitoring_plugin.py` top-to-bottom. Note the `_build_plugin` helper (if any). Identify tests that depend on the OLD per-execute lifecycle — those will need updating (the standing-task model means the monitor task persists across execute calls, not cancel-after-each).

- [ ] **C.1.2: Write failing tests** for the new lifecycle:

```python
def test_monitor_task_starts_lazily_on_first_execute():
    """Standing monitor task is created on first execute, not at plugin construction."""
    plugin, svc, driver_dialect = _build_plugin()
    svc._current_connection = MagicMock(name="conn")
    svc._current_host_info = HostInfo(host="h1", port=5432, role=HostRole.WRITER)
    driver_dialect.ping = AsyncMock(return_value=True)

    # No task before first execute
    assert plugin._monitor_task is None

    async def _noop():
        return None

    asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _noop))
    # After first execute the task exists (may already be canceled since
    # plugin didn't exit cleanly, but it was created)
    assert plugin._monitor_task is not None


def test_monitor_task_persists_across_multiple_executes():
    """Single standing task, not a new task per execute."""
    plugin, svc, driver_dialect = _build_plugin()
    svc._current_connection = MagicMock(name="conn")
    svc._current_host_info = HostInfo(host="h1", port=5432, role=HostRole.WRITER)
    driver_dialect.ping = AsyncMock(return_value=True)

    async def _run_two_executes():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        task_1 = plugin._monitor_task
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        task_2 = plugin._monitor_task
        return task_1, task_2

    t1, t2 = asyncio.run(_run_two_executes())
    assert t1 is t2


def test_failure_count_persists_across_intervals():
    """Consecutive failures accumulate; a single successful ping resets the counter."""
    plugin, svc, driver_dialect = _build_plugin(
        failure_count_threshold=3,
        interval_ms=10,
        grace_ms=0)
    svc._current_connection = MagicMock(name="conn")
    svc._current_host_info = HostInfo(host="h1", port=5432, role=HostRole.WRITER)

    ping_results = [False, False, True, False]
    driver_dialect.ping = AsyncMock(side_effect=ping_results)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        # Let monitor run through enough iterations
        await asyncio.sleep(0.2)
        return plugin._consecutive_failures

    final_count = asyncio.run(_run())
    # After False, False, True: counter reset to 0. Then False: 1. So <3.
    assert final_count < 3
```

- [ ] **C.1.3: Run — verify fail**

- [ ] **C.1.4: Rewrite the plugin**

Key structural changes:
- Add instance attrs: `_monitor_task: Optional[asyncio.Task] = None`, `_monitor_stop: asyncio.Event`, `_consecutive_failures: int = 0`, `_monitored_aliases: FrozenSet[str] = frozenset()`, `_host_unavailable: bool = False`.
- Add `_ensure_monitor_started(conn, host_info)` — starts the task if not already running. Records `_monitored_aliases = host_info.as_aliases()`.
- The monitor coroutine loops until `_monitor_stop` is set OR `_host_unavailable` is True. Probes via `ping`. Updates `_consecutive_failures`. On threshold, sets `_host_unavailable = True` and exits.
- `execute` calls `_ensure_monitor_started`, then awaits `execute_func()` as before. Does NOT cancel the task afterward.
- If `_host_unavailable` is True at `execute` entry, raise `AwsWrapperError` (deferred to C.2 — for C.1, leave `_host_unavailable` as a signal only, don't raise yet; add the raise in C.2).

- [ ] **C.1.5: Run — verify pass** + full suite no regressions + lint/types clean.

- [ ] **C.1.6: Commit**:

```bash
git add aws_advanced_python_wrapper/aio/host_monitoring_plugin.py tests/unit/test_aio_host_monitoring_plugin.py
git commit -m "refactor(aio): standing monitor task for EFM (phase C.1)

Replaces the short-lived per-execute monitor task with a standing
asyncio.Task started lazily on the first execute and living for the
connection's lifetime. Failure counts accumulate across probes rather
than resetting on every query.

Tracks HostInfo.as_aliases() so UNAVAILABLE markings in later tasks
apply to the full alias set (same shape as sync EFM uses).

Partial close of audit P0 #5. C.2 adds the UNAVAILABLE-host raise;
C.3 adds notify_connection_changed / notify_host_list_changed hooks;
C.4 wires release_resources cleanup."
```

---

## Task C.2: Mark host UNAVAILABLE + close connection + raise on threshold

When the monitor detects `failure_count_threshold` consecutive failures:
1. Call `plugin_service.set_availability(_monitored_aliases, HostAvailability.UNAVAILABLE)`.
2. Abort the connection via `driver_dialect.abort_connection(conn)`.
3. Set `_host_unavailable = True` so subsequent `execute` calls immediately raise `AwsWrapperError`.

Mirrors sync `host_monitoring_plugin.py:139-151`.

### Files
- Modify: `aws_advanced_python_wrapper/aio/host_monitoring_plugin.py`
- Test: `tests/unit/test_aio_host_monitoring_plugin.py`

### TDD

- [ ] **C.2.1: Write failing tests** — append:

```python
def test_threshold_breach_marks_host_unavailable_and_aborts():
    plugin, svc, driver_dialect = _build_plugin(
        failure_count_threshold=2, interval_ms=10, grace_ms=0)
    svc._current_connection = MagicMock(name="conn")
    svc._current_host_info = HostInfo(host="h1", port=5432, role=HostRole.WRITER)
    svc.set_availability = MagicMock()
    driver_dialect.abort_connection = AsyncMock()
    driver_dialect.ping = AsyncMock(return_value=False)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        # Let monitor burn through threshold
        await asyncio.sleep(0.2)

    asyncio.run(_run())

    svc.set_availability.assert_any_call(
        svc._current_host_info.as_aliases(), HostAvailability.UNAVAILABLE)
    driver_dialect.abort_connection.assert_awaited()


def test_execute_raises_when_host_already_unavailable():
    plugin, svc, driver_dialect = _build_plugin()
    svc._current_connection = MagicMock(name="conn")
    svc._current_host_info = HostInfo(host="h1", port=5432, role=HostRole.WRITER)
    plugin._host_unavailable = True  # simulate prior threshold breach

    async def _noop():
        return None

    with pytest.raises(AwsWrapperError):
        asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _noop))
```

- [ ] **C.2.2: Run — verify fail**.

- [ ] **C.2.3: Implement** in the monitor loop: on threshold hit, execute the 3-step sequence and exit the loop. In `execute`: short-circuit raise if `_host_unavailable` is True.

- [ ] **C.2.4: Run — verify pass**.

- [ ] **C.2.5: Commit**:

```bash
git add aws_advanced_python_wrapper/aio/host_monitoring_plugin.py tests/unit/test_aio_host_monitoring_plugin.py
git commit -m "fix(aio): mark host UNAVAILABLE and abort connection on EFM threshold (phase C.2)

AsyncHostMonitoringPlugin now matches sync host_monitoring_plugin.py:139-151:
when the consecutive-failure threshold is hit, the host is marked
UNAVAILABLE on all aliases, the connection is aborted, and subsequent
execute calls raise AwsWrapperError immediately.

Closes audit P0 #5 MED finding (pool cleanup on failure)."
```

---

## Task C.3: `notify_connection_changed` / `notify_host_list_changed` hooks

When the connection swaps (e.g., post-failover) or the topology reports the host went down, reset the monitor state. Sync analog: `host_monitoring_plugin.py:155-175`.

### Files
- Modify: `aws_advanced_python_wrapper/aio/host_monitoring_plugin.py`
- Test: `tests/unit/test_aio_host_monitoring_plugin.py`

### TDD

- [ ] **C.3.1: Write failing tests** — append:

```python
def test_notify_connection_changed_resets_monitor():
    """Connection swap clears the failure counter and restarts the task."""
    plugin, svc, driver_dialect = _build_plugin(
        failure_count_threshold=5, interval_ms=50, grace_ms=0)
    svc._current_connection = MagicMock(name="conn_old")
    svc._current_host_info = HostInfo(host="h1", port=5432, role=HostRole.WRITER)
    driver_dialect.ping = AsyncMock(return_value=False)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        await asyncio.sleep(0.15)
        prior_failures = plugin._consecutive_failures
        from aws_advanced_python_wrapper.utils.notifications import \
            ConnectionEvent
        plugin.notify_connection_changed({ConnectionEvent.CONNECTION_OBJECT_CHANGED})
        return prior_failures, plugin._consecutive_failures

    prior, after = asyncio.run(_run())
    assert prior > 0  # accumulated some failures
    assert after == 0  # reset by notify


def test_notify_host_list_changed_with_host_down_stops_monitor():
    """If topology says our host WENT_DOWN, stop the monitor."""
    plugin, svc, driver_dialect = _build_plugin()
    svc._current_connection = MagicMock(name="conn")
    h1 = HostInfo(host="h1", port=5432, role=HostRole.WRITER)
    svc._current_host_info = h1
    driver_dialect.ping = AsyncMock(return_value=True)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        from aws_advanced_python_wrapper.utils.notifications import HostEvent
        plugin.notify_host_list_changed({h1.as_alias(): {HostEvent.WENT_DOWN}})

    asyncio.run(_run())
    # Monitor stop event fired
    assert plugin._monitor_stop.is_set()
```

- [ ] **C.3.2: Run — verify fail**.

- [ ] **C.3.3: Implement** `notify_connection_changed(changes: Set[ConnectionEvent])` and override the existing `notify_host_list_changed(changes: Dict[str, Set[HostEvent]])`:

```python
    def notify_connection_changed(self, changes):
        """Connection swap -> reset monitor counter + cancel task (restart on next execute)."""
        self._consecutive_failures = 0
        self._host_unavailable = False
        self._monitor_stop.set()
        self._monitor_stop = asyncio.Event()
        # Task will be restarted lazily by the next execute.
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            self._monitor_task = None

    def notify_host_list_changed(self, changes):
        """Topology change -> if our monitored host WENT_DOWN, stop the monitor."""
        if not self._monitored_aliases:
            return
        for alias, events in changes.items():
            if alias in self._monitored_aliases:
                # Check for WENT_DOWN or HOST_DELETED
                from aws_advanced_python_wrapper.utils.notifications import \
                    HostEvent
                if HostEvent.WENT_DOWN in events or HostEvent.HOST_DELETED in events:
                    self._monitor_stop.set()
                    return
```

- [ ] **C.3.4: Run — verify pass**.

- [ ] **C.3.5: Commit**:

```bash
git add aws_advanced_python_wrapper/aio/host_monitoring_plugin.py tests/unit/test_aio_host_monitoring_plugin.py
git commit -m "feat(aio): EFM reacts to connection-change + host-list-change events (phase C.3)

Mirrors sync host_monitoring_plugin.py:155-175. On
notify_connection_changed the failure counter and unavailable flag
reset + the old monitor task is canceled; the next execute starts a
fresh one. On notify_host_list_changed with WENT_DOWN/HOST_DELETED
for a monitored alias, the current monitor stops.

Closes audit P0 #5 MED finding (notify hooks missing)."
```

---

## Task C.4: Cleanup via AsyncCanReleaseResources + register_shutdown_hook

The plugin currently has no teardown. With a standing task, we need deterministic cancellation on connection close. Two avenues:
1. Make the plugin itself implement `AsyncCanReleaseResources` so `release_resources_async` catches it (requires wiring — plugins aren't currently hook-registered).
2. Register a shutdown hook via `aio.cleanup.register_shutdown_hook` on plugin construction that cancels the task.

Option 2 is simpler and doesn't require re-plumbing the plugin lifecycle.

### Files
- Modify: `aws_advanced_python_wrapper/aio/host_monitoring_plugin.py`
- Test: `tests/unit/test_aio_host_monitoring_plugin.py`

### TDD

- [ ] **C.4.1: Write failing test** — append:

```python
def test_release_resources_async_cancels_monitor_task():
    from aws_advanced_python_wrapper.aio.cleanup import release_resources_async

    plugin, svc, driver_dialect = _build_plugin()
    svc._current_connection = MagicMock(name="conn")
    svc._current_host_info = HostInfo(host="h1", port=5432, role=HostRole.WRITER)
    driver_dialect.ping = AsyncMock(return_value=True)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        await asyncio.sleep(0.05)
        task = plugin._monitor_task
        await release_resources_async()
        return task

    task = asyncio.run(_run())
    # After release_resources_async the monitor task should be finished
    assert task is None or task.done()
```

- [ ] **C.4.2: Run — verify fail**.

- [ ] **C.4.3: Implement**: in `__init__`, register a shutdown hook:

```python
    def __init__(self, plugin_service, props):
        ...
        from aws_advanced_python_wrapper.aio.cleanup import \
            register_shutdown_hook
        register_shutdown_hook(self._shutdown)

    async def _shutdown(self):
        self._monitor_stop.set()
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except (asyncio.CancelledError, Exception):
                pass
            self._monitor_task = None
```

- [ ] **C.4.4: Run — verify pass**.

- [ ] **C.4.5: Commit**:

```bash
git add aws_advanced_python_wrapper/aio/host_monitoring_plugin.py tests/unit/test_aio_host_monitoring_plugin.py
git commit -m "feat(aio): EFM registers shutdown hook for deterministic cleanup (phase C.4)

AsyncHostMonitoringPlugin registers a shutdown hook via
aio.cleanup.register_shutdown_hook on construction. On
release_resources_async the standing monitor task is canceled cleanly,
so async apps calling release_resources_async() on exit don't leak
running tasks.

Closes Phase C."
```

---

## Task C.5: Final /verify

- [ ] mypy + flake8 + isort + pytest -Werror clean.
- [ ] Spot-check `aws_advanced_python_wrapper/aio/host_monitoring_plugin.py` diff from Phase B baseline — confirm standing monitor, failure count persistence, UNAVAILABLE + abort on threshold, notify hooks, cleanup hook.

---

## Out of scope (for Phase C)

- Cross-connection monitor sharing (one Monitor per host alias set, like sync v1) — Phase C delivers per-connection watchdog, which closes audit P0 #5 behaviorally. Cross-connection sharing is an optimization, not a parity fix.
- Telemetry (`efm.monitored_connections.count` etc.) — Phase I.
- Aurora instance identification for cluster endpoints (sync `_get_monitoring_host_info` calls `fill_aliases` for cluster endpoints) — Phase G (topology monitor).
- Sync v2 alignment (the new async model with per-connection standing task is closer to sync v1 shape; sync v2 uses an inline-probe-only approach that async already approximates). The audit flagged v1 behavior gaps; we match v1.

## Self-review

- [x] All P0 #5 audit findings mapped: standing monitor (C.1), alias tracking (C.1), failure count across intervals (C.1), pool cleanup on failure (C.2), notify hooks (C.3).
- [x] Each task references sync line ranges.
- [x] Python 3.10-compatible throughout.
- [x] No forward refs to Phase D-J.
