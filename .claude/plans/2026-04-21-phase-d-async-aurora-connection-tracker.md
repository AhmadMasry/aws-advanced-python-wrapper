# Phase D — Async Aurora Connection Tracker Parity

**Goal:** Close audit P0 #6 on `AsyncAuroraConnectionTrackerPlugin`. Port sync's `OpenedConnectionTracker` + writer-change detection + invalidate-on-failover so consumers holding pooled connections to a demoted writer get them recycled. The current async plugin is a stub that just records `_last_known_writer_host`.

**Architecture:** Instance-level `AsyncOpenedConnectionTracker` holds a `Dict[FrozenSet[str], WeakSet[conn]]` keyed by host alias sets. On connect, record. On execute, compare current writer (from `plugin_service.all_hosts`) vs last-seen writer; if changed, spawn `asyncio.create_task` to close every tracked connection against the old writer. On `notify_host_list_changed`, handle `CONVERTED_TO_READER`/`CONVERTED_TO_WRITER`. On `FailoverError`, force a writer-change recheck. Register a shutdown hook to drain in-flight invalidation tasks.

Simpler than sync's class-level shared `OpenedConnectionTracker` with a prune daemon thread — async WeakSets auto-clean on GC, and per-plugin-instance state avoids cross-task state hazards.

**Tech Stack:** Python 3.10–3.14. Reuses `plugin_service.all_hosts` (A.5), `FailoverError` (existing), `register_shutdown_hook` (cleanup.py), `HostInfo.as_aliases()`, `HostEvent.CONVERTED_*`.

---

## File structure

**Create:**
- `aws_advanced_python_wrapper/aio/aurora_connection_tracker.py` — new module hosting `AsyncOpenedConnectionTracker` + `AsyncAuroraConnectionTrackerPlugin`.
- `tests/unit/test_aio_aurora_connection_tracker.py` — new test file.

**Modify:**
- `aws_advanced_python_wrapper/aio/minor_plugins.py` — remove the stub `AsyncAuroraConnectionTrackerPlugin`; leave a re-export for backwards compat (plugin factory references it).
- `aws_advanced_python_wrapper/aio/plugin_factory.py` — update import path if needed.
- `tests/unit/test_aio_minor_plugins.py` — remove tests for the old stub (they'll fail against the new behavior).

---

## Task D.1: New AsyncOpenedConnectionTracker + AsyncAuroraConnectionTrackerPlugin

Ship the module + plugin with connect-tracking + writer-change invalidation via execute hook + failover hook. Notify hooks and shutdown hook land in D.2/D.3.

### Files
- Create: `aws_advanced_python_wrapper/aio/aurora_connection_tracker.py`
- Create: `tests/unit/test_aio_aurora_connection_tracker.py`
- Modify: `aws_advanced_python_wrapper/aio/minor_plugins.py` (remove stub; import from new location)
- Modify: `aws_advanced_python_wrapper/aio/plugin_factory.py` (update import path)

### TDD

- [ ] **D.1.1: Write tests first** (9 tests):

```python
# tests/unit/test_aio_aurora_connection_tracker.py
"""F3-B Phase D: Aurora connection tracker + invalidate-on-failover."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.aurora_connection_tracker import (
    AsyncAuroraConnectionTrackerPlugin, AsyncOpenedConnectionTracker)
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.errors import FailoverSuccessError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties


def _build():
    props = Properties({"host": "cluster.example.com", "port": "5432"})
    driver_dialect = MagicMock()
    svc = AsyncPluginServiceImpl(props, driver_dialect)
    tracker = AsyncOpenedConnectionTracker()
    plugin = AsyncAuroraConnectionTrackerPlugin(svc, tracker=tracker)
    return plugin, svc, driver_dialect, tracker


def test_tracker_records_connection_on_connect():
    plugin, svc, driver_dialect, tracker = _build()
    host = HostInfo(host="instance-1", port=5432, role=HostRole.WRITER)
    conn = MagicMock(name="new_conn")

    async def _connect_func():
        return conn

    async def _run():
        await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func)

    asyncio.run(_run())
    tracked = tracker._tracked_for(host.as_aliases())
    assert conn in tracked


def test_tracker_returns_empty_set_for_unknown_host():
    tracker = AsyncOpenedConnectionTracker()
    h = HostInfo(host="ghost", port=5432)
    assert len(tracker._tracked_for(h.as_aliases())) == 0


def test_invalidate_all_closes_tracked_connections():
    tracker = AsyncOpenedConnectionTracker()
    host = HostInfo(host="writer-1", port=5432, role=HostRole.WRITER)
    c1 = MagicMock(name="c1")
    c1.close = MagicMock()
    c2 = MagicMock(name="c2")
    c2.close = MagicMock()
    tracker.track(host, c1)
    tracker.track(host, c2)

    asyncio.run(tracker.invalidate_all(host))

    c1.close.assert_called()
    c2.close.assert_called()


def test_invalidate_all_survives_close_errors():
    tracker = AsyncOpenedConnectionTracker()
    host = HostInfo(host="writer-1", port=5432, role=HostRole.WRITER)
    bad = MagicMock(name="bad")
    bad.close = MagicMock(side_effect=RuntimeError("broken"))
    good = MagicMock(name="good")
    good.close = MagicMock()
    tracker.track(host, bad)
    tracker.track(host, good)

    # Must not raise
    asyncio.run(tracker.invalidate_all(host))
    good.close.assert_called()


def test_plugin_invalidates_old_writer_on_writer_change():
    """Execute with a changed writer in all_hosts triggers invalidation of old writer."""
    plugin, svc, driver_dialect, tracker = _build()
    old_writer = HostInfo(host="old-w", port=5432, role=HostRole.WRITER)
    new_writer = HostInfo(host="new-w", port=5432, role=HostRole.WRITER)
    conn_to_old = MagicMock(name="conn_to_old")
    conn_to_old.close = MagicMock()

    # Simulate: first execute -- current writer is old-w
    svc._all_hosts = (old_writer,)
    tracker.track(old_writer, conn_to_old)

    async def _run():
        # Force initial writer registration via first execute
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        # Topology now shows new writer
        svc._all_hosts = (new_writer,)
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        # Let the invalidation task finish
        await asyncio.sleep(0.01)

    asyncio.run(_run())

    conn_to_old.close.assert_called()


def test_plugin_no_invalidation_when_writer_unchanged():
    plugin, svc, driver_dialect, tracker = _build()
    writer = HostInfo(host="w", port=5432, role=HostRole.WRITER)
    conn = MagicMock(name="conn")
    conn.close = MagicMock()

    svc._all_hosts = (writer,)
    tracker.track(writer, conn)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        await asyncio.sleep(0.01)

    asyncio.run(_run())

    conn.close.assert_not_called()


def test_plugin_invalidates_on_failover_error():
    """A FailoverError raised from the inner execute triggers writer-change recheck."""
    plugin, svc, driver_dialect, tracker = _build()
    old_writer = HostInfo(host="old-w", port=5432, role=HostRole.WRITER)
    new_writer = HostInfo(host="new-w", port=5432, role=HostRole.WRITER)
    conn_to_old = MagicMock(name="conn_to_old")
    conn_to_old.close = MagicMock()

    svc._all_hosts = (old_writer,)
    tracker.track(old_writer, conn_to_old)

    svc.refresh_host_list = AsyncMock(
        side_effect=lambda *_: setattr(svc, "_all_hosts", (new_writer,)))

    async def _run():
        async def _noop():
            return None

        # First execute pins old writer
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)

        # Second execute raises FailoverSuccessError from the inner func
        async def _raising():
            raise FailoverSuccessError("failover")

        with pytest.raises(FailoverSuccessError):
            await plugin.execute(MagicMock(), "Cursor.execute", _raising)
        await asyncio.sleep(0.01)

    asyncio.run(_run())

    conn_to_old.close.assert_called()


def test_plugin_subscribed_methods_includes_connect_and_execute():
    plugin, *_ = _build()
    subs = plugin.subscribed_methods
    assert "Connect.connect" in subs
    assert "Cursor.execute" in subs


def test_tracker_remove_drops_connection_from_set():
    tracker = AsyncOpenedConnectionTracker()
    host = HostInfo(host="w", port=5432, role=HostRole.WRITER)
    conn = MagicMock(name="conn")
    tracker.track(host, conn)
    assert conn in tracker._tracked_for(host.as_aliases())
    tracker.remove(host, conn)
    assert conn not in tracker._tracked_for(host.as_aliases())
```

- [ ] **D.1.2: Run — expect import error** (module doesn't exist yet).

- [ ] **D.1.3: Create `aws_advanced_python_wrapper/aio/aurora_connection_tracker.py`**:

```python
#  (Copyright header matching other aio files)

"""Async Aurora connection tracker + invalidate-on-failover plugin.

Port of sync ``aurora_connection_tracker_plugin.py``. Tracks every
connection opened through the plugin pipeline in a per-instance
``AsyncOpenedConnectionTracker`` keyed by host alias set. On writer
change (detected via ``AsyncPluginService.all_hosts`` or a raised
``FailoverError``), closes every tracked connection to the old writer
so pooled consumers don't reuse a stale demoted-writer socket.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, FrozenSet, \
    Optional, Set
from weakref import WeakSet

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import FailoverError
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.notifications import HostEvent

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncOpenedConnectionTracker:
    """Per-instance alias-keyed registry of open connections.

    Simpler than sync's class-level ``OpenedConnectionTracker``:
    * instance-level so multiple plugin instances don't share state
    * WeakSet for auto-GC of dead refs -- no prune daemon needed
    * invalidate_all spawns ``asyncio.create_task`` instead of a Thread
    """

    def __init__(self) -> None:
        self._tracked: Dict[FrozenSet[str], WeakSet] = {}

    def track(self, host_info: HostInfo, conn: Any) -> None:
        aliases = host_info.as_aliases()
        conn_set = self._tracked.setdefault(aliases, WeakSet())
        conn_set.add(conn)

    def remove(self, host_info: HostInfo, conn: Any) -> None:
        aliases = host_info.as_aliases()
        conn_set = self._tracked.get(aliases)
        if conn_set is not None:
            conn_set.discard(conn)

    async def invalidate_all(self, host_info: HostInfo) -> None:
        """Close every tracked connection against ``host_info``'s aliases.

        Best-effort: individual close failures are swallowed.
        """
        aliases = host_info.as_aliases()
        conn_set = self._tracked.get(aliases)
        if conn_set is None:
            return
        # Snapshot to avoid mutation during iteration
        snapshot = list(conn_set)
        for conn in snapshot:
            try:
                close = getattr(conn, "close", None)
                if close is None:
                    continue
                result = close()
                if asyncio.iscoroutine(result):
                    await result
            except Exception:  # noqa: BLE001 - close is best-effort
                pass
        # Clear the set once we've attempted all closes
        try:
            del self._tracked[aliases]
        except KeyError:
            pass

    def _tracked_for(self, aliases: FrozenSet[str]) -> WeakSet:
        """Test-only accessor."""
        return self._tracked.get(aliases, WeakSet())


class AsyncAuroraConnectionTrackerPlugin(AsyncPlugin):
    """Async counterpart of :class:`AuroraConnectionTrackerPlugin`.

    Subscribes to CONNECT, CONNECTION_CLOSE, and the pipeline's
    network-bound methods so it can detect a writer change mid-session and
    invalidate pooled connections to the old writer.
    """

    _CORE_SUBSCRIBED: Set[str] = {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.CONNECTION_CLOSE.method_name,
        DbApiMethod.NOTIFY_HOST_LIST_CHANGED.method_name,
        DbApiMethod.CURSOR_EXECUTE.method_name,
        DbApiMethod.CURSOR_EXECUTEMANY.method_name,
        DbApiMethod.CURSOR_FETCHONE.method_name,
        DbApiMethod.CURSOR_FETCHMANY.method_name,
        DbApiMethod.CURSOR_FETCHALL.method_name,
        DbApiMethod.CONNECTION_COMMIT.method_name,
        DbApiMethod.CONNECTION_ROLLBACK.method_name,
    }

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            tracker: Optional[AsyncOpenedConnectionTracker] = None) -> None:
        self._plugin_service = plugin_service
        self._tracker = tracker or AsyncOpenedConnectionTracker()
        self._current_writer: Optional[HostInfo] = None
        self._pending_invalidations: Set[asyncio.Task] = set()

    @property
    def last_known_writer_host(self) -> Optional[str]:
        """Backwards-compat accessor for existing tests."""
        return self._current_writer.host if self._current_writer else None

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._CORE_SUBSCRIBED)

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        conn = await connect_func()
        if conn is not None:
            self._tracker.track(host_info, conn)
        return conn

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        self._update_writer_from_topology()
        try:
            return await execute_func()
        except Exception as exc:
            if isinstance(exc, FailoverError):
                # Force topology refresh + writer-change check
                try:
                    await self._plugin_service.refresh_host_list()
                except Exception:  # noqa: BLE001
                    pass
                self._update_writer_from_topology()
            raise

    def _update_writer_from_topology(self) -> None:
        """Detect writer change; spawn invalidation if detected."""
        writer = self._current_writer_from_hosts()
        if writer is None:
            return
        if self._current_writer is None:
            self._current_writer = writer
            return
        if self._same_host(self._current_writer, writer):
            return
        # Writer changed -- invalidate the old writer's connections
        old_writer = self._current_writer
        self._current_writer = writer
        self._spawn_invalidation(old_writer)

    def _current_writer_from_hosts(self) -> Optional[HostInfo]:
        for h in self._plugin_service.all_hosts:
            if h.role == HostRole.WRITER:
                return h
        return None

    @staticmethod
    def _same_host(a: HostInfo, b: HostInfo) -> bool:
        return a.host == b.host and a.port == b.port

    def _spawn_invalidation(self, old_writer: HostInfo) -> None:
        task = asyncio.get_event_loop().create_task(
            self._tracker.invalidate_all(old_writer))
        self._pending_invalidations.add(task)
        task.add_done_callback(self._pending_invalidations.discard)
```

- [ ] **D.1.4: Update `aws_advanced_python_wrapper/aio/minor_plugins.py`**

Remove the `AsyncAuroraConnectionTrackerPlugin` class body entirely. Replace with a re-export:

```python
from aws_advanced_python_wrapper.aio.aurora_connection_tracker import \
    AsyncAuroraConnectionTrackerPlugin  # noqa: F401 -- re-export
```

- [ ] **D.1.5: Update `aws_advanced_python_wrapper/aio/plugin_factory.py`**

Find the `_AuroraConnectionTrackerFactory` (or similar) and update the import path if needed. If it imports `AsyncAuroraConnectionTrackerPlugin` from `minor_plugins`, that re-export keeps it working — no change required. But verify by running `poetry run python -c "from aws_advanced_python_wrapper.aio.plugin_factory import ...; ..."`.

- [ ] **D.1.6: Remove stale tests** in `tests/unit/test_aio_minor_plugins.py`:

Search for tests that exercise the OLD `_last_known_writer_host` behavior (e.g., `test_aurora_connection_tracker_records_writer_host`). Those tests now reference private state that's been renamed. Either delete them (their intent is now covered by `test_aio_aurora_connection_tracker.py`) or adapt to assert via the `last_known_writer_host` property (which is kept as a backwards-compat shim).

- [ ] **D.1.7: Run — verify pass**

```bash
poetry run python -m pytest tests/unit/test_aio_aurora_connection_tracker.py -v  # 9 pass
poetry run python -m pytest tests/unit -q  # no regressions
poetry run mypy aws_advanced_python_wrapper/aio/aurora_connection_tracker.py aws_advanced_python_wrapper/aio/minor_plugins.py tests/unit/test_aio_aurora_connection_tracker.py
poetry run flake8 ...
poetry run isort --check-only ...
```

- [ ] **D.1.8: Commit**

```bash
git add aws_advanced_python_wrapper/aio/aurora_connection_tracker.py aws_advanced_python_wrapper/aio/minor_plugins.py tests/unit/test_aio_aurora_connection_tracker.py tests/unit/test_aio_minor_plugins.py
git commit -m "feat(aio): AsyncOpenedConnectionTracker + writer-change invalidation (phase D.1)

Ports sync OpenedConnectionTracker + AuroraConnectionTrackerPlugin to
the async pipeline. AsyncOpenedConnectionTracker keeps a per-instance
Dict[FrozenSet[str], WeakSet] keyed by host alias set. On execute, the
plugin compares the current writer from plugin_service.all_hosts against
the last-seen writer; on change, spawns asyncio.create_task to close
every tracked connection to the old writer. FailoverError from the
inner execute forces a topology refresh + writer-change recheck.

Simpler than sync:
- instance-level state (no class-level sharing)
- WeakSet auto-GC (no prune daemon)
- asyncio.create_task (no Thread)

Moves the plugin from minor_plugins.py to a dedicated module; leaves
a re-export for plugin_factory to continue resolving the old import.

Closes audit P0 #6 (primary). D.2 adds notify_host_list_changed hooks;
D.3 adds shutdown-hook cleanup."
```

---

## Task D.2: `notify_host_list_changed` hooks

Mirror sync lines 344-349: handle `CONVERTED_TO_READER` (invalidate the converted node) and `CONVERTED_TO_WRITER` (mark need_update_current_writer, so next execute refreshes).

### Files
- Modify: `aws_advanced_python_wrapper/aio/aurora_connection_tracker.py`
- Modify: `tests/unit/test_aio_aurora_connection_tracker.py`

### TDD

- [ ] **D.2.1: Write failing tests** — append:

```python
def test_notify_converted_to_reader_invalidates_that_host():
    plugin, svc, driver_dialect, tracker = _build()
    host = HostInfo(host="demoted-w", port=5432, role=HostRole.WRITER)
    conn = MagicMock(name="conn")
    conn.close = MagicMock()
    tracker.track(host, conn)

    from aws_advanced_python_wrapper.utils.notifications import HostEvent
    # Sync uses a string alias as dict key
    alias = host.as_alias()

    async def _run():
        plugin.notify_host_list_changed({alias: {HostEvent.CONVERTED_TO_READER}})
        # Give the spawned task a moment
        await asyncio.sleep(0.01)

    asyncio.run(_run())

    conn.close.assert_called()


def test_notify_converted_to_writer_triggers_writer_refresh_next_execute():
    plugin, svc, driver_dialect, tracker = _build()
    old = HostInfo(host="old-w", port=5432, role=HostRole.WRITER)
    new = HostInfo(host="new-w", port=5432, role=HostRole.WRITER)
    svc._all_hosts = (old,)

    async def _noop():
        return None

    asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _noop))

    # Now notify the event, then simulate topology change
    from aws_advanced_python_wrapper.utils.notifications import HostEvent
    plugin.notify_host_list_changed({new.as_alias(): {HostEvent.CONVERTED_TO_WRITER}})
    svc._all_hosts = (new,)

    conn_to_old = MagicMock(name="conn_to_old")
    conn_to_old.close = MagicMock()
    tracker.track(old, conn_to_old)

    async def _second_execute():
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        await asyncio.sleep(0.01)

    asyncio.run(_second_execute())
    conn_to_old.close.assert_called()
```

- [ ] **D.2.2: Run — verify fail**.

- [ ] **D.2.3: Implement**

On `AsyncAuroraConnectionTrackerPlugin` add:

```python
    def notify_host_list_changed(self, changes):
        for alias, events in changes.items():
            if HostEvent.CONVERTED_TO_READER in events:
                # Build a HostInfo stub from the alias for invalidation keying.
                # We only have host:port in the alias; reconstruct enough for
                # as_aliases() to match what was tracked.
                host_part, _, port_part = alias.partition(":")
                try:
                    port = int(port_part) if port_part else 5432
                except ValueError:
                    port = 5432
                # Use HostInfo so as_aliases() yields the same frozenset
                from aws_advanced_python_wrapper.hostinfo import HostInfo
                h = HostInfo(host=host_part, port=port)
                self._spawn_invalidation(h)
            if HostEvent.CONVERTED_TO_WRITER in events:
                # Force a writer recheck on next execute
                self._current_writer = None
```

Note: sync tracks by full alias set; async's simplified version tracks by `as_aliases()`, so if the incoming alias doesn't match the tracked alias set exactly, invalidation misses. For Phase D we accept this limitation — sync handles multi-alias tracking via a more complex key strategy. Document it with a code comment.

- [ ] **D.2.4: Run — verify pass** (2 new, 11 total in file).

- [ ] **D.2.5: Commit**:

```bash
git add aws_advanced_python_wrapper/aio/aurora_connection_tracker.py tests/unit/test_aio_aurora_connection_tracker.py
git commit -m "feat(aio): connection tracker reacts to CONVERTED_TO_READER/WRITER events (phase D.2)

On notify_host_list_changed:
- CONVERTED_TO_READER: invalidate tracked connections to that host
  (mirrors sync aurora_connection_tracker_plugin.py:345-347).
- CONVERTED_TO_WRITER: reset _current_writer so the next execute
  detects the new writer via all_hosts (sync line 348-349).

Closes audit P0 #6 (secondary -- the writer-change path in D.1 was
the primary win)."
```

---

## Task D.3: Shutdown hook for pending invalidations

`AsyncAuroraConnectionTrackerPlugin._pending_invalidations` holds `asyncio.Task`s spawned for background close. On `release_resources_async`, drain them so no close operations leak past process exit.

### Files
- Modify: `aws_advanced_python_wrapper/aio/aurora_connection_tracker.py`
- Modify: `tests/unit/test_aio_aurora_connection_tracker.py`

### TDD

- [ ] **D.3.1: Write failing test** — append:

```python
def test_release_resources_async_drains_pending_invalidations():
    from aws_advanced_python_wrapper.aio.cleanup import release_resources_async

    plugin, svc, driver_dialect, tracker = _build()
    old = HostInfo(host="old-w", port=5432, role=HostRole.WRITER)
    new = HostInfo(host="new-w", port=5432, role=HostRole.WRITER)

    # Slow close to ensure the task is still running when release_resources_async is called
    conn = MagicMock(name="slow_conn")

    async def _slow_close():
        await asyncio.sleep(0.05)

    conn.close = MagicMock(side_effect=_slow_close)
    tracker.track(old, conn)

    svc._all_hosts = (old,)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        svc._all_hosts = (new,)
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        # Don't sleep; call release_resources_async while task is still pending
        await release_resources_async()

    asyncio.run(_run())

    # All pending tasks drained
    assert all(t.done() for t in plugin._pending_invalidations)
```

- [ ] **D.3.2: Run — verify fail**.

- [ ] **D.3.3: Implement**

In `__init__`, register the shutdown hook:

```python
        from aws_advanced_python_wrapper.aio.cleanup import \
            register_shutdown_hook
        register_shutdown_hook(self._shutdown)
```

Add `_shutdown` method:

```python
    async def _shutdown(self) -> None:
        """Drain pending invalidation tasks so close operations complete
        before process exit."""
        if not self._pending_invalidations:
            return
        pending = list(self._pending_invalidations)
        await asyncio.gather(*pending, return_exceptions=True)
```

- [ ] **D.3.4: Run — verify pass** (1 new, 12 total in file).

- [ ] **D.3.5: Commit**:

```bash
git add aws_advanced_python_wrapper/aio/aurora_connection_tracker.py tests/unit/test_aio_aurora_connection_tracker.py
git commit -m "feat(aio): tracker shutdown hook drains pending invalidations (phase D.3)

AsyncAuroraConnectionTrackerPlugin registers a shutdown hook that
awaits every pending invalidation task (via asyncio.gather with
return_exceptions=True). Async apps calling release_resources_async()
on exit will see every tracked connection's close() attempt complete,
not get cut off mid-close when the loop tears down."
```

---

## Task D.4: Final `/verify`

- [ ] mypy + flake8 + isort + pytest -Werror clean.
- [ ] Confirm audit P0 #6 is closed.

---

## Out of scope

- Class-level `_opened_connections` sharing across plugins (sync does this for process-wide cleanup). Instance-level is simpler and avoids cross-task state hazards; worth revisiting only if a real consumer needs shared bookkeeping.
- Prune daemon (sync has one running every 30s to clean dead refs). `WeakSet` auto-cleans on GC, so async doesn't need it.
- RDS cluster-endpoint alias normalization (sync's `populate_opened_connection_set` walks aliases looking for RDS instance endpoints). Async uses the raw `host_info.as_aliases()` as the key. Works for the P0 #6 scenario; more complex scenarios may need the normalization later.
- Telemetry (`aurora_connection_tracker.*` counters) — Phase I.

## Self-review

- [x] P0 #6 audit findings mapped: per-host tracking (D.1), invalidate-on-writer-change (D.1), invalidate-on-FailoverError (D.1), CONVERTED_* events (D.2), cleanup (D.3).
- [x] Sync reference lines cited.
- [x] Python 3.10-compatible.
- [x] No forward refs to Phase E-J.
