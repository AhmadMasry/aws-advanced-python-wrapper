#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Async Aurora connection tracker + invalidate-on-failover plugin.

Port of sync ``aurora_connection_tracker_plugin.py``. Tracks every
connection opened through the plugin pipeline in a per-instance
:class:`AsyncOpenedConnectionTracker` keyed by host alias set. On
writer change (detected via ``AsyncPluginService.all_hosts`` or a
raised :class:`FailoverError`), closes every tracked connection to
the old writer so pooled consumers don't reuse a stale
demoted-writer socket.

Simpler than the sync implementation:

* Instance-level state (no class-level sharing across plugin instances).
* ``WeakSet`` auto-GC -- no prune daemon thread.
* :func:`asyncio.create_task` for fire-and-forget invalidation -- no
  background ``Thread``.
"""

from __future__ import annotations

import asyncio
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Dict, FrozenSet,
                    Optional, Set)
from weakref import WeakSet

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import FailoverError
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod

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

    * instance-level so multiple plugin instances don't share state,
    * :class:`WeakSet` for auto-GC of dead references (no prune daemon),
    * :meth:`invalidate_all` awaits ``conn.close()`` (async or sync) directly.
    """

    def __init__(self) -> None:
        self._tracked: Dict[FrozenSet[str], WeakSet[Any]] = {}

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
        """Close every tracked connection to ``host_info``'s aliases.

        Best-effort: individual close failures are swallowed so one
        broken connection doesn't block closing the rest.
        """
        aliases = host_info.as_aliases()
        conn_set = self._tracked.get(aliases)
        if conn_set is None:
            return
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
        try:
            del self._tracked[aliases]
        except KeyError:
            pass

    def _tracked_for(self, aliases: FrozenSet[str]) -> WeakSet[Any]:
        """Test-only accessor; returns the tracked ``WeakSet`` or an empty one."""
        return self._tracked.get(aliases, WeakSet())


class AsyncAuroraConnectionTrackerPlugin(AsyncPlugin):
    """Async counterpart of :class:`AuroraConnectionTrackerPlugin`.

    Subscribes to CONNECT, CONNECTION_CLOSE, NOTIFY_HOST_LIST_CHANGED, and
    the pipeline's network-bound cursor/connection methods so it can detect
    a writer change mid-session and invalidate pooled connections to the
    demoted writer.
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
        """Backwards-compat accessor (was present on the SP-8 stub)."""
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
                try:
                    await self._plugin_service.refresh_host_list()
                except Exception:  # noqa: BLE001 - refresh best-effort
                    pass
                self._update_writer_from_topology()
            raise

    def _update_writer_from_topology(self) -> None:
        writer = self._current_writer_from_hosts()
        if writer is None:
            return
        if self._current_writer is None:
            self._current_writer = writer
            return
        if self._same_host(self._current_writer, writer):
            return
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


__all__ = ["AsyncAuroraConnectionTrackerPlugin", "AsyncOpenedConnectionTracker"]
