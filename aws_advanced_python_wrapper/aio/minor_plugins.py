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

"""F3-B SP-8: remaining small async plugins.

Async counterparts for the sync "minor plugins":

* :class:`AsyncConnectTimePlugin` -- records time spent in connect()
* :class:`AsyncExecuteTimePlugin` -- records time spent in execute()
* :class:`AsyncDeveloperPlugin` -- optionally injects an exception into
  the pipeline on the next call; used for testing.
* :class:`AsyncAuroraConnectionTrackerPlugin` -- real tracker + writer-change
  invalidation. Lives in its own module
  (:mod:`aws_advanced_python_wrapper.aio.aurora_connection_tracker`); re-exported
  here so existing imports keep working.
* :class:`AsyncCustomEndpointPlugin` -- pass-through stub; full custom
  endpoint monitoring lands in its own SP.

Each plugin is kept intentionally small; none spawn background tasks.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable, List, Optional, Set

# Re-export: moved to its own module in Phase D.1. Keeping the import path stable
# so downstream users and the plugin factory don't care where the class lives.
from aws_advanced_python_wrapper.aio.aurora_connection_tracker import \
    AsyncAuroraConnectionTrackerPlugin  # noqa: F401
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncConnectTimePlugin(AsyncPlugin):
    """Record wall-clock time spent in the connect phase.

    State is per-instance; applications can read ``total_connect_time_ns``
    to aggregate.
    """

    _SUBSCRIBED: Set[str] = {DbApiMethod.CONNECT.method_name}

    def __init__(self) -> None:
        self.total_connect_time_ns: int = 0
        self.connect_count: int = 0

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        start = asyncio.get_event_loop().time()
        try:
            return await connect_func()
        finally:
            self.total_connect_time_ns += int(
                (asyncio.get_event_loop().time() - start) * 1_000_000_000
            )
            self.connect_count += 1


class AsyncExecuteTimePlugin(AsyncPlugin):
    """Record wall-clock time spent in execute().

    Subscribes to everything network-bound; state is per-instance.
    """

    _SUBSCRIBED: Set[str] = {
        DbApiMethod.CURSOR_EXECUTE.method_name,
        DbApiMethod.CURSOR_EXECUTEMANY.method_name,
        DbApiMethod.CURSOR_FETCHONE.method_name,
        DbApiMethod.CURSOR_FETCHMANY.method_name,
        DbApiMethod.CURSOR_FETCHALL.method_name,
        DbApiMethod.CONNECTION_COMMIT.method_name,
        DbApiMethod.CONNECTION_ROLLBACK.method_name,
    }

    def __init__(self) -> None:
        self.total_execute_time_ns: int = 0
        self.execute_count: int = 0

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        start = asyncio.get_event_loop().time()
        try:
            return await execute_func()
        finally:
            self.total_execute_time_ns += int(
                (asyncio.get_event_loop().time() - start) * 1_000_000_000
            )
            self.execute_count += 1


class AsyncDeveloperPlugin(AsyncPlugin):
    """Test-only plugin that injects an exception on the next pipeline call.

    Useful for simulating failover conditions in unit tests. Sync counterpart
    exists for the same purpose.
    """

    _SUBSCRIBED: Set[str] = {DbApiMethod.ALL.method_name}

    def __init__(self) -> None:
        self._next_exception: Optional[BaseException] = None

    def set_next_exception(self, exc: BaseException) -> None:
        self._next_exception = exc

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        if self._next_exception is not None:
            exc = self._next_exception
            self._next_exception = None
            raise exc
        return await execute_func()


class AsyncCustomEndpointPlugin(AsyncPlugin):
    """Placeholder for async custom-endpoint support.

    Sync custom endpoint plugin maintains a background monitor resolving
    Aurora custom endpoints to member instances. The async version follows
    the same pattern as AsyncClusterTopologyMonitor; 3.0.0 ships a
    pass-through so existing apps don't error when the plugin code is in
    their profile.
    """

    @property
    def subscribed_methods(self) -> Set[str]:
        # Subscribing to nothing means PluginManager skips this plugin for
        # every method; the class exists only so `plugins="custom_endpoint"`
        # doesn't fail when the async engine starts up.
        return set()


__all__ = [
    "AsyncConnectTimePlugin",
    "AsyncExecuteTimePlugin",
    "AsyncDeveloperPlugin",
    "AsyncAuroraConnectionTrackerPlugin",
    "AsyncCustomEndpointPlugin",
]


_unused_list: List[str] = []
