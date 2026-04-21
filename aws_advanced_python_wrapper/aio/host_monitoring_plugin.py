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

"""Async Enhanced Failure Monitoring (EFM) plugin.

Wraps each network-bound call with a watchdog that spawns an
:class:`asyncio.Task` probing the connection over a short timeout. If
the probe fails, the watchdog aborts the connection so the surrounding
call returns the underlying ``OperationalError`` immediately instead of
waiting for kernel TCP timeouts.

Shares the sync EFM plugin's connection properties:
  * ``failure_detection_enabled``
  * ``failure_detection_time_ms`` -- grace period before probing starts
  * ``failure_detection_interval_ms`` -- probe cadence
  * ``failure_detection_count`` -- consecutive failures before declaring outage
"""

from __future__ import annotations

import asyncio
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, FrozenSet,
                    Optional, Set)

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncHostMonitoringPlugin(AsyncPlugin):
    """Async counterpart of the Host Monitoring Plugin v2.

    Runs a single long-lived monitor task per plugin (one plugin per
    connection in the async wrapper). The task is started LAZILY on the
    first :meth:`execute` call -- we can't start it in ``__init__`` because
    the plugin is constructed before a connection exists. Once running, it
    probes ``driver_dialect.ping(conn)`` on ``interval_ms`` cadence after
    an initial ``grace_ms`` grace period, accumulating consecutive failures
    across probes.

    Phase C.1 only establishes the standing task + failure accumulator.
    C.2 adds the UNAVAILABLE/abort behavior; C.3 adds notify hooks;
    C.4 wires teardown via ``release_resources``.
    """

    _SUBSCRIBED: Set[str] = {
        DbApiMethod.CURSOR_EXECUTE.method_name,
        DbApiMethod.CURSOR_EXECUTEMANY.method_name,
        DbApiMethod.CURSOR_FETCHONE.method_name,
        DbApiMethod.CURSOR_FETCHMANY.method_name,
        DbApiMethod.CURSOR_FETCHALL.method_name,
    }

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props
        self._enabled = WrapperProperties.FAILURE_DETECTION_ENABLED.get_bool(
            props
        )
        ms = WrapperProperties.FAILURE_DETECTION_TIME_MS.get(props)
        self._grace_sec = (float(ms) / 1000.0) if ms else 30.0
        ms = WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.get(props)
        self._interval_sec = (float(ms) / 1000.0) if ms else 5.0
        count = WrapperProperties.FAILURE_DETECTION_COUNT.get(props)
        self._failure_count_threshold = int(count) if count else 3

        # Standing-task state (Phase C.1). These are populated lazily on the
        # first execute() call; tracked on the instance so C.2 tests (and
        # release_resources in C.4) can observe/manipulate them.
        self._monitor_task: Optional[asyncio.Task] = None
        self._monitor_stop: asyncio.Event = asyncio.Event()
        self._consecutive_failures: int = 0
        self._monitored_aliases: FrozenSet[str] = frozenset()
        self._host_unavailable: bool = False

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
        return await connect_func()

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        if not self._enabled:
            return await execute_func()

        conn = self._plugin_service.current_connection
        host_info = self._plugin_service.current_host_info
        if conn is None or host_info is None:
            return await execute_func()

        self._ensure_monitor_started(conn, host_info)
        return await execute_func()

    def _ensure_monitor_started(self, conn: Any, host_info: HostInfo) -> None:
        """Start the standing monitor task if it's not already running."""
        if self._monitor_task is not None and not self._monitor_task.done():
            return
        self._monitored_aliases = host_info.as_aliases()
        self._monitor_stop = asyncio.Event()
        self._consecutive_failures = 0
        driver_dialect = self._plugin_service.driver_dialect
        self._monitor_task = asyncio.get_event_loop().create_task(
            self._monitor(conn, driver_dialect, self._monitor_stop)
        )

    async def _monitor(
            self,
            conn: Any,
            driver_dialect: AsyncDriverDialect,
            stop_event: asyncio.Event) -> None:
        """Standing monitor: probe on ``interval_sec`` cadence, track failures.

        Grace period first, then probe forever until ``stop_event`` fires.
        Threshold-based abort + UNAVAILABLE marking land in C.2.
        """
        try:
            # Grace period: don't probe immediately -- long-running but
            # healthy queries shouldn't trigger the monitor.
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self._grace_sec
                )
                return
            except asyncio.TimeoutError:
                pass

            while not stop_event.is_set():
                try:
                    ok = await asyncio.wait_for(
                        driver_dialect.ping(conn), timeout=self._interval_sec
                    )
                except (asyncio.TimeoutError, Exception):
                    ok = False

                if ok:
                    self._consecutive_failures = 0
                else:
                    self._consecutive_failures += 1

                try:
                    await asyncio.wait_for(
                        stop_event.wait(), timeout=self._interval_sec
                    )
                    return
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            return


# Optional alias for consistency with sync "v2" naming.
AsyncHostMonitoringV2Plugin = AsyncHostMonitoringPlugin
