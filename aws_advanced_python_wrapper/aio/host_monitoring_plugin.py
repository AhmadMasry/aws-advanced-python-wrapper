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
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional, Set

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

    Design: no per-host background task pool (sync EFM v2 uses one). In
    3.0.0 the async version runs a short-lived monitor task ONLY during
    network-bound calls. This keeps the event-loop footprint minimal and
    sidesteps cross-task connection ownership issues. The downside is that
    outages are detected only while the app is actively issuing queries;
    SP-5 v2 can add a standing per-connection monitor task if required.
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
        if conn is None:
            return await execute_func()

        driver_dialect = self._plugin_service.driver_dialect
        stop_event = asyncio.Event()
        monitor_task = asyncio.get_event_loop().create_task(
            self._monitor(conn, driver_dialect, stop_event)
        )
        try:
            return await execute_func()
        finally:
            stop_event.set()
            monitor_task.cancel()
            try:
                await monitor_task
            except (asyncio.CancelledError, Exception):
                pass

    async def _monitor(
            self,
            conn: Any,
            driver_dialect: AsyncDriverDialect,
            stop_event: asyncio.Event) -> None:
        """Ping the connection on an interval after the grace period.

        Aborts the connection if ``_failure_count_threshold`` consecutive
        pings fail. Exits cleanly on ``stop_event``.
        """
        try:
            # Grace period: don't probe immediately -- long-running but
            # healthy queries shouldn't trigger the monitor.
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._grace_sec)
                return
            except asyncio.TimeoutError:
                pass

            consecutive_failures = 0
            while not stop_event.is_set():
                try:
                    ok = await asyncio.wait_for(
                        driver_dialect.ping(conn), timeout=self._interval_sec
                    )
                except (asyncio.TimeoutError, Exception):
                    ok = False

                if ok:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1

                if consecutive_failures >= self._failure_count_threshold:
                    # Abort the connection so the surrounding query returns
                    # an OperationalError; failover (if enabled) then takes
                    # over through its own subscription.
                    try:
                        await driver_dialect.abort_connection(conn)
                    except Exception:
                        pass
                    return

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


def _unused() -> Optional[int]:
    """Prevent unused-import warnings when Optional isn't consumed."""
    return None
