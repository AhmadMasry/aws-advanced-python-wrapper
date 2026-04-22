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

"""Async Aurora Limitless plugin (minimal port).

Ports sync :class:`LimitlessPlugin`'s core behavior: on each connect,
discover the current limitless routers via
``aurora_limitless_router_endpoints()`` (exposed on the database dialect
as ``limitless_router_endpoint_query``), pick one via the standard
plugin-service strategy, and route the connection to that router.

Minimal vs sync:

* On-demand router discovery -- no standing background monitor thread.
* Per-instance router cache with a hard-coded 5-minute TTL (mirrors
  sync's ``LIMITLESS_MONITOR_DISPOSAL_TIME_MS`` default intent; the
  exact knob is sync-storage-service-scoped and doesn't map cleanly to
  a per-instance async cache).
* Router selection uses ``weighted_random`` (matches sync's primary
  path through ``LimitlessRouterService.establish_connection``).
* No ``LIMITLESS_QUERY_TIMEOUT`` enforcement, no telemetry.
* The router connection is opened directly via
  :meth:`AsyncDriverDialect.connect`, bypassing the plugin pipeline.
  Caller plugins (IAM auth etc.) will NOT re-apply on the router conn
  -- documented limitation shared with other async connect-swapping
  plugins (see :mod:`stale_dns_plugin`).

A full port with the background monitor can land later if a consumer
needs it; Task 1-B's ``AsyncCustomEndpointMonitor`` is a ready
template.
"""

from __future__ import annotations

import asyncio
import math
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, List, Optional,
                    Set, Tuple)

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import Properties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService


logger = Logger(__name__)

# Matches sync's default cache window intent: routers are refreshed at
# most once per 5 minutes per plugin instance. Sync's storage-service
# default is 10 min (LIMITLESS_MONITOR_DISPOSAL_TIME_MS = 600_000);
# we pick a conservative 5 min for the minimal port so a stale cache
# never outlives a single user session.
_CACHE_TTL_SEC: float = 300.0

# Sync LimitlessRouterService uses ``weighted_random`` on the primary
# path. We match that so identical config strings produce the same
# router selection behavior across sync/async.
_STRATEGY: str = "weighted_random"


class AsyncLimitlessPlugin(AsyncPlugin):
    """Async counterpart of :class:`LimitlessPlugin` (minimal port)."""

    _SUBSCRIBED: Set[str] = {DbApiMethod.CONNECT.method_name}

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props
        self._cached_routers: List[HostInfo] = []
        self._cache_expires_at: float = 0.0

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
        # Open an initial connection via the pipeline's connect_func so
        # caller plugins (auth etc.) apply to the probe connection.
        initial_conn = await connect_func()

        try:
            routers = await self._get_routers(
                initial_conn, host_info.port)
        except Exception:  # noqa: BLE001 - router discovery is best-effort
            return initial_conn

        if not routers:
            logger.debug(
                "LimitlessRouterService.LimitlessRouterCacheEmpty")
            return initial_conn

        # If the current host is already one of the routers, keep the
        # initial connection -- matches sync's
        # ``Utils.contains_host_and_port`` short-circuit.
        for r in routers:
            if r.host == host_info.host and r.port == host_info.port:
                logger.debug(
                    "LimitlessRouterService.ConnectWithHost", host_info.host)
                return initial_conn

        # Pick a router via the configured strategy.
        try:
            picked = self._plugin_service.get_host_info_by_strategy(
                HostRole.WRITER, _STRATEGY, routers)
        except Exception:  # noqa: BLE001 - strategy probe is best-effort
            return initial_conn
        if picked is None:
            return initial_conn
        logger.debug(
            "LimitlessRouterService.SelectedHost", picked.host)

        # Open a connection to the picked router. Bypasses the pipeline
        # to avoid recursion back through this plugin; caller plugins
        # (IAM etc.) do NOT re-apply. Documented async limitation.
        router_props = self._props_with_host(props, picked)
        try:
            router_conn = await driver_dialect.connect(
                picked, router_props, target_driver_func)
        except Exception:  # noqa: BLE001
            logger.debug(
                "LimitlessRouterService.FailedToConnectToHost", picked.host)
            return initial_conn

        # Close the initial conn best-effort; return the router conn.
        try:
            await driver_dialect.abort_connection(initial_conn)
        except Exception:  # noqa: BLE001
            pass
        return router_conn

    async def _get_routers(
            self,
            conn: Any,
            host_port_to_map: int) -> List[HostInfo]:
        """Return cached routers or query the DB for fresh ones.

        Mirrors sync's ``LimitlessQueryHelper.query_for_limitless_routers``:
        pulls the query off the database dialect, executes it on ``conn``,
        maps each row to a :class:`HostInfo` weighted by reported load.
        """
        now = self._loop_time()
        if self._cached_routers and now < self._cache_expires_at:
            return self._cached_routers

        database_dialect = self._plugin_service.database_dialect
        query = getattr(
            database_dialect, "limitless_router_endpoint_query", None)
        if not query:
            # Not an AuroraLimitlessDialect -- nothing to discover.
            return []

        rows = await self._run_query(conn, query)
        routers: List[HostInfo] = []
        for row in rows:
            host_info = self._row_to_host_info(row, host_port_to_map)
            if host_info is not None:
                routers.append(host_info)

        self._cached_routers = routers
        self._cache_expires_at = now + _CACHE_TTL_SEC
        return routers

    @staticmethod
    async def _run_query(conn: Any, query: str) -> List[tuple]:
        # The underlying connection is a raw async driver connection
        # (psycopg.AsyncConnection at 3.0.0 / aiomysql Connection). Both
        # expose a sync ``cursor()`` that returns an async cursor.
        cur = conn.cursor()
        try:
            async with cur:
                await cur.execute(query)
                return list(await cur.fetchall())
        except Exception:  # noqa: BLE001 - probe failure -> empty list
            return []

    @staticmethod
    def _row_to_host_info(
            row: Tuple[Any, ...], host_port_to_map: int) -> Optional[HostInfo]:
        """Turn a ``(router_endpoint, load)`` tuple into a HostInfo.

        Weight math matches sync's ``LimitlessQueryHelper._create_host_info``:
        ``weight = clamp(10 - floor(cpu * 10), 1, 10)``. Invalid / missing
        loads fall back to weight 1.
        """
        if not row:
            return None
        host_name = str(row[0])
        try:
            cpu = float(row[1]) if len(row) > 1 else 0.0
        except (TypeError, ValueError):
            cpu = 0.0

        weight = 10 - math.floor(cpu * 10)
        if weight < 1 or weight > 10:
            weight = 1

        return HostInfo(
            host=host_name,
            port=host_port_to_map,
            role=HostRole.WRITER,
            weight=weight,
            host_id=host_name,
        )

    @staticmethod
    def _props_with_host(
            props: Properties, host_info: HostInfo) -> Properties:
        new_props = Properties(dict(props))
        new_props["host"] = host_info.host
        if host_info.is_port_specified():
            new_props["port"] = str(host_info.port)
        return new_props

    @staticmethod
    def _loop_time() -> float:
        """Monotonic clock via running loop, or :func:`time.monotonic`.

        Matches the pattern in :class:`AsyncFastestResponseStrategyPlugin`:
        callers may invoke cache checks outside a running loop (unit
        tests) so fall back to monotonic() when there is no loop.
        """
        try:
            return asyncio.get_running_loop().time()
        except RuntimeError:
            import time
            return time.monotonic()


__all__ = ["AsyncLimitlessPlugin"]
