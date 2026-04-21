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

"""Async custom endpoint monitor (Task 1-B).

Periodically resolves Aurora custom endpoints to their member instance
IDs via the AWS RDS DescribeDBClusterEndpoints API. The real
:class:`AsyncCustomEndpointPlugin` reads the cached member list to
filter the topology used by failover / RWS so the wrapper respects the
custom endpoint's instance membership.

3.0.0 shipped :class:`AsyncCustomEndpointPlugin` as a subscribe-to-nothing
stub. Task 1-B replaces the stub with a plugin that starts the monitor
on connect and stops it on :func:`release_resources_async`.
"""

from __future__ import annotations

import asyncio
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, List, Optional,
                    Set, Tuple)

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


class AsyncCustomEndpointMonitor:
    """Periodically fetches the instance list for an Aurora custom endpoint.

    Uses boto3 ``rds.describe_db_cluster_endpoints`` in
    ``asyncio.to_thread`` so the sync AWS SDK doesn't block the event
    loop. Caches the result as a tuple of instance IDs; plugin code
    reads it via :meth:`get_member_instance_ids`.
    """

    def __init__(
            self,
            cluster_identifier: str,
            custom_endpoint_identifier: str,
            region: Optional[str] = None,
            refresh_interval_sec: float = 30.0) -> None:
        self._cluster_id = cluster_identifier
        self._endpoint_id = custom_endpoint_identifier
        self._region = region
        # Lower-bound of 10 ms prevents a misconfigured 0 or negative
        # interval from burning CPU; the production default is 30 s.
        self._interval_sec = max(0.01, float(refresh_interval_sec))
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._member_instance_ids: Tuple[str, ...] = ()
        self._last_refresh_ns: int = 0

    @property
    def member_instance_ids(self) -> Tuple[str, ...]:
        """Most-recently-resolved member list. Empty tuple if not yet refreshed."""
        return self._member_instance_ids

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if self.is_running():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    members = await self._fetch_members()
                    if members:
                        self._member_instance_ids = tuple(members)
                        self._last_refresh_ns = int(
                            asyncio.get_event_loop().time() * 1_000_000_000
                        )
                except Exception:
                    # Transient AWS failures must not kill the monitor.
                    pass
                # Sleep the refresh interval, but wake early if stop_event
                # is set. Using asyncio.sleep + is_set check instead of
                # wait_for(event.wait()) sidesteps wait_for's cancellation
                # timing quirks under heavy load.
                slept = 0.0
                step = min(0.02, self._interval_sec)
                while slept < self._interval_sec and not self._stop_event.is_set():
                    await asyncio.sleep(step)
                    slept += step
        except asyncio.CancelledError:
            return

    async def _fetch_members(self) -> List[str]:
        return await asyncio.to_thread(
            self._fetch_members_blocking,
            self._cluster_id,
            self._endpoint_id,
            self._region,
        )

    @staticmethod
    def _fetch_members_blocking(
            cluster_id: str,
            endpoint_id: str,
            region: Optional[str]) -> List[str]:
        import boto3
        kwargs: dict = {}
        if region:
            kwargs["region_name"] = region
        client = boto3.client("rds", **kwargs)
        resp = client.describe_db_cluster_endpoints(
            DBClusterIdentifier=cluster_id,
            DBClusterEndpointIdentifier=endpoint_id,
        )
        members: List[str] = []
        for endpoint in resp.get("DBClusterEndpoints", []):
            members.extend(endpoint.get("StaticMembers") or [])
        return members

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is None:
            return
        if not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None


class AsyncCustomEndpointPlugin(AsyncPlugin):
    """Async custom endpoint plugin (Task 1-B -- replaces SP-8 stub).

    On initial connect, spawns an :class:`AsyncCustomEndpointMonitor` to
    keep the member instance list fresh. On cleanup (via
    :func:`release_resources_async`), stops the monitor.

    Connection properties (shared with sync custom endpoint plugin):
      * ``custom_endpoint_monitor_info_refresh_rate_sec`` -- default 30s
      * ``cluster_id`` -- Aurora cluster identifier (also used for
        topology monitor)
      * Users identify the custom endpoint by the host portion of the
        connection URL (the endpoint name is the leftmost DNS label).
    """

    _SUBSCRIBED: Set[str] = {DbApiMethod.CONNECT.method_name}

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props
        self._monitor: Optional[AsyncCustomEndpointMonitor] = None

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    @property
    def monitor(self) -> Optional[AsyncCustomEndpointMonitor]:
        """Test hook: inspect the monitor instance."""
        return self._monitor

    @property
    def member_instance_ids(self) -> Tuple[str, ...]:
        """Most-recently-cached member instance IDs; empty if no monitor."""
        return self._monitor.member_instance_ids if self._monitor else ()

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        conn = await connect_func()
        if is_initial_connection and self._monitor is None:
            monitor = self._build_monitor(host_info, props)
            if monitor is not None:
                monitor.start()
                self._monitor = monitor
                # Register for cleanup via release_resources_async.
                from aws_advanced_python_wrapper.aio.cleanup import \
                    register_shutdown_hook
                register_shutdown_hook(monitor.stop)
        return conn

    def _build_monitor(
            self,
            host_info: HostInfo,
            props: Properties) -> Optional[AsyncCustomEndpointMonitor]:
        """Extract cluster + custom endpoint IDs from host info / props."""
        # Aurora custom endpoint host format:
        # <endpoint-name>.cluster-custom-<hash>.<region>.rds.amazonaws.com
        host = host_info.host
        if ".cluster-custom-" not in host:
            return None
        endpoint_id = host.split(".", 1)[0]
        cluster_id = WrapperProperties.CLUSTER_ID.get(props)
        if not cluster_id or cluster_id == "1":  # default placeholder
            # Fall back to deriving from the cluster- prefix in the host.
            cluster_id = None
        if not cluster_id:
            # Can't monitor without a cluster identifier.
            return None
        region_prop = WrapperProperties.IAM_REGION.get(props)
        # IAM_REGION is the closest shared property; custom endpoint
        # monitoring would benefit from a dedicated `rds_region` prop but
        # reusing IAM_REGION keeps config surface small.
        return AsyncCustomEndpointMonitor(
            cluster_identifier=str(cluster_id),
            custom_endpoint_identifier=endpoint_id,
            region=str(region_prop) if region_prop else None,
        )


__all__ = ["AsyncCustomEndpointMonitor", "AsyncCustomEndpointPlugin"]
