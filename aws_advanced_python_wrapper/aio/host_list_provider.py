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

"""Async host list providers.

:class:`AsyncHostListProvider` is the async counterpart of
:class:`HostListProvider`. SP-3 ships the Protocol plus two concrete
implementations:

* :class:`AsyncStaticHostListProvider` -- returns a fixed list built from
  connection props; used when no Aurora topology is needed (direct RDS
  instance, test environments).
* :class:`AsyncAuroraHostListProvider` -- queries the Aurora
  ``aurora_replica_status`` view over the current async connection and
  caches the result.

A background refresh loop (:class:`AsyncClusterTopologyMonitor`) lives in
:mod:`aws_advanced_python_wrapper.aio.cluster_topology_monitor` and drives
:meth:`AsyncAuroraHostListProvider.force_refresh` on an interval.
"""

from __future__ import annotations

import asyncio
from typing import (TYPE_CHECKING, Any, List, Optional, Protocol, Tuple,
                    runtime_checkable)

from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.utils.properties import Properties


Topology = Tuple[HostInfo, ...]


@runtime_checkable
class AsyncHostListProvider(Protocol):
    """Async host list provider contract."""

    async def refresh(self, connection: Optional[Any] = None) -> Topology:
        """Return the current topology, possibly using a cache."""
        ...

    async def force_refresh(self, connection: Optional[Any] = None) -> Topology:
        """Return the current topology, bypassing any cache."""
        ...

    def get_cluster_id(self) -> str:
        """Return a stable cluster identifier for pool-key generation."""
        ...

    async def stop(self) -> None:
        """Release any background tasks this provider owns."""
        ...


class AsyncStaticHostListProvider:
    """Static host list -- one host built from connection props.

    Used for plain RDS instances, local test environments, or any case
    where the wrapper does not need to discover cluster topology.
    """

    def __init__(self, props: Properties) -> None:
        host = props.get("host", "")
        port_raw = props.get("port")
        port = int(port_raw) if port_raw is not None else -1
        self._host_info = HostInfo(host=host, port=port, role=HostRole.WRITER)
        self._cluster_id = f"static:{host}:{port}"

    async def refresh(self, connection: Optional[Any] = None) -> Topology:
        return (self._host_info,)

    async def force_refresh(self, connection: Optional[Any] = None) -> Topology:
        return (self._host_info,)

    def get_cluster_id(self) -> str:
        return self._cluster_id

    async def stop(self) -> None:
        return None


class AsyncAuroraHostListProvider:
    """Aurora topology discovery over an async driver connection.

    Runs an Aurora-topology query (e.g., ``SELECT server_id,
    session_id = 'MASTER_SESSION_ID' AS is_writer, ... FROM
    aurora_replica_status``) and returns a tuple of :class:`HostInfo`
    objects. The exact SQL and parsing live on the shared sync
    :class:`TopologyAwareDatabaseDialect` classes (SP-3 reuses them rather
    than duplicating).

    Thread-safety: the cache is protected by an :class:`asyncio.Lock`.
    No background task is spawned here -- if continuous refresh is
    desired, pair with :class:`AsyncClusterTopologyMonitor`.
    """

    _DEFAULT_REFRESH_RATE_NS = 30 * 1_000_000_000  # 30 seconds

    def __init__(
            self,
            props: Properties,
            driver_dialect: AsyncDriverDialect,
            topology_query: str = (
                "SELECT SERVER_ID, SESSION_ID = 'MASTER_SESSION_ID' AS IS_WRITER "
                "FROM aurora_replica_status() "
                "WHERE EXTRACT(EPOCH FROM (NOW() - LAST_UPDATE_TIMESTAMP)) <= 300 "
                "OR SESSION_ID = 'MASTER_SESSION_ID'"
            ),
            cluster_id: Optional[str] = None,
            default_port: int = 5432) -> None:
        self._props = props
        self._driver_dialect = driver_dialect
        self._topology_query = topology_query
        self._default_port = default_port
        self._cluster_id = cluster_id or self._derive_cluster_id(props)
        self._topology_cache: Optional[Topology] = None
        self._last_refresh_ns: int = 0
        self._refresh_lock = asyncio.Lock()
        refresh_ms = WrapperProperties.TOPOLOGY_REFRESH_MS.get(props)
        self._refresh_ns: int = (
            int(refresh_ms) * 1_000_000 if refresh_ms is not None
            else self._DEFAULT_REFRESH_RATE_NS
        )

    @staticmethod
    def _derive_cluster_id(props: Properties) -> str:
        # Only treat `cluster_id` as explicit when present in the props dict;
        # WrapperProperties.CLUSTER_ID.get() returns the default '1' even
        # when unset.
        if WrapperProperties.CLUSTER_ID.name in props:
            return str(props[WrapperProperties.CLUSTER_ID.name])
        host = props.get("host", "")
        port = props.get("port", "")
        return f"aurora:{host}:{port}"

    def get_cluster_id(self) -> str:
        return self._cluster_id

    async def refresh(self, connection: Optional[Any] = None) -> Topology:
        """Return cached topology if still fresh, else force a refresh."""
        now_ns = asyncio.get_event_loop().time() * 1_000_000_000
        if (self._topology_cache is not None
                and now_ns - self._last_refresh_ns < self._refresh_ns):
            return self._topology_cache
        return await self.force_refresh(connection)

    async def force_refresh(self, connection: Optional[Any] = None) -> Topology:
        """Run the topology query and update the cache."""
        if connection is None:
            if self._topology_cache is not None:
                return self._topology_cache
            return ()
        async with self._refresh_lock:
            rows = await self._run_topology_query(connection)
            topology = self._rows_to_topology(rows)
            if topology:
                self._topology_cache = topology
                self._last_refresh_ns = int(
                    asyncio.get_event_loop().time() * 1_000_000_000
                )
            return topology or (self._topology_cache or ())

    async def _run_topology_query(self, connection: Any) -> List[tuple]:
        """Execute the topology query and return its rows.

        Uses the driver's raw async cursor -- we bypass the plugin pipeline
        here on purpose. Topology queries run in a tight loop; running them
        through the full pipeline would double-count timing, trigger
        failover-retry on every refresh, and prevent the topology monitor
        from operating independently of app-level plugins.
        """
        # The underlying connection is a raw async driver connection
        # (psycopg.AsyncConnection at 3.0.0). It exposes a sync ``cursor()``
        # method that returns an async cursor.
        cur = connection.cursor()
        try:
            async with cur:
                await cur.execute(self._topology_query)
                return list(await cur.fetchall())
        except Exception:
            # A failed topology probe should not raise into the caller;
            # the caller will see an empty topology and fall back to cache.
            return []

    def _rows_to_topology(self, rows: List[tuple]) -> Topology:
        hosts: List[HostInfo] = []
        for row in rows:
            if not row:
                continue
            server_id = row[0]
            is_writer = bool(row[1]) if len(row) > 1 else False
            host = self._host_from_server_id(server_id)
            hosts.append(
                HostInfo(
                    host=host,
                    port=self._default_port,
                    role=HostRole.WRITER if is_writer else HostRole.READER,
                )
            )
        # Writer first, readers after.
        hosts.sort(key=lambda h: 0 if h.role == HostRole.WRITER else 1)
        return tuple(hosts)

    def _host_from_server_id(self, server_id: str) -> str:
        """Translate an Aurora server ID into a reachable host name.

        Uses the ``cluster_instance_host_pattern`` connection property when
        set (e.g., ``?.cluster-xyz.us-east-1.rds.amazonaws.com``). Falls
        back to the server ID itself if unset.
        """
        pattern = WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.get(
            self._props
        )
        if pattern and "?" in pattern:
            return pattern.replace("?", server_id)
        return server_id

    async def stop(self) -> None:
        return None
