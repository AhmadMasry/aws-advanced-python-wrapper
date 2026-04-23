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
from typing import (TYPE_CHECKING, Any, Dict, List, Optional, Protocol, Tuple,
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

    Refresh flow (N.1b, matches sync RdsHostListProvider -> ClusterTopologyMonitor):

    * :meth:`refresh` / :meth:`force_refresh` delegate to a per-provider
      :class:`AsyncClusterTopologyMonitor` so panic-mode probing
      engages automatically on initial connect when writer discovery
      stalls. The monitor also keeps a cached ``last_topology`` so
      cache-hit refreshes don't hit the DB.
    * The monitor itself calls :meth:`_fetch_from_db` (private, no
      recursion) to run the actual SQL probe.

    Thread-safety: the cache is protected by an :class:`asyncio.Lock`.
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
        # Monitor wiring (N.1b). Lazy-created on first refresh so
        # provider construction stays cheap and test-friendly.
        self._monitor: Optional[Any] = None  # AsyncClusterTopologyMonitor
        self._last_conn: Optional[Any] = None

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
        """Return cached topology if still fresh, else force a refresh.

        Matches sync :meth:`RdsHostListProvider.refresh` semantics: if the
        cache is within the refresh window, short-circuit; otherwise
        delegate to :meth:`force_refresh` which goes through the
        cluster topology monitor (panic mode engages automatically on
        initial connect when writer discovery stalls).
        """
        if connection is not None:
            self._last_conn = connection
        now_ns = asyncio.get_event_loop().time() * 1_000_000_000
        if (self._topology_cache is not None
                and now_ns - self._last_refresh_ns < self._refresh_ns):
            return self._topology_cache
        return await self.force_refresh(connection)

    async def force_refresh(self, connection: Optional[Any] = None) -> Topology:
        """Run the topology query and update the cache.

        Delegates to :class:`AsyncClusterTopologyMonitor.force_refresh_with_connection`
        so panic-mode probing engages when writer discovery stalls --
        sync parity with :meth:`RdsHostListProvider._force_refresh_monitor`.
        Falls back to a direct DB query if the monitor can't be built
        (e.g., no connection available and no cache).
        """
        if connection is not None:
            self._last_conn = connection

        if connection is None:
            if self._topology_cache is not None:
                return self._topology_cache
            return ()

        async with self._refresh_lock:
            monitor = self._get_or_create_monitor()
            if monitor is None:
                # Monitor couldn't be built; fall back to direct query.
                return await self._fetch_and_cache(connection)
            try:
                topology = await monitor.force_refresh_with_connection(
                    connection, bypass_ignore_window=True)
            except Exception:  # noqa: BLE001 - monitor failure
                return await self._fetch_and_cache(connection)
            if topology:
                self._topology_cache = topology
                self._last_refresh_ns = int(
                    asyncio.get_event_loop().time() * 1_000_000_000
                )
            return topology or (self._topology_cache or ())

    def _get_or_create_monitor(self) -> Optional[Any]:
        """Lazy-construct the per-provider topology monitor.

        Returns ``None`` if monitor construction fails or if the
        topology_query is absent (e.g., static/unsupported dialects).
        Mirrors sync :meth:`RdsHostListProvider._get_or_create_monitor`.
        """
        if self._monitor is not None and self._monitor.is_running():
            return self._monitor
        if not self._topology_query:
            return None
        try:
            from aws_advanced_python_wrapper.aio.cluster_topology_monitor import \
                AsyncClusterTopologyMonitor
        except Exception:  # noqa: BLE001 - defensive import guard
            return None

        high_refresh_ms = WrapperProperties.CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS.get_int(
            self._props)
        monitor = AsyncClusterTopologyMonitor(
            provider=self,
            connection_getter=lambda: self._last_conn,
            refresh_interval_sec=self._refresh_ns / 1_000_000_000,
            high_refresh_rate_sec=(
                (high_refresh_ms / 1000.0) if high_refresh_ms else 1.0),
        )
        monitor.start()
        self._monitor = monitor
        # Register monitor teardown with the global cleanup hook so
        # release_resources_async() tears it down.
        try:
            from aws_advanced_python_wrapper.aio.cleanup import \
                register_shutdown_hook
            register_shutdown_hook(monitor.stop)
        except Exception:  # noqa: BLE001
            pass
        return monitor

    async def _fetch_and_cache(self, connection: Any) -> Topology:
        """Direct DB query + cache update, used as a monitor fallback."""
        rows = await self._run_topology_query(connection)
        topology = self._rows_to_topology(rows)
        if topology:
            self._topology_cache = topology
            self._last_refresh_ns = int(
                asyncio.get_event_loop().time() * 1_000_000_000
            )
        return topology or (self._topology_cache or ())

    async def _fetch_from_db(self, connection: Any) -> Topology:
        """Raw DB-query path used BY the cluster topology monitor.

        Does NOT re-enter the monitor (which would recurse). The
        monitor's ``_run`` / ``force_refresh_with_connection`` both
        call this; all other callers should go through the public
        :meth:`refresh` / :meth:`force_refresh`.
        """
        rows = await self._run_topology_query(connection)
        return self._rows_to_topology(rows)

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


class AsyncMultiAzHostListProvider(AsyncAuroraHostListProvider):
    """Async MultiAz Cluster topology provider.

    Ports sync :class:`MultiAzTopologyUtils`
    (``host_list_provider.py:630-702``). Runs a two-step query:
    ``writer_host_query`` to identify the writer's ID, then
    ``topology_query`` for the full host list. Row shape:
    ``(id, host, port)``; role assigned by ``id == writer_id``.

    Instantiate with explicit SQL strings. Full auto-detection from
    :class:`DatabaseDialect` introspection is a future enhancement (sync
    reads them from ``MultiAzClusterMysqlDialect`` /
    ``MultiAzClusterPgDialect`` -- those dialects don't yet flow through
    the async PluginService dialect resolution chain).

    Two-step behavior matches sync:
    1. Execute ``writer_host_query``. A non-empty row => its first column
       is the writer's ID.
    2. Empty row (MySQL "we're the writer" case) => fall back to
       ``host_id_query`` for our own ID.
    3. Execute ``topology_query`` and parse ``(id, host, port)`` rows;
       role is ``WRITER`` iff ``id == writer_id``.

    Optional ``instance_template_host`` substitutes ``?`` in the template
    with the instance-ID extracted from the row's host via
    :meth:`RdsUtils.get_instance_id`, matching sync
    ``MultiAzTopologyUtils._create_multi_az_host``.
    """

    def __init__(
            self,
            props: Properties,
            driver_dialect: AsyncDriverDialect,
            writer_host_query: str,
            topology_query: str,
            host_id_query: str,
            cluster_id: Optional[str] = None,
            default_port: int = 5432,
            instance_template_host: Optional[str] = None,
    ) -> None:
        super().__init__(
            props=props,
            driver_dialect=driver_dialect,
            topology_query=topology_query,
            cluster_id=cluster_id,
            default_port=default_port,
        )
        self._writer_host_query = writer_host_query
        self._host_id_query = host_id_query
        self._instance_template_host = instance_template_host
        # Import locally to avoid widening the module's top-level import set.
        from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils
        self._rds_utils = RdsUtils()
        self._writer_id: Optional[str] = None

    async def _run_topology_query(self, connection: Any) -> List[tuple]:
        """Two-step MultiAz topology query.

        Step 1: ``writer_host_query``. Empty row => fall through to
        ``host_id_query`` (we are the writer, MySQL-style).
        Step 2: ``topology_query`` for the full host list.

        We stash the identified writer ID on ``self._writer_id`` so
        :meth:`_rows_to_topology` can assign roles. A failure at any step
        returns ``[]`` so callers fall back to cache, matching
        :meth:`AsyncAuroraHostListProvider._run_topology_query`.
        """
        try:
            cur = connection.cursor()
            async with cur:
                # Step 1: writer host query. Empty row => we're the writer
                # (MySQL), fall back to host_id_query.
                await cur.execute(self._writer_host_query)
                writer_row = await cur.fetchone()
                if writer_row is not None:
                    writer_id = str(writer_row[0])
                else:
                    await cur.execute(self._host_id_query)
                    self_row = await cur.fetchone()
                    if self_row is None:
                        return []
                    writer_id = str(self_row[0])

                # Step 2: topology query.
                await cur.execute(self._topology_query)
                rows = list(await cur.fetchall())

            self._writer_id = writer_id
            return rows
        except Exception:
            # Mirror AsyncAuroraHostListProvider: a failed probe yields
            # an empty topology rather than raising into the caller.
            return []

    def _rows_to_topology(self, rows: List[tuple]) -> Topology:
        writer_id = self._writer_id
        hosts: List[HostInfo] = []
        for row in rows:
            if len(row) < 3:
                continue
            row_id = str(row[0])
            host = str(row[1])
            try:
                port = int(row[2])
            except (TypeError, ValueError):
                port = self._default_port

            role = HostRole.WRITER if row_id == writer_id else HostRole.READER

            # Optional instance-template substitution. Matches sync
            # MultiAzTopologyUtils._create_multi_az_host.
            if self._instance_template_host:
                instance_name = self._rds_utils.get_instance_id(host)
                if instance_name:
                    substituted = self._instance_template_host.replace(
                        "?", instance_name
                    )
                    if ":" in substituted:
                        host_part, _, port_part = substituted.partition(":")
                        host = host_part
                        if port_part.isdigit():
                            port = int(port_part)
                    else:
                        host = substituted

            host_info = HostInfo(
                host=host,
                port=port,
                role=role,
                host_id=row_id,
            )
            host_info.add_alias(host)
            hosts.append(host_info)

        # Validate: must have exactly one writer (sync MultiAz clears
        # everything if no writer is found).
        writers = [h for h in hosts if h.role == HostRole.WRITER]
        if not writers:
            return ()
        # Writer first, readers after -- matches AsyncAuroraHostListProvider.
        hosts.sort(key=lambda h: 0 if h.role == HostRole.WRITER else 1)
        return tuple(hosts)


class AsyncGlobalAuroraHostListProvider(AsyncAuroraHostListProvider):
    """Async Global Aurora topology provider.

    Subclasses :class:`AsyncAuroraHostListProvider` and overrides row
    parsing to handle Global Aurora's cross-region topology. Row shape:
    ``(server_id, region, is_writer)``. Each host is built from the
    region-specific template configured via the ``instance_templates_by_region``
    ctor argument or the ``GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS`` property
    (comma-separated ``region=pattern`` pairs, where ``pattern`` may include
    a ``:port`` suffix).

    Minimal port -- sync's :class:`GlobalAuroraTopologyMonitor` thread
    machinery is not mirrored; topology is fetched on-demand via the base
    class's standard refresh path. Full auto-detection via ``DatabaseDialect``
    is a future enhancement (sync reads the topology query from
    ``GlobalAuroraTopologyDialect`` -- that path does not yet flow through
    the async ``PluginService`` dialect resolution chain).
    """

    def __init__(
            self,
            props: Properties,
            driver_dialect: AsyncDriverDialect,
            topology_query: str,
            *,
            instance_templates_by_region: Optional[Dict[str, str]] = None,
            cluster_id: Optional[str] = None,
            default_port: int = 5432,
    ) -> None:
        super().__init__(
            props=props,
            driver_dialect=driver_dialect,
            topology_query=topology_query,
            cluster_id=cluster_id,
            default_port=default_port,
        )
        if instance_templates_by_region is None:
            raw = WrapperProperties.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.get(
                props
            )
            if raw:
                instance_templates_by_region = self._parse_templates(raw)
            else:
                instance_templates_by_region = {}
        self._templates_by_region: Dict[str, str] = instance_templates_by_region

    @staticmethod
    def _parse_templates(raw: str) -> Dict[str, str]:
        """Parse ``'region=pattern,region=pattern,...'`` into a dict.

        Malformed pairs (missing ``=``, empty) are silently dropped so
        one bad entry doesn't drop the whole provider.
        """
        result: Dict[str, str] = {}
        for pair in raw.split(","):
            pair = pair.strip()
            if not pair or "=" not in pair:
                continue
            region, pattern = pair.split("=", 1)
            region = region.strip()
            pattern = pattern.strip()
            if region and pattern:
                result[region] = pattern
        return result

    def _rows_to_topology(self, rows: List[tuple]) -> Topology:
        """Override. Rows are ``(server_id, region, is_writer)``.

        Each row's host is built from the region-specific template. Rows
        referencing a region without a configured template are skipped.
        Returns an empty tuple if no writer is present (matches
        :class:`AsyncMultiAzHostListProvider`).
        """
        hosts: List[HostInfo] = []
        for row in rows:
            if len(row) < 3:
                continue
            server_id = str(row[0])
            region = str(row[1])
            is_writer = bool(row[2])
            template = self._templates_by_region.get(region)
            if template is None:
                # No template for this region -- skip the host.
                continue
            host_str = template.replace("?", server_id)
            host = host_str
            port = self._default_port
            if ":" in host_str:
                host_part, port_part = host_str.rsplit(":", 1)
                try:
                    port = int(port_part)
                    host = host_part
                except ValueError:
                    # Non-numeric port -- treat whole string as host.
                    pass
            role = HostRole.WRITER if is_writer else HostRole.READER
            host_info = HostInfo(
                host=host,
                port=port,
                role=role,
                host_id=server_id,
            )
            host_info.add_alias(host)
            hosts.append(host_info)

        # Validate: at least one writer. Match the MultiAz/sync convention
        # of returning an empty topology when no writer is found so callers
        # fall back to the cache.
        if not any(h.role == HostRole.WRITER for h in hosts):
            return ()
        # Writer first, readers after -- matches the other async providers.
        hosts.sort(key=lambda h: 0 if h.role == HostRole.WRITER else 1)
        return tuple(hosts)
