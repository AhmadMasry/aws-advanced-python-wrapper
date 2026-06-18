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

"""Async connection-retry helper.

Async counterpart of :class:`aws_advanced_python_wrapper.utils.retry_util.RetryUtil`.
Drives the deadline-bounded retry loop the async GDB failover plugin uses to
acquire a connection to an *allowed* host (writer, reader, or a region-filtered
subset), verifying the host's role with a data-plane probe before accepting it.

Async adaptations of the sync logic:

* ``time.time()`` / ``time.sleep`` -> ``asyncio.get_event_loop().time()`` /
  ``await asyncio.sleep``.
* ``plugin_service.connect`` -> ``await plugin_service.force_connect`` -- the
  reconnect must re-run the plugin pipeline (so auth plugins re-apply, e.g. the
  IAM token that is regenerated per-connect) while BYPASSING any pooled provider
  and skipping the failover plugin itself, exactly as
  ``AsyncFailoverPlugin._open_connection`` documents.
* Sync ``plugin_service.hosts`` (allow/block filtered) ->
  ``plugin_service.filter_hosts(plugin_service.all_hosts)``.
* Sync ``plugin_service.refresh_host_list`` (cheap cache read, kept fresh by a
  high-rate background monitor thread) -> ``force_refresh_host_list`` each pass.
  Async ``refresh`` honors the ``topology_refresh_ms`` cache window (default
  30s) and would keep returning the stale pre-failover topology; force refresh
  routes through the cluster topology monitor's panic-mode probing so the new
  writer is discovered even while the trigger connection is dead.
* Connection close -> ``await driver_dialect.abort_connection`` (sever the raw
  socket; a pooled proxy's ``close`` would only return it to the pool).
* Each ``force_connect`` await is bounded by the remaining deadline so a
  blackholed host (no connect timeout) can't hang failover past
  ``failover_timeout_sec`` -- mirrors ``AsyncFailoverPlugin._within_deadline``.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable, List, Optional

from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.utils import LogUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.utils.properties import Properties

logger = Logger(__name__)


class AsyncRetryUtil:
    _SHORT_DELAY_SEC = 0.1

    class Results:
        def __init__(self, connection: Any, host_info: HostInfo):
            self._connection = connection
            self._host_info = host_info

        @property
        def connection(self) -> Any:
            return self._connection

        @property
        def host_info(self) -> HostInfo:
            return self._host_info

    async def get_writer_connection(
            self,
            plugin_service: AsyncPluginService,
            properties: Properties,
            plugin: Optional[AsyncPlugin],
            verify_role: bool,
            timeout_end_time: float) -> AsyncRetryUtil.Results:
        def allowed_writer_hosts() -> Optional[List[HostInfo]]:
            updated_hosts = plugin_service.all_hosts
            writer = next((host for host in updated_hosts if host.role == HostRole.WRITER), None)
            if writer is None:
                logger.debug("RetryUtil.NoWriterHost", LogUtils.log_topology(updated_hosts))
                return None

            allowed_hosts = plugin_service.filter_hosts(list(updated_hosts))
            if not any(host.host == writer.host and host.port == writer.port for host in allowed_hosts):
                logger.debug("RetryUtil.NewWriterNotAllowed", writer.host, LogUtils.log_topology(tuple(allowed_hosts)))
                return None
            return [writer]

        return await self.get_allowed_connection(
            plugin_service,
            properties,
            plugin,
            allowed_writer_hosts,
            None,
            HostRole.WRITER if verify_role else None,
            timeout_end_time)

    async def get_allowed_connection(
            self,
            plugin_service: AsyncPluginService,
            properties: Properties,
            plugin: Optional[AsyncPlugin],
            allowed_hosts: Callable[[], Optional[List[HostInfo]]],
            strategy: Optional[str],
            verify_role: Optional[HostRole],
            retry_end_time: float) -> AsyncRetryUtil.Results:
        if strategy is None or not strategy.strip():
            strategy = "random"

        candidate_conn: Optional[Any] = None
        try:
            while asyncio.get_event_loop().time() < retry_end_time:
                # Re-probe live topology each pass via FORCE refresh, not a plain
                # ``refresh_host_list``. Plain refresh returns the cached
                # pre-failover topology for up to ``topology_refresh_ms``
                # (default 30s) and does NOT engage the cluster topology
                # monitor's independent (panic-mode) host probing -- so it could
                # never discover the newly elected writer while the trigger
                # connection is dead. ``force_refresh_host_list`` routes through
                # the monitor, mirroring ``AsyncFailoverPlugin``'s use of
                # ``force_refresh`` in its own failover loops. The roles in the
                # returned list may still be stale, which is why each candidate's
                # role is re-verified with a data-plane query below.
                await plugin_service.force_refresh_host_list()
                updated_allowed_hosts = allowed_hosts()
                if updated_allowed_hosts is None:
                    await self._short_delay()
                    continue

                # Make a copy of hosts and mark them available so the host selector considers them.
                remaining_hosts = [self._available_copy(host) for host in updated_allowed_hosts]
                if not remaining_hosts:
                    await self._short_delay()
                    continue

                while remaining_hosts and asyncio.get_event_loop().time() < retry_end_time:
                    candidate_host = None
                    # When a specific role must be verified (writer failover), ask the
                    # selector for exactly that role. Otherwise -- the GDB *_OR_WRITER
                    # modes pass verify_role=None with an allowed list that already
                    # includes the writer -- the selector still requires a concrete
                    # role, so try a reader first (read-offload preference) then fall
                    # back to the writer. Right after a failover the newly elected
                    # writer can be the only reachable host; asking only for READER
                    # would spin until the deadline and fail with
                    # UnableToConnectToReader even though the writer is a valid target.
                    if verify_role is not None:
                        candidate_roles = [verify_role]
                    else:
                        candidate_roles = [HostRole.READER, HostRole.WRITER]
                    for candidate_role in candidate_roles:
                        try:
                            candidate_host = plugin_service.get_host_info_by_strategy(
                                candidate_role, strategy, remaining_hosts)
                        except Exception:
                            # Strategy can't get a host of this role; try the next.
                            candidate_host = None
                        if candidate_host is not None:
                            break

                    if candidate_host is None:
                        logger.debug("RetryUtil.CandidateNone", verify_role)
                        await self._short_delay()
                        break  # Exit loop over remaining_hosts and refresh topology.

                    try:
                        candidate_conn = await self._bounded(
                            plugin_service.force_connect(candidate_host, properties, plugin),
                            retry_end_time)
                        # Roles in the host list might be stale, so verify the role with a query.
                        role = await plugin_service.get_host_role(candidate_conn) if verify_role is not None else None
                        if verify_role is None or verify_role == role:
                            if role is not None and role != candidate_host.role:
                                updated_host_info = candidate_host.__copy__()
                                updated_host_info.role = role
                            else:
                                updated_host_info = candidate_host
                            result = AsyncRetryUtil.Results(candidate_conn, updated_host_info)
                            candidate_conn = None  # Prevents closing the returned connection below.
                            return result
                    except Exception as ex:
                        logger.debug("RetryUtil.ExceptionConnectingToWriter", candidate_host.host, ex)

                    # The connection couldn't be opened or the role is not as expected, so it is not valid.
                    remaining_hosts = [host for host in remaining_hosts
                                       if not (host.host == candidate_host.host and host.port == candidate_host.port)]
                    if candidate_conn is not None:
                        await self.close_connection(plugin_service, candidate_conn)
                        candidate_conn = None

                # Throttle the outer pass: when every candidate failed to
                # connect (e.g. the new writer isn't accepting connections yet),
                # the inner loop exits with no delay. Without this, the next
                # ``force_refresh_host_list`` (a real monitor probe) would be
                # issued back-to-back, hammering topology discovery for the whole
                # ``failover_timeout_sec``. Mirrors the inter-pass sleep in
                # ``AsyncFailoverPlugin``'s failover loops.
                await self._short_delay()

            raise TimeoutError(Messages.get("RetryUtil.Timeout"))
        finally:
            if candidate_conn is not None:
                await self.close_connection(plugin_service, candidate_conn)

    async def _bounded(self, coro: Awaitable[Any], deadline: float) -> Any:
        """Await ``coro`` but never past ``deadline``.

        Mirrors ``AsyncFailoverPlugin._within_deadline``: an individual
        ``force_connect`` against a blackholed host (no connect timeout) can
        block indefinitely and never return to the loop's deadline check.
        Bounding it by the remaining budget keeps ``failover_timeout_sec`` real;
        on expiry the raised ``TimeoutError`` is treated like any other failed
        candidate attempt by the caller's ``except`` clause.
        """
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            close = getattr(coro, "close", None)
            if callable(close):
                close()  # avoid 'coroutine was never awaited' warning
            raise TimeoutError(Messages.get("RetryUtil.Timeout"))
        return await asyncio.wait_for(coro, timeout=remaining)

    @staticmethod
    def _available_copy(host: HostInfo) -> HostInfo:
        host_copy = host.__copy__()
        host_copy.set_availability(HostAvailability.AVAILABLE)
        return host_copy

    @staticmethod
    async def close_connection(plugin_service: AsyncPluginService, conn: Any) -> None:
        try:
            driver_dialect = plugin_service.driver_dialect
            await driver_dialect.abort_connection(getattr(conn, "driver_connection", conn))
        except Exception:
            pass

    async def _short_delay(self) -> None:
        await asyncio.sleep(self._SHORT_DELAY_SEC)
