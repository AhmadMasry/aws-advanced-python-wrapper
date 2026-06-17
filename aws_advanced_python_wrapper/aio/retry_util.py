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
                logger.debug("RetryUtil.NewWriterNotAllowed", writer.host, LogUtils.log_topology(allowed_hosts))
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
                # The roles in this list might not be accurate, depending on whether the new
                # topology has become available yet.
                await plugin_service.refresh_host_list()
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
                    try:
                        # The host selector requires a non-null role, so default to READER when
                        # no specific role needs to be verified.
                        candidate_host = plugin_service.get_host_info_by_strategy(
                            verify_role if verify_role is not None else HostRole.READER,
                            strategy,
                            remaining_hosts)
                    except Exception:
                        # Strategy can't get a host according to the requested conditions.
                        # Do nothing
                        pass

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
