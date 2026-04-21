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

"""``AsyncPluginService`` -- async counterpart of :class:`PluginService`.

SP-1 ships the shell: the Protocol that plugins depend on + a minimal
``AsyncPluginServiceImpl`` good enough to wire toy plugins in unit tests.
Rich behavior (dialect detection, host list management, session-state
service, status storage) lands in later SPs that need it.
"""

from __future__ import annotations

from typing import (TYPE_CHECKING, Any, ClassVar, FrozenSet, List, Optional,
                    Protocol, Set, Tuple)

from aws_advanced_python_wrapper.aio.plugin import AsyncCanReleaseResources
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.exception_handling import ExceptionManager
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.storage.cache_map import CacheMap

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncHostListProvider
    from aws_advanced_python_wrapper.aio.plugin_manager import \
        AsyncPluginManager
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.host_availability import HostAvailability
    from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncPluginService(Protocol):
    """State + driver coordination for the async plugin pipeline.

    Pure getters stay sync. Anything that may talk to the database is async
    (e.g., ``is_in_transaction`` probes the connection; ``set_availability``
    is cache-only so stays sync).
    """

    @property
    def current_connection(self) -> Optional[Any]:
        """The currently-bound async driver connection, if any."""
        ...

    @property
    def current_host_info(self) -> Optional[HostInfo]:
        ...

    @property
    def props(self) -> Properties:
        ...

    @property
    def driver_dialect(self) -> AsyncDriverDialect:
        ...

    @property
    def database_dialect(self) -> Optional[DatabaseDialect]:
        """The resolved :class:`DatabaseDialect`, or ``None`` before connect."""
        ...

    @database_dialect.setter
    def database_dialect(self, value: Optional[DatabaseDialect]) -> None:
        ...

    def is_network_exception(
            self,
            error: Optional[Exception] = None,
            sql_state: Optional[str] = None) -> bool:
        ...

    def is_login_exception(
            self,
            error: Optional[Exception] = None,
            sql_state: Optional[str] = None) -> bool:
        ...

    def set_availability(
            self,
            host_aliases: FrozenSet[str],
            availability: HostAvailability) -> None:
        ...

    def get_availability(self, host_url: str) -> Optional[HostAvailability]:
        ...

    @property
    def plugin_manager(self) -> Optional[AsyncPluginManager]:
        ...

    @plugin_manager.setter
    def plugin_manager(self, value: AsyncPluginManager) -> None:
        ...

    @property
    def host_list_provider(self) -> Optional[AsyncHostListProvider]:
        ...

    @host_list_provider.setter
    def host_list_provider(self, value: AsyncHostListProvider) -> None:
        ...

    @property
    def all_hosts(self) -> Tuple[HostInfo, ...]:
        """Last-refreshed topology. Empty tuple before first refresh."""
        ...

    @property
    def initial_connection_host_info(self) -> Optional[HostInfo]:
        """First host the user connected to. ``None`` before connect."""
        ...

    @initial_connection_host_info.setter
    def initial_connection_host_info(self, value: Optional[HostInfo]) -> None:
        ...

    async def refresh_host_list(
            self,
            connection: Optional[Any] = None) -> None:
        ...

    async def force_refresh_host_list(
            self,
            connection: Optional[Any] = None) -> None:
        ...

    async def release_resources(self) -> None:
        """Best-effort shutdown. Idempotent. Does not raise."""
        ...

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        ...

    def get_host_info_by_strategy(
            self,
            role: HostRole,
            strategy: str,
            host_list: Optional[List[HostInfo]] = None) -> Optional[HostInfo]:
        ...

    @property
    def network_bound_methods(self) -> Set[str]:
        ...

    def is_network_bound_method(self, method_name: str) -> bool:
        ...

    async def set_current_connection(
            self,
            connection: Any,
            host_info: HostInfo) -> None:
        """Rebind the service to a new connection.

        Async because callers may need to transfer session state between the
        outgoing and incoming connections via the driver dialect.
        """
        ...


class AsyncPluginServiceImpl(AsyncPluginService):
    """Minimal concrete ``AsyncPluginService`` for SP-1.

    Holds the connection, host info, driver dialect, and properties. Does NOT
    yet manage:
      * host list refresh (SP-3)
      * dialect auto-upgrade (later SP)
      * session state service (later SP)
      * telemetry context (later SP)

    Those land as dedicated sub-projects. The shell is enough for toy plugins
    and for SP-2's ``AsyncAwsWrapperConnection`` to drive initial connect.
    """

    _host_availability_expiring_cache: ClassVar[CacheMap[str, HostAvailability]] = CacheMap()
    _HOST_AVAILABILITY_EXPIRATION_NANO: ClassVar[int] = 5 * 60 * 1_000_000_000  # 5 min

    def __init__(
            self,
            props: Properties,
            driver_dialect: AsyncDriverDialect,
            host_info: Optional[HostInfo] = None) -> None:
        self._props: Properties = props
        self._driver_dialect: AsyncDriverDialect = driver_dialect
        self._database_dialect: Optional[DatabaseDialect] = None
        self._exception_manager: ExceptionManager = ExceptionManager()
        self._plugin_manager: Optional[AsyncPluginManager] = None
        self._host_list_provider: Optional[AsyncHostListProvider] = None
        self._all_hosts: Tuple[HostInfo, ...] = ()
        self._initial_connection_host_info: Optional[HostInfo] = None
        self._current_host_info: Optional[HostInfo] = host_info
        self._current_connection: Optional[Any] = None

    @property
    def current_connection(self) -> Optional[Any]:
        return self._current_connection

    @property
    def current_host_info(self) -> Optional[HostInfo]:
        return self._current_host_info

    @property
    def props(self) -> Properties:
        return self._props

    @property
    def driver_dialect(self) -> AsyncDriverDialect:
        return self._driver_dialect

    @property
    def database_dialect(self) -> Optional[DatabaseDialect]:
        return self._database_dialect

    @database_dialect.setter
    def database_dialect(self, value: Optional[DatabaseDialect]) -> None:
        self._database_dialect = value

    def is_network_exception(
            self,
            error: Optional[Exception] = None,
            sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_network_exception(
            self._database_dialect, error=error, sql_state=sql_state)

    def is_login_exception(
            self,
            error: Optional[Exception] = None,
            sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_login_exception(
            self._database_dialect, error=error, sql_state=sql_state)

    def set_availability(
            self,
            host_aliases: FrozenSet[str],
            availability: HostAvailability) -> None:
        for alias in host_aliases:
            AsyncPluginServiceImpl._host_availability_expiring_cache.put(
                alias,
                availability,
                AsyncPluginServiceImpl._HOST_AVAILABILITY_EXPIRATION_NANO,
            )

    def get_availability(self, host_url: str) -> Optional[HostAvailability]:
        return AsyncPluginServiceImpl._host_availability_expiring_cache.get(host_url)

    @property
    def plugin_manager(self) -> Optional[AsyncPluginManager]:
        return self._plugin_manager

    @plugin_manager.setter
    def plugin_manager(self, value: AsyncPluginManager) -> None:
        self._plugin_manager = value

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        if self._plugin_manager is None:
            return False
        return self._plugin_manager.accepts_strategy(role, strategy)

    def get_host_info_by_strategy(
            self,
            role: HostRole,
            strategy: str,
            host_list: Optional[List[HostInfo]] = None) -> Optional[HostInfo]:
        if self._plugin_manager is None:
            return None
        return self._plugin_manager.get_host_info_by_strategy(role, strategy, host_list)

    @property
    def host_list_provider(self) -> Optional[AsyncHostListProvider]:
        return self._host_list_provider

    @host_list_provider.setter
    def host_list_provider(self, value: AsyncHostListProvider) -> None:
        self._host_list_provider = value

    @property
    def all_hosts(self) -> Tuple[HostInfo, ...]:
        return self._all_hosts

    @property
    def initial_connection_host_info(self) -> Optional[HostInfo]:
        return self._initial_connection_host_info

    @initial_connection_host_info.setter
    def initial_connection_host_info(self, value: Optional[HostInfo]) -> None:
        self._initial_connection_host_info = value

    async def refresh_host_list(
            self,
            connection: Optional[Any] = None) -> None:
        if self._host_list_provider is None:
            raise AwsWrapperError(
                Messages.get("AsyncPluginService.HostListProviderNotSet"))
        conn = connection if connection is not None else self._current_connection
        self._all_hosts = await self._host_list_provider.refresh(conn)

    async def force_refresh_host_list(
            self,
            connection: Optional[Any] = None) -> None:
        if self._host_list_provider is None:
            raise AwsWrapperError(
                Messages.get("AsyncPluginService.HostListProviderNotSet"))
        conn = connection if connection is not None else self._current_connection
        self._all_hosts = await self._host_list_provider.force_refresh(conn)

    async def release_resources(self) -> None:
        """Best-effort shutdown. Idempotent. Does not raise.

        Aborts the current connection via ``driver_dialect.abort_connection``
        and, if the host list provider exposes ``release_resources``, awaits
        that too. Exceptions are swallowed so one bad cleanup step doesn't
        block others. Plugins can register their own async shutdown via
        :func:`aws_advanced_python_wrapper.aio.cleanup.register_shutdown_hook`.
        """
        conn = self._current_connection
        if conn is not None:
            try:
                await self._driver_dialect.abort_connection(conn)
            except Exception:  # noqa: BLE001 - intentional best-effort teardown
                pass

        hlp = self._host_list_provider
        if isinstance(hlp, AsyncCanReleaseResources):
            try:
                await hlp.release_resources()
            except Exception:  # noqa: BLE001
                pass

    @property
    def network_bound_methods(self) -> Set[str]:
        return self._driver_dialect.network_bound_methods

    def is_network_bound_method(self, method_name: str) -> bool:
        from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
        nb = self.network_bound_methods
        return DbApiMethod.ALL.method_name in nb or method_name in nb

    async def set_current_connection(
            self,
            connection: Any,
            host_info: HostInfo) -> None:
        prev = self._current_connection
        self._current_connection = connection
        self._current_host_info = host_info
        if prev is not None and prev is not connection:
            # Give the driver dialect a chance to transfer session state.
            await self._driver_dialect.transfer_session_state(prev, connection)


__all__ = ["AsyncPluginService", "AsyncPluginServiceImpl"]
