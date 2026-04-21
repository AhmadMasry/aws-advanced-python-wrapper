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

from typing import TYPE_CHECKING, Any, Optional, Protocol, Set

from aws_advanced_python_wrapper.exception_handling import ExceptionManager

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo
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

    def __init__(
            self,
            props: Properties,
            driver_dialect: AsyncDriverDialect,
            host_info: Optional[HostInfo] = None) -> None:
        self._props: Properties = props
        self._driver_dialect: AsyncDriverDialect = driver_dialect
        self._database_dialect: Optional[DatabaseDialect] = None
        self._exception_manager: ExceptionManager = ExceptionManager()
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
