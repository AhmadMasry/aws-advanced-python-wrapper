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

"""Async read/write splitting plugin.

Routes connections based on ``set_read_only``: when the app sets
read-only True, the plugin swaps the current connection for one
targeting a reader; when read-only flips back to False, it swaps back
to the writer. Saves the writer connection so returning from read-only
is cheap.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional, Set

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import ReadWriteSplittingError
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncHostListProvider
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncReadWriteSplittingPlugin(AsyncPlugin):
    """Async counterpart of :class:`ReadWriteSplittingPlugin`."""

    _SUBSCRIBED: Set[str] = {
        DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
    }

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            host_list_provider: AsyncHostListProvider,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._host_list_provider = host_list_provider
        self._props = props
        self._writer_conn: Optional[Any] = None
        self._reader_conn: Optional[Any] = None

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
        conn = await connect_func()
        # Seed the writer cache on the initial connection so the first
        # set_read_only flip doesn't need to re-open the writer later.
        if is_initial_connection and self._writer_conn is None:
            self._writer_conn = conn
        return conn

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        if method_name != DbApiMethod.CONNECTION_SET_READ_ONLY.method_name:
            return await execute_func()

        read_only = bool(args[0]) if args else False

        driver_dialect = self._plugin_service.driver_dialect
        current = self._plugin_service.current_connection
        if current is None:
            return await execute_func()

        if read_only:
            # Sync parity (read_write_splitting_plugin.py:243-249): mid-txn
            # reader swap silently no-ops; caller's set_read_only(True) still
            # proceeds on the writer connection. The swap happens only when
            # safe.
            if not await driver_dialect.is_in_transaction(current):
                await self._switch_to_reader(driver_dialect, current)
        else:
            # Sync parity (read_write_splitting_plugin.py:261-265): mid-txn
            # writer swap raises -- the transaction's fate is undefined if we
            # swap connections underneath it.
            if await driver_dialect.is_in_transaction(current):
                raise ReadWriteSplittingError(
                    "Cannot switch back to the writer while in a transaction. "
                    "Commit or roll back before setting the connection "
                    "read-write.")
            await self._switch_to_writer(driver_dialect, current)
        return await execute_func()

    async def _switch_to_reader(
            self,
            driver_dialect: AsyncDriverDialect,
            current: Any) -> None:
        # Cache the writer we came from so we can bounce back quickly.
        if self._writer_conn is None:
            self._writer_conn = current

        # Reuse an existing reader connection if we have one.
        if self._reader_conn is not None:
            if not await driver_dialect.is_closed(self._reader_conn):
                await self._plugin_service.set_current_connection(
                    self._reader_conn,
                    self._plugin_service.current_host_info,  # type: ignore[arg-type]
                )
                return

        topology = await self._host_list_provider.refresh(current)
        reader_candidates = [h for h in topology if h.role == HostRole.READER]
        strategy = (WrapperProperties.READER_HOST_SELECTOR_STRATEGY.get(self._props)
                    or "random")
        reader = self._plugin_service.get_host_info_by_strategy(
            HostRole.READER, strategy, reader_candidates)
        if reader is None:
            raise ReadWriteSplittingError(
                "No reader host available in the current topology.")
        new_conn = await self._open(driver_dialect, reader)
        self._reader_conn = new_conn
        await self._plugin_service.set_current_connection(new_conn, reader)

    async def _switch_to_writer(
            self,
            driver_dialect: AsyncDriverDialect,
            current: Any) -> None:
        if self._writer_conn is not None and self._writer_conn is not current:
            if not await driver_dialect.is_closed(self._writer_conn):
                await self._plugin_service.set_current_connection(
                    self._writer_conn,
                    self._plugin_service.current_host_info,  # type: ignore[arg-type]
                )
                return

        topology = await self._host_list_provider.refresh(current)
        writer = next(
            (h for h in topology if h.role == HostRole.WRITER),
            None,
        )
        if writer is None:
            raise ReadWriteSplittingError(
                "No writer host available in the current topology."
            )
        new_conn = await self._open(driver_dialect, writer)
        self._writer_conn = new_conn
        await self._plugin_service.set_current_connection(new_conn, writer)

    async def _open(
            self,
            driver_dialect: AsyncDriverDialect,
            target: HostInfo) -> Any:
        # SP-6 currently hardcodes psycopg async connect; SP-8 will
        # generalize via driver-dialect registry.
        import psycopg

        from aws_advanced_python_wrapper.utils.properties import Properties
        props_copy: Properties = Properties(self._plugin_service.props.copy())  # type: ignore[attr-defined]
        props_copy["host"] = target.host
        if target.is_port_specified():
            props_copy["port"] = str(target.port)
        return await driver_dialect.connect(
            target, props_copy, psycopg.AsyncConnection.connect
        )
