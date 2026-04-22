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

import asyncio
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
        self._writer_host_info: Optional[HostInfo] = None
        self._reader_conn: Optional[Any] = None
        self._reader_host_info: Optional[HostInfo] = None

        # Telemetry counters -- one per successful reader/writer swap.
        # NullTelemetryFactory returns a no-op object; real factories may
        # return None, so every .inc() guards with ``is not None``.
        tf = self._plugin_service.get_telemetry_factory()
        self._switch_to_reader_counter = tf.create_counter(
            "rws.switches.to_reader.count")
        self._switch_to_writer_counter = tf.create_counter(
            "rws.switches.to_writer.count")

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
            self._writer_host_info = host_info
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

    @staticmethod
    def _host_in_topology(
            host_info: HostInfo,
            topology: tuple) -> bool:
        """Return True if ``host_info`` (by host+port) is in the topology."""
        for h in topology:
            if h.host == host_info.host and h.port == host_info.port:
                return True
        return False

    @staticmethod
    def _is_pool_connection(conn: Any) -> bool:
        """Return True if ``conn`` appears to be managed by SQLAlchemy's
        internal pool.

        Sync uses ConnectionProviderManager to look up the provider class
        (read_write_splitting_plugin.py:214-216). Async uses a simpler
        heuristic: check the connection's module path. Misses custom
        pool wrappers but covers the common SA pool case.
        """
        if conn is None:
            return False
        module = getattr(type(conn), "__module__", "")
        return module.startswith("sqlalchemy.pool")

    async def _release_pool_conn(
            self,
            conn: Any,
            driver_dialect: AsyncDriverDialect) -> None:
        """Close ``conn`` if it's from SQLAlchemy's pool so it returns to
        the pool rather than staying held by our plugin.
        """
        if not self._is_pool_connection(conn):
            return
        try:
            if await driver_dialect.is_closed(conn):
                return
        except Exception:  # noqa: BLE001 - probe is best-effort
            return
        try:
            close = conn.close
            result = close()
            if asyncio.iscoroutine(result):
                await result
        except Exception:  # noqa: BLE001 - close is best-effort
            pass

    async def _switch_to_reader(
            self,
            driver_dialect: AsyncDriverDialect,
            current: Any) -> None:
        # Cache the writer we came from so we can bounce back quickly.
        if self._writer_conn is None:
            self._writer_conn = current
            if self._plugin_service.current_host_info is not None:
                self._writer_host_info = self._plugin_service.current_host_info

        topology = await self._host_list_provider.refresh(current)

        # Try to reuse cached reader if (a) it's not closed AND (b) its
        # host is still in topology.
        if (self._reader_conn is not None
                and self._reader_host_info is not None
                and self._host_in_topology(self._reader_host_info, topology)
                and not await driver_dialect.is_closed(self._reader_conn)):
            await self._plugin_service.set_current_connection(
                self._reader_conn, self._reader_host_info)
            await self._release_pool_conn(current, driver_dialect)
            if self._switch_to_reader_counter is not None:
                self._switch_to_reader_counter.inc()
            return

        # Cache invalid -> drop and reopen
        self._reader_conn = None
        self._reader_host_info = None

        reader_candidates = [h for h in topology if h.role == HostRole.READER]
        if not reader_candidates:
            raise ReadWriteSplittingError(
                "No reader host available in the current topology.")

        strategy = (WrapperProperties.READER_HOST_SELECTOR_STRATEGY.get(self._props)
                    or "random")

        # Sync parity (read_write_splitting_plugin.py:578-593): iterate up
        # to 2*len candidates, removing the dead ones. A non-deterministic
        # strategy (round_robin with cluster-level state) may repeat picks;
        # removing from the pool ensures we make forward progress.
        remaining = list(reader_candidates)
        last_error: Optional[Exception] = None
        for _ in range(2 * len(reader_candidates)):
            if not remaining:
                break
            reader = self._plugin_service.get_host_info_by_strategy(
                HostRole.READER, strategy, remaining)
            if reader is None:
                break
            try:
                new_conn = await self._open(driver_dialect, reader)
            except Exception as exc:
                last_error = exc
                remaining = [h for h in remaining if h != reader]
                continue
            self._reader_conn = new_conn
            self._reader_host_info = reader
            await self._plugin_service.set_current_connection(new_conn, reader)
            await self._release_pool_conn(current, driver_dialect)
            if self._switch_to_reader_counter is not None:
                self._switch_to_reader_counter.inc()
            return

        raise ReadWriteSplittingError(
            f"Could not open a reader connection after "
            f"{2 * len(reader_candidates)} candidate attempts."
        ) from last_error

    async def _switch_to_writer(
            self,
            driver_dialect: AsyncDriverDialect,
            current: Any) -> None:
        topology = await self._host_list_provider.refresh(current)

        # Try to reuse cached writer if valid + in topology + not closed.
        if (self._writer_conn is not None
                and self._writer_conn is not current
                and self._writer_host_info is not None
                and self._host_in_topology(self._writer_host_info, topology)
                and not await driver_dialect.is_closed(self._writer_conn)):
            await self._plugin_service.set_current_connection(
                self._writer_conn, self._writer_host_info)
            await self._release_pool_conn(current, driver_dialect)
            if self._switch_to_writer_counter is not None:
                self._switch_to_writer_counter.inc()
            return

        # Cache invalid -> drop and reopen
        self._writer_conn = None
        self._writer_host_info = None

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
        self._writer_host_info = writer
        await self._plugin_service.set_current_connection(new_conn, writer)
        await self._release_pool_conn(current, driver_dialect)
        if self._switch_to_writer_counter is not None:
            self._switch_to_writer_counter.inc()

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
