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

"""``AsyncAwsWrapperConnection`` + ``AsyncAwsWrapperCursor``.

Async counterparts of :class:`AwsWrapperConnection` and :class:`AwsWrapperCursor`.
Every DB operation routes through ``AsyncPluginManager`` so plugins
(failover, EFM, R/W splitting -- delivered in later sub-projects) can
intercept it.
"""

from __future__ import annotations

import asyncio
from typing import (TYPE_CHECKING, Any, Callable, List, Optional, Sequence,
                    Type, Union)

from aws_advanced_python_wrapper.aio.plugin_manager import AsyncPluginManager
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.database_dialect import DatabaseDialectManager
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncHostListProvider
    from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect


_TOPOLOGY_REQUIRING_PLUGINS = frozenset({
    "failover",
    "failover_v2",
    "read_write_splitting",
    "custom_endpoint",
    "aurora_connection_tracker",
})


def _build_host_list_provider(
        props: Properties,
        driver_dialect: AsyncDriverDialect,
        database_dialect: Any = None) -> AsyncHostListProvider:
    """Pick an async host list provider based on plugins + DB dialect.

    Selection order, matching sync database_dialect.py:
      * ``plugins`` references no topology-requiring plugin -> static.
      * database_dialect is a MultiAz dialect -> MultiAz provider.
      * database_dialect is a GlobalAurora dialect -> GlobalAurora provider.
      * Otherwise -> Aurora provider (the default topology path).

    ``database_dialect`` is passed optionally because early-phase callers
    (tests, legacy paths) may not have resolved the dialect yet; the
    function falls back to a plain Aurora provider in that case.
    """
    from aws_advanced_python_wrapper.aio.host_list_provider import (
        AsyncAuroraHostListProvider, AsyncGlobalAuroraHostListProvider,
        AsyncMultiAzHostListProvider, AsyncStaticHostListProvider)
    from aws_advanced_python_wrapper.aio.plugin_factory import \
        parse_plugins_property

    codes = parse_plugins_property(props) or []
    if not any(c.strip() in _TOPOLOGY_REQUIRING_PLUGINS for c in codes):
        return AsyncStaticHostListProvider(props)

    # Topology-aware path. Pick the provider matching the dialect.
    if database_dialect is not None:
        dialect_name = type(database_dialect).__name__
        if "MultiAz" in dialect_name:
            # Pull MultiAz queries off the dialect class. Sync exposes
            # them as class-level constants; fall back to no provider
            # if they're missing so callers degrade gracefully.
            writer_q = getattr(
                database_dialect, "_WRITER_HOST_QUERY", None)
            topology_q = getattr(
                database_dialect, "_TOPOLOGY_QUERY", None)
            host_id_q = getattr(
                database_dialect, "_HOST_ID_QUERY",
                getattr(database_dialect, "host_id_query", None))
            if writer_q and topology_q and host_id_q:
                return AsyncMultiAzHostListProvider(
                    props, driver_dialect,
                    writer_host_query=writer_q,
                    topology_query=topology_q,
                    host_id_query=host_id_q,
                )
        if "GlobalAurora" in dialect_name:
            topology_q = getattr(
                database_dialect, "topology_query", None)
            if topology_q:
                return AsyncGlobalAuroraHostListProvider(
                    props, driver_dialect, topology_query=topology_q)

    return AsyncAuroraHostListProvider(props, driver_dialect)


def _resolve_database_dialect(
        driver_dialect: AsyncDriverDialect,
        props: Properties) -> DatabaseDialect:
    """Minimal DatabaseDialect resolution for Phase A.

    Honors the ``wrapper_dialect`` prop if set; otherwise falls back to the
    driver-dialect's default database dialect via :class:`DatabaseDialectManager`.
    Full auto-upgrade (Aurora-vs-stock detection via DB query) lands in a
    later phase.
    """
    manager = DatabaseDialectManager(props)
    return manager.get_dialect(driver_dialect.dialect_code, props)


class AsyncAwsWrapperCursor:
    """Async counterpart of :class:`AwsWrapperCursor`.

    Wraps a driver-async cursor; every query/fetch routes through the plugin
    pipeline via :class:`AsyncPluginManager`.
    """

    def __init__(
            self,
            conn: AsyncAwsWrapperConnection,
            target_cursor: Any) -> None:
        self._conn = conn
        self._target_cursor = target_cursor

    @property
    def connection(self) -> AsyncAwsWrapperConnection:
        return self._conn

    @property
    def target_cursor(self) -> Any:
        return self._target_cursor

    @property
    def description(self) -> Any:
        return self._target_cursor.description

    @property
    def rowcount(self) -> int:
        return self._target_cursor.rowcount

    @property
    def arraysize(self) -> int:
        return self._target_cursor.arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._target_cursor.arraysize = value

    @property
    def lastrowid(self) -> Any:
        return self._target_cursor.lastrowid

    async def execute(
            self,
            query: Any,
            params: Any = None,
            **kwargs: Any) -> AsyncAwsWrapperCursor:
        async def _call() -> Any:
            if params is None:
                return await self._target_cursor.execute(query, **kwargs)
            return await self._target_cursor.execute(query, params, **kwargs)

        await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_EXECUTE, _call, query, params,
        )
        return self

    async def executemany(
            self,
            query: Any,
            seq_of_params: Sequence[Any],
            **kwargs: Any) -> AsyncAwsWrapperCursor:
        async def _call() -> Any:
            return await self._target_cursor.executemany(
                query, seq_of_params, **kwargs)

        await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_EXECUTEMANY, _call, query, seq_of_params,
        )
        return self

    async def fetchone(self) -> Any:
        async def _call() -> Any:
            return await self._target_cursor.fetchone()

        return await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_FETCHONE, _call,
        )

    async def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        async def _call() -> Any:
            if size is None:
                return await self._target_cursor.fetchmany()
            return await self._target_cursor.fetchmany(size)

        return await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_FETCHMANY, _call, size,
        )

    async def fetchall(self) -> List[Any]:
        async def _call() -> Any:
            return await self._target_cursor.fetchall()

        return await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_FETCHALL, _call,
        )

    async def close(self) -> None:
        async def _call() -> Any:
            return await self._target_cursor.close()

        await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_CLOSE, _call,
        )

    async def scroll(self, value: int, mode: str = "relative") -> Any:
        """Advance/rewind the cursor (PEP 249 optional extension).

        Sync drivers expose ``scroll`` as sync; async drivers may make it
        a coroutine. We probe the return value and await only when needed
        so the same wrapper method works for both shapes.
        """
        async def _call() -> Any:
            result = self._target_cursor.scroll(value, mode)
            if asyncio.iscoroutine(result):
                return await result
            return result

        return await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_SCROLL, _call, value, mode,
        )

    async def callproc(self, procname: str, args: Any = ()) -> Any:
        """Call a stored procedure (PEP 249 optional extension).

        Like :meth:`scroll`, probes the target's return value and awaits
        only if the driver made ``callproc`` async.
        """
        async def _call() -> Any:
            result = self._target_cursor.callproc(procname, args)
            if asyncio.iscoroutine(result):
                return await result
            return result

        return await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_CALLPROC, _call, procname, args,
        )

    async def nextset(self) -> Optional[bool]:
        """Advance to the next result set (PEP 249).

        Probes the target's return value and awaits only if async.
        """
        async def _call() -> Any:
            result = self._target_cursor.nextset()
            if asyncio.iscoroutine(result):
                return await result
            return result

        return await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_NEXTSET, _call,
        )

    def setinputsizes(self, sizes: Any) -> None:
        """PEP 249 input-size hint. Pass-through to target cursor (sync,
        no network I/O worth intercepting)."""
        self._target_cursor.setinputsizes(sizes)

    def setoutputsize(self, size: int, column: Optional[int] = None) -> None:
        """PEP 249 output-size hint. Pass-through to target cursor (sync,
        no network I/O worth intercepting)."""
        self._target_cursor.setoutputsize(size, column)

    async def __aenter__(self) -> AsyncAwsWrapperCursor:
        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Any) -> None:
        await self.close()

    def __getattr__(self, name: str) -> Any:
        """Proxy unknown attributes to the underlying cursor.

        Lets driver-specific attrs (e.g., psycopg's ``statusmessage``) work
        transparently when SA or application code asks for them.
        """
        return getattr(self._target_cursor, name)


class AsyncAwsWrapperConnection:
    """Async counterpart of :class:`AwsWrapperConnection`."""

    __module__ = "aws_advanced_python_wrapper.aio"

    def __init__(
            self,
            plugin_service: AsyncPluginServiceImpl,
            plugin_manager: AsyncPluginManager,
            target_conn: Any) -> None:
        self._plugin_service = plugin_service
        self._plugin_manager = plugin_manager
        self._target_conn = target_conn

    @property
    def target_connection(self) -> Any:
        """The underlying driver async connection."""
        return self._target_conn

    @property
    def autocommit(self) -> Any:
        """Current autocommit setting, read from the driver dialect.

        This is the getter half of an async-aware autocommit API. The
        setter is spelled :meth:`set_autocommit` (a coroutine) rather
        than an ``@autocommit.setter`` because property setters cannot
        be ``async``. Routing stays at the driver-dialect layer because
        autocommit is session state -- the plugin pipeline doesn't
        intercept it on the sync side either.

        Return type is :class:`typing.Any` rather than ``bool`` because
        :meth:`AsyncDriverDialect.get_autocommit` is itself async -- callers
        may need to ``await`` the returned coroutine. Sync dialects (or
        mocked dialects in tests) may return ``bool`` directly; the runtime
        value is whatever the dialect hands back.
        """
        return self._plugin_service.driver_dialect.get_autocommit(self._target_conn)

    async def set_autocommit(self, value: bool) -> None:
        """Set autocommit on the underlying driver connection.

        Awaited counterpart of the :attr:`autocommit` getter. See that
        docstring for the reason this isn't an ``@autocommit.setter``.
        """
        await self._plugin_service.driver_dialect.set_autocommit(self._target_conn, value)

    @property
    def isolation_level(self) -> Any:
        """Current isolation level, read directly from the driver connection.

        Drivers vary in how they model isolation level (psycopg exposes
        it as an attribute; mysql drivers typically don't). We return
        whatever the target exposes, or ``None`` if the driver doesn't
        surface it.
        """
        return getattr(self._target_conn, "isolation_level", None)

    async def set_isolation_level(self, level: Any) -> None:
        """Set isolation level on the underlying driver connection.

        Drivers vary (psycopg uses an enum; mysql uses a SQL string).
        Delegates to the raw connection's ``set_isolation_level`` if
        present (awaiting if async), else assigns the attribute.
        """
        target = self._target_conn
        setter = getattr(target, "set_isolation_level", None)
        if setter is not None:
            result = setter(level)
            if asyncio.iscoroutine(result):
                await result
            return
        # Fallback: attribute assignment.
        target.isolation_level = level

    # Notice/notify handler passthroughs (psycopg3 parity).
    #
    # SQLAlchemy's psycopg dialect registers a notice handler on every
    # engine.connect() (sqlalchemy/dialects/postgresql/psycopg.py:575).
    # Without these passthroughs the wrapper would AttributeError there
    # and break every connect on the Aurora path.
    #
    # These delegate straight to the target connection and BYPASS the
    # plugin chain: notice/notify handler registration is pure local
    # client-side state (it just installs a callback slot on the
    # connection object), it does not hit the database, and plugins
    # have no reason to intercept it.
    #
    # Signatures are plain ``def`` (not ``async def``) because
    # ``psycopg.AsyncConnection.add_notice_handler`` is itself a sync
    # method -- it does not await anything. Verified against
    # ``psycopg 3.3.3`` in this repo's venv (iscoroutinefunction=False
    # for both sync and async variants).
    #
    # The aiomysql driver does not expose these methods -- callers on
    # MySQL targets will get the underlying driver's AttributeError,
    # which matches psycopg3 parity (these are PostgreSQL-only
    # features).

    def add_notice_handler(self, callback: Any) -> Any:
        return self._target_conn.add_notice_handler(callback)

    def remove_notice_handler(self, callback: Any) -> Any:
        return self._target_conn.remove_notice_handler(callback)

    def add_notify_handler(self, callback: Any) -> Any:
        return self._target_conn.add_notify_handler(callback)

    def remove_notify_handler(self, callback: Any) -> Any:
        return self._target_conn.remove_notify_handler(callback)

    @staticmethod
    async def connect(
            target: Union[None, str, Callable] = None,
            conninfo: str = "",
            *args: Any,
            plugins: Union[None, str, List[AsyncPlugin]] = None,
            **kwargs: Any) -> AsyncAwsWrapperConnection:
        """Open a new async wrapper connection.

        :param target: the target driver's async connect callable (e.g.,
            ``psycopg.AsyncConnection.connect``). Required.
        :param conninfo: connection info string (driver-specific format).
        :param plugins: accepts three shapes:
            * ``list[AsyncPlugin]`` -- explicit plugin instances; takes
              precedence over any ``plugins`` connection-property string.
            * ``str`` -- comma-separated plugin codes (e.g.,
              ``"failover,efm"``); routed into the props dict and
              resolved via :mod:`plugin_factory`.
            * ``None`` -- defer to the ``plugins`` connection-property
              string (if present); when absent, no plugins load.
        :param kwargs: merged into the connection properties alongside
            ``conninfo``.
        """
        if not target:
            raise AwsWrapperError(Messages.get("Wrapper.RequiredTargetDriver"))
        if not callable(target):
            raise AwsWrapperError(Messages.get("Wrapper.ConnectMethod"))
        target_func: Callable = target

        # Normalize `plugins` kwarg: string form folds into the props
        # dict so the factory path resolves it just like a property.
        if isinstance(plugins, str):
            kwargs["plugins"] = plugins
            plugins = None

        props: Properties = PropertiesUtils.parse_properties(
            conn_info=conninfo, **kwargs)

        # Pick the driver dialect. SP-2 hardcodes psycopg-async; later SPs
        # will add a DriverDialectManager for async drivers so this becomes
        # dispatch by target_func identity.
        from aws_advanced_python_wrapper.aio.driver_dialect.psycopg import \
            AsyncPsycopgDriverDialect
        driver_dialect: AsyncDriverDialect = AsyncPsycopgDriverDialect()

        host = props.get("host", "")
        port_raw = props.get("port")
        port = int(port_raw) if port_raw is not None else -1
        host_info = HostInfo(host=host, port=port)

        # Resolve the database dialect first so the host-list-provider
        # selection can pick a MultiAz/GlobalAurora variant when the
        # dialect class indicates it (matches sync's behavior).
        database_dialect = _resolve_database_dialect(driver_dialect, props)

        # Host-list-provider selection: if `plugins=...` references any
        # topology-requiring plugin (failover, rws), build a topology
        # provider matching the dialect; otherwise static is enough.
        host_list_provider = _build_host_list_provider(
            props, driver_dialect, database_dialect)

        plugin_service = AsyncPluginServiceImpl(
            props=props,
            driver_dialect=driver_dialect,
            host_info=host_info,
        )
        # Phase A wiring: populate plugin service slots so plugins that
        # reach for them in their own ``connect`` hook (e.g., failover
        # checking ``is_network_exception``) have them available.
        plugin_service.database_dialect = database_dialect
        plugin_service.host_list_provider = host_list_provider
        plugin_service.initial_connection_host_info = host_info

        # Resolve plugin list. Explicit `plugins=[...]` wins; otherwise
        # parse the `plugins` connection-property string via the factory
        # registry (Task 1-A).
        if plugins is None:
            from aws_advanced_python_wrapper.aio.plugin_factory import \
                build_async_plugins
            plugins = build_async_plugins(
                plugin_service=plugin_service,
                props=props,
                host_list_provider=host_list_provider,
            )

        plugin_manager = AsyncPluginManager(
            plugin_service=plugin_service,
            props=props,
            plugins=plugins,
        )
        plugin_service.plugin_manager = plugin_manager
        plugin_service.set_target_driver_func(target_func)

        target_conn = await plugin_manager.connect(
            target_driver_func=target_func,
            driver_dialect=driver_dialect,
            host_info=host_info,
            props=props,
            is_initial_connection=True,
        )

        if target_conn is None:
            raise AwsWrapperError(
                Messages.get("AwsWrapperConnection.ConnectionNotOpen")
            )

        await plugin_service.set_current_connection(target_conn, host_info)
        return AsyncAwsWrapperConnection(plugin_service, plugin_manager, target_conn)

    def cursor(self, *args: Any, **kwargs: Any) -> AsyncAwsWrapperCursor:
        """Return a new :class:`AsyncAwsWrapperCursor`.

        Matches :meth:`psycopg.AsyncConnection.cursor` semantics: sync call
        that returns an async cursor. Query execution on the cursor is async.
        """
        target_cursor = self._target_conn.cursor(*args, **kwargs)
        return AsyncAwsWrapperCursor(self, target_cursor)

    async def close(self) -> None:
        async def _call() -> Any:
            return await self._target_conn.close()

        await self._plugin_manager.execute(
            self, DbApiMethod.CONNECTION_CLOSE, _call,
        )

    async def commit(self) -> None:
        async def _call() -> Any:
            return await self._target_conn.commit()

        await self._plugin_manager.execute(
            self, DbApiMethod.CONNECTION_COMMIT, _call,
        )

    async def rollback(self) -> None:
        async def _call() -> Any:
            return await self._target_conn.rollback()

        await self._plugin_manager.execute(
            self, DbApiMethod.CONNECTION_ROLLBACK, _call,
        )

    async def __aenter__(self) -> AsyncAwsWrapperConnection:
        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Any) -> None:
        await self.close()

    def __getattr__(self, name: str) -> Any:
        """Proxy unknown attributes to the underlying connection.

        Lets SA's PG dialect and application code reach driver-specific
        state (``info``, ``pgconn``, ``adapters``, etc.) without special
        casing. The connection field is hit only when the attribute is
        NOT defined on the wrapper itself.
        """
        return getattr(self._target_conn, name)
