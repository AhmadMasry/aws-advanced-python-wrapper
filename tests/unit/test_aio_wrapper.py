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

"""F3-B SP-2: AsyncAwsWrapperConnection + AsyncAwsWrapperCursor.

Tests exercise the wrapper with a mock ``psycopg.AsyncConnection`` so no
database is required. Covers:
  - ``connect`` factory routes through the plugin pipeline and stores
    the opened connection on the service.
  - ``cursor()`` returns an ``AsyncAwsWrapperCursor`` wrapping the driver cursor.
  - Cursor operations route through ``AsyncPluginManager.execute``.
  - Connection operations (close/commit/rollback) route through the pipeline.
  - ``__getattr__`` forwards unknown attrs to the underlying driver conn/cursor.
  - Async context manager protocol closes on exit.
  - Target-driver validation (``target`` must be a callable).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable, List, Set
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.driver_dialect.psycopg import \
    AsyncPsycopgDriverDialect
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.aio.wrapper import (AsyncAwsWrapperConnection,
                                                     AsyncAwsWrapperCursor)
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties

# ---- Plugin fixtures ----------------------------------------------------


class RecorderPlugin(AsyncPlugin):
    """Records every method seen through the execute pipeline."""

    def __init__(self, log: List[str]) -> None:
        self.log = log

    @property
    def subscribed_methods(self) -> Set[str]:
        return {DbApiMethod.ALL.method_name}

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: Any,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        self.log.append("connect:enter")
        result = await connect_func()
        self.log.append("connect:exit")
        return result

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        self.log.append(f"execute:{method_name}")
        return await execute_func()


# ---- Mock driver setup --------------------------------------------------


def _build_mock_psycopg_connect(returned_conn: Any) -> Callable[..., Awaitable[Any]]:
    """Build an awaitable that returns ``returned_conn``. Mimics
    :func:`psycopg.AsyncConnection.connect`."""

    async def _connect(**kwargs: Any) -> Any:
        return returned_conn

    return _connect


def _make_mock_async_conn() -> MagicMock:
    """Build a MagicMock shaped like a psycopg.AsyncConnection."""
    conn = MagicMock()
    conn.close = AsyncMock()
    conn.commit = AsyncMock()
    conn.rollback = AsyncMock()
    conn.closed = False
    conn.autocommit = True

    def _cursor(*args: Any, **kwargs: Any) -> MagicMock:
        return _make_mock_async_cursor()

    conn.cursor = MagicMock(side_effect=_cursor)
    return conn


def _make_mock_async_cursor() -> MagicMock:
    cur = MagicMock()
    cur.execute = AsyncMock(return_value=None)
    cur.executemany = AsyncMock(return_value=None)
    cur.fetchone = AsyncMock(return_value=("row",))
    cur.fetchmany = AsyncMock(return_value=[("a",), ("b",)])
    cur.fetchall = AsyncMock(return_value=[("r1",), ("r2",), ("r3",)])
    cur.close = AsyncMock()
    cur.description = [("col",)]
    cur.rowcount = 3
    cur.arraysize = 1
    return cur


# ---- Tests --------------------------------------------------------------


def test_connect_rejects_missing_target():
    async def _body() -> None:
        with pytest.raises(AwsWrapperError):
            await AsyncAwsWrapperConnection.connect()

    asyncio.run(_body())


def test_connect_rejects_non_callable_target():
    async def _body() -> None:
        with pytest.raises(AwsWrapperError):
            await AsyncAwsWrapperConnection.connect("not-a-callable")

    asyncio.run(_body())


def test_connect_opens_via_plugin_pipeline_and_returns_wrapper():
    log: List[str] = []
    plugin = RecorderPlugin(log)
    raw_conn = _make_mock_async_conn()

    async def _body() -> AsyncAwsWrapperConnection:
        return await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=example.com user=u password=p dbname=d port=5432",
            plugins=[plugin],
        )

    wrapper_conn = asyncio.run(_body())
    assert isinstance(wrapper_conn, AsyncAwsWrapperConnection)
    assert wrapper_conn.target_connection is raw_conn
    # Pipeline ordering: RecorderPlugin wraps AsyncDefaultPlugin.
    assert log == ["connect:enter", "connect:exit"]
    # Connection bound to the plugin service.
    assert wrapper_conn._plugin_service.current_connection is raw_conn


def test_connect_passes_host_and_port_from_props():
    raw_conn = _make_mock_async_conn()
    captured_kwargs: List[dict] = []

    async def _target(**kwargs: Any) -> Any:
        captured_kwargs.append(kwargs)
        return raw_conn

    async def _body() -> None:
        await AsyncAwsWrapperConnection.connect(
            target=_target,
            conninfo="host=h.example user=u password=p dbname=db port=6543",
        )

    asyncio.run(_body())
    assert captured_kwargs, "target_func was never invoked"
    kw = captured_kwargs[0]
    assert kw["host"] == "h.example"
    assert kw["port"] == "6543"
    assert kw["user"] == "u"
    assert kw["dbname"] == "db"


def test_cursor_is_sync_and_returns_async_cursor_wrapper():
    raw_conn = _make_mock_async_conn()

    async def _body() -> AsyncAwsWrapperConnection:
        return await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )

    wrapper = asyncio.run(_body())
    cur = wrapper.cursor()
    assert isinstance(cur, AsyncAwsWrapperCursor)
    assert cur.connection is wrapper


def test_cursor_execute_routes_through_plugin_pipeline():
    log: List[str] = []
    plugin = RecorderPlugin(log)
    raw_conn = _make_mock_async_conn()

    async def _body() -> None:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins=[plugin],
        )
        cur = wrapper.cursor()
        await cur.execute("SELECT 1")
        await cur.fetchone()
        await cur.fetchall()
        await cur.close()

    asyncio.run(_body())
    assert log == [
        "connect:enter",
        "connect:exit",
        "execute:Cursor.execute",
        "execute:Cursor.fetchone",
        "execute:Cursor.fetchall",
        "execute:Cursor.close",
    ]


def test_connection_commit_rollback_close_route_through_pipeline():
    log: List[str] = []
    plugin = RecorderPlugin(log)
    raw_conn = _make_mock_async_conn()

    async def _body() -> None:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins=[plugin],
        )
        await wrapper.commit()
        await wrapper.rollback()
        await wrapper.close()

    asyncio.run(_body())
    commit_calls = [e for e in log if e == "execute:Connection.commit"]
    rollback_calls = [e for e in log if e == "execute:Connection.rollback"]
    close_calls = [e for e in log if e == "execute:Connection.close"]
    assert len(commit_calls) == 1
    assert len(rollback_calls) == 1
    assert len(close_calls) == 1
    raw_conn.commit.assert_awaited_once()
    raw_conn.rollback.assert_awaited_once()
    raw_conn.close.assert_awaited_once()


def test_connection_async_context_manager_closes_on_exit():
    raw_conn = _make_mock_async_conn()

    async def _body() -> None:
        async with await AsyncAwsWrapperConnection.connect(
                target=_build_mock_psycopg_connect(raw_conn),
                conninfo="host=h user=u password=p dbname=d port=5432",
        ) as conn:
            assert isinstance(conn, AsyncAwsWrapperConnection)

    asyncio.run(_body())
    raw_conn.close.assert_awaited_once()


def test_connection_getattr_forwards_to_raw_conn():
    raw_conn = _make_mock_async_conn()
    raw_conn.info = "pgconn-info-sentinel"

    async def _body() -> AsyncAwsWrapperConnection:
        return await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )

    wrapper = asyncio.run(_body())
    assert wrapper.info == "pgconn-info-sentinel"


def test_cursor_getattr_forwards_to_target_cursor():
    raw_conn = _make_mock_async_conn()

    async def _body() -> AsyncAwsWrapperCursor:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )
        return wrapper.cursor()

    cur = asyncio.run(_body())
    # The mock async cursor has a description attribute on the target.
    assert cur.description == [("col",)]
    assert cur.rowcount == 3


def test_cursor_async_context_manager_closes_on_exit():
    raw_conn = _make_mock_async_conn()
    # Capture the single cursor mock the conn will hand out.
    mock_cursor = _make_mock_async_cursor()
    raw_conn.cursor = MagicMock(return_value=mock_cursor)

    async def _body() -> None:
        wrapper = await AsyncAwsWrapperConnection.connect(
            target=_build_mock_psycopg_connect(raw_conn),
            conninfo="host=h user=u password=p dbname=d port=5432",
        )
        async with wrapper.cursor() as cur:
            assert isinstance(cur, AsyncAwsWrapperCursor)
        mock_cursor.close.assert_awaited_once()

    asyncio.run(_body())


def test_psycopg_driver_dialect_is_dialect_recognizes_psycopg_connect():
    import psycopg

    dialect = AsyncPsycopgDriverDialect()
    assert dialect.is_dialect(psycopg.AsyncConnection.connect) is True

    def _other_connect() -> None:  # pragma: no cover - identity-only check
        pass

    # is_dialect still returns True as a default for unknown callables
    # (matches sync DriverDialect base behavior). Verifying it doesn't
    # raise, not that it's False.
    result = dialect.is_dialect(_other_connect)
    assert result in (True, False)


def test_psycopg_driver_dialect_lifecycle_ops_against_mock():
    """Exercise the dialect's async ops using a mock `AsyncConnection` shape."""

    async def _body() -> None:
        dialect = AsyncPsycopgDriverDialect()
        conn = _make_mock_async_conn()
        # Install a fake transaction_status on conn.info
        import psycopg
        conn.info = MagicMock()
        conn.info.transaction_status = psycopg.pq.TransactionStatus.IDLE

        assert await dialect.is_closed(conn) is False
        assert await dialect.is_in_transaction(conn) is False
        assert await dialect.get_autocommit(conn) is True
        conn.set_autocommit = AsyncMock()
        await dialect.set_autocommit(conn, False)
        conn.set_autocommit.assert_awaited_once_with(False)

        conn.read_only = False
        assert await dialect.is_read_only(conn) is False
        conn.set_read_only = AsyncMock()
        await dialect.set_read_only(conn, True)
        conn.set_read_only.assert_awaited_once_with(True)

        assert await dialect.can_execute_query(conn) is True
        await dialect.abort_connection(conn)
        conn.close.assert_awaited_once()

    asyncio.run(_body())


def test_psycopg_driver_dialect_network_bound_methods_covers_core():
    dialect = AsyncPsycopgDriverDialect()
    nb = dialect.network_bound_methods
    assert DbApiMethod.CONNECT.method_name in nb
    assert DbApiMethod.CURSOR_EXECUTE.method_name in nb
    assert DbApiMethod.CONNECTION_COMMIT.method_name in nb


def test_connect_populates_plugin_service_slots():
    """AsyncAwsWrapperConnection.connect populates database_dialect,
    host_list_provider, plugin_manager, and initial_connection_host_info
    on the plugin service."""
    import asyncio
    from unittest.mock import MagicMock

    from aws_advanced_python_wrapper.aio.wrapper import \
        AsyncAwsWrapperConnection
    from aws_advanced_python_wrapper.database_dialect import PgDatabaseDialect

    async def _fake_target(**kwargs):
        mock = MagicMock(spec=["close", "cursor"])
        mock.close = MagicMock()
        return mock

    conn = asyncio.run(
        AsyncAwsWrapperConnection.connect(
            target=_fake_target,
            host="localhost",
            dbname="test",
            user="u",
            password="p",
        )
    )

    # The plugin service slots should be populated
    svc = conn._plugin_service
    assert isinstance(svc.database_dialect, PgDatabaseDialect), \
        f"database_dialect was {svc.database_dialect!r}"
    assert svc.host_list_provider is not None
    assert svc.plugin_manager is not None
    assert svc.initial_connection_host_info is not None
    assert svc.initial_connection_host_info.host == "localhost"
