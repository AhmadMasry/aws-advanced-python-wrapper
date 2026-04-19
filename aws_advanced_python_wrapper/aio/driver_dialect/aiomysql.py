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

"""``AsyncAiomysqlDriverDialect`` -- concrete ``AsyncDriverDialect`` for aiomysql.

Adapts aiomysql's API surface into the wrapper's ABC:

- ``aiomysql.connect(**kwargs)`` returns a ``_ConnectionContextManager``;
  awaiting it yields a ``Connection``. The ABC's ``connect`` awaits the
  connect callable directly and returns the real connection.
- ``conn.close()`` is sync (closes the socket immediately); async-safe
  shutdown uses ``await conn.ensure_closed()``.
- ``conn.commit()`` / ``conn.rollback()`` / ``conn.autocommit(bool)`` /
  ``conn.ping(reconnect=False)`` are all coroutines.
- ``conn.cursor()`` is sync and returns an awaitable cursor.
- aiomysql's connect kwarg for database is ``db`` (not ``database``).
  The dialect renames ``database`` -> ``db`` before invoking the driver
  so users can stay on the wrapper's preferred property name.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Awaitable, Callable

from aws_advanced_python_wrapper.aio.driver_dialect.base import \
    AsyncDriverDialect
from aws_advanced_python_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo


class AsyncAiomysqlDriverDialect(AsyncDriverDialect):
    """Concrete :class:`AsyncDriverDialect` backed by aiomysql."""

    _dialect_code: str = DriverDialectCodes.MYSQL_CONNECTOR_PYTHON  # reuse MySQL code
    _driver_name: str = "aiomysql"
    _network_bound_methods = {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.CONNECTION_COMMIT.method_name,
        DbApiMethod.CONNECTION_ROLLBACK.method_name,
        DbApiMethod.CURSOR_EXECUTE.method_name,
        DbApiMethod.CURSOR_EXECUTEMANY.method_name,
        DbApiMethod.CURSOR_FETCHONE.method_name,
        DbApiMethod.CURSOR_FETCHMANY.method_name,
        DbApiMethod.CURSOR_FETCHALL.method_name,
    }

    def supports_connect_timeout(self) -> bool:
        return True

    def supports_socket_timeout(self) -> bool:
        return False  # aiomysql doesn't expose per-socket timeout

    def supports_tcp_keepalive(self) -> bool:
        return False

    def supports_abort_connection(self) -> bool:
        return False  # no async abort primitive; close is closest

    def is_dialect(self, connect_func: Callable) -> bool:
        """Match if ``connect_func`` is aiomysql's connect."""
        import aiomysql
        return connect_func is aiomysql.connect

    def prepare_connect_info(
            self, host_info: HostInfo, props: Properties) -> Properties:
        from aws_advanced_python_wrapper.utils.properties import \
            PropertiesUtils

        # Can't just call super() -- the base class's
        # remove_wrapper_props() strips `database` (a known wrapper
        # property) before we'd see it. Copy + rename + strip locally
        # in that order.
        prop_copy = Properties(props.copy())
        prop_copy["host"] = host_info.host
        if host_info.is_port_specified():
            prop_copy["port"] = str(host_info.port)
        # aiomysql's connect expects `db=`, not `database=`. Translate
        # before the wrapper-prop stripper drops `database`. Explicit
        # `db=` beats a generic `database=`.
        if "db" not in prop_copy and "database" in prop_copy:
            prop_copy["db"] = prop_copy["database"]
        PropertiesUtils.remove_wrapper_props(prop_copy)
        return prop_copy

    async def connect(
            self,
            host_info: HostInfo,
            props: Properties,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        prepared = self.prepare_connect_info(host_info, props)
        # Coerce port to int -- aiomysql rejects string ports.
        if "port" in prepared:
            prepared["port"] = int(prepared["port"])
        # aiomysql.connect returns a _ConnectionContextManager; awaiting
        # it yields the Connection.
        return await connect_func(**prepared)

    async def is_closed(self, conn: Any) -> bool:
        return not conn.open

    async def abort_connection(self, conn: Any) -> None:
        # aiomysql has no abort primitive. Closing the socket is the
        # closest analog -- matches sync MySQL driver dialect.
        conn.close()

    async def is_in_transaction(self, conn: Any) -> bool:
        # aiomysql tracks transaction state via autocommit + query
        # history. We approximate by checking autocommit: if
        # autocommit is False AND the connection has issued at least
        # one statement, we're in a transaction. Without a
        # statement-history hook, we conservatively treat
        # autocommit-off connections as potentially in a transaction.
        return not conn.get_autocommit()

    async def get_autocommit(self, conn: Any) -> bool:
        return bool(conn.get_autocommit())

    async def set_autocommit(self, conn: Any, autocommit: bool) -> None:
        await conn.autocommit(autocommit)

    async def is_read_only(self, conn: Any) -> bool:
        # aiomysql has no dedicated read_only flag at the Python API
        # level. MySQL's session variable `transaction_read_only`
        # would need a server round-trip; we'd have to track intent
        # ourselves. Approximation: we don't know, return False.
        return bool(getattr(conn, "_aws_read_only", False))

    async def set_read_only(self, conn: Any, read_only: bool) -> None:
        # Apply via SET TRANSACTION and stash intent for
        # transfer_session_state.
        async with conn.cursor() as cur:
            await cur.execute(
                "SET SESSION TRANSACTION READ ONLY"
                if read_only else
                "SET SESSION TRANSACTION READ WRITE"
            )
        # Stash intent on the conn so is_read_only can read it back.
        conn._aws_read_only = bool(read_only)  # type: ignore[attr-defined]

    async def can_execute_query(self, conn: Any) -> bool:
        return bool(conn.open)

    async def transfer_session_state(
            self, from_conn: Any, to_conn: Any) -> None:
        await self.set_autocommit(to_conn, await self.get_autocommit(from_conn))
        try:
            await self.set_read_only(to_conn, await self.is_read_only(from_conn))
        except Exception:
            # Session-state transfer must not block failover.
            pass

    async def ping(self, conn: Any) -> bool:
        if await self.is_closed(conn):
            return False
        try:
            await conn.ping(reconnect=False)
            return True
        except Exception:
            return False
