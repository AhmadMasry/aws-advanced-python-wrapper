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

"""``AsyncPsycopgDriverDialect`` -- concrete ``AsyncDriverDialect`` for psycopg v3.

Implements every abstract method by talking to :class:`psycopg.AsyncConnection`
directly. No plugin code should ever bypass this dialect and import psycopg
itself (F3-B master spec invariant 8a).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Awaitable, Callable

from aws_advanced_python_wrapper.aio.driver_dialect.base import \
    AsyncDriverDialect
from aws_advanced_python_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncPsycopgDriverDialect(AsyncDriverDialect):
    """Concrete :class:`AsyncDriverDialect` backed by :mod:`psycopg` v3 async API."""

    _dialect_code: str = DriverDialectCodes.PSYCOPG
    _driver_name: str = "psycopg-async"
    _network_bound_methods = {
        DbApiMethod.CONNECTION_COMMIT.method_name,
        DbApiMethod.CONNECTION_ROLLBACK.method_name,
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.CURSOR_EXECUTE.method_name,
        DbApiMethod.CURSOR_EXECUTEMANY.method_name,
        DbApiMethod.CURSOR_FETCHONE.method_name,
        DbApiMethod.CURSOR_FETCHMANY.method_name,
        DbApiMethod.CURSOR_FETCHALL.method_name,
    }

    def supports_connect_timeout(self) -> bool:
        return True

    def supports_socket_timeout(self) -> bool:
        return True

    def supports_tcp_keepalive(self) -> bool:
        return True

    def supports_abort_connection(self) -> bool:
        # psycopg.AsyncConnection doesn't expose pthread_cancel-style abort;
        # closing the connection from another task is the closest analog.
        return False

    def is_dialect(self, connect_func: Callable) -> bool:
        """Match if ``connect_func`` is psycopg's async connect."""
        import psycopg
        if connect_func is psycopg.AsyncConnection.connect:
            return True
        # Bound-method identity: classmethod access creates a new bound obj
        # each time, so compare via ``__func__`` if available.
        target = getattr(connect_func, "__func__", connect_func)
        expected = getattr(psycopg.AsyncConnection.connect, "__func__",
                           psycopg.AsyncConnection.connect)
        return target is expected

    async def connect(
            self,
            host_info: HostInfo,
            props: Properties,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        prepared = self.prepare_connect_info(host_info, props)
        # psycopg.AsyncConnection.connect accepts conninfo + kwargs or all
        # kwargs. Our prepared dict is kwargs-style (host, port, user, ...).
        return await connect_func(**prepared)

    async def is_closed(self, conn: Any) -> bool:
        # psycopg.AsyncConnection exposes `closed` as a sync property.
        return bool(conn.closed)

    async def abort_connection(self, conn: Any) -> None:
        await conn.close()

    async def is_in_transaction(self, conn: Any) -> bool:
        # psycopg.AsyncConnection.info.transaction_status. IDLE (0) means no
        # active transaction; any other status means there is one.
        import psycopg
        status = conn.info.transaction_status
        return status != psycopg.pq.TransactionStatus.IDLE

    async def get_autocommit(self, conn: Any) -> bool:
        return bool(conn.autocommit)

    async def set_autocommit(self, conn: Any, autocommit: bool) -> None:
        await conn.set_autocommit(autocommit)

    async def is_read_only(self, conn: Any) -> bool:
        # psycopg exposes `read_only` as a sync property when available.
        return bool(getattr(conn, "read_only", False))

    async def set_read_only(self, conn: Any, read_only: bool) -> None:
        await conn.set_read_only(read_only)

    async def can_execute_query(self, conn: Any) -> bool:
        if await self.is_closed(conn):
            return False
        import psycopg
        status = conn.info.transaction_status
        # INERROR or UNKNOWN means we can't run new queries until rollback.
        return status not in {
            psycopg.pq.TransactionStatus.INERROR,
            psycopg.pq.TransactionStatus.UNKNOWN,
        }

    async def transfer_session_state(self, from_conn: Any, to_conn: Any) -> None:
        # Copy autocommit + read_only from the previous connection onto the
        # new one. These are the two most common session-state bits plugins
        # expect to survive failover.
        await self.set_autocommit(to_conn, await self.get_autocommit(from_conn))
        try:
            await self.set_read_only(to_conn, await self.is_read_only(from_conn))
        except Exception:
            # Some psycopg versions reject set_read_only outside an idle txn.
            # A failed state transfer shouldn't block failover itself.
            pass

    async def ping(self, conn: Any) -> bool:
        if await self.is_closed(conn):
            return False
        try:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                await cur.fetchone()
            return True
        except Exception:
            return False
