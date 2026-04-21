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

"""Async Simple Read/Write Splitting plugin.

Ports sync ``simple_read_write_splitting_plugin.py``. Unlike the regular
read/write splitting plugin (which picks hosts from cluster topology),
SRW routes connections to user-configured ``SRW_READ_ENDPOINT`` /
``SRW_WRITE_ENDPOINT`` based on ``set_read_only`` toggling.

Async-specific adaptations:

* The verification retry loop uses :func:`asyncio.sleep` instead of the
  sync plugin's blocking ``time.sleep``.
* Fresh endpoint connections are opened via ``driver_dialect.connect``,
  not through the full plugin pipeline. Caller plugins (IAM auth, etc.)
  will NOT re-apply to these connections. Matches the pattern in
  :mod:`aws_advanced_python_wrapper.aio.stale_dns_plugin` and is tracked
  as a Phase H deferred limitation.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional, Set

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                ReadWriteSplittingError)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService


logger = Logger(__name__)


class AsyncSimpleReadWriteSplittingPlugin(AsyncPlugin):
    """Async counterpart of :class:`SimpleReadWriteSplittingPlugin`."""

    _SUBSCRIBED: Set[str] = {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
    }

    _DEFAULT_RETRY_TIMEOUT_SEC = 60.0
    _DEFAULT_RETRY_INTERVAL_SEC = 1.0
    _DEFAULT_PORT = 5432

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props
        self._rds_utils = RdsUtils()

        read_endpoint = WrapperProperties.SRW_READ_ENDPOINT.get(props)
        write_endpoint = WrapperProperties.SRW_WRITE_ENDPOINT.get(props)
        if not read_endpoint:
            raise AwsWrapperError(
                f"'{WrapperProperties.SRW_READ_ENDPOINT.name}' is required.")
        if not write_endpoint:
            raise AwsWrapperError(
                f"'{WrapperProperties.SRW_WRITE_ENDPOINT.name}' is required.")

        self._verify_new_connections = bool(
            WrapperProperties.SRW_VERIFY_NEW_CONNECTIONS.get_bool(props))

        timeout_ms = WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS.get_int(props)
        self._retry_timeout_sec: float = (
            timeout_ms / 1000.0 if timeout_ms and timeout_ms > 0
            else self._DEFAULT_RETRY_TIMEOUT_SEC)

        interval_ms = WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS.get_int(props)
        self._retry_interval_sec: float = (
            interval_ms / 1000.0 if interval_ms and interval_ms > 0
            else self._DEFAULT_RETRY_INTERVAL_SEC)

        initial_type_raw = WrapperProperties.SRW_VERIFY_INITIAL_CONNECTION_TYPE.get(props)
        self._verify_initial_type = self._parse_role(initial_type_raw)

        self._write_endpoint = write_endpoint.casefold()
        self._read_endpoint = read_endpoint.casefold()
        self._write_host_info = self._create_host_info(
            write_endpoint, HostRole.WRITER)
        self._read_host_info = self._create_host_info(
            read_endpoint, HostRole.READER)

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
        if not is_initial_connection or not self._verify_new_connections:
            return await connect_func()

        # Determine what role we expect for the initial connection based on
        # the URL type or the override prop.
        url_type = self._rds_utils.identify_rds_type(host_info.host)
        expected: Optional[HostRole] = None
        if (url_type == RdsUrlType.RDS_WRITER_CLUSTER
                or url_type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER
                or self._verify_initial_type == HostRole.WRITER):
            expected = HostRole.WRITER
        elif (url_type == RdsUrlType.RDS_READER_CLUSTER
                or self._verify_initial_type == HostRole.READER):
            expected = HostRole.READER

        if expected is None:
            return await connect_func()

        conn = await self._verify_role(
            driver_dialect, host_info, expected, connect_func=connect_func)
        if conn is None:
            conn = await connect_func()
        self._plugin_service.initial_connection_host_info = host_info
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

        if read_only:
            await self._switch_to(
                self._read_host_info, HostRole.READER, driver_dialect)
        else:
            await self._switch_to(
                self._write_host_info, HostRole.WRITER, driver_dialect)
        return await execute_func()

    async def _switch_to(
            self,
            host_info: HostInfo,
            expected_role: HostRole,
            driver_dialect: AsyncDriverDialect) -> None:
        # Reuse cached if open.
        cached = (self._reader_conn if expected_role == HostRole.READER
                  else self._writer_conn)
        if cached is not None and not await driver_dialect.is_closed(cached):
            await self._plugin_service.set_current_connection(cached, host_info)
            return

        # Open a fresh connection (with optional role-verification retry).
        if self._verify_new_connections:
            new_conn = await self._verify_role(
                driver_dialect, host_info, expected_role)
        else:
            new_conn = await self._open(driver_dialect, host_info)

        if new_conn is None:
            raise ReadWriteSplittingError(
                f"Could not open a verified {expected_role.name} connection "
                f"to {host_info.host}:{host_info.port}.")

        if expected_role == HostRole.READER:
            self._reader_conn = new_conn
        else:
            self._writer_conn = new_conn
        await self._plugin_service.set_current_connection(new_conn, host_info)

    async def _verify_role(
            self,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            expected_role: HostRole,
            connect_func: Optional[Callable[..., Awaitable[Any]]] = None
    ) -> Optional[Any]:
        """Open a connection, probe its role, retry until timeout."""
        loop = asyncio.get_event_loop()
        deadline = loop.time() + self._retry_timeout_sec
        # Ensure at least one attempt even if timeout is 0/negative.
        first_pass = True
        while first_pass or loop.time() < deadline:
            first_pass = False
            conn: Optional[Any] = None
            try:
                if connect_func is not None:
                    conn = await connect_func()
                else:
                    conn = await self._open(driver_dialect, host_info)
                if conn is None:
                    await asyncio.sleep(self._retry_interval_sec)
                    continue
                actual = await self._plugin_service.get_host_role(conn)
                if actual == expected_role:
                    return conn
                # Wrong role: close and retry.
                await self._close_best_effort(driver_dialect, conn)
            except Exception:  # noqa: BLE001 - best-effort retry
                if conn is not None:
                    await self._close_best_effort(driver_dialect, conn)
            if loop.time() >= deadline:
                break
            await asyncio.sleep(self._retry_interval_sec)
        return None

    async def _open(
            self,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo) -> Any:
        """Open a new connection via the driver dialect.

        Bypasses the plugin pipeline; caller plugins (IAM auth etc.) will
        not re-apply to this connection. Tracked as a Phase H deferred
        limitation; matches :class:`AsyncStaleDnsPlugin`'s pattern.
        """
        # Local import avoids pulling psycopg at module load time for
        # consumers using a different async driver.
        import psycopg  # type: ignore

        new_props = Properties(dict(self._props))
        new_props["host"] = host_info.host
        if host_info.is_port_specified():
            new_props["port"] = str(host_info.port)
        return await driver_dialect.connect(
            host_info, new_props, psycopg.AsyncConnection.connect)

    @staticmethod
    async def _close_best_effort(
            driver_dialect: AsyncDriverDialect, conn: Any) -> None:
        try:
            await driver_dialect.abort_connection(conn)
        except Exception:  # noqa: BLE001 - close is best-effort
            pass

    def _create_host_info(
            self, endpoint: str, role: HostRole) -> HostInfo:
        endpoint = endpoint.strip()
        host = endpoint

        # Default port: database_dialect.default_port if available,
        # falling back to the current host's port (if set), else 5432.
        default_port = self._DEFAULT_PORT
        db_dialect = self._plugin_service.database_dialect
        if db_dialect is not None:
            try:
                default_port = db_dialect.default_port
            except Exception:  # noqa: BLE001 - fall through to fallback
                default_port = self._DEFAULT_PORT
        port = default_port
        current = self._plugin_service.current_host_info
        if current is not None and current.is_port_specified():
            port = current.port

        colon = endpoint.rfind(":")
        if colon != -1:
            port_str = endpoint[colon + 1:]
            if port_str.isdigit():
                host = endpoint[:colon]
                port = int(port_str)
        return HostInfo(host=host, port=port, role=role)

    @staticmethod
    def _parse_role(role_str: Optional[str]) -> Optional[HostRole]:
        if not role_str:
            return None
        s = role_str.lower()
        if s == "reader":
            return HostRole.READER
        if s == "writer":
            return HostRole.WRITER
        raise ValueError(
            f"Invalid {WrapperProperties.SRW_VERIFY_INITIAL_CONNECTION_TYPE.name}: "
            f"{role_str!r}")


__all__ = ["AsyncSimpleReadWriteSplittingPlugin"]
