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

"""Async failover plugin.

Listens on the async plugin pipeline for connection/query failures,
probes topology, and opens a replacement connection against the new
writer (or a reader, per ``failover_mode``). On success raises
``FailoverSuccessError`` so the caller retries its unit of work against
the new connection.

Shares the sync failover plugin's connection properties
(``failover_mode``, ``failover_timeout_sec``, ``enable_failover``).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable, List, Optional, Set

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import (
    AwsWrapperError, FailoverFailedError, FailoverSuccessError,
    TransactionResolutionUnknownError)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.failover_mode import (FailoverMode,
                                                             get_failover_mode)
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.host_list_provider import (
        AsyncHostListProvider, Topology)
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncFailoverPlugin(AsyncPlugin):
    """Async counterpart of :class:`FailoverPlugin`."""

    _SUBSCRIBED: Set[str] = {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.FORCE_CONNECT.method_name,
        DbApiMethod.CURSOR_EXECUTE.method_name,
        DbApiMethod.CURSOR_EXECUTEMANY.method_name,
        DbApiMethod.CURSOR_FETCHONE.method_name,
        DbApiMethod.CURSOR_FETCHMANY.method_name,
        DbApiMethod.CURSOR_FETCHALL.method_name,
        DbApiMethod.CONNECTION_COMMIT.method_name,
        DbApiMethod.CONNECTION_ROLLBACK.method_name,
    }

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            host_list_provider: AsyncHostListProvider,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._host_list_provider = host_list_provider
        self._props = props
        self._enabled = WrapperProperties.ENABLE_FAILOVER.get_bool(props)
        timeout = WrapperProperties.FAILOVER_TIMEOUT_SEC.get_float(props)
        self._failover_timeout_sec = float(timeout) if timeout is not None else 300.0
        self._mode = self._determine_mode(props)

    @staticmethod
    def _determine_mode(props: Properties) -> FailoverMode:
        mode = get_failover_mode(props)
        return mode if mode is not None else FailoverMode.STRICT_WRITER

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
        # Initial connect just passes through; failover only kicks in on
        # later errors.
        return await connect_func()

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        if not self._enabled:
            return await execute_func()
        try:
            return await execute_func()
        except Exception as exc:
            if not self._should_failover(exc):
                raise
            await self._do_failover(driver_dialect=self._plugin_service.driver_dialect)
            await self._raise_failover_success_or_txn_unknown(exc)

    def _should_failover(self, exc: Exception) -> bool:
        """Decide whether ``exc`` indicates a failover-worthy error.

        Mirrors sync v2 _should_exception_trigger_connection_switch at
        failover_v2_plugin.py:416-427: delegate to the dialect-aware
        ExceptionHandler through the plugin service, with a STRICT_WRITER
        escape hatch for read-only-connection exceptions (promotes stale
        read-only replicas to failover triggers).
        """
        # Avoid catching our own failover signals.
        if isinstance(exc, (FailoverSuccessError, FailoverFailedError)):
            return False
        if self._plugin_service.is_network_exception(error=exc):
            return True
        return (self._mode == FailoverMode.STRICT_WRITER
                and self._plugin_service.is_read_only_connection_exception(error=exc))

    async def _raise_failover_success_or_txn_unknown(
            self, original_exc: Exception) -> None:
        """Signal successful failover with the right exception type.

        Mirrors sync v2 _throw_failover_success_exception at
        failover_v2_plugin.py:312-321. If the caller was mid-transaction,
        their transaction's fate is unknown -- raise
        TransactionResolutionUnknownError so they don't blindly retry.
        Otherwise raise FailoverSuccessError so they can retry cleanly.
        """
        in_txn = False
        current = self._plugin_service.current_connection
        if current is not None:
            try:
                in_txn = await self._plugin_service.driver_dialect.is_in_transaction(current)
            except Exception:  # noqa: BLE001 - probe is best-effort
                in_txn = False
        if in_txn:
            raise TransactionResolutionUnknownError(
                "Failover succeeded mid-transaction; transaction state is unknown."
            ) from original_exc
        raise FailoverSuccessError(
            "Connection was replaced as part of failover; please retry the transaction."
        ) from original_exc

    async def _do_failover(self, driver_dialect: AsyncDriverDialect) -> None:
        """Orchestrate the failover: probe topology, pick target, reconnect.

        On connection attempts, mark each candidate's availability
        through the plugin service so the retry loop (and other
        plugins) stop hammering hosts that are known-dead and so a
        host that recovers is put back into rotation promptly
        (B.3 / A.3 TTL cache).
        """
        deadline = asyncio.get_event_loop().time() + self._failover_timeout_sec
        last_error: Optional[BaseException] = None

        while asyncio.get_event_loop().time() < deadline:
            try:
                topology = await self._host_list_provider.force_refresh(
                    self._plugin_service.current_connection
                )
            except Exception as e:
                last_error = e
                await asyncio.sleep(1.0)
                continue

            target = self._pick_target(topology)
            if target is None:
                await asyncio.sleep(1.0)
                continue

            try:
                new_conn = await self._open_connection(target, driver_dialect)
            except Exception as e:
                self._plugin_service.set_availability(
                    target.as_aliases(), HostAvailability.UNAVAILABLE)
                last_error = e
                await asyncio.sleep(1.0)
                continue

            if new_conn is not None:
                self._plugin_service.set_availability(
                    target.as_aliases(), HostAvailability.AVAILABLE)
                await self._plugin_service.set_current_connection(new_conn, target)
                return

            await asyncio.sleep(1.0)

        raise FailoverFailedError(
            "Failover could not establish a new connection within "
            f"{self._failover_timeout_sec}s"
        ) from last_error

    def _pick_target(self, topology: Topology) -> Optional[HostInfo]:
        if not topology:
            return None
        if self._mode == FailoverMode.STRICT_READER:
            readers = [h for h in topology if h.role == HostRole.READER]
            return readers[0] if readers else None
        if self._mode == FailoverMode.READER_OR_WRITER:
            # Prefer readers but fall back to writer.
            readers = [h for h in topology if h.role == HostRole.READER]
            if readers:
                return readers[0]
        # STRICT_WRITER or READER_OR_WRITER-no-readers fallback.
        writers = [h for h in topology if h.role == HostRole.WRITER]
        return writers[0] if writers else None

    async def _open_connection(
            self,
            target: HostInfo,
            driver_dialect: AsyncDriverDialect) -> Any:
        """Open a raw driver connection to ``target`` bypassing the
        pipeline to avoid recursive failover-on-failover."""
        props = self._build_target_props(target)
        # Fetch the target connect callable from the plugin service.
        # For SP-4 we assume psycopg.AsyncConnection.connect; SP-6 / SP-8
        # will generalize via the driver-dialect registry.
        try:
            import psycopg
            target_func = psycopg.AsyncConnection.connect
        except ImportError as e:  # pragma: no cover - psycopg is required
            raise AwsWrapperError(
                "psycopg is required for async failover"
            ) from e

        return await driver_dialect.connect(target, props, target_func)

    def _build_target_props(self, target: HostInfo) -> Properties:
        props_copy = self._plugin_service.props.copy()  # type: ignore[attr-defined]
        props_copy["host"] = target.host
        if target.is_port_specified():
            props_copy["port"] = str(target.port)
        return props_copy  # type: ignore[return-value]

    def notify_host_list_changed(self, changes: Any) -> None:
        """Host list changes from the topology monitor are cached on the
        provider already; failover reads live topology via force_refresh
        so no action needed here."""
        return None


# Re-export for parity with sync failover plugin's public surface.
__all__ = ["AsyncFailoverPlugin"]


# The following attrs keep mypy happy when AsyncFailoverPlugin is used
# with asyncio.get_event_loop in older Python versions.
_unused: List[str] = []
