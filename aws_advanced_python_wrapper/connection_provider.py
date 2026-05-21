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

from __future__ import annotations

from types import MappingProxyType
from typing import (TYPE_CHECKING, Callable, ClassVar, Dict, Mapping, Optional,
                    Protocol, Tuple)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
    from aws_advanced_python_wrapper.pep249 import Connection

from threading import Lock

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_selector import (
    HighestWeightHostSelector, HostSelector, RandomHostSelector,
    RoundRobinHostSelector, WeightedRandomHostSelector)
from aws_advanced_python_wrapper.plugin import CanReleaseResources
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils,
                                                          WrapperProperties)

logger = Logger(__name__)


# Per-host registry of locks serializing native-extension connects.
#
# Drivers whose ``DriverDialect.requires_connect_serialization`` returns
# True (currently only psycopg via libpq) acquire the host-keyed lock
# below before invoking ``target_driver_func(...)``. This prevents two
# concurrent ``PQconnectStart`` calls to the same Aurora endpoint, which
# has been observed to crash the pytest process with SIGSEGV inside
# libpq when paired with another libpq operation on a peer connection
# (see env-4 multi-5 PG faulthandler dump in
# tests/integration/host/build/test-results/...).
#
# Per-host (not global) so concurrent connects to *different* endpoints
# remain parallel - SA pool warm-up against a single writer endpoint
# pays the cost; cross-cluster traffic does not. MySQL with
# ``use_pure=True`` is pure-Python and skips the lock entirely
# (``requires_connect_serialization`` defaults to False).
_NATIVE_CONNECT_LOCKS: Dict[Tuple[str, str], Lock] = {}
_NATIVE_CONNECT_LOCKS_GUARD: Lock = Lock()


def _get_native_connect_lock(prepared_properties: Properties) -> Lock:
    host = str(prepared_properties.get("host", ""))
    port = str(prepared_properties.get("port", ""))
    key = (host, port)
    # Fast path: dict reads are atomic under the GIL, but we still need
    # the guard for the create-on-miss case to avoid two Locks racing.
    lock = _NATIVE_CONNECT_LOCKS.get(key)
    if lock is not None:
        return lock
    with _NATIVE_CONNECT_LOCKS_GUARD:
        return _NATIVE_CONNECT_LOCKS.setdefault(key, Lock())


class ConnectionProvider(Protocol):

    def accepts_host_info(self, host_info: HostInfo, props: Properties) -> bool:
        """
        Indicates whether this ConnectionProvider can provide connections for the given host and
        properties. Some :py:class:`ConnectionProvider` implementations may not be able to handle certain connection properties.

        :param host_info: the :py:class:`HostInfo` containing the host-port information for the host to connect to.
        :param props: the connection properties.
        :return: `True` if this :py:class:`ConnectionProvider` can provide connections for the given host. `False` otherwise.
        """
        ...

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        """
        Indicates whether the given selection strategy is supported by the connection provider.

        :param role: determines if the connection provider should return a reader host or a writer host.
        :param strategy: the host selection strategy to use.
        :return: whether the host selection strategy is supported.
        """
        ...

    def get_host_info_by_strategy(
            self, hosts: Tuple[HostInfo, ...], role: HostRole, strategy: str, props: Optional[Properties]) -> HostInfo:
        """
        Return a reader or a writer host using the specified strategy.

        This method should raise an :py:class:`AwsWrapperError` if the specified strategy is unsupported.

        :param hosts: the list of hosts to select from.
        :param role: determines if the connection provider should return a reader or a writer host.
        :param strategy: the host selection strategy to use.
        :param props: the connection properties.
        :return: the host selected using the specified strategy.
        """
        ...

    def connect(
            self,
            target_func: Callable,
            driver_dialect: DriverDialect,
            database_dialect: DatabaseDialect,
            host_info: HostInfo,
            props: Properties) -> Connection:
        """
        Called once per connection that needs to be created.

        :param target_func: the `Connect` method used by target driver dialect.
        :param driver_dialect: a dialect that handles target driver specific implementation.
        :param database_dialect: a dialect that handles database engine specific implementation.
        :param host_info: the host details for the desired connection.
        :param props: the connection properties.
        :return: the established connection resulting from the given connection information.
        """
        ...


class DriverConnectionProvider(ConnectionProvider):
    _accepted_strategies: Dict[str, HostSelector] = {"random": RandomHostSelector(),
                                                     "round_robin": RoundRobinHostSelector(),
                                                     "weighted_random": WeightedRandomHostSelector(),
                                                     "highest_weight": HighestWeightHostSelector()}

    @classmethod
    def accepted_strategies(cls) -> Mapping[str, HostSelector]:
        """Public read-only view of the HostSelector registry.

        Returns an immutable view so async callers can reuse the sync
        registry without reaching into the private ``_accepted_strategies``
        attribute. New selectors added to the sync dict become visible
        here automatically.
        """
        return MappingProxyType(cls._accepted_strategies)

    def accepts_host_info(self, host_info: HostInfo, props: Properties) -> bool:
        return True

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return strategy in self._accepted_strategies

    def get_host_info_by_strategy(
            self, hosts: Tuple[HostInfo, ...], role: HostRole, strategy: str, props: Optional[Properties]) -> HostInfo:
        host_selector: Optional[HostSelector] = self._accepted_strategies.get(strategy)
        if host_selector is not None:
            return host_selector.get_host(hosts, role, props)

        raise AwsWrapperError(
            Messages.get_formatted("DriverConnectionProvider.UnsupportedStrategy", strategy))

    def connect(
            self,
            target_func: Callable,
            driver_dialect: DriverDialect,
            database_dialect: DatabaseDialect,
            host_info: HostInfo,
            props: Properties) -> Connection:
        prepared_properties = driver_dialect.prepare_connect_info(host_info, props)
        database_dialect.prepare_conn_props(prepared_properties)
        logger.debug("DriverConnectionProvider.ConnectingToHost", host_info.host,
                     PropertiesUtils.log_properties(PropertiesUtils.mask_properties(prepared_properties)))

        def _do_native_connect() -> Connection:
            # Lock acquisition is in the worker thread so that on
            # Fix B2 timeout, the worker continues to hold the lock
            # while libpq is still blocked; subsequent submits queue
            # up behind it. Fix B1's ``tcp_user_timeout`` errors the
            # zombie's socket at the kernel layer, the worker
            # finally returns with an exception, the lock releases,
            # and the next attempt can acquire it.
            if driver_dialect.requires_connect_serialization:
                with _get_native_connect_lock(prepared_properties):
                    return target_func(**prepared_properties)
            return target_func(**prepared_properties)

        # Fix B2: bound the connect at the Python level so callers
        # don't block past the configured ``connect_timeout`` when
        # libpq's own timeout doesn't fire (TLS handshake / startup-
        # packet wait that ``psycopg.waiting.wait_conn`` was observed
        # hanging in on py3.13-pg env-4 multi-5). On TimeoutError the
        # OS thread inside libpq still can't be killed from Python --
        # we rely on Fix B1's kernel-level ``tcp_user_timeout`` to
        # eventually unblock the executor worker so the next attempt
        # can run on a fresh thread.
        try:
            connect_timeout_sec = WrapperProperties.CONNECT_TIMEOUT_SEC.get_int(prepared_properties)
        except Exception:
            connect_timeout_sec = 0
        if not connect_timeout_sec or connect_timeout_sec <= 0:
            return _do_native_connect()

        executor = services_container.get_thread_pool("DriverConnectionProviderConnect")
        future = executor.submit(_do_native_connect)
        # +2s buffer so libpq's own connect_timeout has a chance to
        # fire first with a more specific error before our Python
        # wrapper steps in.
        return future.result(timeout=connect_timeout_sec + 2)


class ConnectionProviderManager:
    _lock: ClassVar[Lock] = Lock()
    _conn_provider: ClassVar[Optional[ConnectionProvider]] = None

    def __init__(self, default_provider: ConnectionProvider = DriverConnectionProvider()):
        self._default_provider: ConnectionProvider = default_provider

    @property
    def default_provider(self):
        return self._default_provider

    @staticmethod
    def set_connection_provider(connection_provider: ConnectionProvider):
        """
        Setter that can optionally be called to request a non-default :py:class:`ConnectionProvider`.
        The requested :py:class:`ConnectionProvider` will be used to establish future connections unless it does not support a requested host,
        in which case the default :py:class:`ConnectionProvider` will be used. See :py:method:`ConnectionProvider.accepts_host_info` for more details.
        :param connection_provider: the :py:class:`ConnectionProvider` to use to establish new connections.
        """
        with ConnectionProviderManager._lock:
            ConnectionProviderManager._conn_provider = connection_provider

    def get_connection_provider(self, host_info: HostInfo, props: Properties) -> ConnectionProvider:
        """
        Get the :py:class:`ConnectionProvider` to use to establish a connection using the given host details and properties.
        If a non-default :py:class:`ConnectionProvider` has been set using :py:method:`ConnectionProvider.set_connection_provider`
        and :py:method:`ConnectionProvider.accepts_host_info` returns `True`, the non-default :py:class:`ConnectionProvider` will be returned.
        Otherwise, the default :py:class:`ConnectionProvider` will be returned.

        :param host_info: the host info for the connection that will be established.
        :param props: the connection properties.
        :return: the :py:class:`ConnectionProvider` to use to establish a connection using the given host details and properties.
        """
        if ConnectionProviderManager._conn_provider is None:
            return self.default_provider

        with ConnectionProviderManager._lock:
            if ConnectionProviderManager._conn_provider is not None \
                    and ConnectionProviderManager._conn_provider.accepts_host_info(host_info, props):
                return ConnectionProviderManager._conn_provider

        return self._default_provider

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        """
        Indicates whether the given selection strategy is supported by the connection provider.

        :param role: determines if the connection provider should return a reader host or a writer host.
        :param strategy: the host selection strategy to use.
        :return: whether the host selection strategy is supported.
        """
        accepts_strategy: bool = False
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                accepts_strategy = ConnectionProviderManager._conn_provider.accepts_strategy(role, strategy)

        if not accepts_strategy:
            accepts_strategy = self._default_provider.accepts_strategy(role, strategy)

        return accepts_strategy

    def get_host_info_by_strategy(
            self, hosts: Tuple[HostInfo, ...], role: HostRole, strategy: str, props: Optional[Properties]) -> HostInfo:
        """
        Return a reader or a writer host using the specified strategy.

        This method should raise an :py:class:`AwsWrapperError` if the specified strategy is unsupported.

        :param hosts: the list of hosts to select from.
        :param role: determines if the connection provider should return a reader or a writer host.
        :param strategy: the host selection strategy to use.
        :param props: the connection properties.
        :return: the host selected using the specified strategy.
        """
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                if ConnectionProviderManager._conn_provider is not None \
                        and ConnectionProviderManager._conn_provider.accepts_strategy(role, strategy):
                    return ConnectionProviderManager._conn_provider.get_host_info_by_strategy(
                        hosts, role, strategy, props)

        return self._default_provider.get_host_info_by_strategy(hosts, role, strategy, props)

    @staticmethod
    def reset_provider():
        """
        Clears the non-default :py:class:`ConnectionProvider` if it has been set.
        """
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                ConnectionProviderManager._conn_provider = None

    @staticmethod
    def release_resources():
        """
        Releases any resources held by the available :py:class:`ConnectionProvider` instances.
        :return:
        """
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                if isinstance(ConnectionProviderManager._conn_provider, CanReleaseResources):
                    ConnectionProviderManager._conn_provider.release_resources()
