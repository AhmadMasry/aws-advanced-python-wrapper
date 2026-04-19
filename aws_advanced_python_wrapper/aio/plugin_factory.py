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

"""Async plugin factory registry.

Mirrors sync :class:`PluginFactory` / `PluginManager.PLUGIN_FACTORIES`. Lets
users configure the async plugin list via a connection-property string
(``plugins="failover,efm,iam"``) instead of instantiating plugin classes in
code.
"""

from __future__ import annotations

from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Dict, List,
                    Optional, Protocol, Type)

from aws_advanced_python_wrapper.aio.auth_plugins import (
    AsyncAwsSecretsManagerPlugin, AsyncIamAuthPlugin)
from aws_advanced_python_wrapper.aio.failover_plugin import AsyncFailoverPlugin
from aws_advanced_python_wrapper.aio.host_monitoring_plugin import \
    AsyncHostMonitoringPlugin
from aws_advanced_python_wrapper.aio.minor_plugins import (
    AsyncAuroraConnectionTrackerPlugin, AsyncConnectTimePlugin,
    AsyncCustomEndpointPlugin, AsyncDeveloperPlugin, AsyncExecuteTimePlugin)
from aws_advanced_python_wrapper.aio.read_write_splitting_plugin import \
    AsyncReadWriteSplittingPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncHostListProvider
    from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncPluginFactory(Protocol):
    """Produce an :class:`AsyncPlugin` instance given service + props."""

    def get_instance(
            self,
            plugin_service: AsyncPluginService,
            props: Properties,
            host_list_provider: Optional[AsyncHostListProvider] = None) -> AsyncPlugin:
        ...


# ---- Concrete factories ------------------------------------------------


class _FailoverFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        if host_list_provider is None:
            raise AwsWrapperError(
                "failover plugin requires a host_list_provider"
            )
        return AsyncFailoverPlugin(plugin_service, host_list_provider, props)


class _HostMonitoringFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncHostMonitoringPlugin(plugin_service, props)


class _ReadWriteSplittingFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        if host_list_provider is None:
            raise AwsWrapperError(
                "read_write_splitting plugin requires a host_list_provider"
            )
        return AsyncReadWriteSplittingPlugin(
            plugin_service, host_list_provider, props
        )


class _IamAuthFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncIamAuthPlugin(plugin_service, props)


class _AwsSecretsManagerFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncAwsSecretsManagerPlugin(plugin_service, props)


class _AuroraConnectionTrackerFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncAuroraConnectionTrackerPlugin(plugin_service)


class _ConnectTimeFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncConnectTimePlugin()


class _ExecuteTimeFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncExecuteTimePlugin()


class _DeveloperFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncDeveloperPlugin()


class _CustomEndpointFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncCustomEndpointPlugin()


# ---- Registry ----------------------------------------------------------


# Plugin code -> factory class (instantiation at registration time).
# Matches sync PluginManager.PLUGIN_FACTORIES naming exactly where
# applicable so users can reuse the same string across sync/async code.
PLUGIN_FACTORIES: Dict[str, AsyncPluginFactory] = {
    "failover": _FailoverFactory(),
    "failover_v2": _FailoverFactory(),  # alias -- sync has two failover
                                        # plugins; async ships one with
                                        # failover_v2's semantics.
    "efm": _HostMonitoringFactory(),
    "host_monitoring": _HostMonitoringFactory(),
    "host_monitoring_v2": _HostMonitoringFactory(),
    "read_write_splitting": _ReadWriteSplittingFactory(),
    "iam": _IamAuthFactory(),
    "aws_secrets_manager": _AwsSecretsManagerFactory(),
    "aurora_connection_tracker": _AuroraConnectionTrackerFactory(),
    "connect_time": _ConnectTimeFactory(),
    "execute_time": _ExecuteTimeFactory(),
    "dev": _DeveloperFactory(),
    "custom_endpoint": _CustomEndpointFactory(),
}


# Relative weights for auto-sort (lower = earlier in pipeline).
# Matches the sync PluginManager.PLUGIN_FACTORY_WEIGHTS semantics.
PLUGIN_FACTORY_WEIGHTS: Dict[Type[Any], int] = {
    _CustomEndpointFactory: 40,
    _AuroraConnectionTrackerFactory: 100,
    _ReadWriteSplittingFactory: 300,
    _FailoverFactory: 400,
    _HostMonitoringFactory: 500,
    _IamAuthFactory: 700,
    _AwsSecretsManagerFactory: 800,
    _ConnectTimeFactory: 900,
    _ExecuteTimeFactory: 910,
    _DeveloperFactory: 1000,
}


def resolve_plugin_factories(plugin_codes: List[str]) -> List[AsyncPluginFactory]:
    """Translate a list of plugin-code strings into factory instances.

    Raises :class:`AwsWrapperError` on unknown plugin codes.
    """
    factories: List[AsyncPluginFactory] = []
    for raw in plugin_codes:
        code = raw.strip()
        if not code:
            continue
        factory = PLUGIN_FACTORIES.get(code)
        if factory is None:
            raise AwsWrapperError(
                Messages.get_formatted("PluginManager.InvalidPlugin", code)
            )
        factories.append(factory)
    return factories


def parse_plugins_property(props: Properties) -> Optional[List[str]]:
    """Return the list of plugin codes from ``props['plugins']`` or None.

    Returns ``None`` when the ``plugins`` key is NOT literally present in
    the props dict (``WrapperProperties.PLUGINS`` has a non-None default,
    which is why we check membership directly rather than calling
    ``.get()``). Blank-string value returns an empty list. Whitespace-only
    entries are dropped.
    """
    key = WrapperProperties.PLUGINS.name
    if key not in props:
        return None
    raw = props[key]
    if raw is None:
        return None
    return [c.strip() for c in str(raw).split(",") if c.strip()]


def sort_factories_by_weight(
        factories: List[AsyncPluginFactory]) -> List[AsyncPluginFactory]:
    """Stable-sort factories by their registered weight.

    Unknown factories get a weight just above the last known weight
    (preserving their relative input order among themselves).
    """
    seen_weights: Dict[Type[Any], int] = {}
    max_known = max(PLUGIN_FACTORY_WEIGHTS.values(), default=0)
    unknown_next = max_known + 1

    def weight_of(f: AsyncPluginFactory) -> int:
        nonlocal unknown_next
        cls = type(f)
        w = PLUGIN_FACTORY_WEIGHTS.get(cls)
        if w is None:
            # Each unknown factory class gets its own distinct weight so
            # sort is stable across different factory types.
            if cls not in seen_weights:
                seen_weights[cls] = unknown_next
                unknown_next += 1
            return seen_weights[cls]
        return w

    return sorted(factories, key=weight_of)


def build_async_plugins(
        plugin_service: AsyncPluginService,
        props: Properties,
        host_list_provider: Optional[AsyncHostListProvider] = None,
        auto_sort: Optional[bool] = None) -> List[AsyncPlugin]:
    """Full pipeline: props -> codes -> factories -> instances.

    :param auto_sort: if ``None`` (default), read
        ``WrapperProperties.AUTO_SORT_PLUGIN_ORDER`` from props. Sync
        wrapper's default is ``True``; we match it.
    """
    codes = parse_plugins_property(props)
    if not codes:
        return []

    factories = resolve_plugin_factories(codes)

    if auto_sort is None:
        auto_sort = WrapperProperties.AUTO_SORT_PLUGIN_ORDER.get_bool(props)
    if auto_sort:
        factories = sort_factories_by_weight(factories)

    return [
        f.get_instance(plugin_service, props, host_list_provider)
        for f in factories
    ]


__all__ = [
    "AsyncPluginFactory",
    "PLUGIN_FACTORIES",
    "PLUGIN_FACTORY_WEIGHTS",
    "resolve_plugin_factories",
    "parse_plugins_property",
    "sort_factories_by_weight",
    "build_async_plugins",
]


# Keep unused-callable for typing consistency across async plugin shape.
_ = Callable[..., Awaitable[Any]]
