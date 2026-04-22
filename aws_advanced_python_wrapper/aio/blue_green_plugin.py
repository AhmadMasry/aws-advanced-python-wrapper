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

"""Async Blue-Green deployment plugin (skeleton).

Scaffolding for the async BlueGreen plugin: enums, status dataclasses,
routing ABCs, and a plugin shell that consults plugin_service.get_status
for the current BG status and dispatches to connect/execute routings.

NOT YET PORTED:
- BlueGreenStatusMonitor / BlueGreenStatusProvider (DB-polling state
  machine). Status source stays empty until a consumer constructs a
  monitor + publishes via plugin_service.set_status.
- SubstituteConnectRouting, SuspendConnectRouting,
  SuspendUntilCorrespondingHostFoundConnectRouting (the actual
  connection-reshaping behaviors during BG phase transitions).
- PassThroughExecuteRouting and SuspendExecuteRouting.

With only the skeleton in place, the plugin effectively passes through
to the default connection path whenever status is absent (the norm for
non-BG deployments) and is therefore safe to register in the factory.
Users enabling 'bg' now get the routing dispatch wired; behavior during
actual BG phase transitions requires porting the status monitor -- a
dedicated follow-up task.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Dict, List,
                    Optional, Set, Tuple)

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


logger = Logger(__name__)


class BlueGreenIntervalRate(Enum):
    BASELINE = "BASELINE"
    INCREASED = "INCREASED"
    HIGH = "HIGH"


class BlueGreenPhase(Enum):
    NOT_CREATED = "NOT_CREATED"
    CREATED = "CREATED"
    PREPARATION = "PREPARATION"
    IN_PROGRESS = "IN_PROGRESS"
    POST = "POST"
    COMPLETED = "COMPLETED"


class BlueGreenRole(Enum):
    SOURCE = "SOURCE"
    TARGET = "TARGET"


@dataclass
class BlueGreenStatus:
    bg_id: str
    phase: BlueGreenPhase
    connect_routings: List[ConnectRouting] = field(default_factory=list)
    execute_routings: List[ExecuteRouting] = field(default_factory=list)
    role_by_host: Dict[str, BlueGreenRole] = field(default_factory=dict)

    def get_role(self, host_info: HostInfo) -> Optional[BlueGreenRole]:
        return self.role_by_host.get(host_info.host)


@dataclass
class BlueGreenInterimStatus:
    """Snapshot used by monitors to publish status updates."""
    bg_id: str
    phase: BlueGreenPhase
    source_hosts: Tuple[str, ...] = ()
    target_hosts: Tuple[str, ...] = ()


class ConnectRouting(ABC):
    @abstractmethod
    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        ...

    @abstractmethod
    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        ...


class ExecuteRouting(ABC):
    @abstractmethod
    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        ...

    @abstractmethod
    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            execute_func: Callable[..., Awaitable[Any]],
            method_name: str) -> Any:
        ...


@dataclass
class _BaseRouting:
    host_matcher: Optional[str] = None
    role_matcher: Optional[BlueGreenRole] = None

    def _matches(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        if self.host_matcher is not None and self.host_matcher != host_info.host:
            return False
        if self.role_matcher is not None and self.role_matcher != role:
            return False
        return True


class PassThroughConnectRouting(_BaseRouting, ConnectRouting):
    """Routing that just forwards to the underlying connect function."""

    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        return self._matches(host_info, role)

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        return await connect_func()


class RejectConnectRouting(_BaseRouting, ConnectRouting):
    """Routing that refuses connections (used during a BG cutover's
    no-new-connections window)."""

    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        return self._matches(host_info, role)

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        raise AwsWrapperError(
            f"Blue-Green deployment currently rejects new connections to "
            f"{host_info.host} (phase: see current status).")


# ---- Stubs for the remaining routings (documented port deferrals) -----


class SubstituteConnectRouting(_BaseRouting, ConnectRouting):
    """TODO: port sync blue_green_plugin.py:355-429 (substitute source
    host with target host based on role-by-host mapping)."""

    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        return False  # skeleton: never matches until port complete

    async def apply(self, *args: Any, **kwargs: Any) -> Optional[Any]:
        raise NotImplementedError(
            "SubstituteConnectRouting: async port deferred "
            "(sync blue_green_plugin.py:355-429). Enable the stub by "
            "subclassing and overriding apply().")


class SuspendConnectRouting(_BaseRouting, ConnectRouting):
    """TODO: port sync blue_green_plugin.py:430-483 (suspend connect
    attempts until BG phase permits them)."""

    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        return False

    async def apply(self, *args: Any, **kwargs: Any) -> Optional[Any]:
        raise NotImplementedError("SuspendConnectRouting: async port deferred.")


class SuspendUntilCorrespondingHostFoundConnectRouting(
        _BaseRouting, ConnectRouting):
    """TODO: port sync blue_green_plugin.py:484-553 (wait for a
    corresponding host to appear in the opposite BG role before
    proceeding)."""

    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        return False

    async def apply(self, *args: Any, **kwargs: Any) -> Optional[Any]:
        raise NotImplementedError(
            "SuspendUntilCorrespondingHostFoundConnectRouting: async port deferred.")


class PassThroughExecuteRouting(_BaseRouting, ExecuteRouting):
    """TODO: port sync blue_green_plugin.py:554-570."""

    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        return self._matches(host_info, role)

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            execute_func: Callable[..., Awaitable[Any]],
            method_name: str) -> Any:
        return await execute_func()


class SuspendExecuteRouting(_BaseRouting, ExecuteRouting):
    """TODO: port sync blue_green_plugin.py:571-629."""

    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        return False

    async def apply(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("SuspendExecuteRouting: async port deferred.")


# ---- Plugin ----------------------------------------------------------


class AsyncBlueGreenPlugin(AsyncPlugin):
    """Async skeleton of sync :class:`BlueGreenPlugin`.

    Dispatches connect/execute through the routing tables published on
    a :class:`BlueGreenStatus` (via ``plugin_service.set_status``). When
    no status is published, passes through -- safe default for non-BG
    deployments.

    Full BG functionality (DB-level status monitor that publishes
    BlueGreenStatus as phases transition) requires porting sync's
    BlueGreenStatusMonitor + BlueGreenStatusProvider (~1000 LOC). That
    is a tracked follow-up.
    """

    _SUBSCRIBED: Set[str] = {DbApiMethod.CONNECT.method_name}

    def __init__(self, plugin_service: AsyncPluginService,
                 props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props
        self._bg_id = str(
            WrapperProperties.BG_ID.get(props) or "default").strip().lower()
        # Subscribe to network-bound methods too so execute routings can
        # intercept. Matches sync blue_green_plugin.py:648.
        self._subscribed = set(self._SUBSCRIBED)
        self._subscribed.update(self._plugin_service.network_bound_methods)

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._subscribed)

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        status = self._get_status()
        if status is None:
            return await connect_func()

        role = status.get_role(host_info)
        if role is None:
            # Host not part of this BG deployment.
            return await connect_func()

        routing = next(
            (r for r in status.connect_routings if r.is_match(host_info, role)),
            None,
        )
        if routing is None:
            return await connect_func()

        result = await routing.apply(
            self, host_info, props, is_initial_connection, connect_func)
        if result is not None:
            return result
        return await connect_func()

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        status = self._get_status()
        if status is None:
            return await execute_func()

        host_info = self._plugin_service.current_host_info
        if host_info is None:
            return await execute_func()

        role = status.get_role(host_info)
        if role is None:
            return await execute_func()

        routing = next(
            (r for r in status.execute_routings if r.is_match(host_info, role)),
            None,
        )
        if routing is None:
            return await execute_func()
        return await routing.apply(self, execute_func, method_name)

    def _get_status(self) -> Optional[BlueGreenStatus]:
        """Look up the published BG status. AsyncPluginService's
        get_status is not yet implemented (Phase A/F didn't port it);
        return None until a consumer wires get/set_status."""
        getter = getattr(self._plugin_service, "get_status", None)
        if getter is None:
            return None
        try:
            result = getter(BlueGreenStatus, self._bg_id)
            if isinstance(result, BlueGreenStatus):
                return result
        except Exception:  # noqa: BLE001 - skeleton tolerates missing infra
            pass
        return None


__all__ = [
    "AsyncBlueGreenPlugin",
    "BlueGreenIntervalRate",
    "BlueGreenPhase",
    "BlueGreenRole",
    "BlueGreenStatus",
    "BlueGreenInterimStatus",
    "ConnectRouting",
    "ExecuteRouting",
    "PassThroughConnectRouting",
    "RejectConnectRouting",
    "SubstituteConnectRouting",
    "SuspendConnectRouting",
    "SuspendUntilCorrespondingHostFoundConnectRouting",
    "PassThroughExecuteRouting",
    "SuspendExecuteRouting",
]
