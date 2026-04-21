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

"""Async Fastest-Response-Strategy plugin (minimal port).

Measures response time on-demand via :func:`asyncio.gather` rather than
sync's background monitor threads. Trades accuracy for a simpler async
lifecycle (no thread/task leaks, no cleanup hook required).

Public surface:

* ``accepts_strategy(role, strategy)`` -- True only for ``"fastest_response"``.
* ``get_host_info_by_strategy(role, strategy, host_list)`` -- returns the
  cached winner if any; otherwise falls back to
  :class:`RandomHostSelector` (the strategy method is sync-callable and
  can't await a parallel probe).
* ``measure_and_cache(role)`` -- async helper that probes every host of
  ``role`` in parallel and caches the fastest.
* ``notify_host_list_changed(changes)`` -- drops the cache.

Intentional gaps vs the sync implementation:

* No standing monitor -- measurements are on-demand.
* No per-host telemetry gauge (Phase I scope).
* No storage-service integration -- the cache lives on the plugin
  instance.

A future port can add standing :class:`asyncio.Task` monitors if
consumers need the extra accuracy.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Set, Tuple

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_selector import RandomHostSelector
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
    from aws_advanced_python_wrapper.utils.notifications import HostEvent
    from aws_advanced_python_wrapper.utils.properties import Properties


logger = Logger(__name__)

_STRATEGY_NAME = "fastest_response"
# Sentinel used when a probe fails / times out. Keeps the comparison
# math integer-only (matches sync's use of MAX_VALUE).
_MAX_RESPONSE_NS = 10 ** 18
_DEFAULT_PROBE_TIMEOUT_SEC = 2.0
_DEFAULT_CACHE_TTL_SEC = 30.0


class AsyncFastestResponseStrategyPlugin(AsyncPlugin):
    """Async counterpart of :class:`FastestResponseStrategyPlugin` (minimal)."""

    _SUBSCRIBED: Set[str] = {
        "connect",
        "accepts_strategy",
        "get_host_info_by_strategy",
        "notify_host_list_changed",
    }

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props
        self._probe_timeout_sec = _DEFAULT_PROBE_TIMEOUT_SEC

        # Sync uses RESPONSE_MEASUREMENT_INTERVAL_MS (default 30s) for both
        # the measure cadence AND the cache TTL. We don't run a standing
        # monitor, so we reuse the same knob purely as cache TTL.
        interval_ms = WrapperProperties.RESPONSE_MEASUREMENT_INTERVAL_MS.get_int(
            props)
        if interval_ms and interval_ms > 0:
            self._cache_ttl_sec = interval_ms / 1000.0
        else:
            self._cache_ttl_sec = _DEFAULT_CACHE_TTL_SEC

        self._random_selector = RandomHostSelector()

        # Cache keyed by HostRole.name -> (winner, expires_at_loop_time_sec).
        self._cached_fastest: Dict[str, Tuple[HostInfo, float]] = {}

        # Captured on the first ``connect`` interception so ``measure_and_cache``
        # can open probe connections without importing the driver module.
        # Preserves invariant 8a (no direct driver imports from plugin code).
        self._target_driver_func: Optional[Callable] = None

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    # ---- connect interception (capture target_driver_func) -------------

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Any]) -> Any:
        # Remember the target driver entry point so measure_and_cache can
        # open probe connections through driver_dialect.connect(...) without
        # importing psycopg / aiomysql directly.
        self._target_driver_func = target_driver_func
        return await connect_func()

    # ---- strategy API --------------------------------------------------

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return strategy == _STRATEGY_NAME

    def get_host_info_by_strategy(
            self,
            role: HostRole,
            strategy: str,
            host_list: Optional[list] = None) -> Optional[HostInfo]:
        # Sync behaviour: raise for any strategy we don't own. The plugin
        # manager only routes fastest_response here, but matching the
        # sync contract keeps direct callers honest.
        if strategy != _STRATEGY_NAME:
            logger.error(
                "FastestResponseStrategyPlugin.UnsupportedHostSelectorStrategy",
                strategy)
            raise AwsWrapperError(
                Messages.get_formatted(
                    "FastestResponseStrategyPlugin.UnsupportedHostSelectorStrategy",
                    strategy))

        candidates_src = (
            host_list if host_list else list(self._plugin_service.all_hosts))
        candidates = [h for h in candidates_src if h.role == role]
        if not candidates:
            return None

        cached = self._cached_fastest.get(role.name)
        if cached is not None:
            winner, expires_at = cached
            if self._loop_time() < expires_at:
                # Winner still cached; confirm it's still in topology
                # (sync does the same: cached host must survive topology
                # changes to remain valid).
                for h in candidates:
                    if (h.host == winner.host
                            and h.port == winner.port):
                        return h
                # Stale winner -- drop and fall through.
                self._cached_fastest.pop(role.name, None)

        # No cache; pick Random as fallback. Proper measurement is async
        # and can't be awaited from this sync method. Callers (or a future
        # standing-monitor enhancement) can invoke measure_and_cache()
        # explicitly to warm the cache.
        logger.debug("FastestResponseStrategyPlugin.RandomHostSelected")
        return self._random_selector.get_host(
            tuple(candidates), role, self._props)

    # ---- async probe ---------------------------------------------------

    async def measure_and_cache(
            self, role: HostRole) -> Optional[HostInfo]:
        """Probe every host of ``role`` in parallel; cache the winner.

        Returns the fastest host or ``None`` if every probe failed /
        timed out.
        """
        candidates = [
            h for h in self._plugin_service.all_hosts if h.role == role]
        if not candidates:
            return None

        driver_dialect = self._plugin_service.driver_dialect
        results = await asyncio.gather(
            *(self._measure_one(h, driver_dialect) for h in candidates),
            return_exceptions=True,
        )

        best: Optional[HostInfo] = None
        best_ns: int = _MAX_RESPONSE_NS
        for host, r in zip(candidates, results):
            if isinstance(r, BaseException):
                continue
            if not isinstance(r, int) or r >= _MAX_RESPONSE_NS:
                continue
            if r < best_ns:
                best = host
                best_ns = r

        if best is not None:
            self._cached_fastest[role.name] = (
                best, self._loop_time() + self._cache_ttl_sec)
        return best

    async def _measure_one(
            self,
            host_info: HostInfo,
            driver_dialect: AsyncDriverDialect) -> int:
        """Open a probe connection, ping, return elapsed ns.

        Returns :data:`_MAX_RESPONSE_NS` on any failure (connection error,
        timeout, ping returning False).
        """
        if self._target_driver_func is None:
            # Haven't seen a connect yet; we don't know how to open a
            # fresh driver connection. Caller should warm the cache on
            # its next run.
            return _MAX_RESPONSE_NS

        conn: Any = None
        start_ns = time.perf_counter_ns()
        try:
            conn = await asyncio.wait_for(
                driver_dialect.connect(
                    host_info, self._props, self._target_driver_func),
                timeout=self._probe_timeout_sec,
            )
            ok = await asyncio.wait_for(
                driver_dialect.ping(conn),
                timeout=self._probe_timeout_sec,
            )
            if not ok:
                return _MAX_RESPONSE_NS
            return time.perf_counter_ns() - start_ns
        except Exception as e:  # noqa: BLE001
            logger.debug(
                "HostResponseTimeMonitor.ExceptionDuringMonitoringStop",
                host_info.host, e)
            return _MAX_RESPONSE_NS
        finally:
            if conn is not None:
                try:
                    await driver_dialect.abort_connection(conn)
                except Exception:  # noqa: BLE001
                    pass

    # ---- notifications -------------------------------------------------

    def notify_host_list_changed(
            self, changes: Dict[str, Set[HostEvent]]) -> None:
        # Topology changes may have added or removed hosts -- drop the
        # cache to force a fresh measurement on the next strategy call.
        self._cached_fastest.clear()

    # ---- helpers -------------------------------------------------------

    @staticmethod
    def _loop_time() -> float:
        """Return the running loop's clock, or monotonic() as a fallback.

        ``get_host_info_by_strategy`` may be called outside a running
        loop (e.g., from a sync-style test path); monotonic() gives a
        consistent monotonic clock for the cache comparison in that case.
        """
        try:
            return asyncio.get_running_loop().time()
        except RuntimeError:
            return time.monotonic()


__all__ = ["AsyncFastestResponseStrategyPlugin"]
