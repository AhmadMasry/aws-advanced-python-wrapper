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

"""Async cluster topology monitor.

Background :class:`asyncio.Task` that periodically awakens, calls
:meth:`AsyncAuroraHostListProvider.force_refresh`, and sleeps. Replaces
the sync :class:`ClusterTopologyMonitor`'s thread-based loop.

3.0.0 keeps the monitor minimal: one task per provider instance, fixed
interval, no suggestions feedback loop (sync EFM uses that; async EFM
in SP-5 may add its own). Cancellation is clean -- ``stop()`` cancels
the task and awaits its exit.

Phase G.1 adds a high-frequency refresh window after a writer change
is detected: once a new writer is observed, the monitor temporarily
shortens its tick interval to ``high_refresh_rate_sec`` (default 1s)
for ``HIGH_REFRESH_PERIOD_SEC`` (default 30s) before reverting to the
normal cadence. Mirrors the sync implementation at
``cluster_topology_monitor.py:86, :121, :192-210, :273-282``.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, Optional

from aws_advanced_python_wrapper.hostinfo import HostRole

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.host_list_provider import (
        AsyncAuroraHostListProvider, Topology)


class AsyncClusterTopologyMonitor:
    """Drive periodic topology refresh against the current connection."""

    HIGH_REFRESH_PERIOD_SEC: float = 30.0
    IGNORE_REQUEST_SEC: float = 10.0

    def __init__(
            self,
            provider: AsyncAuroraHostListProvider,
            connection_getter: Any,
            refresh_interval_sec: float = 30.0,
            high_refresh_rate_sec: float = 1.0) -> None:
        """
        :param provider: the host list provider whose ``force_refresh`` to
            call each tick.
        :param connection_getter: a zero-arg callable returning the current
            async driver connection (or ``None``). Using a getter lets the
            monitor track connection replacement on failover.
        :param refresh_interval_sec: seconds between refreshes in normal
            (non-panic) mode.
        :param high_refresh_rate_sec: seconds between refreshes while in
            the post-writer-change high-frequency window. Must be small
            enough to catch topology settling quickly (default 1s).
        """
        self._provider = provider
        self._connection_getter = connection_getter
        self._interval_sec = max(0.005, float(refresh_interval_sec))
        self._high_refresh_rate_sec = max(0.005, float(high_refresh_rate_sec))
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._last_known_writer: Optional[str] = None
        self._high_refresh_until_ns: int = 0
        self._ignore_requests_until_ns: int = 0
        self._last_topology: Topology = ()

    @property
    def high_refresh_rate_sec(self) -> float:
        """Seconds between refreshes while in high-freq mode (read-only)."""
        return self._high_refresh_rate_sec

    @property
    def last_topology(self) -> Topology:
        """Most recently refreshed topology (empty tuple before first tick)."""
        return self._last_topology

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        """Spawn the background refresh task. No-op if already running."""
        if self.is_running():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                conn = self._connection_getter()
                if conn is not None:
                    try:
                        topology = await self._provider.force_refresh(conn)
                        self._last_topology = topology
                        self._check_for_writer_change(topology)
                    except Exception:
                        # Monitor failures shouldn't crash the task;
                        # cached topology remains usable.
                        pass
                # Pick tick interval based on whether we're in high-freq mode.
                interval = self._current_tick_interval()
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=interval
                    )
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            return

    def _current_tick_interval(self) -> float:
        """High-freq window active -> short interval; else normal interval."""
        if time.time_ns() < self._high_refresh_until_ns:
            return self._high_refresh_rate_sec
        return self._interval_sec

    def _check_for_writer_change(self, topology: Any) -> None:
        """Detect writer change and enter high-freq mode if so.

        Compares the writer in ``topology`` (a sequence of ``HostInfo``)
        against :attr:`_last_known_writer`. The first-ever writer seen
        does *not* trigger high-freq mode -- only a subsequent *change*
        does. Empty topology or no writer is a no-op.
        """
        if topology is None:
            return
        new_writer: Optional[str] = None
        for h in topology:
            if h.role == HostRole.WRITER:
                new_writer = f"{h.host}:{h.port}"
                break
        if new_writer is None:
            return
        writer_changed = (self._last_known_writer is not None
                          and new_writer != self._last_known_writer)
        is_new_writer = self._last_known_writer is None
        if writer_changed:
            # Writer changed -- enter high-freq mode.
            self._high_refresh_until_ns = (
                time.time_ns()
                + int(self.HIGH_REFRESH_PERIOD_SEC * 1_000_000_000))
        self._last_known_writer = new_writer
        # Writer is confirmed (first-seen or changed) -- start the
        # ignore-request window. Subsequent ticks that re-observe the
        # same writer do NOT re-extend the window, so it naturally
        # expires IGNORE_REQUEST_SEC after the last writer transition.
        if is_new_writer or writer_changed:
            self._ignore_requests_until_ns = (
                time.time_ns()
                + int(self.IGNORE_REQUEST_SEC * 1_000_000_000))

    def should_ignore_refresh_request(self) -> bool:
        """Return True if the monitor recently confirmed the writer and
        external refresh requests should be deferred.

        Mirrors sync cluster_topology_monitor.py:136-141.
        """
        return time.time_ns() < self._ignore_requests_until_ns

    async def force_refresh_with_connection(
            self,
            conn: Any,
            timeout_sec: float = 5.0,
            bypass_ignore_window: bool = False) -> Topology:
        """Probe the topology provider with the caller's ``conn``.

        Short-circuits to the cached ``last_topology`` when the ignore-
        request window is active UNLESS ``bypass_ignore_window`` is True
        (failover recovery wants a fresh probe regardless). Otherwise
        delegates to ``provider.force_refresh(conn)`` under an
        ``asyncio.wait_for(timeout=timeout_sec)`` gate.

        Raises ``TimeoutError`` when the provider doesn't respond within
        ``timeout_sec``.
        """
        if not bypass_ignore_window and self.should_ignore_refresh_request():
            return self._last_topology
        try:
            topology = await asyncio.wait_for(
                self._provider.force_refresh(conn),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError as e:
            raise TimeoutError(
                f"Topology refresh did not complete within {timeout_sec}s"
            ) from e
        self._last_topology = topology
        self._check_for_writer_change(topology)
        return topology

    async def stop(self) -> None:
        """Signal the task to exit and await its termination."""
        self._stop_event.set()
        if self._task is None:
            return
        if not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None
