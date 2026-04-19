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
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncAuroraHostListProvider


class AsyncClusterTopologyMonitor:
    """Drive periodic topology refresh against the current connection."""

    def __init__(
            self,
            provider: AsyncAuroraHostListProvider,
            connection_getter: Any,
            refresh_interval_sec: float = 30.0) -> None:
        """
        :param provider: the host list provider whose ``force_refresh`` to
            call each tick.
        :param connection_getter: a zero-arg callable returning the current
            async driver connection (or ``None``). Using a getter lets the
            monitor track connection replacement on failover.
        :param refresh_interval_sec: seconds between refreshes.
        """
        self._provider = provider
        self._connection_getter = connection_getter
        self._interval_sec = max(0.5, float(refresh_interval_sec))
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        """Spawn the background refresh task. No-op if already running."""
        if self.is_running():
            return
        self._stop_event.clear()
        self._task = asyncio.get_event_loop().create_task(self._run())

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                conn = self._connection_getter()
                if conn is not None:
                    try:
                        await self._provider.force_refresh(conn)
                    except Exception:
                        # Monitor failures shouldn't crash the task;
                        # cached topology remains usable.
                        pass
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=self._interval_sec
                    )
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            return

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
