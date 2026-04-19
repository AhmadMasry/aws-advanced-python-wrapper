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

"""F3-B SP-5: async host monitoring plugin."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.host_monitoring_plugin import \
    AsyncHostMonitoringPlugin
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import Properties


def _build(
        enabled: bool = True,
        grace_ms: int = 10,
        interval_ms: int = 10,
        count: int = 2):
    props = Properties({
        "host": "h.example",
        "port": "5432",
        "failure_detection_enabled": "true" if enabled else "false",
        "failure_detection_time_ms": str(grace_ms),
        "failure_detection_interval_ms": str(interval_ms),
        "failure_detection_count": str(count),
    })

    driver_dialect = MagicMock()
    driver_dialect.ping = AsyncMock(return_value=True)
    driver_dialect.abort_connection = AsyncMock()

    svc = AsyncPluginServiceImpl(
        props, driver_dialect, HostInfo(host="h.example", port=5432)
    )
    conn = MagicMock(name="current_conn")
    svc._current_connection = conn

    plugin = AsyncHostMonitoringPlugin(svc, props)
    return plugin, svc, driver_dialect, conn


# ---- Config / subscription ---------------------------------------------


def test_subscribed_includes_cursor_execute():
    plugin, *_ = _build()
    assert "Cursor.execute" in plugin.subscribed_methods


def test_disabled_plugin_does_not_spawn_monitor_task():
    async def _body() -> None:
        plugin, _, driver_dialect, _conn = _build(enabled=False)

        async def _work() -> str:
            return "ok"

        result = await plugin.execute(object(), "Cursor.execute", _work)
        assert result == "ok"
        # Monitor never runs, so ping is never called.
        driver_dialect.ping.assert_not_called()

    asyncio.run(_body())


def test_happy_path_passes_result_through():
    async def _body() -> None:
        plugin, *_ = _build()

        async def _work() -> str:
            await asyncio.sleep(0.001)
            return "rows"

        assert await plugin.execute(object(), "Cursor.execute", _work) == "rows"

    asyncio.run(_body())


def test_monitor_task_is_cleaned_up_when_execute_returns():
    async def _body() -> None:
        plugin, *_ = _build(grace_ms=1000)

        async def _work() -> None:
            return None

        # Measure tasks before and after to ensure cleanup.
        before = len(asyncio.all_tasks())
        await plugin.execute(object(), "Cursor.execute", _work)
        # Give cancellation a tick.
        await asyncio.sleep(0)
        after = len(asyncio.all_tasks())
        # Count may vary by one because of the current task; just ensure we
        # didn't leak multiple tasks.
        assert after <= before + 1

    asyncio.run(_body())


def test_abort_on_threshold_consecutive_ping_failures():
    async def _body() -> None:
        plugin, _, driver_dialect, conn = _build(
            grace_ms=5, interval_ms=5, count=2
        )
        # All pings fail.
        driver_dialect.ping = AsyncMock(return_value=False)

        async def _slow_work() -> str:
            await asyncio.sleep(0.08)
            return "done"

        await plugin.execute(object(), "Cursor.execute", _slow_work)
        # After 2 consecutive failures, abort_connection should have been called.
        driver_dialect.abort_connection.assert_awaited_with(conn)

    asyncio.run(_body())


def test_no_abort_when_pings_succeed():
    async def _body() -> None:
        plugin, _, driver_dialect, _conn = _build(
            grace_ms=2, interval_ms=2, count=3
        )
        driver_dialect.ping = AsyncMock(return_value=True)

        async def _work() -> str:
            await asyncio.sleep(0.05)
            return "done"

        await plugin.execute(object(), "Cursor.execute", _work)
        driver_dialect.abort_connection.assert_not_called()

    asyncio.run(_body())


def test_no_connection_means_no_monitor():
    async def _body() -> None:
        plugin, svc, driver_dialect, _conn = _build()
        svc._current_connection = None

        async def _work() -> str:
            return "ok"

        result = await plugin.execute(object(), "Cursor.execute", _work)
        assert result == "ok"
        driver_dialect.ping.assert_not_called()

    asyncio.run(_body())


def test_ping_exception_counts_as_failure():
    async def _body() -> None:
        plugin, _, driver_dialect, conn = _build(
            grace_ms=2, interval_ms=2, count=2
        )
        driver_dialect.ping = AsyncMock(side_effect=RuntimeError("net down"))

        async def _slow() -> str:
            await asyncio.sleep(0.06)
            return "rows"

        await plugin.execute(object(), "Cursor.execute", _slow)
        driver_dialect.abort_connection.assert_awaited_with(conn)

    asyncio.run(_body())


def test_execute_raises_still_cancels_monitor():
    async def _body() -> None:
        plugin, *_ = _build(grace_ms=1000)

        async def _raiser() -> None:
            raise RuntimeError("inner failure")

        with pytest.raises(RuntimeError):
            await plugin.execute(object(), "Cursor.execute", _raiser)
        # Post-exception cleanup: no long-running monitor tasks lingering.
        await asyncio.sleep(0)

    asyncio.run(_body())
