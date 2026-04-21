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

"""F3-B Phase D: Aurora connection tracker + invalidate-on-failover."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.aurora_connection_tracker import (
    AsyncAuroraConnectionTrackerPlugin, AsyncOpenedConnectionTracker)
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.errors import FailoverSuccessError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties


def _build():
    props = Properties({"host": "cluster.example.com", "port": "5432"})
    driver_dialect = MagicMock()
    svc = AsyncPluginServiceImpl(props, driver_dialect)
    tracker = AsyncOpenedConnectionTracker()
    plugin = AsyncAuroraConnectionTrackerPlugin(svc, tracker=tracker)
    return plugin, svc, driver_dialect, tracker


def test_tracker_records_connection_on_connect():
    plugin, svc, driver_dialect, tracker = _build()
    host = HostInfo(host="instance-1", port=5432, role=HostRole.WRITER)
    conn = MagicMock(name="new_conn")

    async def _connect_func():
        return conn

    async def _run():
        await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func)

    asyncio.run(_run())
    tracked = tracker._tracked_for(host.as_aliases())
    assert conn in tracked


def test_tracker_returns_empty_set_for_unknown_host():
    tracker = AsyncOpenedConnectionTracker()
    h = HostInfo(host="ghost", port=5432)
    assert len(tracker._tracked_for(h.as_aliases())) == 0


def test_invalidate_all_closes_tracked_connections():
    tracker = AsyncOpenedConnectionTracker()
    host = HostInfo(host="writer-1", port=5432, role=HostRole.WRITER)
    c1 = MagicMock(name="c1")
    c1.close = MagicMock()
    c2 = MagicMock(name="c2")
    c2.close = MagicMock()
    tracker.track(host, c1)
    tracker.track(host, c2)

    asyncio.run(tracker.invalidate_all(host))

    c1.close.assert_called()
    c2.close.assert_called()


def test_invalidate_all_survives_close_errors():
    tracker = AsyncOpenedConnectionTracker()
    host = HostInfo(host="writer-1", port=5432, role=HostRole.WRITER)
    bad = MagicMock(name="bad")
    bad.close = MagicMock(side_effect=RuntimeError("broken"))
    good = MagicMock(name="good")
    good.close = MagicMock()
    tracker.track(host, bad)
    tracker.track(host, good)

    asyncio.run(tracker.invalidate_all(host))
    good.close.assert_called()


def test_plugin_invalidates_old_writer_on_writer_change():
    """Execute with a changed writer in all_hosts triggers invalidation of old writer."""
    plugin, svc, driver_dialect, tracker = _build()
    old_writer = HostInfo(host="old-w", port=5432, role=HostRole.WRITER)
    new_writer = HostInfo(host="new-w", port=5432, role=HostRole.WRITER)
    conn_to_old = MagicMock(name="conn_to_old")
    conn_to_old.close = MagicMock()

    svc._all_hosts = (old_writer,)
    tracker.track(old_writer, conn_to_old)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        svc._all_hosts = (new_writer,)
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        await asyncio.sleep(0.01)

    asyncio.run(_run())
    conn_to_old.close.assert_called()


def test_plugin_no_invalidation_when_writer_unchanged():
    plugin, svc, driver_dialect, tracker = _build()
    writer = HostInfo(host="w", port=5432, role=HostRole.WRITER)
    conn = MagicMock(name="conn")
    conn.close = MagicMock()

    svc._all_hosts = (writer,)
    tracker.track(writer, conn)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        await asyncio.sleep(0.01)

    asyncio.run(_run())
    conn.close.assert_not_called()


def test_plugin_invalidates_on_failover_error():
    """A FailoverError raised from the inner execute triggers writer-change recheck."""
    plugin, svc, driver_dialect, tracker = _build()
    old_writer = HostInfo(host="old-w", port=5432, role=HostRole.WRITER)
    new_writer = HostInfo(host="new-w", port=5432, role=HostRole.WRITER)
    conn_to_old = MagicMock(name="conn_to_old")
    conn_to_old.close = MagicMock()

    svc._all_hosts = (old_writer,)
    tracker.track(old_writer, conn_to_old)

    async def _refresh(*args, **kwargs):
        svc._all_hosts = (new_writer,)

    svc.refresh_host_list = AsyncMock(side_effect=_refresh)

    async def _run():
        async def _noop():
            return None

        # First execute pins old writer
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)

        # Second execute raises FailoverSuccessError
        async def _raising():
            raise FailoverSuccessError("failover")

        with pytest.raises(FailoverSuccessError):
            await plugin.execute(MagicMock(), "Cursor.execute", _raising)
        await asyncio.sleep(0.01)

    asyncio.run(_run())
    conn_to_old.close.assert_called()


def test_plugin_subscribed_methods_includes_connect_and_execute():
    plugin, *_ = _build()
    subs = plugin.subscribed_methods
    # DbApiMethod.CONNECT.method_name is literally "connect" (pipeline
    # marker), not "Connect.connect".
    assert "connect" in subs
    assert "Cursor.execute" in subs


def test_tracker_remove_drops_connection_from_set():
    tracker = AsyncOpenedConnectionTracker()
    host = HostInfo(host="w", port=5432, role=HostRole.WRITER)
    conn = MagicMock(name="conn")
    tracker.track(host, conn)
    assert conn in tracker._tracked_for(host.as_aliases())
    tracker.remove(host, conn)
    assert conn not in tracker._tracked_for(host.as_aliases())
