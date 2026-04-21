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

"""F3-B SP-6: async read/write splitting plugin."""

from __future__ import annotations

import asyncio
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.aio.read_write_splitting_plugin import \
    AsyncReadWriteSplittingPlugin
from aws_advanced_python_wrapper.errors import ReadWriteSplittingError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties


def _build(topology: Optional[tuple] = None):
    props = Properties({"host": "cluster.example", "port": "5432"})

    driver_dialect = MagicMock()
    driver_dialect.connect = AsyncMock(
        side_effect=lambda host_info, props, fn: MagicMock(
            name=f"conn-to-{host_info.host}"
        )
    )
    driver_dialect.is_closed = AsyncMock(return_value=False)
    driver_dialect.is_in_transaction = AsyncMock(return_value=False)
    driver_dialect.transfer_session_state = AsyncMock()

    svc = AsyncPluginServiceImpl(
        props, driver_dialect, HostInfo(host="writer.example", port=5432, role=HostRole.WRITER)
    )
    # Simulate initial writer conn.
    writer_conn = MagicMock(name="writer_conn")
    svc._current_connection = writer_conn

    # Default stub for get_host_info_by_strategy: preserve the old
    # "first matching host in candidates" semantics so existing tests
    # that don't care about the selector strategy keep working.
    svc.get_host_info_by_strategy = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda role, strategy, candidates: (
            next((h for h in (candidates or ()) if h.role == role), None)))

    hlp = MagicMock()
    hlp.refresh = AsyncMock(
        return_value=topology or (
            HostInfo(host="writer.example", port=5432, role=HostRole.WRITER),
            HostInfo(host="reader.example", port=5432, role=HostRole.READER),
        )
    )

    plugin = AsyncReadWriteSplittingPlugin(svc, hlp, props)
    plugin._writer_conn = writer_conn
    return plugin, svc, hlp, driver_dialect, writer_conn


def test_non_set_read_only_call_is_pass_through():
    async def _body() -> None:
        plugin, _, hlp, dd, _ = _build()

        async def _work() -> str:
            return "ok"

        result = await plugin.execute(
            target=object(),
            method_name=DbApiMethod.CURSOR_EXECUTE.method_name,
            execute_func=_work,
        )
        assert result == "ok"
        hlp.refresh.assert_not_called()

    asyncio.run(_body())


def test_set_read_only_true_switches_to_reader():
    async def _body() -> None:
        plugin, svc, hlp, dd, writer_conn = _build()

        async def _work() -> None:
            return None

        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )
        # Reader conn cached + bound.
        assert plugin._reader_conn is not None
        assert svc.current_connection is plugin._reader_conn
        # Writer conn still remembered.
        assert plugin._writer_conn is writer_conn
        # A new driver connect was made to the reader.
        dd.connect.assert_awaited()

    asyncio.run(_body())


def test_set_read_only_false_switches_back_to_writer():
    async def _body() -> None:
        plugin, svc, hlp, dd, writer_conn = _build()

        async def _work() -> None:
            return None

        # First: flip to read-only (reader).
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )
        reader_conn = svc.current_connection
        assert reader_conn is plugin._reader_conn

        # Reset connect mock call count so we can observe the second flip.
        dd.connect.reset_mock()

        # Then: flip back to writer.
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            False,
        )
        # Cached writer reused (no new driver connect).
        dd.connect.assert_not_called()
        assert svc.current_connection is writer_conn

    asyncio.run(_body())


def test_set_read_only_true_reuses_cached_reader():
    async def _body() -> None:
        plugin, svc, hlp, dd, writer_conn = _build()

        async def _work() -> None:
            return None

        # Flip to reader (first time: opens a new reader conn).
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )
        first_reader = plugin._reader_conn
        dd.connect.reset_mock()

        # Flip back to writer, then to reader again.
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            False,
        )
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )
        # Cached reader reused.
        dd.connect.assert_not_called()
        assert svc.current_connection is first_reader

    asyncio.run(_body())


def test_set_read_only_true_raises_when_no_reader_in_topology():
    async def _body() -> None:
        plugin, *_ = _build(
            topology=(HostInfo(host="writer.example", port=5432, role=HostRole.WRITER),)
        )

        async def _work() -> None:
            return None

        with pytest.raises(ReadWriteSplittingError):
            await plugin.execute(
                object(),
                DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
                _work,
                True,
            )

    asyncio.run(_body())


def test_reopens_reader_when_cached_reader_closed():
    async def _body() -> None:
        plugin, svc, hlp, dd, writer_conn = _build()

        async def _work() -> None:
            return None

        # Initial flip -> cache reader.
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )
        # Simulate reader conn being closed between requests.
        dd.is_closed = AsyncMock(return_value=True)
        dd.connect.reset_mock()

        # Flip to writer, then back to reader -- should reopen.
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            False,
        )
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )
        # Connect was called again (at least once) to reopen.
        assert dd.connect.call_count >= 1

    asyncio.run(_body())


def test_subscribed_methods_only_covers_set_read_only():
    plugin, *_ = _build()
    assert plugin.subscribed_methods == {
        DbApiMethod.CONNECTION_SET_READ_ONLY.method_name
    }


def test_initial_connect_seeds_writer_cache():
    async def _body() -> None:
        plugin, svc, hlp, dd, _ = _build()
        # Clear the writer cache set in _build().
        plugin._writer_conn = None

        new_conn = MagicMock(name="fresh_writer")

        async def _connect_func() -> object:
            return new_conn

        result = await plugin.connect(
            target_driver_func=lambda: None,
            driver_dialect=dd,
            host_info=HostInfo(host="w", port=5432),
            props=Properties({"host": "w"}),
            is_initial_connection=True,
            connect_func=_connect_func,
        )
        assert result is new_conn
        assert plugin._writer_conn is new_conn

    asyncio.run(_body())


def test_switch_to_reader_uses_configured_strategy():
    """RWS picks via plugin_service.get_host_info_by_strategy with the configured strategy."""
    r1 = HostInfo(host="r1.example", port=5432, role=HostRole.READER)
    r2 = HostInfo(host="r2.example", port=5432, role=HostRole.READER)
    writer = HostInfo(host="writer.example", port=5432, role=HostRole.WRITER)
    plugin, svc, hlp, _, _ = _build(topology=(writer, r1, r2))

    # Inject strategy into props
    svc._props["reader_host_selector_strategy"] = "round_robin"

    # Stub get_host_info_by_strategy
    svc.get_host_info_by_strategy = MagicMock(return_value=r1)

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    asyncio.run(_run())

    svc.get_host_info_by_strategy.assert_called_once()
    args = svc.get_host_info_by_strategy.call_args.args
    # (role, strategy, candidates)
    assert args[0] == HostRole.READER
    assert args[1] == "round_robin"
    # candidates exclude the writer
    assert writer not in args[2]
    assert r1 in args[2] and r2 in args[2]


def test_switch_to_reader_defaults_to_random_strategy():
    """No strategy prop -> 'random' passed to get_host_info_by_strategy."""
    r = HostInfo(host="r.example", port=5432, role=HostRole.READER)
    plugin, svc, hlp, _, _ = _build(topology=(
        HostInfo(host="w.example", port=5432, role=HostRole.WRITER),
        r,
    ))
    svc.get_host_info_by_strategy = MagicMock(return_value=r)

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    asyncio.run(_run())

    assert svc.get_host_info_by_strategy.call_args.args[1] == "random"


def test_switch_to_reader_raises_when_strategy_returns_none():
    """If the strategy can't pick a reader (empty candidates), raise."""
    plugin, svc, hlp, _, _ = _build(topology=(
        HostInfo(host="w", port=5432, role=HostRole.WRITER),
    ))  # no readers
    svc.get_host_info_by_strategy = MagicMock(return_value=None)

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    with pytest.raises(ReadWriteSplittingError):
        asyncio.run(_run())


def test_switch_to_reader_refuses_mid_transaction():
    """Mid-txn attempt to go read-only raises ReadWriteSplittingError."""
    plugin, svc, hlp, dd, _ = _build()
    dd.is_in_transaction = AsyncMock(return_value=True)

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    with pytest.raises(ReadWriteSplittingError):
        asyncio.run(_run())


def test_switch_to_writer_is_allowed_mid_transaction():
    """Writer swap (read_only=False) doesn't check transaction state."""
    plugin, svc, hlp, dd, _ = _build()
    dd.is_in_transaction = AsyncMock(return_value=True)
    # Seed reader as current so flipping back to writer is a real swap
    writer_conn = MagicMock(name="writer")
    plugin._writer_conn = writer_conn

    async def _run():
        async def _set_ro():
            return None

        # read_only=False -- should NOT raise even in transaction
        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, False)

    asyncio.run(_run())  # no exception


def test_switch_to_reader_allowed_when_not_in_transaction():
    """Not-in-txn case works normally (sanity)."""
    plugin, svc, hlp, dd, _ = _build()
    dd.is_in_transaction = AsyncMock(return_value=False)

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    asyncio.run(_run())  # no exception
    assert plugin._reader_conn is not None
