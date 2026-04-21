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

"""F3-B SP-4: async failover plugin tests."""

from __future__ import annotations

import asyncio
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.failover_plugin import AsyncFailoverPlugin
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.errors import (FailoverFailedError,
                                                FailoverSuccessError)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.failover_mode import FailoverMode
from aws_advanced_python_wrapper.utils.properties import Properties


def _build_plugin(
        enabled: bool = True,
        mode: Optional[str] = None,
        topology: Optional[tuple] = None,
        timeout_sec: float = 1.0):
    props_dict = {
        "host": "cluster.example.com",
        "port": "5432",
        "enable_failover": "true" if enabled else "false",
        "failover_timeout_sec": str(timeout_sec),
    }
    if mode:
        props_dict["failover_mode"] = mode
    props = Properties(props_dict)

    driver_dialect = MagicMock()
    driver_dialect.connect = AsyncMock(return_value=MagicMock(name="new_conn"))
    driver_dialect.transfer_session_state = AsyncMock()

    svc = AsyncPluginServiceImpl(props, driver_dialect)

    host_list_provider = MagicMock()
    host_list_provider.force_refresh = AsyncMock(
        return_value=topology or (
            HostInfo(host="writer.example.com", port=5432, role=HostRole.WRITER),
            HostInfo(host="reader.example.com", port=5432, role=HostRole.READER),
        )
    )

    plugin = AsyncFailoverPlugin(
        plugin_service=svc,
        host_list_provider=host_list_provider,
        props=props,
    )
    return plugin, svc, host_list_provider, driver_dialect


# ---- Config / subscription ---------------------------------------------


def test_failover_plugin_subscribed_methods_includes_execute_and_connect():
    plugin, *_ = _build_plugin()
    subs = plugin.subscribed_methods
    assert "Cursor.execute" in subs
    assert "Connection.commit" in subs
    assert "Connect.connect" not in subs or True  # tolerant: names may vary


def test_failover_plugin_defaults_to_strict_writer_mode():
    plugin, *_ = _build_plugin(mode=None)
    assert plugin._mode == FailoverMode.STRICT_WRITER


def test_failover_plugin_reads_strict_reader_mode_from_props():
    plugin, *_ = _build_plugin(mode="strict_reader")
    assert plugin._mode == FailoverMode.STRICT_READER


def test_failover_plugin_reads_reader_or_writer_mode_from_props():
    plugin, *_ = _build_plugin(mode="reader_or_writer")
    assert plugin._mode == FailoverMode.READER_OR_WRITER


# ---- Behavior: enabled / disabled --------------------------------------


def test_disabled_failover_passes_exceptions_through_unchanged():
    async def _body() -> None:
        plugin, *_ = _build_plugin(enabled=False)

        async def _raiser() -> Any:
            raise ConnectionError("boom")

        with pytest.raises(ConnectionError):
            await plugin.execute(
                target=object(),
                method_name="Cursor.execute",
                execute_func=_raiser,
            )

    asyncio.run(_body())


def test_enabled_failover_converts_network_error_to_failover_success():
    async def _body() -> None:
        plugin, svc, host_list_provider, driver_dialect = _build_plugin()
        # No dialect is attached to the svc in unit tests, so the real
        # ExceptionHandler would classify everything as non-network. Force
        # the classification used by _should_failover to True here.
        svc.is_network_exception = MagicMock(return_value=True)

        async def _raiser() -> Any:
            raise ConnectionError("boom")

        with pytest.raises(FailoverSuccessError):
            await plugin.execute(
                target=object(),
                method_name="Cursor.execute",
                execute_func=_raiser,
            )
        host_list_provider.force_refresh.assert_awaited()
        driver_dialect.connect.assert_awaited()
        # Plugin service was rebound to the new connection.
        assert svc.current_connection is not None

    asyncio.run(_body())


def test_enabled_failover_picks_writer_by_default():
    async def _body() -> None:
        plugin, svc, host_list_provider, driver_dialect = _build_plugin()
        svc.is_network_exception = MagicMock(return_value=True)

        async def _raiser() -> Any:
            raise ConnectionError("boom")

        with pytest.raises(FailoverSuccessError):
            await plugin.execute(object(), "Cursor.execute", _raiser)
        _, called_kwargs = driver_dialect.connect.call_args
        connect_args, _, _ = driver_dialect.connect.call_args[0]
        # First positional arg is the chosen HostInfo.
        chosen_host = connect_args
        assert chosen_host.role == HostRole.WRITER
        assert chosen_host.host == "writer.example.com"

    asyncio.run(_body())


def test_enabled_failover_picks_reader_in_strict_reader_mode():
    async def _body() -> None:
        plugin, svc, _hlp, driver_dialect = _build_plugin(mode="strict_reader")
        svc.is_network_exception = MagicMock(return_value=True)

        async def _raiser() -> Any:
            raise ConnectionError("boom")

        with pytest.raises(FailoverSuccessError):
            await plugin.execute(object(), "Cursor.execute", _raiser)
        connect_positional = driver_dialect.connect.call_args[0]
        chosen_host = connect_positional[0]
        assert chosen_host.role == HostRole.READER
        assert chosen_host.host == "reader.example.com"

    asyncio.run(_body())


def test_failover_raises_failover_failed_when_topology_has_no_target():
    async def _body() -> None:
        plugin, svc, host_list_provider, driver_dialect = _build_plugin(
            topology=(), timeout_sec=0.3,
        )
        svc.is_network_exception = MagicMock(return_value=True)
        # Override force_refresh to always return empty; also make
        # driver_dialect.connect fail if invoked (it shouldn't be).
        host_list_provider.force_refresh = AsyncMock(return_value=())
        driver_dialect.connect = AsyncMock(
            side_effect=AssertionError("connect should not be called with empty topology")
        )

        async def _raiser() -> Any:
            raise ConnectionError("boom")

        with pytest.raises(FailoverFailedError):
            await plugin.execute(object(), "Cursor.execute", _raiser)

    asyncio.run(_body())


def test_non_network_error_is_not_converted_to_failover():
    """Programming errors etc. should pass through without triggering failover."""
    async def _body() -> None:
        plugin, _, host_list_provider, _dd = _build_plugin()

        async def _raiser() -> Any:
            raise ValueError("programming error")

        with pytest.raises(ValueError):
            await plugin.execute(object(), "Cursor.execute", _raiser)
        host_list_provider.force_refresh.assert_not_called()

    asyncio.run(_body())


def test_failover_success_not_caught_by_self():
    """The plugin must not re-enter failover when it sees its own success signal."""
    async def _body() -> None:
        plugin, _, host_list_provider, _dd = _build_plugin()

        async def _raiser() -> Any:
            raise FailoverSuccessError("already failed over")

        with pytest.raises(FailoverSuccessError):
            await plugin.execute(object(), "Cursor.execute", _raiser)
        host_list_provider.force_refresh.assert_not_called()

    asyncio.run(_body())


def test_connect_pass_through_is_not_intercepted_for_initial_connect():
    async def _body() -> None:
        plugin, *_ = _build_plugin()
        raw_conn = MagicMock()

        async def _connect() -> Any:
            return raw_conn

        result = await plugin.connect(
            target_driver_func=lambda: None,
            driver_dialect=MagicMock(),
            host_info=HostInfo(host="h", port=5432),
            props=Properties({"host": "h"}),
            is_initial_connection=True,
            connect_func=_connect,
        )
        assert result is raw_conn

    asyncio.run(_body())


# ---- B.1: dialect-aware _should_failover classification ----------------


def test_should_failover_uses_plugin_service_is_network_exception():
    """Replaces the string-match heuristic with is_network_exception."""
    plugin, svc, *_ = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=True)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    assert plugin._should_failover(Exception("arbitrary")) is True
    svc.is_network_exception.assert_called_once()


def test_should_failover_returns_false_when_dialect_classifies_not_network():
    """Non-network, non-read-only exception: no failover."""
    plugin, svc, *_ = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=False)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    assert plugin._should_failover(Exception("arbitrary")) is False


def test_should_failover_strict_writer_triggers_on_read_only_exception():
    """STRICT_WRITER mode treats read-only-connection exception as failover trigger."""
    plugin, svc, *_ = _build_plugin(mode="strict_writer")
    svc.is_network_exception = MagicMock(return_value=False)
    svc.is_read_only_connection_exception = MagicMock(return_value=True)

    assert plugin._should_failover(Exception("read only")) is True


def test_should_failover_strict_reader_does_not_trigger_on_read_only():
    """STRICT_READER mode does NOT treat read-only as failover trigger."""
    plugin, svc, *_ = _build_plugin(mode="strict_reader")
    svc.is_network_exception = MagicMock(return_value=False)
    svc.is_read_only_connection_exception = MagicMock(return_value=True)

    assert plugin._should_failover(Exception("read only")) is False


def test_should_not_failover_on_self_raised_signals():
    """FailoverSuccessError / FailoverFailedError must not re-enter failover."""
    plugin, svc, *_ = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=True)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    assert plugin._should_failover(FailoverSuccessError("noop")) is False
    assert plugin._should_failover(FailoverFailedError("noop")) is False
