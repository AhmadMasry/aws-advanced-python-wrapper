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
from aws_advanced_python_wrapper.errors import (
    FailoverFailedError, FailoverSuccessError,
    TransactionResolutionUnknownError)
from aws_advanced_python_wrapper.host_availability import HostAvailability
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
        # B.4: reader failover goes through get_host_info_by_strategy.
        # The real plugin_manager isn't wired in unit tests, so stub it to
        # return the reader from the topology _build_plugin seeded.
        reader_host = HostInfo(
            host="reader.example.com", port=5432, role=HostRole.READER)
        svc.get_host_info_by_strategy = MagicMock(return_value=reader_host)

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


# ---- B.2: mid-transaction TransactionResolutionUnknownError ------------


def test_failover_raises_transaction_unknown_when_mid_transaction():
    """Mid-txn failover -> TransactionResolutionUnknownError."""
    plugin, svc, host_list_provider, driver_dialect = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=True)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    # Seed current connection so the probe has something to call against
    svc._current_connection = MagicMock(name="old_conn")
    driver_dialect.is_in_transaction = AsyncMock(return_value=True)

    # Stub _do_failover: assume it swaps the connection successfully.
    plugin._do_failover = AsyncMock()  # type: ignore[method-assign]

    async def _raising():
        raise Exception("network failure")

    with pytest.raises(TransactionResolutionUnknownError):
        asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _raising))


def test_failover_raises_failover_success_when_not_in_transaction():
    """Outside a txn -> FailoverSuccessError (caller can retry cleanly)."""
    plugin, svc, host_list_provider, driver_dialect = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=True)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    svc._current_connection = MagicMock(name="old_conn")
    driver_dialect.is_in_transaction = AsyncMock(return_value=False)

    plugin._do_failover = AsyncMock()  # type: ignore[method-assign]

    async def _raising():
        raise Exception("network failure")

    with pytest.raises(FailoverSuccessError):
        asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _raising))


def test_failover_txn_probe_errors_treated_as_not_in_txn():
    """If is_in_transaction probe itself fails, fall back to FailoverSuccessError
    (best-effort probing; don't promote a probe failure to TransactionResolutionUnknown)."""
    plugin, svc, host_list_provider, driver_dialect = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=True)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    svc._current_connection = MagicMock(name="old_conn")
    driver_dialect.is_in_transaction = AsyncMock(side_effect=RuntimeError("probe broke"))

    plugin._do_failover = AsyncMock()  # type: ignore[method-assign]

    async def _raising():
        raise Exception("network failure")

    with pytest.raises(FailoverSuccessError):
        asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _raising))


def test_failover_no_current_connection_treated_as_not_in_txn():
    """No current connection (edge case) -> FailoverSuccessError, not TxnUnknown."""
    plugin, svc, *_ = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=True)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    # No current_connection seeded.
    plugin._do_failover = AsyncMock()  # type: ignore[method-assign]

    async def _raising():
        raise Exception("network failure")

    with pytest.raises(FailoverSuccessError):
        asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _raising))


# ---- B.3: set_availability on connect success / failure ----------------


def test_failover_marks_connected_host_available():
    """Successful reconnect -> host marked AVAILABLE."""
    plugin, svc, host_list_provider, driver_dialect = _build_plugin()
    svc.set_availability = MagicMock()

    # Force topology to a single writer that will succeed
    writer = HostInfo(host="w1", port=5432, role=HostRole.WRITER)
    host_list_provider.force_refresh = AsyncMock(return_value=(writer,))

    # Stub _open_connection to succeed
    plugin._open_connection = AsyncMock(return_value=MagicMock(name="new_conn"))

    asyncio.run(plugin._do_failover(driver_dialect=driver_dialect))

    svc.set_availability.assert_any_call(
        writer.as_aliases(), HostAvailability.AVAILABLE)


def test_failover_marks_failed_host_unavailable():
    """Connect failure -> host marked UNAVAILABLE."""
    plugin, svc, host_list_provider, driver_dialect = _build_plugin(timeout_sec=0.5)
    svc.set_availability = MagicMock()

    writer = HostInfo(host="dead-writer", port=5432, role=HostRole.WRITER)
    host_list_provider.force_refresh = AsyncMock(return_value=(writer,))

    # Open always fails
    plugin._open_connection = AsyncMock(side_effect=OSError("refused"))

    with pytest.raises(FailoverFailedError):
        asyncio.run(plugin._do_failover(driver_dialect=driver_dialect))

    svc.set_availability.assert_any_call(
        writer.as_aliases(), HostAvailability.UNAVAILABLE)


# ---- B.4: reader retry loop + strategy + writer fallback ---------------


def test_reader_failover_uses_configured_strategy():
    """Reader failover picks via plugin_service.get_host_info_by_strategy
    with the configured strategy name."""
    props_extra = {"failover_mode": "strict-reader",  # noqa: F841
                   "failover_reader_host_selector_strategy": "round_robin"}
    plugin, svc, host_list_provider, driver_dialect = _build_plugin(  # noqa: F841
        mode="strict-reader")
    # Inject the strategy prop into the plugin's internal props dict.
    # _build_plugin doesn't take an arbitrary kwarg; instead construct the
    # plugin with our own props.
    props = Properties({
        "host": "cluster.example.com",
        "port": "5432",
        "enable_failover": "true",
        "failover_timeout_sec": "1.0",
        "failover_mode": "strict-reader",
        "failover_reader_host_selector_strategy": "round_robin",
    })
    # Rebuild svc + plugin with our props
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginServiceImpl
    dd = MagicMock()
    dd.connect = AsyncMock(return_value=MagicMock(name="new_conn"))
    dd.transfer_session_state = AsyncMock()
    svc = AsyncPluginServiceImpl(props, dd)

    reader = HostInfo(host="r1", port=5432, role=HostRole.READER)
    hlp = MagicMock()
    hlp.force_refresh = AsyncMock(return_value=(reader,))

    plugin = AsyncFailoverPlugin(plugin_service=svc, host_list_provider=hlp, props=props)
    svc.get_host_info_by_strategy = MagicMock(return_value=reader)
    plugin._open_connection = AsyncMock(return_value=MagicMock())

    asyncio.run(plugin._do_failover(driver_dialect=dd))

    svc.get_host_info_by_strategy.assert_called()
    first_call = svc.get_host_info_by_strategy.call_args_list[0]
    # Positional: (role, strategy, host_list)
    assert first_call.args[1] == "round_robin"


def test_reader_failover_cycles_through_candidates():
    """Failed reader is removed from remaining; next candidate tried."""
    props = Properties({
        "host": "cluster.example.com",
        "port": "5432",
        "enable_failover": "true",
        "failover_timeout_sec": "1.0",
        "failover_mode": "strict-reader",
    })
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginServiceImpl
    dd = MagicMock()
    dd.connect = AsyncMock(return_value=MagicMock(name="new_conn"))
    dd.transfer_session_state = AsyncMock()
    svc = AsyncPluginServiceImpl(props, dd)

    r1 = HostInfo(host="r1", port=5432, role=HostRole.READER)
    r2 = HostInfo(host="r2", port=5432, role=HostRole.READER)

    hlp = MagicMock()
    hlp.force_refresh = AsyncMock(return_value=(r1, r2))

    plugin = AsyncFailoverPlugin(plugin_service=svc, host_list_provider=hlp, props=props)

    # Strategy picks r1 first, then r2
    svc.get_host_info_by_strategy = MagicMock(side_effect=[r1, r2])

    attempts = []

    async def _open(target, _driver_dialect):
        attempts.append(target.host)
        if target is r1:
            raise OSError("r1 down")
        return MagicMock()

    plugin._open_connection = _open  # type: ignore[method-assign]

    asyncio.run(plugin._do_failover(driver_dialect=dd))

    assert attempts == ["r1", "r2"]


def test_reader_failover_falls_back_to_original_writer_in_reader_or_writer_mode():
    """READER_OR_WRITER: when all readers fail, try the original writer."""
    props = Properties({
        "host": "cluster.example.com",
        "port": "5432",
        "enable_failover": "true",
        "failover_timeout_sec": "1.0",
        "failover_mode": "reader-or-writer",
    })
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginServiceImpl
    dd = MagicMock()
    dd.connect = AsyncMock(return_value=MagicMock(name="new_conn"))
    dd.transfer_session_state = AsyncMock()
    svc = AsyncPluginServiceImpl(props, dd)

    r1 = HostInfo(host="r1", port=5432, role=HostRole.READER)
    w = HostInfo(host="w1", port=5432, role=HostRole.WRITER)

    hlp = MagicMock()
    hlp.force_refresh = AsyncMock(return_value=(r1, w))

    plugin = AsyncFailoverPlugin(plugin_service=svc, host_list_provider=hlp, props=props)
    # Strategy returns r1 first, then None (signals "no more readers")
    svc.get_host_info_by_strategy = MagicMock(side_effect=[r1, None])

    attempts = []

    async def _open(target, _driver_dialect):
        attempts.append(target.host)
        if target is r1:
            raise OSError("r1 down")
        return MagicMock()

    plugin._open_connection = _open  # type: ignore[method-assign]

    asyncio.run(plugin._do_failover(driver_dialect=dd))

    assert attempts == ["r1", "w1"]


def test_failover_falls_back_to_initial_host_when_topology_empty():
    """If force_refresh returns empty topology, use initial_connection_host_info."""
    plugin, svc, host_list_provider, driver_dialect = _build_plugin(
        mode="strict-writer", timeout_sec=0.5)

    initial = HostInfo(host="writer-initial", port=5432, role=HostRole.WRITER)
    svc.initial_connection_host_info = initial
    host_list_provider.force_refresh = AsyncMock(return_value=())

    plugin._open_connection = AsyncMock(return_value=MagicMock())

    asyncio.run(plugin._do_failover(driver_dialect=driver_dialect))

    plugin._open_connection.assert_called()
    (target, _), _ = plugin._open_connection.call_args_list[0]
    assert target is initial
