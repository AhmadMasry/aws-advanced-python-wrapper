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

"""Async SimpleReadWriteSplittingPlugin unit tests (Phase H deferred port).

Covers construction-time validation, read/write-only swap behavior,
cache reuse, role-verification retry, and initial-connection verification
against a cluster URL.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.simple_read_write_splitting_plugin import \
    AsyncSimpleReadWriteSplittingPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


def _base_props(**overrides: Any) -> Properties:
    """Build a minimal Properties dict with both SRW endpoints set."""
    props: Properties = Properties({
        "host": "cluster.example.com",
        "port": "5432",
        WrapperProperties.SRW_READ_ENDPOINT.name: "reader.example.com",
        WrapperProperties.SRW_WRITE_ENDPOINT.name: "writer.example.com",
        # Keep the retry loop tight so tests don't have to wait.
        WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS.name: "50",
        WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS.name: "5",
    })
    for k, v in overrides.items():
        props[k] = v
    return props


def _build_plugin_service(
        role_returns: Optional[HostRole] = HostRole.WRITER,
        current_host: Optional[HostInfo] = None):
    """Build a mock AsyncPluginService covering the APIs the plugin touches."""
    svc = MagicMock()

    # driver_dialect: still exposed for close/abort. The plugin no
    # longer opens connections via driver_dialect.connect -- those now
    # route through plugin_service.connect (pipeline).
    dd = MagicMock()
    dd.is_closed = AsyncMock(return_value=False)
    dd.abort_connection = AsyncMock()
    svc.driver_dialect = dd

    # plugin_service.connect now stands in for the old pipeline-bypass
    # driver_dialect.connect call. Returns a fresh per-host conn.
    def _connect_factory(host_info, props, plugin_to_skip=None):
        return MagicMock(name=f"conn-to-{host_info.host}")

    svc.connect = AsyncMock(side_effect=_connect_factory)

    # database_dialect.default_port used by _create_host_info.
    db_dialect = MagicMock()
    db_dialect.default_port = 5432
    svc.database_dialect = db_dialect

    svc.current_host_info = current_host
    svc.get_host_role = AsyncMock(return_value=role_returns)
    svc.set_current_connection = AsyncMock()
    svc.initial_connection_host_info = None
    return svc, dd


# ---- Construction-time validation -----------------------------------------


def test_missing_read_endpoint_raises_at_construction():
    svc, _ = _build_plugin_service()
    props = _base_props()
    del props[WrapperProperties.SRW_READ_ENDPOINT.name]
    with pytest.raises(AwsWrapperError):
        AsyncSimpleReadWriteSplittingPlugin(svc, props)


def test_missing_write_endpoint_raises_at_construction():
    svc, _ = _build_plugin_service()
    props = _base_props()
    del props[WrapperProperties.SRW_WRITE_ENDPOINT.name]
    with pytest.raises(AwsWrapperError):
        AsyncSimpleReadWriteSplittingPlugin(svc, props)


# ---- set_read_only swap behavior ------------------------------------------


def test_set_read_only_true_switches_to_read_endpoint():
    async def _body() -> None:
        svc, dd = _build_plugin_service(role_returns=HostRole.READER)
        props = _base_props()
        plugin = AsyncSimpleReadWriteSplittingPlugin(svc, props)

        async def _noop() -> None:
            return None

        await plugin.execute(
            MagicMock(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _noop, True,
        )

        # Cached reader conn set; current conn rebound to the read endpoint.
        assert plugin._reader_conn is not None
        svc.set_current_connection.assert_awaited()
        called_host = svc.set_current_connection.await_args.args[1]
        assert called_host.host == "reader.example.com"
        assert called_host.role == HostRole.READER

    asyncio.run(_body())


def test_set_read_only_false_switches_to_write_endpoint():
    async def _body() -> None:
        svc, dd = _build_plugin_service(role_returns=HostRole.WRITER)
        props = _base_props()
        plugin = AsyncSimpleReadWriteSplittingPlugin(svc, props)

        async def _noop() -> None:
            return None

        await plugin.execute(
            MagicMock(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _noop, False,
        )

        assert plugin._writer_conn is not None
        called_host = svc.set_current_connection.await_args.args[1]
        assert called_host.host == "writer.example.com"
        assert called_host.role == HostRole.WRITER

    asyncio.run(_body())


# ---- Role-verification retry ----------------------------------------------


def test_verify_retries_when_role_mismatches_and_times_out():
    """If get_host_role never matches, _verify_role eventually returns None
    and _switch_to raises."""
    from aws_advanced_python_wrapper.errors import ReadWriteSplittingError

    async def _body() -> None:
        # Asked for a reader, but every probe says WRITER -> exhaust retries.
        svc, dd = _build_plugin_service(role_returns=HostRole.WRITER)
        # Extra-short timeout to keep the test quick; still exercises >1 loop.
        props = _base_props(
            **{
                WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS.name: "30",
                WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS.name: "5",
            }
        )
        plugin = AsyncSimpleReadWriteSplittingPlugin(svc, props)

        async def _noop() -> None:
            return None

        with pytest.raises(ReadWriteSplittingError):
            await plugin.execute(
                MagicMock(),
                DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
                _noop, True,  # request reader
            )
        # Multiple connect attempts made before giving up.
        assert svc.connect.await_count >= 2
        # Each wrong-role connection was aborted.
        assert dd.abort_connection.await_count >= 2

    asyncio.run(_body())


def test_verify_returns_immediately_when_role_matches():
    """Single connect attempt when role matches on the first probe."""
    async def _body() -> None:
        svc, dd = _build_plugin_service(role_returns=HostRole.READER)
        props = _base_props()
        plugin = AsyncSimpleReadWriteSplittingPlugin(svc, props)

        async def _noop() -> None:
            return None

        await plugin.execute(
            MagicMock(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _noop, True,
        )

        assert svc.connect.await_count == 1
        dd.abort_connection.assert_not_awaited()
        assert plugin._reader_conn is not None

    asyncio.run(_body())


# ---- Cache reuse ----------------------------------------------------------


def test_cached_connection_reused_when_toggling_back():
    """After a toggle cycle, the cached reader/writer conns are reused
    without a fresh driver_dialect.connect call."""
    async def _body() -> None:
        # Initial setup: get_host_role flips per call to match what the
        # plugin asks for (WRITER first, then READER, then WRITER again).
        role_queue = [HostRole.READER, HostRole.WRITER, HostRole.READER]

        async def _role(_conn, timeout_sec=5.0):
            return role_queue.pop(0) if role_queue else HostRole.WRITER

        svc, dd = _build_plugin_service()
        svc.get_host_role = AsyncMock(side_effect=_role)
        props = _base_props()
        plugin = AsyncSimpleReadWriteSplittingPlugin(svc, props)

        async def _noop() -> None:
            return None

        # 1st flip: open reader.
        await plugin.execute(
            MagicMock(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _noop, True,
        )
        first_reader = plugin._reader_conn
        # 2nd flip: open writer.
        await plugin.execute(
            MagicMock(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _noop, False,
        )
        first_writer = plugin._writer_conn

        svc.connect.reset_mock()

        # 3rd flip: back to reader; expect cached reader reused (no connect).
        await plugin.execute(
            MagicMock(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _noop, True,
        )
        svc.connect.assert_not_awaited()
        assert plugin._reader_conn is first_reader

        # 4th flip: back to writer; expect cached writer reused too.
        await plugin.execute(
            MagicMock(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _noop, False,
        )
        svc.connect.assert_not_awaited()
        assert plugin._writer_conn is first_writer

    asyncio.run(_body())


# ---- Initial-connection role verification ---------------------------------


def test_initial_connect_with_reader_hint_verifies_role():
    """SRW_VERIFY_INITIAL_CONNECTION_TYPE=reader + a cluster URL: the plugin
    verifies the connection by calling get_host_role on it."""
    async def _body() -> None:
        svc, dd = _build_plugin_service(role_returns=HostRole.READER)
        props = _base_props(**{
            WrapperProperties.SRW_VERIFY_INITIAL_CONNECTION_TYPE.name: "reader",
        })
        plugin = AsyncSimpleReadWriteSplittingPlugin(svc, props)

        fresh_conn = MagicMock(name="fresh_initial")

        async def _connect_func():
            return fresh_conn

        # Use any host URL -- the 'reader' override is enough to trigger
        # verification even without a recognized cluster URL pattern.
        host_info = HostInfo(
            host="my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com",
            port=5432)
        result = await plugin.connect(
            target_driver_func=lambda: None,
            driver_dialect=dd,
            host_info=host_info,
            props=props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

        # Verified connection returned.
        assert result is fresh_conn
        # get_host_role was consulted on the verified conn.
        svc.get_host_role.assert_awaited()
        # initial_connection_host_info was updated.
        assert svc.initial_connection_host_info is host_info

    asyncio.run(_body())
