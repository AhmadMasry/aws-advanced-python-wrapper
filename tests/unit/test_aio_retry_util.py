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

"""Unit tests for the async connection-retry helper.

Async counterpart of the sync :class:`RetryUtil` behavior
(``utils/retry_util.py``): a deadline-bounded retry loop that selects an
allowed host, opens a connection through ``force_connect`` (so auth plugins
re-apply and the pooled provider is bypassed), optionally verifies its role
with a data-plane probe, and closes rejected connections via
``driver_dialect.abort_connection``.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.retry_util import AsyncRetryUtil
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole

_W = HostInfo(host="writer.example.com", port=5432, role=HostRole.WRITER)
_R1 = HostInfo(host="r1.example.com", port=5432, role=HostRole.READER)
_R2 = HostInfo(host="r2.example.com", port=5432, role=HostRole.READER)


def _svc(*, all_hosts, role=HostRole.WRITER, connect_side_effect=None):
    """Build a MagicMock async plugin_service for the retry helper."""
    svc = MagicMock()
    svc.all_hosts = tuple(all_hosts)
    svc.filter_hosts = MagicMock(side_effect=lambda hosts: list(hosts))
    # The retry loop FORCE-refreshes each pass (so the monitor re-probes and
    # discovers a newly elected writer); plain refresh would return the cached
    # pre-failover topology for up to topology_refresh_ms.
    svc.refresh_host_list = AsyncMock()
    svc.force_refresh_host_list = AsyncMock()
    # Strategy picks the first remaining candidate (tests seed `remaining`
    # via the allowed-hosts closure / all_hosts).
    svc.get_host_info_by_strategy = MagicMock(
        side_effect=lambda role_, strat, hosts: hosts[0] if hosts else None)
    svc.get_host_role = AsyncMock(return_value=role)
    conn = MagicMock(name="new_conn")
    if connect_side_effect is not None:
        svc.force_connect = AsyncMock(side_effect=connect_side_effect)
    else:
        svc.force_connect = AsyncMock(return_value=conn)
    dd = MagicMock()
    dd.abort_connection = AsyncMock()
    svc.driver_dialect = dd
    return svc, conn


def _deadline(seconds=0.5):
    return asyncio.get_event_loop().time() + seconds


def test_get_allowed_connection_returns_connection_when_role_matches():
    async def _body():
        svc, conn = _svc(all_hosts=[_R1], role=HostRole.READER)
        util = AsyncRetryUtil()
        result = await util.get_allowed_connection(
            svc, MagicMock(), object(),
            lambda: [_R1], "random", HostRole.READER, _deadline())
        assert result.connection is conn
        assert result.host_info.host == _R1.host

    asyncio.run(_body())


def test_get_allowed_connection_uses_force_connect_not_connect():
    async def _body():
        svc, conn = _svc(all_hosts=[_R1], role=HostRole.READER)
        util = AsyncRetryUtil()
        await util.get_allowed_connection(
            svc, MagicMock(), object(),
            lambda: [_R1], "random", HostRole.READER, _deadline())
        svc.force_connect.assert_awaited()

    asyncio.run(_body())


def test_get_allowed_connection_times_out_when_no_allowed_hosts():
    async def _body():
        svc, _ = _svc(all_hosts=[])
        util = AsyncRetryUtil()
        with pytest.raises(TimeoutError):
            await util.get_allowed_connection(
                svc, MagicMock(), object(),
                lambda: None, "random", HostRole.WRITER, _deadline(0.25))

    asyncio.run(_body())


def test_get_allowed_connection_rejects_role_mismatch_and_times_out():
    async def _body():
        # Only one candidate, but its probed role never matches WRITER -> the
        # candidate is rejected, its connection closed, and the loop times out.
        svc, conn = _svc(all_hosts=[_R1], role=HostRole.READER)
        util = AsyncRetryUtil()
        with pytest.raises(TimeoutError):
            await util.get_allowed_connection(
                svc, MagicMock(), object(),
                lambda: [_R1], "random", HostRole.WRITER, _deadline(0.25))
        # The mismatched connection must have been aborted.
        svc.driver_dialect.abort_connection.assert_awaited()

    asyncio.run(_body())


def test_get_allowed_connection_falls_back_to_writer_when_no_reader():
    # Regression (#1246 GDB live-run bug): the *_OR_WRITER failover modes pass
    # verify_role=None with an allowed list that includes the writer. Right
    # after a writer failover the newly elected writer can be the only reachable
    # host. The host selector requires a concrete role, so the helper must try
    # READER then fall back to WRITER. The old code asked only for READER, so it
    # could never select the writer and timed out with UnableToConnectToReader
    # even though the writer was a valid target.
    async def _body():
        svc, conn = _svc(all_hosts=[_W], role=HostRole.WRITER)

        def _role_selector(role_, strat, hosts):
            matches = [h for h in hosts if h.role == role_]
            if not matches:
                raise Exception("strategy can't get a host of the requested role")
            return matches[0]
        svc.get_host_info_by_strategy = MagicMock(side_effect=_role_selector)

        util = AsyncRetryUtil()
        result = await util.get_allowed_connection(
            svc, MagicMock(), object(),
            lambda: [_W], "random", None, _deadline())
        assert result.connection is conn
        assert result.host_info.host == _W.host
        roles_asked = [call.args[0] for call in svc.get_host_info_by_strategy.call_args_list]
        assert HostRole.WRITER in roles_asked  # the fix's writer fallback fired

    asyncio.run(_body())


def test_get_writer_connection_selects_writer_from_topology():
    async def _body():
        svc, conn = _svc(all_hosts=[_W, _R1], role=HostRole.WRITER)
        util = AsyncRetryUtil()
        result = await util.get_writer_connection(
            svc, MagicMock(), object(), True, _deadline())
        assert result.connection is conn
        assert result.host_info.host == _W.host

    asyncio.run(_body())


def test_get_writer_connection_times_out_when_no_writer():
    async def _body():
        svc, _ = _svc(all_hosts=[_R1, _R2], role=HostRole.READER)
        util = AsyncRetryUtil()
        with pytest.raises(TimeoutError):
            await util.get_writer_connection(
                svc, MagicMock(), object(), True, _deadline(0.25))

    asyncio.run(_body())


def test_loop_force_refreshes_and_does_not_use_cache_windowed_refresh():
    # Regression: a plain refresh_host_list honors topology_refresh_ms (default
    # 30s) and would return the stale pre-failover topology, so failover could
    # never see the new writer. The loop must FORCE refresh each pass.
    async def _body():
        svc, _ = _svc(all_hosts=[_W], role=HostRole.WRITER)
        util = AsyncRetryUtil()
        await util.get_writer_connection(
            svc, MagicMock(), object(), True, _deadline())
        svc.force_refresh_host_list.assert_awaited()
        svc.refresh_host_list.assert_not_awaited()

    asyncio.run(_body())


def test_loop_discovers_writer_that_appears_on_a_later_probe():
    # The new writer isn't elected at the first probe; a later force refresh
    # surfaces it. The loop must keep force-probing and pick it up rather than
    # stalling on the stale cached topology.
    async def _body():
        svc, conn = _svc(all_hosts=[_R1], role=HostRole.WRITER)
        calls = {"n": 0}

        async def _force_refresh(*_a, **_k):
            calls["n"] += 1
            if calls["n"] >= 2:
                svc.all_hosts = (_R1, _W)  # writer appears on the 2nd probe

        svc.force_refresh_host_list = AsyncMock(side_effect=_force_refresh)
        util = AsyncRetryUtil()
        result = await util.get_writer_connection(
            svc, MagicMock(), object(), True, _deadline())
        assert result.connection is conn
        assert result.host_info.host == _W.host
        assert calls["n"] >= 2

    asyncio.run(_body())


def test_close_connection_aborts_via_driver_dialect():
    async def _body():
        svc, conn = _svc(all_hosts=[_R1])
        await AsyncRetryUtil.close_connection(svc, conn)
        svc.driver_dialect.abort_connection.assert_awaited()

    asyncio.run(_body())


def test_close_connection_swallows_errors():
    async def _body():
        svc, conn = _svc(all_hosts=[_R1])
        svc.driver_dialect.abort_connection = AsyncMock(
            side_effect=RuntimeError("boom"))
        # Must not raise.
        await AsyncRetryUtil.close_connection(svc, conn)

    asyncio.run(_body())
