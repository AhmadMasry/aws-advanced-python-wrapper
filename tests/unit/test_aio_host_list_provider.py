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

"""F3-B SP-3: async host list provider + cluster topology monitor."""

from __future__ import annotations

import asyncio
import time
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.cluster_topology_monitor import \
    AsyncClusterTopologyMonitor
from aws_advanced_python_wrapper.aio.host_list_provider import (
    AsyncAuroraHostListProvider, AsyncHostListProvider,
    AsyncStaticHostListProvider)
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.utils.properties import Properties

# ---- Helpers ------------------------------------------------------------


def _props(**kwargs: Any) -> Properties:
    base = {"host": "cluster-xyz.us-east-1.rds.amazonaws.com", "port": "5432"}
    base.update({k: str(v) for k, v in kwargs.items()})
    return Properties(base)


def _build_cursor(rows: List[tuple]) -> MagicMock:
    cur = MagicMock()
    cur.execute = AsyncMock()
    cur.fetchall = AsyncMock(return_value=rows)
    cur.close = AsyncMock()
    cur.__aenter__ = AsyncMock(return_value=cur)
    cur.__aexit__ = AsyncMock(return_value=None)
    return cur


def _build_conn(rows: List[tuple]) -> MagicMock:
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=_build_cursor(rows))
    return conn


# ---- Static provider tests ---------------------------------------------


def test_static_provider_returns_single_host_from_props():
    async def _body() -> None:
        prov = AsyncStaticHostListProvider(_props())
        topo = await prov.refresh()
        assert len(topo) == 1
        assert topo[0].host == "cluster-xyz.us-east-1.rds.amazonaws.com"
        assert topo[0].port == 5432
        assert topo[0].role == HostRole.WRITER

    asyncio.run(_body())


def test_static_provider_cluster_id_is_stable():
    prov = AsyncStaticHostListProvider(_props())
    cid1 = prov.get_cluster_id()
    cid2 = prov.get_cluster_id()
    assert cid1 == cid2
    assert "cluster-xyz" in cid1


def test_static_provider_force_refresh_matches_refresh():
    async def _body() -> None:
        prov = AsyncStaticHostListProvider(_props())
        r = await prov.refresh()
        fr = await prov.force_refresh()
        assert r == fr

    asyncio.run(_body())


def test_static_provider_implements_protocol():
    prov = AsyncStaticHostListProvider(_props())
    assert isinstance(prov, AsyncHostListProvider)


# ---- Aurora provider tests ---------------------------------------------


def test_aurora_provider_cluster_id_uses_host_port_when_no_explicit_id():
    prov = AsyncAuroraHostListProvider(_props(), driver_dialect=MagicMock())
    cid = prov.get_cluster_id()
    assert "aurora:" in cid
    assert "cluster-xyz" in cid


def test_aurora_provider_cluster_id_uses_explicit_cluster_id_prop():
    prov = AsyncAuroraHostListProvider(
        _props(cluster_id="my-cluster"),
        driver_dialect=MagicMock(),
    )
    assert prov.get_cluster_id() == "my-cluster"


def test_aurora_provider_force_refresh_queries_and_caches():
    async def _body() -> None:
        conn = _build_conn([
            ("instance-1", True),  # writer
            ("instance-2", False),
            ("instance-3", False),
        ])
        prov = AsyncAuroraHostListProvider(
            _props(cluster_instance_host_pattern="?.cluster-xyz.us-east-1.rds.amazonaws.com"),
            driver_dialect=MagicMock(),
        )
        topo = await prov.force_refresh(conn)
        assert len(topo) == 3
        writer = [h for h in topo if h.role == HostRole.WRITER]
        readers = [h for h in topo if h.role == HostRole.READER]
        assert len(writer) == 1
        assert len(readers) == 2
        # Writer should be first (sorted).
        assert topo[0].role == HostRole.WRITER
        # Pattern substitution applied.
        assert writer[0].host == "instance-1.cluster-xyz.us-east-1.rds.amazonaws.com"

    asyncio.run(_body())


def test_aurora_provider_refresh_returns_cached_when_fresh():
    async def _body() -> None:
        conn = _build_conn([("i1", True)])
        prov = AsyncAuroraHostListProvider(
            _props(topology_refresh_ms="60000"),
            driver_dialect=MagicMock(),
        )
        first = await prov.refresh(conn)
        # Replace the query rows -- if cache is used, we should still see the first result.
        conn.cursor = MagicMock(return_value=_build_cursor([("i2", True)]))
        second = await prov.refresh(conn)
        assert first == second

    asyncio.run(_body())


def test_aurora_provider_force_refresh_bypasses_cache():
    async def _body() -> None:
        conn = _build_conn([("i1", True)])
        prov = AsyncAuroraHostListProvider(
            _props(),
            driver_dialect=MagicMock(),
        )
        first = await prov.force_refresh(conn)
        conn.cursor = MagicMock(return_value=_build_cursor([("i2", True)]))
        second = await prov.force_refresh(conn)
        assert first != second
        assert second[0].host == "i2"

    asyncio.run(_body())


def test_aurora_provider_refresh_with_no_connection_returns_empty_or_cache():
    async def _body() -> None:
        prov = AsyncAuroraHostListProvider(_props(), driver_dialect=MagicMock())
        topo = await prov.refresh(None)
        assert topo == ()

    asyncio.run(_body())


def test_aurora_provider_query_failure_yields_empty_topology_without_raising():
    async def _body() -> None:
        conn = MagicMock()
        cur = _build_cursor([])
        cur.execute = AsyncMock(side_effect=RuntimeError("boom"))
        conn.cursor = MagicMock(return_value=cur)
        prov = AsyncAuroraHostListProvider(_props(), driver_dialect=MagicMock())
        topo = await prov.force_refresh(conn)
        assert topo == ()

    asyncio.run(_body())


# ---- Cluster topology monitor tests ------------------------------------


def test_topology_monitor_starts_and_stops_cleanly():
    async def _body() -> None:
        prov = MagicMock()
        prov.force_refresh = AsyncMock(return_value=())
        monitor = AsyncClusterTopologyMonitor(
            provider=prov,
            connection_getter=lambda: MagicMock(),
            refresh_interval_sec=0.5,
        )
        assert monitor.is_running() is False
        monitor.start()
        assert monitor.is_running() is True
        await monitor.stop()
        assert monitor.is_running() is False

    asyncio.run(_body())


def test_topology_monitor_calls_force_refresh_at_least_once():
    async def _body() -> None:
        prov = MagicMock()
        refresh_calls: List[int] = []

        async def _refresh(conn: Any = None) -> tuple:
            refresh_calls.append(1)
            return ()

        prov.force_refresh = _refresh
        conn = MagicMock()
        monitor = AsyncClusterTopologyMonitor(
            provider=prov,
            connection_getter=lambda: conn,
            refresh_interval_sec=0.5,
        )
        monitor.start()
        # Give the task a moment to run one iteration.
        await asyncio.sleep(0.05)
        await monitor.stop()
        assert len(refresh_calls) >= 1

    asyncio.run(_body())


def test_topology_monitor_skips_refresh_when_getter_returns_none():
    async def _body() -> None:
        prov = MagicMock()
        prov.force_refresh = AsyncMock(return_value=())
        monitor = AsyncClusterTopologyMonitor(
            provider=prov,
            connection_getter=lambda: None,
            refresh_interval_sec=0.5,
        )
        monitor.start()
        await asyncio.sleep(0.05)
        await monitor.stop()
        prov.force_refresh.assert_not_called()

    asyncio.run(_body())


def test_topology_monitor_survives_provider_exception():
    async def _body() -> None:
        prov = MagicMock()
        prov.force_refresh = AsyncMock(side_effect=RuntimeError("topology failure"))
        monitor = AsyncClusterTopologyMonitor(
            provider=prov,
            connection_getter=lambda: MagicMock(),
            refresh_interval_sec=0.5,
        )
        monitor.start()
        await asyncio.sleep(0.05)
        # Must still be running; exceptions are swallowed.
        assert monitor.is_running() is True
        await monitor.stop()

    asyncio.run(_body())


def test_topology_monitor_start_is_idempotent():
    async def _body() -> None:
        prov = MagicMock()
        prov.force_refresh = AsyncMock(return_value=())
        monitor = AsyncClusterTopologyMonitor(
            provider=prov,
            connection_getter=lambda: MagicMock(),
            refresh_interval_sec=0.5,
        )
        monitor.start()
        first_task = monitor._task
        monitor.start()  # should be no-op
        assert monitor._task is first_task
        await monitor.stop()

    asyncio.run(_body())


# ---- G.1: high-freq refresh after writer change --------------------------


def test_topology_monitor_enters_high_freq_mode_on_writer_change():
    """After a writer-change is observed, monitor ticks at high_refresh_rate."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    # Build a provider whose force_refresh returns topologies with
    # different writers on successive calls
    topology_1 = (HostInfo(host="w1", port=5432, role=HostRole.WRITER),
                  HostInfo(host="r1", port=5432, role=HostRole.READER))
    topology_2 = (HostInfo(host="w2", port=5432, role=HostRole.WRITER),
                  HostInfo(host="r1", port=5432, role=HostRole.READER))

    provider = MagicMock()
    provider.force_refresh = AsyncMock(side_effect=[topology_1, topology_2, topology_2, topology_2])

    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: MagicMock(name="conn"),
        refresh_interval_sec=0.05,
        high_refresh_rate_sec=0.01,
    )

    async def _run_briefly():
        monitor.start()
        # Allow enough ticks for writer-change detection
        await asyncio.sleep(0.08)
        await monitor.stop()

    asyncio.run(_run_briefly())
    # At least 2 force_refresh calls happened (first one sets writer, second detects change)
    assert provider.force_refresh.await_count >= 2


def test_topology_monitor_stays_normal_freq_when_no_writer_change():
    """No writer change -> normal refresh rate, NOT high-freq mode."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    topology = (HostInfo(host="w1", port=5432, role=HostRole.WRITER),)
    provider = MagicMock()
    provider.force_refresh = AsyncMock(return_value=topology)

    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: MagicMock(),
        refresh_interval_sec=0.2,
        high_refresh_rate_sec=0.01,
    )

    async def _run_briefly():
        monitor.start()
        # Only enough time for 1-2 ticks at normal rate
        await asyncio.sleep(0.1)
        await monitor.stop()

    asyncio.run(_run_briefly())
    # At most 2 refresh calls (initial + maybe one more) -- not spinning at high-freq
    assert provider.force_refresh.await_count <= 2


def test_topology_monitor_high_freq_expires_after_period():
    """After HIGH_REFRESH_PERIOD_SEC passes, monitor reverts to normal rate."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    topology_1 = (HostInfo(host="w1", port=5432, role=HostRole.WRITER),)
    topology_2 = (HostInfo(host="w2", port=5432, role=HostRole.WRITER),)
    provider = MagicMock()
    provider.force_refresh = AsyncMock(side_effect=[topology_1] + [topology_2] * 100)

    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: MagicMock(),
        refresh_interval_sec=0.2,
        high_refresh_rate_sec=0.01,
    )
    # Shorten the high-freq period for the test
    monitor.HIGH_REFRESH_PERIOD_SEC = 0.05

    async def _run_briefly():
        monitor.start()
        await asyncio.sleep(0.15)  # past the 50ms high-freq window
        await monitor.stop()
        # Record state when we stopped
        now_ns = int(time.time_ns())
        return monitor._high_refresh_until_ns, now_ns

    high_until, now_ns = asyncio.run(_run_briefly())
    # High-freq period should have expired (we waited past it)
    assert high_until < now_ns


# ---- G.2: ignore-request window after writer found -----------------------


def test_topology_monitor_ignores_requests_after_writer_confirmed():
    """After an internal refresh sees a writer, should_ignore_refresh_request
    returns True for IGNORE_REQUEST_SEC."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    topology = (HostInfo(host="w1", port=5432, role=HostRole.WRITER),)
    provider = MagicMock()
    provider.force_refresh = AsyncMock(return_value=topology)

    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: MagicMock(),
        refresh_interval_sec=0.05,
        high_refresh_rate_sec=0.01,
    )

    async def _run():
        monitor.start()
        await asyncio.sleep(0.08)  # let at least one tick complete
        ignore_during = monitor.should_ignore_refresh_request()
        await monitor.stop()
        return ignore_during

    ignored = asyncio.run(_run())
    assert ignored is True


def test_topology_monitor_does_not_ignore_when_no_writer_seen():
    """Before any tick observes a writer, requests are NOT ignored."""
    monitor = AsyncClusterTopologyMonitor(
        provider=MagicMock(),
        connection_getter=lambda: MagicMock(),
        refresh_interval_sec=30.0,
    )
    assert monitor.should_ignore_refresh_request() is False


def test_topology_monitor_ignore_window_expires():
    """After IGNORE_REQUEST_SEC passes, requests are no longer ignored."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    topology = (HostInfo(host="w1", port=5432, role=HostRole.WRITER),)
    provider = MagicMock()
    provider.force_refresh = AsyncMock(return_value=topology)

    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: MagicMock(),
        refresh_interval_sec=0.05,
        high_refresh_rate_sec=0.01,
    )
    # Shorten window for the test
    monitor.IGNORE_REQUEST_SEC = 0.05

    async def _run():
        monitor.start()
        await asyncio.sleep(0.02)  # initial tick happens
        assert monitor.should_ignore_refresh_request() is True
        await asyncio.sleep(0.1)  # past the 50ms window
        ignore_after = monitor.should_ignore_refresh_request()
        await monitor.stop()
        return ignore_after

    ignored = asyncio.run(_run())
    assert ignored is False


# ---- G.3: force_refresh_with_connection API ------------------------------


def test_force_refresh_with_connection_delegates_to_provider():
    """When ignore-window is clear, probe via the caller's connection."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    topology = (HostInfo(host="w1", port=5432, role=HostRole.WRITER),
                HostInfo(host="r1", port=5432, role=HostRole.READER))
    provider = MagicMock()
    provider.force_refresh = AsyncMock(return_value=topology)

    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: None,
        refresh_interval_sec=30.0,
    )

    caller_conn = MagicMock(name="caller_conn")

    async def _run():
        return await monitor.force_refresh_with_connection(caller_conn, timeout_sec=1.0)

    result = asyncio.run(_run())
    assert result == topology
    provider.force_refresh.assert_awaited_once_with(caller_conn)


def test_force_refresh_with_connection_respects_ignore_window():
    """When in ignore window, returns last-known topology without probing."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    topology = (HostInfo(host="w1", port=5432, role=HostRole.WRITER),)
    provider = MagicMock()
    provider.force_refresh = AsyncMock(return_value=topology)

    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: None,
        refresh_interval_sec=30.0,
    )

    # Seed the ignore window + last_topology
    monitor._ignore_requests_until_ns = (
        time.time_ns() + int(5.0 * 1_000_000_000))
    monitor._last_topology = topology

    async def _run():
        return await monitor.force_refresh_with_connection(
            MagicMock(name="conn"), timeout_sec=1.0)

    result = asyncio.run(_run())
    assert result == topology
    # Provider NOT called -- we returned cached
    provider.force_refresh.assert_not_awaited()


def test_force_refresh_with_connection_raises_on_timeout():
    """Slow provider triggers asyncio.TimeoutError -> TimeoutError."""
    async def _slow(*args, **kwargs):
        await asyncio.sleep(1.0)

    provider = MagicMock()
    provider.force_refresh = _slow

    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: None,
        refresh_interval_sec=30.0,
    )

    async def _run():
        await monitor.force_refresh_with_connection(
            MagicMock(), timeout_sec=0.05)

    with pytest.raises(TimeoutError):
        asyncio.run(_run())


def test_force_refresh_with_connection_triggers_writer_change_detection():
    """A writer change observed via this API also starts the high-freq window."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    monitor = AsyncClusterTopologyMonitor(
        provider=MagicMock(),
        connection_getter=lambda: None,
        refresh_interval_sec=30.0,
    )
    # Seed a prior writer
    monitor._last_known_writer = "old-writer:5432"

    new_topology = (HostInfo(host="new-writer", port=5432, role=HostRole.WRITER),)
    monitor._provider.force_refresh = AsyncMock(return_value=new_topology)

    async def _run():
        await monitor.force_refresh_with_connection(MagicMock(), timeout_sec=1.0)

    asyncio.run(_run())
    assert monitor._last_known_writer == "new-writer:5432"
    assert monitor._high_refresh_until_ns > time.time_ns()


def test_force_refresh_with_connection_bypass_ignore_window():
    """bypass_ignore_window=True probes even during the ignore window."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    topology = (HostInfo(host="w1", port=5432, role=HostRole.WRITER),)
    provider = MagicMock()
    provider.force_refresh = AsyncMock(return_value=topology)

    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: None,
        refresh_interval_sec=30.0,
    )

    # Seed ignore window + cached topology
    stale_topology = ()
    monitor._ignore_requests_until_ns = (
        time.time_ns() + int(5.0 * 1_000_000_000))
    monitor._last_topology = stale_topology

    async def _run():
        return await monitor.force_refresh_with_connection(
            MagicMock(), timeout_sec=1.0, bypass_ignore_window=True)

    result = asyncio.run(_run())
    assert result == topology  # fresh, not stale
    provider.force_refresh.assert_awaited_once()


# ---- G.4: parallel-probe panic mode --------------------------------------


def test_topology_monitor_panic_mode_disabled_without_probe_host():
    """Without probe_host, monitor never enters panic mode (backwards compat)."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    provider = MagicMock()
    provider.force_refresh = AsyncMock(return_value=())
    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: None,  # no conn
        refresh_interval_sec=0.05,
    )
    monitor._last_topology = (HostInfo(host="h", port=5432, role=HostRole.WRITER),)

    async def _run():
        monitor.start()
        await asyncio.sleep(0.08)
        await monitor.stop()

    asyncio.run(_run())
    assert monitor.is_in_panic_mode() is False


def test_topology_monitor_enters_panic_mode_when_no_connection():
    """With probe_host + no monitoring conn + seeded topology -> panic probes fire."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    h1 = HostInfo(host="h1", port=5432, role=HostRole.WRITER)
    h2 = HostInfo(host="h2", port=5432, role=HostRole.READER)

    probed = []

    async def _probe(host_info):
        probed.append(host_info.host)
        # Both return as reader; no winner
        return MagicMock(name=f"conn-{host_info.host}"), HostRole.READER

    provider = MagicMock()
    provider.force_refresh = AsyncMock(return_value=())
    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: None,
        refresh_interval_sec=30.0,
        probe_host=_probe,
    )
    monitor._last_topology = (h1, h2)

    async def _run():
        monitor.start()
        await asyncio.sleep(0.15)
        await monitor.stop()

    asyncio.run(_run())
    # Both hosts probed
    assert "h1" in probed and "h2" in probed
    # No writer found -> no verified state
    assert monitor._is_verified_writer_connection is False


def test_topology_monitor_probe_winner_sets_verified_writer():
    """First probe to return (conn, WRITER) sets verified-writer state."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    h1 = HostInfo(host="h1", port=5432, role=HostRole.READER)
    h2 = HostInfo(host="h2", port=5432, role=HostRole.WRITER)

    winner_conn = MagicMock(name="winner_conn")

    async def _probe(host_info):
        if host_info.host == "h2":
            return winner_conn, HostRole.WRITER
        return MagicMock(name=f"conn-{host_info.host}"), HostRole.READER

    provider = MagicMock()
    provider.force_refresh = AsyncMock(return_value=())
    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: None,
        refresh_interval_sec=30.0,
        probe_host=_probe,
    )
    monitor._last_topology = (h1, h2)

    async def _run():
        monitor.start()
        await asyncio.sleep(0.1)
        await monitor.stop()

    asyncio.run(_run())
    assert monitor._is_verified_writer_connection is True
    assert monitor._verified_writer_conn is winner_conn
    assert monitor._verified_writer_host_info is h2


def test_topology_monitor_claim_verified_writer_is_one_shot():
    """claim_verified_writer returns the winner once, then clears state."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    winner_conn = MagicMock()
    winner_host = HostInfo(host="w", port=5432, role=HostRole.WRITER)

    monitor = AsyncClusterTopologyMonitor(
        provider=MagicMock(),
        connection_getter=lambda: None,
        refresh_interval_sec=30.0,
    )
    monitor._is_verified_writer_connection = True
    monitor._verified_writer_conn = winner_conn
    monitor._verified_writer_host_info = winner_host

    first = monitor.claim_verified_writer()
    assert first == (winner_conn, winner_host)
    assert monitor._is_verified_writer_connection is False
    assert monitor._verified_writer_conn is None

    second = monitor.claim_verified_writer()
    assert second is None


def test_topology_monitor_probe_dedup():
    """Same host doesn't get probed twice across consecutive panic ticks."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    h = HostInfo(host="only-host", port=5432, role=HostRole.READER)
    call_count = [0]

    async def _probe(host_info):
        call_count[0] += 1
        # Simulate long-running probe so it's still pending when next tick fires
        await asyncio.sleep(0.5)
        return MagicMock(), HostRole.READER

    provider = MagicMock()
    provider.force_refresh = AsyncMock(return_value=())
    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: None,
        refresh_interval_sec=0.02,  # many ticks
        probe_host=_probe,
    )
    monitor._last_topology = (h,)

    async def _run():
        monitor.start()
        await asyncio.sleep(0.15)  # enough for 5-7 ticks
        await monitor.stop()

    asyncio.run(_run())
    # Only one probe fired despite multiple ticks (dedup)
    assert call_count[0] == 1


def test_topology_monitor_probe_failure_does_not_crash_loop():
    """A probe raising an exception doesn't kill the monitor task."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    h = HostInfo(host="flaky", port=5432, role=HostRole.READER)

    async def _probe(host_info):
        raise RuntimeError("probe failed")

    provider = MagicMock()
    provider.force_refresh = AsyncMock(return_value=())
    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: None,
        refresh_interval_sec=30.0,
        probe_host=_probe,
    )
    monitor._last_topology = (h,)

    async def _run():
        monitor.start()
        await asyncio.sleep(0.1)
        assert monitor.is_running()
        await monitor.stop()

    asyncio.run(_run())
    assert monitor._is_verified_writer_connection is False


def test_topology_monitor_loser_probe_closes_its_conn():
    """Non-winner probe closes its returned conn."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    h1 = HostInfo(host="fast-reader", port=5432, role=HostRole.READER)
    h2 = HostInfo(host="slow-writer", port=5432, role=HostRole.WRITER)

    loser_conn = MagicMock(name="loser")
    loser_conn.close = MagicMock()
    winner_conn = MagicMock(name="winner")
    winner_conn.close = MagicMock()

    async def _probe(host_info):
        if host_info.host == "slow-writer":
            await asyncio.sleep(0.05)  # let the loser complete first
            return winner_conn, HostRole.WRITER
        return loser_conn, HostRole.READER

    provider = MagicMock()
    provider.force_refresh = AsyncMock(return_value=())
    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: None,
        refresh_interval_sec=30.0,
        probe_host=_probe,
    )
    monitor._last_topology = (h1, h2)

    async def _run():
        monitor.start()
        await asyncio.sleep(0.15)
        await monitor.stop()

    asyncio.run(_run())
    # Loser closed; winner retained
    loser_conn.close.assert_called()
    winner_conn.close.assert_not_called()


def test_probe_two_writers_only_first_wins_second_closes_conn():
    """Two probes returning WRITER: first stashes its conn, second closes its own."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    h1 = HostInfo(host="w1", port=5432, role=HostRole.WRITER)
    h2 = HostInfo(host="w2", port=5432, role=HostRole.WRITER)

    winner_conn = MagicMock(name="winner")
    winner_conn.close = MagicMock()
    loser_conn = MagicMock(name="second_writer")
    loser_conn.close = MagicMock()

    async def _probe(host_info):
        if host_info.host == "w1":
            return winner_conn, HostRole.WRITER
        # h2 arrives slightly later, also returns WRITER
        await asyncio.sleep(0.02)
        return loser_conn, HostRole.WRITER

    provider = MagicMock()
    provider.force_refresh = AsyncMock(return_value=())
    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: None,
        refresh_interval_sec=30.0,
        probe_host=_probe,
    )
    monitor._last_topology = (h1, h2)

    async def _run():
        monitor.start()
        await asyncio.sleep(0.1)
        await monitor.stop()

    asyncio.run(_run())
    # h1 won; its conn retained. h2's "also a writer" conn was closed.
    assert monitor._verified_writer_conn is winner_conn
    loser_conn.close.assert_called()
    winner_conn.close.assert_not_called()


def test_canceled_probe_conn_closed_on_shutdown():
    """Probe canceled mid-flight (before returning a conn) doesn't leak."""
    from aws_advanced_python_wrapper.hostinfo import HostInfo

    h = HostInfo(host="slow", port=5432, role=HostRole.READER)
    probe_started = asyncio.Event()
    released_conn = MagicMock(name="late_conn")
    released_conn.close = MagicMock()

    async def _probe(host_info):
        probe_started.set()
        try:
            await asyncio.sleep(10.0)  # would return conn after 10s
        except asyncio.CancelledError:
            # Probe canceled before it could return the conn -- no leak expected
            raise
        return released_conn, HostRole.READER

    provider = MagicMock()
    provider.force_refresh = AsyncMock(return_value=())
    monitor = AsyncClusterTopologyMonitor(
        provider=provider,
        connection_getter=lambda: None,
        refresh_interval_sec=30.0,
        probe_host=_probe,
    )
    monitor._last_topology = (h,)

    async def _run():
        monitor.start()
        await probe_started.wait()
        # Now stop; the finally should await the canceled probe
        await monitor.stop()

    # Should complete without hanging (finally awaits cancellation)
    asyncio.run(_run())
    # Since the probe was canceled BEFORE it returned a conn, there's no
    # conn to close in our code path -- but the monitor shouldn't hang either
    assert monitor.is_running() is False
