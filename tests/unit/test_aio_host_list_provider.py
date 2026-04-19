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
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock

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
