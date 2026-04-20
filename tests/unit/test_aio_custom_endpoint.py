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

"""Task 1-B: async custom endpoint monitor + plugin."""

from __future__ import annotations

import asyncio
from typing import List
from unittest.mock import MagicMock, patch

from aws_advanced_python_wrapper.aio import cleanup as aio_cleanup
from aws_advanced_python_wrapper.aio.custom_endpoint_monitor import (
    AsyncCustomEndpointMonitor, AsyncCustomEndpointPlugin)
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties


# ---- Monitor lifecycle -------------------------------------------------


def test_monitor_starts_and_stops_cleanly():
    async def _body() -> None:
        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=["instance-1", "instance-2"],
        ):
            monitor = AsyncCustomEndpointMonitor(
                cluster_identifier="my-cluster",
                custom_endpoint_identifier="my-endpoint",
                refresh_interval_sec=0.5,
            )
            assert monitor.is_running() is False
            monitor.start()
            assert monitor.is_running() is True
            await asyncio.sleep(0.05)
            await monitor.stop()
            assert monitor.is_running() is False
            assert monitor.member_instance_ids == ("instance-1", "instance-2")

    asyncio.run(_body())


def test_monitor_start_is_idempotent():
    async def _body() -> None:
        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=[],
        ):
            monitor = AsyncCustomEndpointMonitor(
                cluster_identifier="c",
                custom_endpoint_identifier="e",
                refresh_interval_sec=0.5,
            )
            monitor.start()
            first_task = monitor._task
            monitor.start()
            assert monitor._task is first_task
            await monitor.stop()

    asyncio.run(_body())


def test_monitor_survives_boto3_errors():
    async def _body() -> None:
        call_count = [0]

        async def _flaky(self: AsyncCustomEndpointMonitor) -> List[str]:
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("transient AWS failure")
            return ["i-good"]

        # Patch the async wrapper directly -- bypasses asyncio.to_thread
        # timing surprises in the test sleep. Patched function must take
        # `self` since patch.object replaces an instance method on the class.
        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members",
            _flaky,
        ):
            monitor = AsyncCustomEndpointMonitor(
                cluster_identifier="c",
                custom_endpoint_identifier="e",
                refresh_interval_sec=0.02,
            )
            monitor.start()
            # Give it time for two iterations.
            await asyncio.sleep(0.2)
            await monitor.stop()
            # Must have survived the first exception and cached the
            # second result.
            assert monitor.member_instance_ids == ("i-good",)
            assert call_count[0] >= 2

    asyncio.run(_body())


def test_monitor_extracts_static_members_from_describe_response():
    """_fetch_members_blocking aggregates StaticMembers across returned endpoints."""
    fake_client = MagicMock()
    fake_client.describe_db_cluster_endpoints = MagicMock(
        return_value={
            "DBClusterEndpoints": [
                {"StaticMembers": ["instance-1", "instance-2"]},
                {"StaticMembers": ["instance-3"]},
            ]
        }
    )
    with patch("boto3.client", return_value=fake_client):
        members = AsyncCustomEndpointMonitor._fetch_members_blocking(
            "c", "e", "us-east-1"
        )
    assert members == ["instance-1", "instance-2", "instance-3"]
    fake_client.describe_db_cluster_endpoints.assert_called_once_with(
        DBClusterIdentifier="c",
        DBClusterEndpointIdentifier="e",
    )


# ---- Plugin integration ------------------------------------------------


def _svc(props: Properties) -> AsyncPluginServiceImpl:
    return AsyncPluginServiceImpl(props, MagicMock(), HostInfo("h", 5432))


def test_plugin_subscription_is_connect_only():
    props = Properties({"host": "h"})
    plugin = AsyncCustomEndpointPlugin(_svc(props), props)
    assert plugin.subscribed_methods == {DbApiMethod.CONNECT.method_name}


def test_plugin_does_not_spawn_monitor_for_non_custom_endpoint_host():
    async def _body() -> None:
        props = Properties({
            "host": "mydb.cluster-xyz.us-east-1.rds.amazonaws.com",
            "cluster_id": "my-cluster",
        })
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)
        raw_conn = MagicMock()

        async def _connect_func() -> object:
            return raw_conn

        await plugin.connect(
            target_driver_func=lambda: None,
            driver_dialect=MagicMock(),
            host_info=HostInfo("mydb.cluster-xyz.us-east-1.rds.amazonaws.com", 5432),
            props=props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )
        assert plugin.monitor is None
        assert plugin.member_instance_ids == ()

    asyncio.run(_body())


def test_plugin_spawns_monitor_for_custom_endpoint_host():
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        props = Properties({
            "host": "my-endpoint.cluster-custom-abc.us-east-1.rds.amazonaws.com",
            "cluster_id": "my-cluster",
            "iam_region": "us-east-1",
        })
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)

        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=["instance-a"],
        ):
            raw_conn = MagicMock()

            async def _connect_func() -> object:
                return raw_conn

            try:
                await plugin.connect(
                    target_driver_func=lambda: None,
                    driver_dialect=MagicMock(),
                    host_info=HostInfo(
                        "my-endpoint.cluster-custom-abc.us-east-1.rds.amazonaws.com",
                        5432,
                    ),
                    props=props,
                    is_initial_connection=True,
                    connect_func=_connect_func,
                )
                assert plugin.monitor is not None
                assert plugin.monitor.is_running() is True
                # Give it a tick to fetch.
                await asyncio.sleep(0.05)
                assert plugin.member_instance_ids == ("instance-a",)
            finally:
                # Drain any scheduled shutdown hooks.
                await aio_cleanup.release_resources_async()
                # Monitor should be stopped after release_resources_async.
                assert plugin.monitor.is_running() is False

    asyncio.run(_body())


def test_plugin_skips_monitor_when_cluster_id_missing():
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        props = Properties({
            "host": "my-endpoint.cluster-custom-abc.us-east-1.rds.amazonaws.com",
            # No cluster_id set.
        })
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)
        raw_conn = MagicMock()

        async def _connect_func() -> object:
            return raw_conn

        await plugin.connect(
            target_driver_func=lambda: None,
            driver_dialect=MagicMock(),
            host_info=HostInfo(
                "my-endpoint.cluster-custom-abc.us-east-1.rds.amazonaws.com",
                5432,
            ),
            props=props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )
        assert plugin.monitor is None

    asyncio.run(_body())


def test_plugin_registers_stop_hook_with_release_resources_async():
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        props = Properties({
            "host": "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
            "cluster_id": "c",
        })
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)

        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=[],
        ):
            raw_conn = MagicMock()

            async def _connect_func() -> object:
                return raw_conn

            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(
                    "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com", 5432,
                ),
                props=props,
                is_initial_connection=True,
                connect_func=_connect_func,
            )
            # Shutdown hook registered.
            assert aio_cleanup._registered_shutdown_hooks, (
                "plugin didn't register its monitor.stop with release_resources_async"
            )
            await aio_cleanup.release_resources_async()

    asyncio.run(_body())


def test_factory_registers_active_plugin_post_task_1b():
    """Task 1-B replaces the SP-8 stub -- factory should build the active one."""
    from aws_advanced_python_wrapper.aio.plugin_factory import \
        build_async_plugins

    props = Properties({
        "host": "h", "port": "5432",
        "plugins": "custom_endpoint",
    })
    plugins = build_async_plugins(_svc(props), props)
    assert len(plugins) == 1
    # The active class -- has `connect` that actually does work.
    assert isinstance(plugins[0], AsyncCustomEndpointPlugin)
    assert plugins[0].subscribed_methods == {DbApiMethod.CONNECT.method_name}
