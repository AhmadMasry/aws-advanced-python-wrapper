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

"""Async twin of test_read_write_splitting.py.

Exercises the read_write_splitting and srw plugins on
AsyncAwsWrapperConnection: read-only routing to reader endpoints, write
routing to writer, and ``await conn.set_read_only(value)`` toggles (since
property setters cannot be async).

Translation notes vs the sync file:
- ``conn.read_only = True`` → ``await conn.set_read_only(True)``
- ``conn.read_only`` (getter) → ``conn.read_only`` unchanged (sync passthrough)
- ``rds_utils.query_instance_id(conn)`` → ``await _query_instance_id_async(conn, rds_utils)``
- ``rds_utils.assert_first_query_throws(conn, exc)`` → ``await _assert_first_query_throws_async(conn, rds_utils, exc)``
- ``rds_utils.create_user(conn, ...)`` → ``await _create_user_async(conn, ...)``
- Pooled-connection tests use ``AsyncConnectionProviderManager`` in place of
  ``ConnectionProviderManager``; ``SqlAlchemyPooledConnectionProvider``
  satisfies the duck-typed ``AsyncConnectionProvider`` protocol for the
  strategy/host-filter calls but its ``connect`` is sync, so those tests
  document the gap until an async pool provider is available.
"""

from __future__ import annotations

import asyncio
import gc
from typing import TYPE_CHECKING, Type

import pytest
from sqlalchemy import PoolProxiedConnection

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.aio.connection_provider import \
    AsyncConnectionProviderManager
from aws_advanced_python_wrapper.errors import (
    AwsWrapperError, FailoverFailedError, FailoverSuccessError,
    ReadWriteSplittingError, TransactionResolutionUnknownError)
from aws_advanced_python_wrapper.sql_alchemy_connection_provider import \
    SqlAlchemyPooledConnectionProvider
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from tests.integration.container.utils.async_connection_helpers import (
    cleanup_async, connect_async)
from tests.integration.container.utils.conditions import (
    disable_on_engines, disable_on_features, enable_on_deployments,
    enable_on_features, enable_on_num_instances)
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.proxy_helper import ProxyHelper
from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from tests.integration.container.utils.test_driver import TestDriver
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.wrapper import \
        AsyncAwsWrapperConnection


# ---------------------------------------------------------------------------
# Module-level async helpers (inlined from sync rds_test_utility to avoid
# calling sync cursor methods on an AsyncAwsWrapperConnection).
# ---------------------------------------------------------------------------

async def _query_instance_id_async(
        conn: AsyncAwsWrapperConnection,
        rds_utils: RdsTestUtility) -> str:
    """Async counterpart of ``rds_utils.query_instance_id(conn)``.

    ``rds_test_utility.query_instance_id`` calls ``conn.cursor()`` and uses
    sync cursor methods, which do not work with ``AsyncAwsWrapperConnection``
    whose ``cursor.execute`` / ``fetchone`` are coroutines.  We replicate
    the Aurora + Multi-AZ paths directly here.
    """
    deployment = TestEnvironment.get_current().get_deployment()
    engine = TestEnvironment.get_current().get_engine()

    if deployment == DatabaseEngineDeployment.AURORA:
        sql = rds_utils.get_instance_id_query(engine)
        async with conn.cursor() as cur:
            await cur.execute(sql)
            record = await cur.fetchone()
            return record[0]

    elif deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER:
        if engine == DatabaseEngine.MYSQL:
            endpoint_sql = "SELECT endpoint FROM mysql.rds_topology WHERE id=(SELECT @@server_id)"
        else:
            endpoint_sql = (
                "SELECT endpoint FROM rds_tools.show_topology() "
                "WHERE id=(SELECT dbi_resource_id FROM rds_tools.dbi_resource_id())"
            )
        async with conn.cursor() as cur:
            await cur.execute(endpoint_sql)
            row = await cur.fetchone()
            endpoint: str = row[0]
            return endpoint[:endpoint.find(".")]

    else:
        raise RuntimeError(
            f"_query_instance_id_async: unsupported deployment {deployment}"
        )


async def _assert_first_query_throws_async(
        conn: AsyncAwsWrapperConnection,
        rds_utils: RdsTestUtility,
        exception_cls: Type[BaseException]) -> None:
    """Async counterpart of ``rds_utils.assert_first_query_throws``."""
    with pytest.raises(exception_cls):
        await _query_instance_id_async(conn, rds_utils)


async def _create_user_async(
        conn: AsyncAwsWrapperConnection,
        username: str,
        password: str) -> None:
    """Async counterpart of ``rds_utils.create_user``."""
    engine = TestEnvironment.get_current().get_engine()
    if engine == DatabaseEngine.PG:
        sql = f"CREATE USER {username} WITH PASSWORD '{password}'"
    elif engine == DatabaseEngine.MYSQL:
        sql = f"CREATE USER {username} IDENTIFIED BY '{password}'"
    else:
        raise RuntimeError(f"_create_user_async: unsupported engine {engine}")
    async with conn.cursor() as cur:
        await cur.execute(sql)


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

@enable_on_num_instances(min_instances=2)
@enable_on_deployments([DatabaseEngineDeployment.AURORA,
                        DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER,
                        DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestReadWriteSplittingAsync:

    logger = Logger(__name__)

    @pytest.fixture(autouse=True)
    def setup_method(self, request):
        self.logger.info(f"Starting test: {request.node.name}")
        yield
        self.logger.info(f"Ending test: {request.node.name}")

        release_resources()
        gc.collect()

    # Plugin configurations
    @pytest.fixture(
        params=[("read_write_splitting", "read_write_splitting"), ("srw", "srw")]
    )
    def plugin_config(self, request):
        return request.param

    @pytest.fixture(scope="class")
    def rds_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    @pytest.fixture(autouse=True)
    def clear_caches(self):
        services_container.get_storage_service().clear_all()
        yield
        AsyncConnectionProviderManager.release_resources()
        AsyncConnectionProviderManager.reset_provider()
        gc.collect()
        ProxyHelper.enable_all_connectivity()

    @pytest.fixture
    def props(self, plugin_config, conn_utils):
        plugin_name, plugin_value = plugin_config
        p: Properties = Properties(
            {
                "plugins": plugin_value,
                "socket_timeout": 10,
                "connect_timeout": 10,
                "autocommit": True,
            }
        )

        # Add simple plugin specific configuration
        if plugin_name == "srw":
            WrapperProperties.SRW_WRITE_ENDPOINT.set(p, conn_utils.writer_cluster_host)
            WrapperProperties.SRW_READ_ENDPOINT.set(p, conn_utils.reader_cluster_host)
            WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS.set(p, "30000")
            WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS.set(p, "1000")

        if (
            TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED
            in TestEnvironment.get_current().get_features()
            or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED
            in TestEnvironment.get_current().get_features()
        ):
            WrapperProperties.ENABLE_TELEMETRY.set(p, "True")
            WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.set(p, "True")

        if (
            TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED
            in TestEnvironment.get_current().get_features()
        ):
            WrapperProperties.TELEMETRY_TRACES_BACKEND.set(p, "XRAY")

        if (
            TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED
            in TestEnvironment.get_current().get_features()
        ):
            WrapperProperties.TELEMETRY_METRICS_BACKEND.set(p, "OTLP")

        return p

    @pytest.fixture
    def failover_props(self, plugin_config, conn_utils):
        plugin_name, plugin_value = plugin_config
        props = {
            "plugins": f"{plugin_value},failover",
            "socket_timeout": 10,
            "connect_timeout": 10,
            "autocommit": True,
            "cluster_id": "cluster1"
        }
        # Add simple plugin specific configuration
        if plugin_name == "srw":
            WrapperProperties.SRW_WRITE_ENDPOINT.set(
                props, conn_utils.writer_cluster_host
            )
            WrapperProperties.SRW_READ_ENDPOINT.set(
                props, conn_utils.reader_cluster_host
            )

        return props

    @pytest.fixture
    def proxied_props(self, props, plugin_config, conn_utils):
        plugin_name, _ = plugin_config
        props_copy = props.copy()

        # Add simple plugin specific configuration
        if plugin_name == "srw":
            WrapperProperties.SRW_WRITE_ENDPOINT.set(
                props_copy,
                f"{conn_utils.proxy_writer_cluster_host}:{conn_utils.proxy_port}",
            )
            WrapperProperties.SRW_READ_ENDPOINT.set(
                props_copy,
                f"{conn_utils.proxy_reader_cluster_host}:{conn_utils.proxy_port}",
            )

        endpoint_suffix = (
            TestEnvironment.get_current()
            .get_proxy_database_info()
            .get_instance_endpoint_suffix()
        )
        WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.set(
            props_copy, f"?.{endpoint_suffix}:{conn_utils.proxy_port}"
        )
        return props_copy

    @pytest.fixture
    def proxied_failover_props(self, failover_props, plugin_config, conn_utils):
        plugin_name, _ = plugin_config
        props_copy = failover_props.copy()

        # Add simple plugin specific configuration
        if plugin_name == "srw":
            WrapperProperties.SRW_WRITE_ENDPOINT.set(
                props_copy,
                f"{conn_utils.proxy_writer_cluster_host}:{conn_utils.proxy_port}",
            )
            WrapperProperties.SRW_READ_ENDPOINT.set(
                props_copy,
                f"{conn_utils.proxy_reader_cluster_host}:{conn_utils.proxy_port}",
            )

        endpoint_suffix = (
            TestEnvironment.get_current()
            .get_proxy_database_info()
            .get_instance_endpoint_suffix()
        )
        WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.set(
            props_copy, f"?.{endpoint_suffix}:{conn_utils.proxy_port}"
        )
        return props_copy

    def test_connect_to_writer__switch_read_only_async(
        self, test_driver: TestDriver, props, conn_utils, rds_utils
    ):
        async def inner():
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(),
                **dict(props),
            )
            try:
                writer_id = await _query_instance_id_async(conn, rds_utils)

                await conn.set_read_only(True)
                reader_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id != reader_id

                await conn.set_read_only(True)
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert reader_id == current_id

                await conn.set_read_only(False)
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id == current_id

                await conn.set_read_only(False)
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id == current_id

                await conn.set_read_only(True)
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert reader_id == current_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    def test_connect_to_reader__switch_read_only_async(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        props,
        conn_utils,
        rds_utils,
        plugin_config,
    ):
        plugin_name, _ = plugin_config
        if plugin_name != "read_write_splitting":
            pytest.skip(
                "Test only applies to read_write_splitting plugin: srw does not connect to instances"
            )

        async def inner():
            reader_instance = test_environment.get_instances()[1]
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(reader_instance.get_host()),
                **dict(props),
            )
            try:
                reader_id = await _query_instance_id_async(conn, rds_utils)

                await conn.set_read_only(True)
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert reader_id == current_id

                await conn.set_read_only(False)
                writer_id = await _query_instance_id_async(conn, rds_utils)
                assert reader_id != writer_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    def test_connect_to_reader_cluster__switch_read_only_async(
        self, test_driver: TestDriver, props, conn_utils, rds_utils
    ):
        async def inner():
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(conn_utils.reader_cluster_host),
                **dict(props),
            )
            try:
                reader_id = await _query_instance_id_async(conn, rds_utils)

                await conn.set_read_only(True)
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert reader_id == current_id

                await conn.set_read_only(False)
                writer_id = await _query_instance_id_async(conn, rds_utils)
                assert reader_id != writer_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    def test_set_read_only_false__read_only_transaction_async(
        self, test_driver: TestDriver, props, conn_utils, rds_utils
    ):
        async def inner():
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(),
                **dict(props),
            )
            try:
                writer_id = await _query_instance_id_async(conn, rds_utils)

                await conn.set_read_only(True)
                reader_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id != reader_id

                async with conn.cursor() as cursor:
                    await cursor.execute("START TRANSACTION READ ONLY")
                    await cursor.execute("SELECT 1")
                    await cursor.fetchone()

                    with pytest.raises(ReadWriteSplittingError):
                        await conn.set_read_only(False)

                    current_id = await _query_instance_id_async(conn, rds_utils)
                    assert reader_id == current_id

                    await cursor.execute("COMMIT")

                    await conn.set_read_only(False)
                    current_id = await _query_instance_id_async(conn, rds_utils)
                    assert writer_id == current_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    def test_set_read_only_false_in_transaction_async(
        self, test_driver: TestDriver, props, conn_utils, rds_utils
    ):
        async def inner():
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(),
                **dict(props),
            )
            try:
                writer_id = await _query_instance_id_async(conn, rds_utils)

                await conn.set_read_only(True)
                reader_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id != reader_id

                async with conn.cursor() as cursor:
                    await conn.set_autocommit(False)
                    await cursor.execute("START TRANSACTION")

                    with pytest.raises(ReadWriteSplittingError):
                        await conn.set_read_only(False)

                    current_id = await _query_instance_id_async(conn, rds_utils)
                    assert reader_id == current_id

                    await cursor.execute("COMMIT")
                    await conn.set_read_only(False)
                    current_id = await _query_instance_id_async(conn, rds_utils)
                    assert writer_id == current_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    def test_set_read_only_true_in_transaction_async(
        self, test_driver: TestDriver, props, conn_utils, rds_utils
    ):
        async def inner():
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(),
                **dict(props),
            )
            try:
                writer_id = await _query_instance_id_async(conn, rds_utils)

                cursor = conn.cursor()
                await conn.set_autocommit(False)
                await cursor.execute("START TRANSACTION")

                # MySQL allows users to change the read_only value during a transaction, Psycopg does not
                if test_driver == TestDriver.MYSQL_ASYNC:
                    await conn.set_read_only(True)
                elif test_driver == TestDriver.PG_ASYNC:
                    with pytest.raises(Exception):
                        await conn.set_read_only(True)
                    assert conn.read_only is False

                current_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id == current_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    @enable_on_num_instances(min_instances=3)
    def test_set_read_only_true__all_readers_down_async(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        proxied_props,
        conn_utils,
        rds_utils,
    ):
        async def inner():
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_proxy_connect_params(),
                **dict(proxied_props),
            )
            try:
                writer_id = await _query_instance_id_async(conn, rds_utils)

                # Disable all reader instance ids and reader cluster endpoint.
                instance_ids = [
                    instance.get_instance_id()
                    for instance in test_environment.get_instances()
                ]
                for i in range(1, len(instance_ids)):
                    ProxyHelper.disable_connectivity(instance_ids[i])
                ProxyHelper.disable_connectivity(
                    test_environment.get_proxy_database_info().get_cluster_read_only_endpoint()
                )

                await conn.set_read_only(True)
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id == current_id

                await conn.set_read_only(False)
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id == current_id

                ProxyHelper.enable_all_connectivity()
                await conn.set_read_only(True)
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id != current_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    def test_set_read_only_true__closed_connection_async(
        self, test_driver: TestDriver, props, conn_utils, rds_utils
    ):
        async def inner():
            try:
                conn = await connect_async(
                    test_driver=test_driver,
                    connect_params=conn_utils.get_connect_params(),
                    **dict(props),
                )
                await conn.close()

                with pytest.raises(AwsWrapperError):
                    await conn.set_read_only(True)
            finally:
                await cleanup_async()

        asyncio.run(inner())

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    @pytest.mark.skip
    def test_set_read_only_false__all_instances_down_async(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        proxied_props,
        conn_utils,
        rds_utils,
    ):
        async def inner():
            reader = test_environment.get_proxy_instances()[1]
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_proxy_connect_params(reader.get_host()),
                **dict(proxied_props),
            )
            try:
                ProxyHelper.disable_all_connectivity()
                with pytest.raises(AwsWrapperError):
                    await conn.set_read_only(False)
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    def test_execute__old_connection_async(
        self,
        test_driver: TestDriver,
        props: Properties,
        conn_utils,
        rds_utils,
        plugin_config,
    ):
        async def inner():
            WrapperProperties.SRW_VERIFY_NEW_CONNECTIONS.set(props, "False")
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(conn_utils.writer_cluster_host),
                **dict(props),
            )
            try:
                writer_id = await _query_instance_id_async(conn, rds_utils)

                old_cursor = conn.cursor()
                await old_cursor.execute("SELECT 1")
                await old_cursor.fetchone()
                await conn.set_read_only(True)  # Switch connection internally
                await conn.set_autocommit(False)

                with pytest.raises(AwsWrapperError):
                    await old_cursor.execute("SELECT 1")

                reader_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id != reader_id

                await old_cursor.close()
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert reader_id == current_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                         TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    @enable_on_num_instances(min_instances=3)
    def test_failover_to_new_writer__switch_read_only_async(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        proxied_failover_props,
        conn_utils,
        rds_utils,
    ):
        async def inner():
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_proxy_connect_params(),
                **dict(proxied_failover_props),
            )
            try:
                original_writer_id = await _query_instance_id_async(conn, rds_utils)

                # Disable all reader instance ids and reader cluster endpoint.
                instance_ids = [
                    instance.get_instance_id()
                    for instance in test_environment.get_instances()
                ]
                for i in range(1, len(instance_ids)):
                    ProxyHelper.disable_connectivity(instance_ids[i])
                ProxyHelper.disable_connectivity(
                    test_environment.get_proxy_database_info().get_cluster_read_only_endpoint()
                )

                # Force internal reader connection to the writer instance
                await conn.set_read_only(True)
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert original_writer_id == current_id
                await conn.set_read_only(False)

                ProxyHelper.enable_all_connectivity()
                rds_utils.failover_cluster_and_wait_until_writer_changed(original_writer_id)
                await _assert_first_query_throws_async(conn, rds_utils, FailoverSuccessError)

                new_writer_id = await _query_instance_id_async(conn, rds_utils)
                assert original_writer_id != new_writer_id
                assert rds_utils.is_db_instance_writer(new_writer_id)

                await conn.set_read_only(True)
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert new_writer_id != current_id

                await conn.set_read_only(False)
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert new_writer_id == current_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @pytest.mark.parametrize("plugins", ["read_write_splitting,failover,host_monitoring", "read_write_splitting,failover,host_monitoring_v2"])
    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                         TestEnvironmentFeatures.ABORT_CONNECTION_SUPPORTED])
    @enable_on_num_instances(min_instances=3)
    @disable_on_engines([DatabaseEngine.MYSQL])
    def test_failover_to_new_reader__switch_read_only_async(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        proxied_failover_props,
        conn_utils,
        rds_utils,
        plugin_config,
        plugins,
    ):
        plugin_name, _ = plugin_config
        if plugin_name != "read_write_splitting":
            # Disabling the reader connection in srw, the srwReadEndpoint, results in defaulting to the writer not connecting to another reader.
            pytest.skip(
                "Test only applies to read_write_splitting plugin: reader connection failover"
            )

        WrapperProperties.FAILOVER_MODE.set(proxied_failover_props, "reader-or-writer")
        WrapperProperties.PLUGINS.set(proxied_failover_props, plugins)

        async def inner():
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_proxy_connect_params(),
                **dict(proxied_failover_props),
            )
            try:
                writer_id = await _query_instance_id_async(conn, rds_utils)

                await conn.set_read_only(True)
                reader_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id != reader_id

                instances = test_environment.get_instances()
                other_reader_id = next(
                    (
                        instance.get_instance_id()
                        for instance in instances[1:]
                        if instance.get_instance_id() != reader_id
                    ),
                    None,
                )
                if other_reader_id is None:
                    pytest.fail("Could not acquire alternate reader ID")

                # Kill all instances except for one other reader
                for instance in instances:
                    instance_id = instance.get_instance_id()
                    if instance_id != other_reader_id:
                        ProxyHelper.disable_connectivity(instance_id)

                await _assert_first_query_throws_async(conn, rds_utils, FailoverSuccessError)
                assert not conn.is_closed
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert other_reader_id == current_id
                assert reader_id != current_id

                ProxyHelper.enable_all_connectivity()
                await conn.set_read_only(False)
                assert not conn.is_closed
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id == current_id

                await conn.set_read_only(True)
                assert not conn.is_closed
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert other_reader_id == current_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @pytest.mark.parametrize("plugins", ["failover,host_monitoring", "failover,host_monitoring_v2"])
    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                         TestEnvironmentFeatures.ABORT_CONNECTION_SUPPORTED])
    @enable_on_num_instances(min_instances=3)
    @disable_on_engines([DatabaseEngine.MYSQL])
    def test_failover_reader_to_writer__switch_read_only_async(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        proxied_failover_props,
        conn_utils,
        rds_utils,
        plugin_config,
        plugins,
    ):
        plugin_name, _ = plugin_config
        WrapperProperties.PLUGINS.set(proxied_failover_props, plugin_name + "," + plugins)

        async def inner():
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_proxy_connect_params(),
                **dict(proxied_failover_props),
            )
            try:
                writer_id = await _query_instance_id_async(conn, rds_utils)

                await conn.set_read_only(True)
                reader_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id != reader_id

                # Kill all instances except the writer
                for instance in test_environment.get_instances():
                    instance_id = instance.get_instance_id()
                    if instance_id != writer_id:
                        ProxyHelper.disable_connectivity(instance_id)
                ProxyHelper.disable_connectivity(
                    test_environment.get_proxy_database_info().get_cluster_read_only_endpoint()
                )

                await _assert_first_query_throws_async(conn, rds_utils, FailoverSuccessError)
                assert not conn.is_closed
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id == current_id

                ProxyHelper.enable_all_connectivity()
                await conn.set_read_only(True)
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id != current_id

                await conn.set_read_only(False)
                current_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_id == current_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    def test_incorrect_reader_endpoint_async(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        conn_utils,
        rds_utils,
        plugin_config,
    ):
        plugin_name, plugin_value = plugin_config
        if plugin_name != "srw":
            pytest.skip(
                "Test only applies to simple_read_write_splitting plugin: uses srwReadEndpoint property"
            )

        async def inner():
            props = Properties(
                {"plugins": plugin_value, "connect_timeout": 30, "autocommit": True}
            )
            port = (
                test_environment.get_info().get_database_info().get_cluster_endpoint_port()
            )
            writer_endpoint = conn_utils.writer_cluster_host

            # Set both endpoints to writer (incorrect reader endpoint)
            WrapperProperties.SRW_WRITE_ENDPOINT.set(props, f"{writer_endpoint}:{port}")
            WrapperProperties.SRW_READ_ENDPOINT.set(props, f"{writer_endpoint}:{port}")

            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(conn_utils.writer_cluster_host),
                **dict(props),
            )
            try:
                writer_connection_id = await _query_instance_id_async(conn, rds_utils)

                # Switch to reader successfully
                await conn.set_read_only(True)
                reader_connection_id = await _query_instance_id_async(conn, rds_utils)
                # Should stay on writer as fallback since reader endpoint points to a writer
                assert writer_connection_id == reader_connection_id

                # Going to the write endpoint will be the same connection again
                await conn.set_read_only(False)
                final_connection_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_connection_id == final_connection_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    def test_autocommit_state_preserved_across_connection_switches_async(
        self, test_driver: TestDriver, props, conn_utils, rds_utils, plugin_config
    ):
        plugin_name, _ = plugin_config
        if plugin_name != "srw":
            pytest.skip(
                "Test only applies to simple_read_write_splitting plugin: autocommit impacts srw verification"
            )

        async def inner():
            WrapperProperties.SRW_VERIFY_NEW_CONNECTIONS.set(props, "False")
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(),
                **dict(props),
            )
            try:
                # Set autocommit to False on writer
                await conn.set_autocommit(False)
                assert await conn.autocommit is False
                writer_connection_id = await _query_instance_id_async(conn, rds_utils)
                await conn.commit()

                # Switch to reader - autocommit should remain False
                await conn.set_read_only(True)
                assert await conn.autocommit is False
                reader_connection_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_connection_id != reader_connection_id
                await conn.commit()

                # Change autocommit on reader
                await conn.set_autocommit(True)
                assert await conn.autocommit is True

                # Switch back to writer - autocommit should be True
                await conn.set_read_only(False)
                assert await conn.autocommit is True
                final_writer_connection_id = await _query_instance_id_async(conn, rds_utils)
                assert writer_connection_id == final_writer_connection_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    def test_pooled_connection__reuses_cached_connection_async(
        self, test_driver: TestDriver, conn_utils, props
    ):
        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        AsyncConnectionProviderManager.set_connection_provider(provider)  # type: ignore[arg-type]

        async def inner():
            try:
                conn1 = await connect_async(
                    test_driver=test_driver,
                    connect_params=conn_utils.get_connect_params(),
                    **dict(props),
                )
                assert isinstance(conn1.target_connection, PoolProxiedConnection)
                driver_conn1 = conn1.target_connection.driver_connection
                await conn1.close()

                conn2 = await connect_async(
                    test_driver=test_driver,
                    connect_params=conn_utils.get_connect_params(),
                    **dict(props),
                )
                assert isinstance(conn2.target_connection, PoolProxiedConnection)
                driver_conn2 = conn2.target_connection.driver_connection
                await conn2.close()

                assert conn1 is not conn2
                assert driver_conn1 is driver_conn2
            finally:
                await cleanup_async()

        asyncio.run(inner())

    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_pooled_connection__failover_async(
        self, test_driver: TestDriver, rds_utils, conn_utils, failover_props
    ):
        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        AsyncConnectionProviderManager.set_connection_provider(provider)  # type: ignore[arg-type]

        async def inner():
            try:
                conn = await connect_async(
                    test_driver=test_driver,
                    connect_params=conn_utils.get_connect_params(),
                    **dict(failover_props),
                )
                assert isinstance(conn.target_connection, PoolProxiedConnection)
                initial_driver_conn = conn.target_connection.driver_connection
                initial_writer_id = await _query_instance_id_async(conn, rds_utils)

                rds_utils.failover_cluster_and_wait_until_writer_changed()
                with pytest.raises(FailoverSuccessError):
                    await _query_instance_id_async(conn, rds_utils)

                new_writer_id = await _query_instance_id_async(conn, rds_utils)
                assert initial_writer_id != new_writer_id

                assert not isinstance(conn.target_connection, PoolProxiedConnection)
                new_driver_conn = conn.target_connection
                assert initial_driver_conn is not new_driver_conn
                await conn.close()

                # New connection to the original writer (now a reader)
                conn2 = await connect_async(
                    test_driver=test_driver,
                    connect_params=conn_utils.get_connect_params(),
                    **dict(failover_props),
                )
                current_id = await _query_instance_id_async(conn2, rds_utils)
                assert initial_writer_id == current_id

                assert isinstance(conn2.target_connection, PoolProxiedConnection)
                current_driver_conn = conn2.target_connection.driver_connection
                # The initial connection should have been evicted from the pool when failover occurred,
                # so this should be a new connection even though it is connected to the same instance.
                assert initial_driver_conn is not current_driver_conn
                await conn2.close()
            finally:
                await cleanup_async()

        asyncio.run(inner())

    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_pooled_connection__cluster_url_failover_async(
        self, test_driver: TestDriver, rds_utils, conn_utils, failover_props
    ):
        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        AsyncConnectionProviderManager.set_connection_provider(provider)  # type: ignore[arg-type]

        async def inner():
            try:
                conn = await connect_async(
                    test_driver=test_driver,
                    connect_params=conn_utils.get_connect_params(conn_utils.writer_cluster_host),
                    **dict(failover_props),
                )
                # The internal connection pool should not be used if the connection is established via a cluster URL.
                assert 0 == len(SqlAlchemyPooledConnectionProvider._database_pools)

                initial_writer_id = await _query_instance_id_async(conn, rds_utils)
                assert not isinstance(conn.target_connection, PoolProxiedConnection)
                initial_driver_conn = conn.target_connection

                rds_utils.failover_cluster_and_wait_until_writer_changed()
                with pytest.raises(FailoverSuccessError):
                    await _query_instance_id_async(conn, rds_utils)

                new_writer_id = await _query_instance_id_async(conn, rds_utils)
                assert initial_writer_id != new_writer_id
                assert 0 == len(SqlAlchemyPooledConnectionProvider._database_pools)

                assert not isinstance(conn.target_connection, PoolProxiedConnection)
                new_driver_conn = conn.target_connection
                assert initial_driver_conn is not new_driver_conn
                await conn.close()
            finally:
                await cleanup_async()

        asyncio.run(inner())

    @pytest.mark.parametrize("plugins", ["failover,host_monitoring", "failover,host_monitoring_v2"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                         TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                         TestEnvironmentFeatures.ABORT_CONNECTION_SUPPORTED])
    @disable_on_engines([DatabaseEngine.MYSQL])
    @pytest.mark.repeat(10)  # Run this test case a few more times since it is a flakey test
    def test_pooled_connection__failover_failed_async(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        rds_utils,
        conn_utils,
        proxied_failover_props,
        plugin_config,
        plugins,
    ):
        plugin_name, _ = plugin_config
        writer_host = test_environment.get_writer().get_host()
        provider = SqlAlchemyPooledConnectionProvider(
            lambda _, __: {"pool_size": 1},
            None,
            lambda host_info, props: writer_host in host_info.host,
        )
        AsyncConnectionProviderManager.set_connection_provider(provider)  # type: ignore[arg-type]

        WrapperProperties.FAILOVER_TIMEOUT_SEC.set(proxied_failover_props, "1")
        WrapperProperties.FAILURE_DETECTION_TIME_MS.set(proxied_failover_props, "1000")
        WrapperProperties.FAILURE_DETECTION_COUNT.set(proxied_failover_props, "1")
        WrapperProperties.PLUGINS.set(proxied_failover_props, plugin_name + "," + plugins)

        async def inner():
            try:
                conn = await connect_async(
                    test_driver=test_driver,
                    connect_params=conn_utils.get_proxy_connect_params(),
                    **dict(proxied_failover_props),
                )
                assert isinstance(conn.target_connection, PoolProxiedConnection)
                initial_driver_conn = conn.target_connection.driver_connection
                writer_id = await _query_instance_id_async(conn, rds_utils)

                ProxyHelper.disable_all_connectivity()
                with pytest.raises(FailoverFailedError):
                    await _query_instance_id_async(conn, rds_utils)

                ProxyHelper.enable_all_connectivity()
                conn2 = await connect_async(
                    test_driver=test_driver,
                    connect_params=conn_utils.get_proxy_connect_params(),
                    **dict(proxied_failover_props),
                )

                current_writer_id = await _query_instance_id_async(conn2, rds_utils)
                assert writer_id == current_writer_id

                assert isinstance(conn2.target_connection, PoolProxiedConnection)
                current_driver_conn = conn2.target_connection.driver_connection
                # The initial connection should have been evicted from the pool when failover occurred,
                # so this should be a new connection even though it is connected to the same instance.
                assert initial_driver_conn is not current_driver_conn
                await conn2.close()
            finally:
                await cleanup_async()

        asyncio.run(inner())

    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_pooled_connection__failover_in_transaction_async(
        self, test_driver: TestDriver, rds_utils, conn_utils, failover_props
    ):
        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        AsyncConnectionProviderManager.set_connection_provider(provider)  # type: ignore[arg-type]

        async def inner():
            try:
                conn = await connect_async(
                    test_driver=test_driver,
                    connect_params=conn_utils.get_connect_params(),
                    **dict(failover_props),
                )
                assert isinstance(conn.target_connection, PoolProxiedConnection)
                initial_driver_conn = conn.target_connection.driver_connection
                initial_writer_id = await _query_instance_id_async(conn, rds_utils)

                await conn.set_autocommit(False)
                cursor = conn.cursor()
                await cursor.execute("START TRANSACTION")

                rds_utils.failover_cluster_and_wait_until_writer_changed()
                with pytest.raises(TransactionResolutionUnknownError):
                    await _query_instance_id_async(conn, rds_utils)

                new_writer_id = await _query_instance_id_async(conn, rds_utils)
                assert initial_writer_id != new_writer_id

                assert not isinstance(conn.target_connection, PoolProxiedConnection)
                new_driver_conn = conn.target_connection
                assert initial_driver_conn is not new_driver_conn
                await conn.close()

                rds_utils.wait_until_cluster_has_desired_status(
                    TestEnvironment.get_current().get_info().get_db_name(), "available"
                )

                conn2 = await connect_async(
                    test_driver=test_driver,
                    connect_params=conn_utils.get_connect_params(),
                    **dict(failover_props),
                )
                current_id = await _query_instance_id_async(conn2, rds_utils)
                assert initial_writer_id == current_id

                assert isinstance(conn2.target_connection, PoolProxiedConnection)
                current_driver_conn = conn2.target_connection.driver_connection
                # The initial connection should have been evicted from the pool when failover occurred,
                # so this should be a new connection even though it is connected to the same instance.
                assert initial_driver_conn is not current_driver_conn
                await conn2.close()
            finally:
                await cleanup_async()

        asyncio.run(inner())

    def test_pooled_connection__different_users_async(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        rds_utils,
        conn_utils,
        props,
    ):
        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        AsyncConnectionProviderManager.set_connection_provider(provider)  # type: ignore[arg-type]

        async def inner():
            privileged_user_props = conn_utils.get_connect_params().copy()
            limited_user_props = conn_utils.get_connect_params().copy()
            limited_user_name = "limited_user"
            limited_user_new_db = "limited_user_db"
            limited_user_password = "limited_user_password"
            WrapperProperties.USER.set(limited_user_props, limited_user_name)
            WrapperProperties.PASSWORD.set(limited_user_props, limited_user_password)

            wrong_user_right_password_props = conn_utils.get_connect_params().copy()
            WrapperProperties.USER.set(wrong_user_right_password_props, "wrong_user")

            try:
                conn = await connect_async(
                    test_driver=test_driver,
                    connect_params=privileged_user_props,
                    **dict(props),
                )
                assert isinstance(conn.target_connection, PoolProxiedConnection)
                privileged_driver_conn = conn.target_connection.driver_connection

                async with conn.cursor() as cursor:
                    await cursor.execute(f"DROP USER IF EXISTS {limited_user_name}")
                    await _create_user_async(conn, limited_user_name, limited_user_password)
                    engine = test_environment.get_engine()
                    if engine == DatabaseEngine.MYSQL:
                        db = test_environment.get_database_info().get_default_db_name()
                        # MySQL needs this extra command to allow the limited user to connect to the default database
                        await cursor.execute(
                            f"GRANT ALL PRIVILEGES ON {db}.* TO {limited_user_name}"
                        )

                    # Validate that the privileged connection established above is not reused and that the new connection is
                    # correctly established under the limited user
                    conn2 = await connect_async(
                        test_driver=test_driver,
                        connect_params=limited_user_props,
                        **dict(props),
                    )
                    assert isinstance(conn2.target_connection, PoolProxiedConnection)
                    limited_driver_conn = conn2.target_connection.driver_connection
                    assert privileged_driver_conn is not limited_driver_conn

                    async with conn2.cursor() as cursor2:
                        with pytest.raises(Exception):
                            # The limited user does not have create permissions on the default database, so this should fail
                            await cursor2.execute(
                                f"CREATE DATABASE {limited_user_new_db}"
                            )

                        with pytest.raises(Exception):
                            await connect_async(
                                test_driver=test_driver,
                                connect_params=wrong_user_right_password_props,
                                **dict(props),
                            )
                    await conn2.close()
                await conn.close()
            finally:
                cleanup_conn = await connect_async(
                    test_driver=test_driver,
                    connect_params=privileged_user_props,
                    **dict(props),
                )
                async with cleanup_conn.cursor() as cleanup_cursor:
                    await cleanup_cursor.execute(f"DROP DATABASE IF EXISTS {limited_user_new_db}")
                    await cleanup_cursor.execute(f"DROP USER IF EXISTS {limited_user_name}")
                await cleanup_conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @enable_on_num_instances(min_instances=5)
    def test_pooled_connection__least_connections_async(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        rds_utils,
        conn_utils,
        props,
        plugin_config,
    ):
        plugin_name, _ = plugin_config
        if plugin_name != "read_write_splitting":
            pytest.skip(
                "Test only applies to read_write_splitting plugin: reader host selector strategy"
            )

        WrapperProperties.READER_HOST_SELECTOR_STRATEGY.set(props, "least_connections")

        instances = test_environment.get_instances()
        provider = SqlAlchemyPooledConnectionProvider(
            lambda _, __: {"pool_size": len(instances)}
        )
        AsyncConnectionProviderManager.set_connection_provider(provider)  # type: ignore[arg-type]

        async def inner():
            connections = []
            connected_reader_ids = []
            try:
                # Assume one writer and [size - 1] readers. Create an internal connection pool for each reader.
                for _ in range(len(instances) - 1):
                    conn = await connect_async(
                        test_driver=test_driver,
                        connect_params=conn_utils.get_connect_params(),
                        **dict(props),
                    )
                    connections.append(conn)

                    await conn.set_read_only(True)
                    reader_id = await _query_instance_id_async(conn, rds_utils)
                    assert reader_id not in connected_reader_ids
                    connected_reader_ids.append(reader_id)
            finally:
                for conn in connections:
                    await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    """Tests custom pool mapping together with internal connection pools and the leastConnections
    host selection strategy. This test overloads one reader with connections and then verifies
    that new connections are sent to the other readers until their connection count equals that of
    the overloaded reader.
    """

    @enable_on_num_instances(min_instances=5)
    def test_pooled_connection__least_connections__pool_mapping_async(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        rds_utils,
        conn_utils,
        props,
        plugin_config,
    ):
        plugin_name, _ = plugin_config
        if plugin_name != "read_write_splitting":
            pytest.skip(
                "Test only applies to read_write_splitting plugin: reader host selector strategy"
            )

        WrapperProperties.READER_HOST_SELECTOR_STRATEGY.set(props, "least_connections")

        # We will be testing all instances excluding the writer and overloaded reader. Each instance
        # should be tested overloaded_reader_connection_count times to increase the pool connection count
        # until it equals the connection count of the overloaded reader.
        instances = test_environment.get_instances()
        overloaded_reader_connection_count = 3
        num_test_connections = (len(instances) - 2) * overloaded_reader_connection_count
        provider = SqlAlchemyPooledConnectionProvider(
            lambda _, __: {"pool_size": num_test_connections},
            # Create a new pool for each instance-arbitrary_prop combination
            lambda host_info, conn_props: f"{host_info.url}-{len(SqlAlchemyPooledConnectionProvider._database_pools)}",
        )
        AsyncConnectionProviderManager.set_connection_provider(provider)  # type: ignore[arg-type]

        async def inner():
            connections = []
            try:
                reader_to_overload = instances[1]
                for _ in range(overloaded_reader_connection_count):
                    # This should result in overloaded_reader_connection_count pools to the same reader instance,
                    # with each pool consisting of just one connection. The total connection count for the
                    # instance should be overloaded_reader_connection_count despite being spread across multiple
                    # pools.
                    conn = await connect_async(
                        test_driver=test_driver,
                        connect_params=conn_utils.get_connect_params(reader_to_overload.get_host()),
                        **dict(props),
                    )
                    connections.append(conn)
                assert overloaded_reader_connection_count == len(
                    SqlAlchemyPooledConnectionProvider._database_pools
                )

                for _ in range(num_test_connections):
                    conn = await connect_async(
                        test_driver=test_driver,
                        connect_params=conn_utils.get_connect_params(),
                        **dict(props),
                    )
                    connections.append(conn)

                    await conn.set_read_only(True)
                    current_id = await _query_instance_id_async(conn, rds_utils)
                    assert reader_to_overload.get_instance_id() != current_id
            finally:
                for conn in connections:
                    await conn.close()
                await cleanup_async()

        asyncio.run(inner())
