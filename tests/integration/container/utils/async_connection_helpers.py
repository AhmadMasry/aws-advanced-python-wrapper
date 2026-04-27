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

"""Async integration-test helpers.

Thin, async-aware counterparts to the sync connect/engine helpers used by
integration tests. Mirrors the sync shape so async test files look like
sync files with ``await`` sprinkled in -- no new abstractions.

Teardown convention: async tests must ``await release_resources_async()``
to tear down wrapper background tasks cleanly (parallel to the sync
``release_resources()`` convention called out in ``CLAUDE.local.md``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional, Type

import aiomysql
import psycopg
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from aws_advanced_python_wrapper.aio.cleanup import release_resources_async
from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection
from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.utils.messages import Messages
from .database_engine import DatabaseEngine
from .database_engine_deployment import DatabaseEngineDeployment
from .test_driver import TestDriver
from .test_environment import TestEnvironment

if TYPE_CHECKING:
    from .rds_test_utility import RdsTestUtility


async def connect_async(
        test_driver: TestDriver,
        connect_params: Dict[str, Any],
        **wrapper_kwargs: Any) -> AsyncAwsWrapperConnection:
    """Open an AsyncAwsWrapperConnection for the given async TestDriver.

    :param test_driver: must be PG_ASYNC or MYSQL_ASYNC.
    :param connect_params: driver-level connect kwargs (host/port/user/...).
    :param wrapper_kwargs: wrapper-level kwargs (e.g., plugins, wrapper_dialect).
    """
    if test_driver == TestDriver.PG_ASYNC:
        target = psycopg.AsyncConnection.connect
    elif test_driver == TestDriver.MYSQL_ASYNC:
        target = aiomysql.connect
    else:
        raise UnsupportedOperationError(
            Messages.get_formatted(
                "Testing.FunctionNotImplementedForDriver", "connect_async", test_driver.value))

    return await AsyncAwsWrapperConnection.connect(
        target=target,
        **connect_params,
        **wrapper_kwargs,
    )


def create_async_engine_for_driver(
        test_driver: TestDriver,
        user: str,
        password: str,
        host: str,
        port: int,
        dbname: str,
        wrapper_dialect: Optional[str] = None,
        wrapper_plugins: Optional[str] = None,
        **engine_kwargs: Any) -> AsyncEngine:
    """Build an AsyncEngine using the wrapper's registered async dialect.

    Dialect URL schemes (registered in pyproject.toml):
      * aws_wrapper_postgresql+psycopg_async  (PG)
      * aws_wrapper_mysql+aiomysql_async      (MySQL)
    """
    if test_driver == TestDriver.PG_ASYNC:
        driver_scheme = "aws_wrapper_postgresql+psycopg_async"
    elif test_driver == TestDriver.MYSQL_ASYNC:
        driver_scheme = "aws_wrapper_mysql+aiomysql_async"
    else:
        raise UnsupportedOperationError(
            Messages.get_formatted(
                "Testing.FunctionNotImplementedForDriver",
                "create_async_engine_for_driver",
                test_driver.value))

    query_parts = []
    if wrapper_dialect is not None:
        query_parts.append(f"wrapper_dialect={wrapper_dialect}")
    if wrapper_plugins is not None:
        query_parts.append(f"wrapper_plugins={wrapper_plugins}")
    query = ("?" + "&".join(query_parts)) if query_parts else ""

    url = f"{driver_scheme}://{user}:{password}@{host}:{port}/{dbname}{query}"
    return create_async_engine(url, **engine_kwargs)


async def cleanup_async() -> None:
    """Teardown to await at the end of every async test fixture.

    Mirrors the sync ``release_resources()`` convention.
    """
    await release_resources_async()


async def query_instance_id_async(
        conn: AsyncAwsWrapperConnection,
        rds_utils: RdsTestUtility) -> str:
    """Return the driver-reported instance ID via an async cursor.

    Deployment-aware: handles both AURORA and RDS_MULTI_AZ_CLUSTER
    deployments. Mirrors the sync ``rds_utils.query_instance_id(conn)``
    but uses an async cursor so it can be called with an
    ``AsyncAwsWrapperConnection`` whose ``cursor.execute`` / ``fetchone``
    are coroutines.
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
            f"query_instance_id_async: unsupported deployment {deployment}"
        )


async def assert_first_query_throws_async(
        conn: AsyncAwsWrapperConnection,
        rds_utils: RdsTestUtility,
        exception_cls: Type[BaseException]) -> None:
    """Execute the instance-id query against an async connection and assert
    the given exception class is raised.

    Mirrors the sync ``rds_utils.assert_first_query_throws`` but uses an
    async cursor so it can be called with an ``AsyncAwsWrapperConnection``.
    """
    with pytest.raises(exception_cls):
        await query_instance_id_async(conn, rds_utils)
