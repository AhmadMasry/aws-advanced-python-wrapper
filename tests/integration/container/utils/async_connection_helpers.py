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

from typing import Any, Dict, Optional

import aiomysql
import psycopg
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from aws_advanced_python_wrapper.aio.cleanup import release_resources_async
from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection
from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.utils.messages import Messages
from .test_driver import TestDriver


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
