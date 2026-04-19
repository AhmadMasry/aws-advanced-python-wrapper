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

"""SQLAlchemy creator-pattern integration tests for Aurora PG and Aurora MySQL.

Proves:
- SA `create_engine(..., creator=lambda: aws_advanced_python_wrapper.<driver>.connect(...))`
  succeeds against real Aurora clusters for both drivers.
- Aurora failover surfaces as sqlalchemy.exc.OperationalError (via the
  OperationalError-classified FailoverSuccessError) and a retry recovers.
- Read/Write Splitting's read_only flip routes to a reader and returns to a writer.

Fixtures follow the existing test_read_write_splitting.py / test_aurora_failover.py
patterns (see conftest.py in this directory).
"""

from __future__ import annotations

import gc

import pytest  # type: ignore
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.mysql_connector import \
    connect as mysql_connect
from aws_advanced_python_wrapper.psycopg import connect as pg_connect
from aws_advanced_python_wrapper.utils.log import Logger
from .utils.conditions import (disable_on_features, enable_on_deployments,
                               enable_on_num_instances)
from .utils.database_engine import DatabaseEngine
from .utils.database_engine_deployment import DatabaseEngineDeployment
from .utils.rds_test_utility import RdsTestUtility
from .utils.test_driver import TestDriver
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures

logger = Logger(__name__)


def _is_mysql(test_driver: TestDriver) -> bool:
    return test_driver == TestDriver.MYSQL


def _sa_url(test_driver: TestDriver) -> str:
    return "mysql+mysqlconnector://" if _is_mysql(test_driver) else "postgresql+psycopg://"


def _wrapper_dialect(test_driver: TestDriver) -> str:
    return "aurora-mysql" if _is_mysql(test_driver) else "aurora-pg"


def _instance_id_sql(test_driver: TestDriver) -> str:
    if _is_mysql(test_driver):
        return "SELECT @@aurora_server_id"
    return "SELECT pg_catalog.aurora_db_instance_identifier()"


def _readonly_option(test_driver: TestDriver) -> dict:
    return {"mysql_readonly": True} if _is_mysql(test_driver) else {"postgresql_readonly": True}


def _build_engine(test_driver: TestDriver, conninfo_kwargs: dict, plugins: str):
    if _is_mysql(test_driver):
        creator = lambda: mysql_connect(  # noqa: E731
            "", wrapper_dialect=_wrapper_dialect(test_driver),
            plugins=plugins, use_pure=True, **conninfo_kwargs,
        )
    else:
        creator = lambda: pg_connect(  # noqa: E731
            "", wrapper_dialect=_wrapper_dialect(test_driver),
            plugins=plugins, **conninfo_kwargs,
        )
    return create_engine(_sa_url(test_driver), creator=creator)


@enable_on_num_instances(min_instances=2)
@enable_on_deployments([DatabaseEngineDeployment.AURORA])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestSqlAlchemy:

    @pytest.fixture(autouse=True)
    def setup_method(self, request):
        logger.info(f"Starting test: {request.node.name}")
        yield
        release_resources()
        logger.info(f"Ending test: {request.node.name}")
        release_resources()
        gc.collect()

    @pytest.fixture(scope="class")
    def aurora_utility(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    def test_sqlalchemy_creator_survives_aurora_failover(
            self, test_driver: TestDriver, conn_utils, aurora_utility):
        """SA engine recovers from Aurora failover via the OperationalError retry path."""
        # Failover test is PG+MySQL symmetric; skip on unsupported engines.
        engine_kind = TestEnvironment.get_current().get_engine()
        if engine_kind not in (DatabaseEngine.PG, DatabaseEngine.MYSQL):
            pytest.skip(f"Unsupported engine: {engine_kind}")

        initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

        engine = _build_engine(
            test_driver, conn_utils.get_connect_params(), plugins="failover,efm",
        )
        try:
            with engine.connect() as conn:
                pre_failover_id = conn.execute(text(_instance_id_sql(test_driver))).scalar_one()
                assert pre_failover_id == initial_writer_id

            # Trigger failover against the cluster.
            aurora_utility.failover_cluster_and_wait_until_writer_changed()

            # First query after failover should raise sqlalchemy.exc.OperationalError
            # because FailoverSuccessError is reclassified as OperationalError.
            recovered = False
            attempts_remaining = 10
            while attempts_remaining > 0 and not recovered:
                try:
                    with engine.connect() as conn:
                        new_writer_id = conn.execute(
                            text(_instance_id_sql(test_driver))
                        ).scalar_one()
                        assert new_writer_id != initial_writer_id
                        assert aurora_utility.is_db_instance_writer(new_writer_id) is True
                        recovered = True
                except OperationalError:
                    attempts_remaining -= 1

            assert recovered, "SA did not recover from failover within 10 attempts"
        finally:
            engine.dispose()
            release_resources()

    def test_sqlalchemy_creator_read_write_splitting(
            self, test_driver: TestDriver, conn_utils, aurora_utility):
        """R/W splitting routes read_only connections to a reader and returns to writer."""
        engine_kind = TestEnvironment.get_current().get_engine()
        if engine_kind not in (DatabaseEngine.PG, DatabaseEngine.MYSQL):
            pytest.skip(f"Unsupported engine: {engine_kind}")

        engine = _build_engine(
            test_driver, conn_utils.get_connect_params(), plugins="read_write_splitting",
        )
        try:
            with engine.connect() as conn:
                writer_id = conn.execute(text(_instance_id_sql(test_driver))).scalar_one()
                conn.commit()

            with engine.connect().execution_options(**_readonly_option(test_driver)) as conn:
                reader_id = conn.execute(text(_instance_id_sql(test_driver))).scalar_one()
                conn.commit()

            assert reader_id != writer_id, (
                f"read-only connection should route to a reader, got {reader_id}"
            )

            # Next non-read-only connection should return to the writer.
            with engine.connect() as conn:
                back_to_writer = conn.execute(text(_instance_id_sql(test_driver))).scalar_one()
                conn.commit()
            assert back_to_writer == writer_id
        finally:
            engine.dispose()
            release_resources()
