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

from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.mysql.mysqlconnector import \
    MySQLDialect_mysqlconnector
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg
from sqlalchemy.engine.url import make_url

import aws_advanced_python_wrapper.mysql_connector as wrapper_mysql
import aws_advanced_python_wrapper.psycopg as wrapper_psycopg
from aws_advanced_python_wrapper.sqlalchemy_dialects.mysql import \
    AwsWrapperMySQLConnectorDialect
from aws_advanced_python_wrapper.sqlalchemy_dialects.pg import \
    AwsWrapperPGPsycopgDialect


def test_pg_dialect_subclasses_pgdialect_psycopg():
    assert issubclass(AwsWrapperPGPsycopgDialect, PGDialect_psycopg)


def test_pg_dialect_import_dbapi_returns_wrapper_submodule():
    assert AwsWrapperPGPsycopgDialect.import_dbapi() is wrapper_psycopg


def test_pg_dialect_driver_attr():
    assert AwsWrapperPGPsycopgDialect.driver == "psycopg"


def test_mysql_dialect_subclasses_mysqldialect_mysqlconnector():
    assert issubclass(AwsWrapperMySQLConnectorDialect, MySQLDialect_mysqlconnector)


def test_mysql_dialect_import_dbapi_returns_wrapper_submodule():
    assert AwsWrapperMySQLConnectorDialect.import_dbapi() is wrapper_mysql


def test_mysql_dialect_driver_attr():
    assert AwsWrapperMySQLConnectorDialect.driver == "mysqlconnector"


def test_registry_resolves_aws_wrapper_postgresql():
    cls = registry.load("aws_wrapper_postgresql")
    assert cls is AwsWrapperPGPsycopgDialect


def test_registry_resolves_aws_wrapper_postgresql_psycopg():
    cls = registry.load("aws_wrapper_postgresql.psycopg")
    assert cls is AwsWrapperPGPsycopgDialect


def test_registry_resolves_aws_wrapper_mysql():
    cls = registry.load("aws_wrapper_mysql")
    assert cls is AwsWrapperMySQLConnectorDialect


def test_registry_resolves_aws_wrapper_mysql_mysqlconnector():
    cls = registry.load("aws_wrapper_mysql.mysqlconnector")
    assert cls is AwsWrapperMySQLConnectorDialect


def test_url_get_dialect_pg():
    url = make_url(
        "aws_wrapper_postgresql+psycopg://u:p@h:5432/db?wrapper_dialect=aurora-pg"
    )
    assert url.get_dialect() is AwsWrapperPGPsycopgDialect


def test_url_get_dialect_mysql():
    url = make_url(
        "aws_wrapper_mysql+mysqlconnector://u:p@h:3306/db?wrapper_dialect=aurora-mysql"
    )
    assert url.get_dialect() is AwsWrapperMySQLConnectorDialect


def test_url_query_args_flow_through_to_wrapper_connect(mocker):
    """URL query args must become kwargs on AwsWrapperConnection.connect."""
    from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection
    mock_connect = mocker.patch.object(
        AwsWrapperConnection, "connect", return_value=mocker.MagicMock(),
    )
    # NOTE: URL uses `wrapper_plugins` instead of `plugins` because SA's
    # create_engine intercepts `plugins` as its own engine-plugin loader.
    # Our dialect's create_connect_args translates wrapper_plugins → plugins
    # before handing kwargs to the DBAPI.
    engine = create_engine(
        "aws_wrapper_postgresql+psycopg://u:p@h:5432/db"
        "?wrapper_dialect=aurora-pg&wrapper_plugins=failover,efm"
    )
    try:
        with engine.connect():
            pass
    except Exception:
        # The MagicMock connection may not satisfy SA's probes.
        # We only care that AwsWrapperConnection.connect was invoked with the
        # right kwargs.
        pass

    assert mock_connect.called, "AwsWrapperConnection.connect was never invoked"
    _args, kwargs = mock_connect.call_args
    assert kwargs.get("wrapper_dialect") == "aurora-pg"
    assert kwargs.get("plugins") == "failover,efm"
    assert "wrapper_plugins" not in kwargs, (
        "dialect should have renamed wrapper_plugins → plugins before the connect call"
    )
