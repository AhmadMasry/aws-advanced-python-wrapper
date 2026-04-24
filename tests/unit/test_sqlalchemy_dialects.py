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


# ---- _type_info_fetch unwrap (sync) -------------------------------------


def test_pg_dialect_type_info_fetch_unwraps_target_connection(mocker):
    """Regression guard for

        TypeError: expected Connection or AsyncConnection,
                   got AwsWrapperConnection

    at ``psycopg/_typeinfo.py:90``. ``psycopg.TypeInfo.fetch`` strictly
    isinstance-checks its first argument -- our wrapper proxy is not a
    subclass of ``psycopg.Connection``. The dialect override unwraps
    to the native psycopg connection via ``target_connection`` before
    calling into psycopg.
    """
    from unittest.mock import MagicMock
    native_conn = MagicMock(name="native_psycopg_connection")
    wrapper = MagicMock(name="AwsWrapperConnection")
    wrapper.target_connection = native_conn
    # SA's sync path: connection.connection IS the DBAPI conn (our
    # wrapper). No extra adapter layer between them.
    sa_connection = MagicMock()
    sa_connection.connection = wrapper
    # Make the wrapper look like it has driver_connection too (mirrors
    # AdaptedConnection contract added in d372b5f):
    wrapper.driver_connection = wrapper

    # Patch TypeInfo.fetch so we can assert its first arg without
    # needing a live Postgres.
    fetch_mock = mocker.patch(
        "psycopg.types.TypeInfo.fetch", return_value="type-info-sentinel")

    dialect = AwsWrapperPGPsycopgDialect()
    result = dialect._type_info_fetch(sa_connection, "hstore")

    assert result == "type-info-sentinel"
    fetch_mock.assert_called_once()
    called_arg = fetch_mock.call_args.args[0]
    assert called_arg is native_conn, (
        "TypeInfo.fetch must receive the native psycopg connection, "
        "not our wrapper proxy")


def test_pg_dialect_type_info_fetch_falls_through_when_no_target_connection(mocker):
    """If the DBAPI connection lacks ``target_connection`` (e.g., SA is
    pointed at a real psycopg connection directly, no wrapper in the
    middle), pass the connection through unchanged -- don't break
    non-wrapper deployments."""
    from unittest.mock import MagicMock

    # spec= so attribute-lookup returns a real AttributeError for
    # target_connection and we fall through to the conn itself.
    class _NativeLike:
        pass
    native = _NativeLike()
    sa_connection = MagicMock()
    sa_connection.connection = native

    fetch_mock = mocker.patch(
        "psycopg.types.TypeInfo.fetch", return_value="ok")

    dialect = AwsWrapperPGPsycopgDialect()
    dialect._type_info_fetch(sa_connection, "hstore")

    called_arg = fetch_mock.call_args.args[0]
    assert called_arg is native, (
        "no-wrapper deployments should pass the native conn through unchanged")
