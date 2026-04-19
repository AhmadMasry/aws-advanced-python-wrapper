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

from sqlalchemy.dialects.mysql.mysqlconnector import \
    MySQLDialect_mysqlconnector
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg

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
