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

"""MySQL SQLAlchemy dialect bound to the AWS Advanced Python Wrapper.

Registered as ``aws-wrapper-mysql`` / ``aws-wrapper-mysql+mysqlconnector``
via a pyproject entry-point. Subclasses SA's standard
MySQLDialect_mysqlconnector and only swaps the DBAPI module to
:mod:`aws_advanced_python_wrapper.mysql_connector`, which routes connect()
through the wrapper's plugin pipeline.
"""

from __future__ import annotations

from sqlalchemy.dialects.mysql.mysqlconnector import \
    MySQLDialect_mysqlconnector


class AwsWrapperMySQLConnectorDialect(MySQLDialect_mysqlconnector):
    """SQLAlchemy dialect that uses the AWS Advanced Python Wrapper as its DBAPI."""

    driver = "mysqlconnector"
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls):
        import aws_advanced_python_wrapper.mysql_connector as dbapi
        return dbapi
