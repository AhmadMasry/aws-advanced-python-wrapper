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

"""PostgreSQL SQLAlchemy dialect bound to the AWS Advanced Python Wrapper.

Registered as ``aws-wrapper-postgresql`` / ``aws-wrapper-postgresql+psycopg``
via a pyproject entry-point. Subclasses SA's standard PGDialect_psycopg and
only swaps the DBAPI module to :mod:`aws_advanced_python_wrapper.psycopg`,
which routes connect() through the wrapper's plugin pipeline.
"""

from __future__ import annotations

from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg


class AwsWrapperPGPsycopgDialect(PGDialect_psycopg):
    """SQLAlchemy dialect that uses the AWS Advanced Python Wrapper as its DBAPI."""

    driver = "psycopg"
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls):
        import aws_advanced_python_wrapper.psycopg as dbapi
        return dbapi

    def create_connect_args(self, url):
        # SQLAlchemy's `create_engine` intercepts `plugins=` in the URL query
        # to load SA engine plugins, stripping it before the dialect sees it.
        # The wrapper's own `plugins` connection property would therefore be
        # swallowed. Allow users to spell it as `wrapper_plugins=` in the URL
        # and rename it to `plugins=` before handing kwargs to the DBAPI.
        args, kwargs = super().create_connect_args(url)
        wrapper_plugins = kwargs.pop("wrapper_plugins", None)
        if wrapper_plugins is not None:
            kwargs["plugins"] = wrapper_plugins
        return args, kwargs
