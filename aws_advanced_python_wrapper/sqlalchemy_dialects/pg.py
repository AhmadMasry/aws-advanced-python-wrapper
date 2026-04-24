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

from typing import Any

from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg


class AwsWrapperPGPsycopgDialect(PGDialect_psycopg):
    """SQLAlchemy dialect that uses the AWS Advanced Python Wrapper as its DBAPI.

    Wrapper-specific override pattern
    ---------------------------------
    The wrapper interposes on DBAPI-level calls -- SA drives those
    through our plugin pipeline. But SA's psycopg dialect also calls
    into psycopg internals directly (``psycopg.types.TypeInfo.fetch``
    and friends), passing a ``driver_connection``. Those helpers do a
    strict ``isinstance`` check against ``psycopg.Connection``; our
    proxy is not a subclass, so they raise TypeError.

    Wherever SA reaches the raw driver connection, we override the
    method here to unwrap to the native psycopg connection via
    ``AwsWrapperConnection.target_connection`` before handing it to
    psycopg. Current overrides: ``_type_info_fetch``.
    """

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

    def _type_info_fetch(self, connection: Any, name: str) -> Any:
        """Unwrap to native psycopg.Connection before TypeInfo.fetch.

        SA native (``sqlalchemy/dialects/postgresql/psycopg.py:462``):
            return TypeInfo.fetch(connection.connection.driver_connection, name)

        In SA's sync psycopg dialect, ``connection.connection`` is the
        DBAPI-level connection and ``driver_connection`` is the native
        psycopg.Connection (SA's ``AdaptedConnection`` base class
        implements ``driver_connection`` as a property returning
        ``self._connection``). When our wrapper is the DBAPI connection,
        ``driver_connection`` returns our wrapper proxy.

        ``psycopg.TypeInfo.fetch`` rejects the proxy with
        ``TypeError: expected Connection or AsyncConnection, got ...``
        (psycopg/_typeinfo.py:90). Reach the underlying native via
        ``target_connection`` (exposed on our wrapper at
        ``wrapper.py:84``) and pass it directly.
        """
        from psycopg.types import TypeInfo

        # connection.connection may BE our wrapper (SA hands us the
        # DBAPI conn directly on the sync path) or SA's adapter
        # wrapping us. Support both shapes: prefer ``driver_connection``
        # when present, else fall back to the conn itself.
        dbapi_conn = connection.connection
        candidate = getattr(dbapi_conn, "driver_connection", dbapi_conn)
        native = getattr(candidate, "target_connection", candidate)
        return TypeInfo.fetch(native, name)
