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

"""Async MySQL SQLAlchemy dialect bound to the AWS Advanced Python Wrapper.

Registered as ``aws_wrapper_mysql_async`` /
``aws_wrapper_mysql+aiomysql_async`` via pyproject entry-points.
Subclasses SA's standard ``MySQLDialect_aiomysql`` and swaps the DBAPI
to an adapter that routes ``connect()`` through the async plugin pipeline
while preserving SA's ``AsyncAdapt_aiomysql_connection`` greenlet-bridge
wrapper that the async engine expects.

Example::

    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(
        "aws_wrapper_mysql+aiomysql_async://user:pwd@"
        "database.cluster-xyz.us-east-1.rds.amazonaws.com:3306/db"
        "?wrapper_dialect=aurora-mysql&wrapper_plugins=failover,efm"
    )
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.mysql.aiomysql import (AsyncAdapt_aiomysql_connection,
                                                MySQLDialect_aiomysql)
from sqlalchemy.util.concurrency import await_only


class AwsWrapperAsyncAiomysqlAdaptDBAPI:
    """DBAPI adapter bridging our async aiomysql submodule into SA's MySQL
    async flow.

    Mirrors the pattern of :class:`AwsWrapperAsyncPsycopgAdaptDBAPI` in
    ``pg_async.py``: wraps the wrapper's async submodule rather than
    ``aiomysql`` itself. The adapter's ``connect`` calls the wrapper's
    async connect (which awaits through the plugin pipeline) and wraps
    the resulting connection in ``AsyncAdapt_aiomysql_connection`` so the
    engine's greenlet bridge can expose it via a sync-looking surface.
    """

    def __init__(self) -> None:
        import aws_advanced_python_wrapper.aio.aiomysql as aio_aiomysql
        self._aio_aiomysql = aio_aiomysql
        # Copy the PEP 249 surface onto self except ``connect`` (handled below).
        for name, value in aio_aiomysql.__dict__.items():
            if name == "connect":
                continue
            self.__dict__[name] = value

    @property
    def aiomysql(self) -> Any:
        return self._aio_aiomysql

    def __getattr__(self, name: str) -> Any:
        return getattr(self._aio_aiomysql, name)

    def connect(self, *args: Any, **kwargs: Any) -> AsyncAdapt_aiomysql_connection:
        # SA may pass `async_creator_fn` for custom pool factories; we
        # are the creator, so discard it.
        kwargs.pop("async_creator_fn", None)
        coro = self._aio_aiomysql.connect(*args, **kwargs)
        # SA's AsyncAdapt_aiomysql_connection takes (dbapi, connection);
        # we are the dbapi for the adapter's purposes. The connection
        # arg is typed as AsyncIODBAPIConnection (structural); our
        # AsyncAwsWrapperConnection proxies unknown attrs to the target
        # driver conn, so duck-typing holds at runtime. Cast for mypy.
        return AsyncAdapt_aiomysql_connection(
            self, await_only(coro)  # type: ignore[arg-type]
        )


class AwsWrapperMySQLAiomysqlAsyncDialect(MySQLDialect_aiomysql):
    """Async SQLAlchemy dialect that uses the AWS Advanced Python Wrapper as its DBAPI."""

    driver = "aiomysql_async"
    supports_statement_cache = True
    is_async = True

    @classmethod
    def import_dbapi(cls) -> Any:  # type: ignore[override]
        # Parent's return type hint is SA's AsyncAdapt_aiomysql_dbapi class
        # specifically; ours is a shim-compatible duck-type. Use Any to
        # avoid variance grief with mypy.
        return AwsWrapperAsyncAiomysqlAdaptDBAPI()

    @classmethod
    def get_dialect_cls(cls, url):
        return cls

    @classmethod
    def get_async_dialect_cls(cls, url):
        # Grandparent MySQLDialect_pymysql.get_async_dialect_cls hard-
        # returns MySQLDialect_aiomysql; override so URL-based dialect
        # selection picks up our subclass instead of the stock SA class.
        return cls

    def create_connect_args(self, url):
        # SA reserves ``plugins=`` in the URL query for its own engine-
        # plugin loader. Allow users to spell the wrapper's ``plugins``
        # connection property as ``wrapper_plugins=`` and rename it back
        # before the DBAPI call. See F2 pg.py / F3 pg_async.py for rationale.
        args, kwargs = super().create_connect_args(url)
        wrapper_plugins = kwargs.pop("wrapper_plugins", None)
        if wrapper_plugins is not None:
            kwargs["plugins"] = wrapper_plugins
        return args, kwargs
