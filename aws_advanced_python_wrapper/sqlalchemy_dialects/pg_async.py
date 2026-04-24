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

"""Async PostgreSQL SQLAlchemy dialect bound to the AWS Advanced Python Wrapper.

Registered as ``aws_wrapper_postgresql_async`` /
``aws_wrapper_postgresql+psycopg_async`` via pyproject entry-points.
Subclasses SA's standard ``PGDialectAsync_psycopg`` and swaps the DBAPI to
an adapter that routes ``connect()`` through the async plugin pipeline
while preserving SA's ``AsyncAdapt_psycopg_connection`` greenlet-bridge
wrapper that the async engine expects.

Example::

    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(
        "aws_wrapper_postgresql+psycopg_async://user:pwd@"
        "database.cluster-xyz.us-east-1.rds.amazonaws.com:5432/db"
        "?wrapper_dialect=aurora-pg&wrapper_plugins=failover,efm"
    )
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.postgresql.psycopg import (
    AsyncAdapt_psycopg_connection, AsyncAdaptFallback_psycopg_connection,
    PGDialectAsync_psycopg)
from sqlalchemy.util import asbool
from sqlalchemy.util.concurrency import await_fallback, await_only


class AwsWrapperAsyncPsycopgAdaptDBAPI:
    """DBAPI adapter that bridges our async ``aio.psycopg`` submodule into
    SA's ``PGDialectAsync_psycopg`` flow.

    Mirrors SA's own ``PsycopgAdaptDBAPI`` but wraps the WRAPPER's async
    submodule rather than ``psycopg`` itself. The adapter's ``connect``
    calls ``aio.psycopg.connect(...)`` (which awaits through the wrapper's
    plugin pipeline) and wraps the resulting connection in
    ``AsyncAdapt_psycopg_connection`` so the engine's greenlet bridge can
    expose it via a sync-looking surface.
    """

    def __init__(self) -> None:
        # Import lazily -- avoids a circular import during pyproject
        # entry-point loading in some environments.
        import aws_advanced_python_wrapper.aio.psycopg as aio_psycopg
        self._aio_psycopg = aio_psycopg
        # Copy the PEP 249 surface onto self (apilevel, paramstyle, Error,
        # Date, STRING, etc.) except the connect callable (handled below).
        for name, value in aio_psycopg.__dict__.items():
            if name == "connect":
                continue
            self.__dict__[name] = value

    @property
    def psycopg(self) -> Any:
        """Back-compat attribute: SA's adapter exposes ``.psycopg`` pointing
        at the wrapped module. We point it at the aio submodule."""
        return self._aio_psycopg

    def __getattr__(self, name: str) -> Any:
        """Forward missing attributes to the wrapped aio psycopg module.

        Copies the PEP 562 forwarding trick from ``aio.psycopg`` up one
        layer: SA's ``PGDialectAsync_psycopg.__init__`` probes the DBAPI
        for ``__version__``, ``adapters``, ``pq``, etc. ``aio.psycopg``
        itself forwards those to the real :mod:`psycopg` via its own
        ``__getattr__``; we forward ours to ``aio.psycopg``.
        """
        return getattr(self._aio_psycopg, name)

    def connect(self, *args: Any, **kwargs: Any) -> AsyncAdapt_psycopg_connection:
        """Open an async connection through the wrapper and hand SA the
        ``AsyncAdapt_psycopg_connection`` it expects."""
        async_fallback = kwargs.pop("async_fallback", False)
        # SA sometimes plumbs an ``async_creator_fn`` down from user code to
        # let the caller provide the raw awaitable. Our wrapper IS the
        # creator, so discard anything that came in under that key.
        kwargs.pop("async_creator_fn", None)

        # aio.psycopg.connect is async; returns an awaitable that resolves
        # to an AsyncAwsWrapperConnection.
        coro = self._aio_psycopg.connect(*args, **kwargs)

        if asbool(async_fallback):
            return AsyncAdaptFallback_psycopg_connection(await_fallback(coro))
        return AsyncAdapt_psycopg_connection(await_only(coro))


class AwsWrapperPGPsycopgAsyncDialect(PGDialectAsync_psycopg):
    """Async SQLAlchemy dialect that uses the AWS Advanced Python Wrapper as its DBAPI."""

    driver = "psycopg_async"
    supports_statement_cache = True
    is_async = True

    @classmethod
    def import_dbapi(cls) -> AwsWrapperAsyncPsycopgAdaptDBAPI:
        # Mirror PGDialectAsync_psycopg.import_dbapi's side effect:
        # SA's AsyncAdapt_psycopg_cursor.execute reads
        # self._psycopg_ExecStatus.TUPLES_OK
        # (sqlalchemy/dialects/postgresql/psycopg.py:679). The class-
        # level attribute defaults to None; SA's native import_dbapi
        # sets it during engine init. Our override replaced the parent
        # import_dbapi wholesale and skipped the assignment, so
        # cursor.execute() crashed with
        # "'NoneType' object has no attribute 'TUPLES_OK'" on first
        # use. Explicit mirror of the side effect here (not
        # super().import_dbapi()) -- avoids pulling in the parent's
        # PsycopgAdaptDBAPI construction we don't need.
        from psycopg.pq import ExecStatus
        from sqlalchemy.dialects.postgresql.psycopg import \
            AsyncAdapt_psycopg_cursor

        # SA types this class attribute as ``None`` (its default value);
        # narrow the ignore to the exact code rather than using a bare
        # ``# type: ignore`` so unrelated errors on this line would
        # still surface.
        AsyncAdapt_psycopg_cursor._psycopg_ExecStatus = ExecStatus  # type: ignore[assignment]
        return AwsWrapperAsyncPsycopgAdaptDBAPI()

    @classmethod
    def get_dialect_cls(cls, url):
        return cls

    @classmethod
    def get_async_dialect_cls(cls, url):
        # The grandparent `PGDialect_psycopg` hard-codes this to return
        # `PGDialectAsync_psycopg`, which would cause `create_async_engine`
        # to swap our subclass out for the stock SA class. Override to
        # return ourselves so URL-based dialect selection actually uses
        # the wrapper.
        return cls

    def create_connect_args(self, url):
        # SQLAlchemy's ``create_engine`` / ``create_async_engine`` reserves
        # ``plugins=`` in the URL query for its own engine-plugin loader.
        # Allow users to spell the wrapper's ``plugins`` connection property
        # as ``wrapper_plugins=`` and rename it back before the DBAPI call.
        # See F2's sync counterpart in ``pg.py`` for the rationale.
        args, kwargs = super().create_connect_args(url)
        wrapper_plugins = kwargs.pop("wrapper_plugins", None)
        if wrapper_plugins is not None:
            kwargs["plugins"] = wrapper_plugins
        return args, kwargs
