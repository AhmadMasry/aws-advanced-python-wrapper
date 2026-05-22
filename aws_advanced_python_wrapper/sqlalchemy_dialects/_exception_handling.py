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

"""Shared exception-handling helpers for the wrapper's SA dialects.

SQLAlchemy classifies DBAPI exceptions in ``Connection._handle_dbapi_exception``
by walking ``dialect.loaded_dbapi.<ErrorClass>`` and wrapping into
``sqlalchemy.exc.<MappedClass>``. SA's classifier needs the raised exception
to be an instance of the **driver-native** ``OperationalError`` class
(e.g. ``psycopg.OperationalError``), not the wrapper's PEP-249
``OperationalError``. Wrapper-internal exceptions like
``FailoverSuccessError`` are single-inherit from ``FailoverError`` (the
driver-native multi-inheritance was reverted in commit ``d994d02`` because
it caused Django's ``wrap_database_errors`` to swallow the failover signal
on MySQL), so SA's classifier lets them escape raw and any user-written
``except sqlalchemy.exc.OperationalError:`` retry loop never fires.

The mixin below sidesteps that by intercepting ``FailoverSuccessError`` at
the ``do_execute`` / ``do_executemany`` boundary and re-raising it as the
driver-native ``OperationalError`` class — which SA's classifier DOES
reclassify reliably to ``sqlalchemy.exc.OperationalError``. The original
wrapper exception is preserved via ``__cause__`` so callers that need the
exact wrapper type can ``isinstance(exc.__cause__, FailoverSuccessError)``.

Each concrete dialect declares its target class via
``_failover_success_target_cls``; the mixin handles both sync and async
``do_execute`` shapes.

Scope: only ``do_execute`` and ``do_executemany`` are wrapped. If
``FailoverSuccessError`` ever surfaces from ``do_commit`` / ``do_rollback``
/ ``do_begin_twophase`` / etc., it will escape raw — extend the mixin then.
"""

from __future__ import annotations

from typing import ClassVar, Optional, Type

from aws_advanced_python_wrapper.errors import FailoverSuccessError


class _FailoverSuccessRewrapMixin:
    """Re-raise ``FailoverSuccessError`` as the driver-native OperationalError.

    Concrete dialect subclasses set ``_failover_success_target_cls`` to the
    driver's own ``OperationalError`` class (e.g. ``psycopg.OperationalError``,
    ``mysql.connector.errors.OperationalError``, ``aiomysql.OperationalError``).
    The mixin's ``do_execute`` wraps the parent's call: on
    ``FailoverSuccessError``, it raises the target class with the same message,
    chaining the original via ``__cause__``. SA's classifier reliably maps
    driver-native ``OperationalError`` -> ``sqlalchemy.exc.OperationalError``,
    so user retry loops (``except sqlalchemy.exc.OperationalError:``) fire.

    Other ``FailoverError`` subclasses (``FailoverFailedError``,
    ``TransactionResolutionUnknownError``) are NOT rewrapped: failed failover
    is a hard error the user should see, and transaction-resolution-unknown
    has its own semantics distinct from a generic OperationalError.
    """

    # Subclasses MUST set this to the driver-native OperationalError class.
    _failover_success_target_cls: ClassVar[Optional[Type[BaseException]]] = None

    def do_execute(  # type: ignore[no-untyped-def]
            self, cursor, statement, parameters, context=None):
        try:
            super().do_execute(  # type: ignore[misc]
                cursor, statement, parameters, context)
        except FailoverSuccessError as e:
            target = self._failover_success_target_cls
            if target is None:
                raise  # mis-configured dialect; surface the raw error
            raise target(str(e)) from e

    def do_executemany(  # type: ignore[no-untyped-def]
            self, cursor, statement, parameters, context=None):
        try:
            super().do_executemany(  # type: ignore[misc]
                cursor, statement, parameters, context)
        except FailoverSuccessError as e:
            target = self._failover_success_target_cls
            if target is None:
                raise
            raise target(str(e)) from e


class _AsyncFailoverSuccessRewrapMixin:
    """Async counterpart of :class:`_FailoverSuccessRewrapMixin`.

    SA async dialects use ``await cursor.execute(...)``; the parent's
    ``do_execute`` is a coroutine and must be awaited inside the try block.
    """

    _failover_success_target_cls: ClassVar[Optional[Type[BaseException]]] = None

    async def do_execute(  # type: ignore[no-untyped-def]
            self, cursor, statement, parameters, context=None):
        try:
            await super().do_execute(  # type: ignore[misc]
                cursor, statement, parameters, context)
        except FailoverSuccessError as e:
            target = self._failover_success_target_cls
            if target is None:
                raise
            raise target(str(e)) from e

    async def do_executemany(  # type: ignore[no-untyped-def]
            self, cursor, statement, parameters, context=None):
        try:
            await super().do_executemany(  # type: ignore[misc]
                cursor, statement, parameters, context)
        except FailoverSuccessError as e:
            target = self._failover_success_target_cls
            if target is None:
                raise
            raise target(str(e)) from e


__all__ = ["_FailoverSuccessRewrapMixin", "_AsyncFailoverSuccessRewrapMixin"]
