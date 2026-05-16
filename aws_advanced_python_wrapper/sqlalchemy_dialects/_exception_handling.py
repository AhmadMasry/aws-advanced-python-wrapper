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
``sqlalchemy.exc.<MappedClass>``. Wrapper-internal exceptions like
``FailoverSuccessError`` are subclasses of the wrapper's PEP-249
``OperationalError`` AND of the driver-native ``OperationalError`` (set up
in ``errors.py``). In principle SA should pick them up via isinstance, but
in practice the PGDialect_psycopg classifier path doesn't reclassify
``FailoverSuccessError`` to ``sqlalchemy.exc.OperationalError`` and the
exception escapes raw, defeating user-written ``except OperationalError:``
retry loops.

The mixin below sidesteps that by intercepting ``FailoverSuccessError`` at
the ``do_execute`` boundary and re-raising it as the driver-native
``OperationalError`` class -- which SA's classifier DOES reclassify
reliably. Each concrete dialect declares its target class via
``_failover_success_target_cls``; the mixin handles both sync and async
``do_execute`` shapes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Optional, Type

from aws_advanced_python_wrapper.errors import FailoverSuccessError

if TYPE_CHECKING:
    pass


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
