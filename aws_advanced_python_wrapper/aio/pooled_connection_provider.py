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

"""Per-host, per-user async connection pool registry.

:class:`AsyncPooledConnectionProvider` is the async counterpart of
:class:`aws_advanced_python_wrapper.sql_alchemy_connection_provider.SqlAlchemyPooledConnectionProvider`,
implemented over asyncio primitives instead of SQLAlchemy's pool. The
underlying pool (:class:`_AsyncPool`) holds raw async connections
(``psycopg.AsyncConnection`` / ``aiomysql.Connection``); the proxy
(:class:`_PooledAsyncConnectionProxy`) routes ``close()`` /
``__aexit__`` to release-to-pool semantics.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages

if TYPE_CHECKING:  # noqa: TC005
    pass

logger = Logger(__name__)


class _AsyncPool:
    """Bounded async connection pool over asyncio primitives.

    The pool's max concurrency is ``max_size + max_overflow``; ``acquire``
    blocks (with a timeout) when the cap is reached. Idle connections are
    held in an ``asyncio.Queue``; new ones are created on demand via the
    async ``creator`` callable.

    Disposal is one-way: once :meth:`dispose` is called the pool refuses
    new acquires and closes idle conns immediately; in-flight conns close
    on the next :meth:`release` instead of returning to the queue.
    """

    def __init__(
            self,
            creator: Callable[[], Awaitable[Any]],
            max_size: int = 5,
            max_overflow: int = 10,
            timeout_seconds: float = 30.0,
    ):
        self._creator = creator
        self._max_size = max_size
        self._max_overflow = max_overflow
        self._timeout_seconds = timeout_seconds
        self._idle: asyncio.Queue[Any] = asyncio.Queue(maxsize=max_size + max_overflow)
        self._semaphore = asyncio.Semaphore(max_size + max_overflow)
        self._checkedout: int = 0
        self._disposing: bool = False

    def checkedout(self) -> int:
        return self._checkedout

    def is_disposing(self) -> bool:
        return self._disposing

    async def acquire(self) -> Any:
        if self._disposing:
            raise AwsWrapperError(
                Messages.get_formatted("AsyncPooledConnectionProvider.PoolDisposed", "<pool>"))
        await asyncio.wait_for(self._semaphore.acquire(), timeout=self._timeout_seconds)
        try:
            try:
                conn = self._idle.get_nowait()
            except asyncio.QueueEmpty:
                conn = await self._creator()
            self._checkedout += 1
            return conn
        except BaseException:
            self._semaphore.release()
            raise

    async def release(self, conn: Any, invalidated: bool = False) -> None:
        try:
            self._checkedout -= 1
            if invalidated or self._disposing:
                try:
                    await conn.close()
                except Exception:  # noqa: BLE001 - best-effort close
                    pass
                return
            try:
                self._idle.put_nowait(conn)
            except asyncio.QueueFull:
                # Should not normally happen — semaphore should have prevented it.
                try:
                    await conn.close()
                except Exception:  # noqa: BLE001 - best-effort close
                    pass
        finally:
            self._semaphore.release()

    async def dispose(self) -> None:
        self._disposing = True
        # Drain idle queue and close each conn.
        while True:
            try:
                conn = self._idle.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                await conn.close()
            except Exception:  # noqa: BLE001 - best-effort close
                pass


class _PooledAsyncConnectionProxy:
    """Wraps a raw async connection acquired from a :class:`_AsyncPool`.

    ``close()`` and ``__aexit__`` route to ``pool.release(...)`` instead
    of closing the underlying connection. Use :meth:`invalidate` before
    closing to force an actual close (e.g., after a SQL error left the
    connection in a bad state).

    All other attributes are delegated to the underlying connection via
    ``__getattr__``, so cursor / transaction / driver-specific methods
    work transparently.
    """

    def __init__(self, raw_conn: Any, pool: _AsyncPool):
        # Set as instance attributes so __getattr__ doesn't fall through to raw_conn.
        self._raw = raw_conn
        self._pool = pool
        self._invalidated = False

    def invalidate(self) -> None:
        self._invalidated = True

    async def close(self) -> None:
        await self._pool.release(self._raw, invalidated=self._invalidated)

    async def __aenter__(self) -> _PooledAsyncConnectionProxy:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    def __getattr__(self, name: str) -> Any:
        # Only fires for attributes not on `self`. The `_raw` / `_pool` /
        # `_invalidated` attributes set in __init__ shadow any equivalents
        # on the underlying conn.
        return getattr(self._raw, name)
