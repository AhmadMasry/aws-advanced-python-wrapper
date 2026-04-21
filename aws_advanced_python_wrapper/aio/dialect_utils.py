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

"""Async variant of :class:`DialectUtils`.

Runs dialect probe queries (is_reader, instance_id) against async
connections. Wraps each probe in ``asyncio.wait_for`` for a per-call
timeout, and performs a best-effort in_transaction check to avoid
polluting the caller's transaction state.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Optional

from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                QueryTimeoutError)
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.utils.messages import Messages

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect


class AsyncDialectUtils:
    """Async counterpart of :class:`DialectUtils`."""

    @staticmethod
    async def get_host_role(
            conn: Any,
            driver_dialect: AsyncDriverDialect,
            is_reader_query: str,
            timeout_sec: float = 5.0) -> HostRole:
        """Query ``conn`` to determine whether it's bound to a reader
        or writer. Mirrors sync DialectUtils.get_host_role at
        database_dialect.py:1045-1059.

        Runs the dialect's ``is_reader_query`` via an async cursor with
        ``asyncio.wait_for`` for the timeout. Probe runs best-effort;
        an empty result or transaction failure raises AwsWrapperError.
        """
        try:
            result = await asyncio.wait_for(
                AsyncDialectUtils._execute_is_reader_query(conn, is_reader_query),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError as e:
            raise QueryTimeoutError(
                Messages.get("DialectUtils.GetHostRoleTimeout")) from e
        except Exception as e:
            raise AwsWrapperError(
                Messages.get("DialectUtils.ErrorGettingHostRole")) from e

        if result is None:
            raise AwsWrapperError(
                Messages.get("DialectUtils.ErrorGettingHostRole"))
        is_reader = bool(result[0])
        return HostRole.READER if is_reader else HostRole.WRITER

    @staticmethod
    async def _execute_is_reader_query(
            conn: Any,
            is_reader_query: str) -> Optional[tuple]:
        """Open an async cursor, run the is_reader_query, return the row.

        Handles both psycopg's sync-returning ``conn.cursor()`` and
        aiomysql's coroutine-returning ``conn.cursor()``.
        """
        cursor_ret = conn.cursor()
        if asyncio.iscoroutine(cursor_ret):
            cursor = await cursor_ret
        else:
            cursor = cursor_ret
        try:
            await cursor.execute(is_reader_query)
            return await cursor.fetchone()
        finally:
            close = getattr(cursor, "close", None)
            if close is not None:
                try:
                    result = close()
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:  # noqa: BLE001 - cursor close best-effort
                    pass


__all__ = ["AsyncDialectUtils"]
