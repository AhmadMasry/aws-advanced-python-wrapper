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

from __future__ import annotations

import functools
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.pep249 import Connection


def preserve_transaction_status_with_timeout(executor: Executor, timeout_sec, driver_dialect: DriverDialect, conn: Connection):
    """
    Timeout decorator, timeout in seconds
    """

    def preserve_transaction_status_with_timeout_decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):

            initial_transaction_status: bool = driver_dialect.is_in_transaction(conn)

            future = executor.submit(func, *args, **kwargs)

            try:
                # raises TimeoutError on timeout
                result = future.result(timeout=timeout_sec)
            except TimeoutError:
                # The Python-level future timed out, but the OS thread
                # inside the executor pool is still blocked in libpq
                # recv() and Python cannot kill it. Two-step unblock:
                #
                # 1. ``conn.cancel()`` (psycopg) sends a server-side
                #    cancellation request through a SEPARATE libpq
                #    connection, so it doesn't wait for the original
                #    connection's lock. The server aborts the running
                #    query and the stuck ``recv()`` returns with an
                #    error, freeing the pool worker.
                # 2. Fallback to ``conn.close()`` for drivers without
                #    a cancel API (eg. mysql.connector) -- best-effort.
                #
                # Without this, repeated timeouts on slow-Aurora
                # topology probes pin every executor pool worker on a
                # stuck connection until the pool is exhausted; after
                # that no further topology refresh can start and the
                # outer wait spins for its full budget.
                try:
                    cancel = getattr(conn, "cancel", None)
                    if callable(cancel):
                        cancel()
                    else:
                        conn.close()
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
                raise

            if not initial_transaction_status and driver_dialect.is_in_transaction(conn):
                # this condition is True when autocommit is False and the query started a new transaction.
                conn.commit()

            return result

        return func_wrapper

    return preserve_transaction_status_with_timeout_decorator


def timeout(executor: Executor, timeout_sec):
    """
    Timeout decorator, timeout in seconds
    """

    def timeout_decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):

            future = executor.submit(func, *args, **kwargs)

            # raises TimeoutError on timeout
            return future.result(timeout=timeout_sec)

        return func_wrapper

    return timeout_decorator
