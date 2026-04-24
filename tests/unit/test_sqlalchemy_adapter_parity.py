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

"""Tests for SQLAlchemy AdaptedConnection duck-type parity on both wrappers.

Guards the fix for the downstream SQLAlchemy+psycopg async dialect
crash reported in Aurora integration tests:

    AttributeError: 'AwsWrapperConnection' object has no attribute 'await_'

SA's async dialects treat the DBAPI connection as an "adapted
connection" (see ``sqlalchemy.engine.interfaces.AdaptedConnection`` and
``sqlalchemy.connectors.asyncio.AsyncAdapt_dbapi_connection``) and
call ``adapted.await_`` / ``adapted.driver_connection`` /
``adapted.run_async`` against it during engine initialize. The wrappers
duck-type that contract so SA's dialect machinery works against them.

Covers both sync and async wrappers -- the sync wrapper needs these
when SA's greenlet bridge is wrapping our sync dialect under
``create_async_engine``; the async wrapper needs them when SA unwraps
its ``AsyncAdapt_psycopg_connection`` or when a downstream path
substitutes our wrapper directly for SA's adapter.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

from sqlalchemy.util.concurrency import greenlet_spawn

from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection

# ---- Fixtures ---------------------------------------------------------


def _sync_wrapper(target: MagicMock) -> AwsWrapperConnection:
    wrapper = AwsWrapperConnection.__new__(AwsWrapperConnection)
    plugin_service = MagicMock()
    plugin_service.current_connection = target
    wrapper._plugin_service = plugin_service
    wrapper._plugin_manager = MagicMock()
    return wrapper


def _async_wrapper(target: MagicMock) -> AsyncAwsWrapperConnection:
    wrapper = AsyncAwsWrapperConnection.__new__(AsyncAwsWrapperConnection)
    wrapper._plugin_service = MagicMock()
    wrapper._plugin_manager = MagicMock()
    wrapper._target_conn = target
    return wrapper


# ---- driver_connection / _connection properties -----------------------


def test_sync_wrapper_driver_connection_returns_target() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    assert wrapper.driver_connection is target


def test_sync_wrapper_underscore_connection_returns_target() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    assert wrapper._connection is target


def test_async_wrapper_driver_connection_returns_target() -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    assert wrapper.driver_connection is target


def test_async_wrapper_underscore_connection_returns_target() -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    assert wrapper._connection is target


# ---- await_ staticmethod (greenlet bridge) ----------------------------


def test_sync_wrapper_await_runs_coroutine_to_completion() -> None:
    """SA's ``AsyncAdapt_dbapi_connection.await_`` must run an awaitable
    to completion from the enclosing greenlet context. Matches SA's
    ``staticmethod(await_only)`` behavior."""
    async def _co() -> int:
        return 42

    target = MagicMock()
    wrapper = _sync_wrapper(target)

    # SA's await_only requires an outer greenlet; greenlet_spawn wraps.
    result = asyncio.run(greenlet_spawn(lambda: wrapper.await_(_co())))
    assert result == 42


def test_async_wrapper_await_runs_coroutine_to_completion() -> None:
    async def _co() -> str:
        return "done"

    target = MagicMock()
    wrapper = _async_wrapper(target)

    result = asyncio.run(greenlet_spawn(lambda: wrapper.await_(_co())))
    assert result == "done"


def test_await_is_staticmethod_on_both_wrappers() -> None:
    """SA references ``adapted.await_(coro)`` with the adapter as
    `adapted` -- the callable must be a staticmethod-like (does NOT
    require an instance to call). Matches SA's own spelling
    (staticmethod(await_only))."""
    # Calling on the class directly should work, per staticmethod semantics.
    async def _co() -> int:
        return 7

    sync_result = asyncio.run(
        greenlet_spawn(lambda: AwsWrapperConnection.await_(_co())))
    assert sync_result == 7

    async def _co2() -> int:
        return 9

    async_result = asyncio.run(
        greenlet_spawn(lambda: AsyncAwsWrapperConnection.await_(_co2())))
    assert async_result == 9


# ---- run_async method ------------------------------------------------


def test_sync_wrapper_run_async_passes_driver_connection_to_fn() -> None:
    """Matches ``AdaptedConnection.run_async``: the function receives the
    raw driver connection."""
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    captured: list = []

    async def _fn(driver_conn: Any) -> str:
        captured.append(driver_conn)
        return "invoked"

    result = asyncio.run(greenlet_spawn(lambda: wrapper.run_async(_fn)))
    assert result == "invoked"
    assert captured == [target]


def test_async_wrapper_run_async_passes_driver_connection_to_fn() -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    captured: list = []

    async def _fn(driver_conn: Any) -> str:
        captured.append(driver_conn)
        return "invoked"

    result = asyncio.run(greenlet_spawn(lambda: wrapper.run_async(_fn)))
    assert result == "invoked"
    assert captured == [target]


# ---- Plugin-chain bypass assertions ---------------------------------


def test_await_bypasses_plugin_chain_on_both_wrappers() -> None:
    """``await_`` is pure greenlet machinery; it must not invoke the
    plugin pipeline."""
    async def _co() -> int:
        return 1

    for wrapper_factory in (_sync_wrapper, _async_wrapper):
        wrapper = wrapper_factory(MagicMock())
        asyncio.run(greenlet_spawn(lambda: wrapper.await_(_co())))
        wrapper._plugin_manager.execute.assert_not_called()  # type: ignore[union-attr]


def test_run_async_bypasses_plugin_chain_on_both_wrappers() -> None:
    async def _fn(_c: Any) -> int:
        return 1

    for wrapper_factory in (_sync_wrapper, _async_wrapper):
        wrapper = wrapper_factory(MagicMock())
        asyncio.run(greenlet_spawn(lambda: wrapper.run_async(_fn)))
        wrapper._plugin_manager.execute.assert_not_called()  # type: ignore[union-attr]


# ---- SA AdaptedConnection duck-type contract verification ------------


def test_wrappers_satisfy_adaptedconnection_duck_type() -> None:
    """Regression guard: SA's AdaptedConnection contract is:
      * ``_connection`` (attribute-or-property) -> driver conn
      * ``driver_connection`` (property) -> driver conn
      * ``run_async(fn)`` -> await_only(fn(_connection))

    Plus ``AsyncAdapt_dbapi_connection`` adds:
      * ``await_`` (staticmethod or similar callable) -> await_only

    If SA changes any of these names we need to update the wrappers
    together; this test makes the contract explicit."""
    for cls in (AwsWrapperConnection, AsyncAwsWrapperConnection):
        assert hasattr(cls, "_connection"), (
            f"{cls.__name__} missing SA AdaptedConnection._connection")
        assert hasattr(cls, "driver_connection"), (
            f"{cls.__name__} missing SA AdaptedConnection.driver_connection")
        assert callable(getattr(cls, "await_", None)), (
            f"{cls.__name__} missing SA AsyncAdapt_dbapi_connection.await_")
        assert callable(getattr(cls, "run_async", None)), (
            f"{cls.__name__} missing SA AdaptedConnection.run_async")


def test_driver_connection_is_the_same_as_underlying_target_connection() -> None:
    """Sanity: all three accessors (``target_connection``,
    ``driver_connection``, ``_connection``) must return the same object
    on both wrappers. They're aliases, not forks."""
    target = MagicMock()
    sync = _sync_wrapper(target)
    assert sync.target_connection is sync.driver_connection
    assert sync.driver_connection is sync._connection

    asyn = _async_wrapper(target)
    assert asyn.target_connection is asyn.driver_connection
    assert asyn.driver_connection is asyn._connection
