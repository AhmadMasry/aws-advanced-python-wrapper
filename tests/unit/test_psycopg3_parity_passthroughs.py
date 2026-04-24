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

"""Tests for the psycopg3-parity passthroughs on sync + async wrappers.

Companion to test_notice_handler_passthrough.py. Covers the remaining
psycopg3.Connection / AsyncConnection surface that SQLAlchemy and
other downstream libraries may touch:

  Properties (both wrappers, plain getters):
    info, broken, adapters, prepare_threshold (+ setter),
    prepared_max (+ setter), deferrable, read_only (async getter;
    sync already has an intercepted property).

  Sync methods (both wrappers):
    fileno, cancel, xid, pipeline, notifies, transaction.

  Sync methods on sync wrapper, async on async wrapper:
    cancel_safe, execute, wait.

  Sync setters on sync wrapper, async on async wrapper:
    set_deferrable, set_isolation_level, set_read_only, set_autocommit.

All passthroughs delegate directly to the target connection (bypass
the plugin chain) -- they are local/client-side operations.

The \`set_read_only\` / \`set_autocommit\` passthroughs on the SYNC
wrapper route through the existing plugin-aware property setters so
their semantics stay consistent with the property assignment form.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection

# ---- Fixtures ------------------------------------------------------------


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


# ---- Properties (both wrappers) -----------------------------------------


@pytest.mark.parametrize("prop_name", [
    "info", "broken", "adapters", "prepare_threshold", "prepared_max",
    "deferrable",
])
def test_sync_wrapper_property_reads_target(prop_name: str) -> None:
    sentinel = object()
    target = MagicMock()
    setattr(target, prop_name, sentinel)
    wrapper = _sync_wrapper(target)
    assert getattr(wrapper, prop_name) is sentinel


@pytest.mark.parametrize("prop_name", [
    "info", "broken", "adapters", "prepare_threshold", "prepared_max",
    "deferrable", "read_only",
])
def test_async_wrapper_property_reads_target(prop_name: str) -> None:
    sentinel = object()
    target = MagicMock()
    setattr(target, prop_name, sentinel)
    wrapper = _async_wrapper(target)
    assert getattr(wrapper, prop_name) is sentinel


@pytest.mark.parametrize("prop_name", ["prepare_threshold", "prepared_max"])
def test_sync_wrapper_property_setter_writes_target(prop_name: str) -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    setattr(wrapper, prop_name, 42)
    assert getattr(target, prop_name) == 42


@pytest.mark.parametrize("prop_name", ["prepare_threshold", "prepared_max"])
def test_async_wrapper_property_setter_writes_target(prop_name: str) -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    setattr(wrapper, prop_name, 42)
    assert getattr(target, prop_name) == 42


# ---- Sync-only methods on both wrappers ---------------------------------


def test_sync_wrapper_fileno_delegates() -> None:
    target = MagicMock()
    target.fileno = MagicMock(return_value=42)
    wrapper = _sync_wrapper(target)
    assert wrapper.fileno() == 42
    target.fileno.assert_called_once_with()


def test_sync_wrapper_cancel_delegates() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.cancel()
    target.cancel.assert_called_once_with()


def test_sync_wrapper_xid_delegates() -> None:
    target = MagicMock()
    target.xid = MagicMock(return_value="xid-obj")
    wrapper = _sync_wrapper(target)
    assert wrapper.xid(1, "gtrid", "bqual") == "xid-obj"
    target.xid.assert_called_once_with(1, "gtrid", "bqual")


def test_sync_wrapper_pipeline_delegates() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.pipeline()
    target.pipeline.assert_called_once_with()


def test_sync_wrapper_notifies_delegates_with_kwargs() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.notifies(timeout=5.0, stop_after=10)
    target.notifies.assert_called_once_with(timeout=5.0, stop_after=10)


def test_sync_wrapper_transaction_delegates_with_kwargs() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.transaction(savepoint_name="sp1", force_rollback=True)
    target.transaction.assert_called_once_with(
        savepoint_name="sp1", force_rollback=True)


def test_async_wrapper_fileno_delegates() -> None:
    target = MagicMock()
    target.fileno = MagicMock(return_value=42)
    wrapper = _async_wrapper(target)
    assert wrapper.fileno() == 42


def test_async_wrapper_cancel_delegates() -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    wrapper.cancel()
    target.cancel.assert_called_once_with()


def test_async_wrapper_xid_delegates() -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    wrapper.xid(1, "g", "b")
    target.xid.assert_called_once_with(1, "g", "b")


def test_async_wrapper_pipeline_delegates() -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    wrapper.pipeline()
    target.pipeline.assert_called_once_with()


def test_async_wrapper_notifies_delegates() -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    wrapper.notifies(timeout=1.0, stop_after=None)
    target.notifies.assert_called_once_with(timeout=1.0, stop_after=None)


def test_async_wrapper_transaction_delegates() -> None:
    target = MagicMock()
    wrapper = _async_wrapper(target)
    wrapper.transaction(savepoint_name=None, force_rollback=False)
    target.transaction.assert_called_once_with(
        savepoint_name=None, force_rollback=False)


# ---- Sync-vs-async split methods ----------------------------------------


def test_sync_wrapper_cancel_safe_delegates() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.cancel_safe(timeout=15.0)
    target.cancel_safe.assert_called_once_with(timeout=15.0)


def test_async_wrapper_cancel_safe_awaits_target() -> None:
    target = MagicMock()
    target.cancel_safe = AsyncMock()
    wrapper = _async_wrapper(target)
    asyncio.run(wrapper.cancel_safe(timeout=5.0))
    target.cancel_safe.assert_awaited_once_with(timeout=5.0)


def test_sync_wrapper_execute_delegates_with_all_kwargs() -> None:
    target = MagicMock()
    target.execute = MagicMock(return_value="cursor-sentinel")
    wrapper = _sync_wrapper(target)
    result = wrapper.execute(
        "SELECT 1", ("p",), prepare=True, binary=True)
    assert result == "cursor-sentinel"
    target.execute.assert_called_once_with(
        "SELECT 1", ("p",), prepare=True, binary=True)


def test_async_wrapper_execute_awaits_target() -> None:
    target = MagicMock()
    target.execute = AsyncMock(return_value="async-cursor-sentinel")
    wrapper = _async_wrapper(target)
    result = asyncio.run(wrapper.execute("SELECT 1"))
    assert result == "async-cursor-sentinel"
    target.execute.assert_awaited_once_with(
        "SELECT 1", None, prepare=None, binary=False)


def test_sync_wrapper_wait_delegates() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.wait("gen", interval=0.5)
    target.wait.assert_called_once_with("gen", interval=0.5)


def test_async_wrapper_wait_awaits_target() -> None:
    target = MagicMock()
    target.wait = AsyncMock()
    wrapper = _async_wrapper(target)
    asyncio.run(wrapper.wait("gen"))
    target.wait.assert_awaited_once_with("gen", interval=0.1)


# ---- Setter parity ------------------------------------------------------


def test_sync_wrapper_set_deferrable_delegates() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.set_deferrable(True)
    target.set_deferrable.assert_called_once_with(True)


def test_async_wrapper_set_deferrable_awaits() -> None:
    target = MagicMock()
    target.set_deferrable = AsyncMock()
    wrapper = _async_wrapper(target)
    asyncio.run(wrapper.set_deferrable(True))
    target.set_deferrable.assert_awaited_once_with(True)


def test_sync_wrapper_set_isolation_level_delegates() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper.set_isolation_level("SERIALIZABLE")
    target.set_isolation_level.assert_called_once_with("SERIALIZABLE")


def test_sync_wrapper_set_read_only_routes_through_property() -> None:
    """sync wrapper's set_read_only uses the existing plugin-aware
    read_only property setter, not a direct target-connection call."""
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    # Stub out the property setter path so we observe plugin_manager.
    wrapper._plugin_manager.execute = MagicMock(return_value=None)
    wrapper.set_read_only(True)
    # Plugin-manager was called (property setter routes through plugin chain).
    wrapper._plugin_manager.execute.assert_called_once()


def test_sync_wrapper_set_autocommit_routes_through_property() -> None:
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    wrapper._plugin_manager.execute = MagicMock(return_value=None)
    wrapper.set_autocommit(True)
    wrapper._plugin_manager.execute.assert_called_once()


def test_async_wrapper_set_read_only_awaits_target() -> None:
    target = MagicMock()
    target.set_read_only = AsyncMock()
    wrapper = _async_wrapper(target)
    asyncio.run(wrapper.set_read_only(True))
    target.set_read_only.assert_awaited_once_with(True)


# ---- Plugin-chain bypass assertions -------------------------------------


@pytest.mark.parametrize("call", [
    ("info",), ("broken",), ("adapters",), ("fileno",), ("cancel",),
    ("pipeline",), ("notifies",), ("xid", 1, "g", "b"),
])
def test_sync_wrapper_passthroughs_bypass_plugin_chain(call) -> None:
    """Property / method accessors that reflect local client state
    must never call through the plugin pipeline."""
    target = MagicMock()
    wrapper = _sync_wrapper(target)
    name, *args = call
    attr = getattr(wrapper, name)
    if callable(attr):
        attr(*args)
    wrapper._plugin_manager.execute.assert_not_called()


# ---- Signature-shape sanity check --------------------------------------


def test_async_wrapper_split_methods_are_correct_asyncness() -> None:
    """Verify that the sync-vs-async shape on the async wrapper matches
    psycopg3.AsyncConnection's actual method asyncness. Regression
    guard for future accidental async-def flips."""
    import inspect
    async_methods = {
        "cancel_safe", "execute", "wait",
        "set_deferrable", "set_read_only", "set_autocommit",
        "set_isolation_level",
    }
    sync_methods = {
        "fileno", "cancel", "xid", "pipeline", "notifies", "transaction",
        "add_notice_handler", "remove_notice_handler",
        "add_notify_handler", "remove_notify_handler",
    }
    for name in async_methods:
        method = getattr(AsyncAwsWrapperConnection, name)
        assert inspect.iscoroutinefunction(method), (
            f"AsyncAwsWrapperConnection.{name} should be async")
    for name in sync_methods:
        method = getattr(AsyncAwsWrapperConnection, name)
        assert not inspect.iscoroutinefunction(method), (
            f"AsyncAwsWrapperConnection.{name} should be sync "
            f"(matches psycopg3.AsyncConnection)")
