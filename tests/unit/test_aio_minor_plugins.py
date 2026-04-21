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

"""F3-B SP-8: minor async plugins."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.aio.minor_plugins import (
    AsyncConnectTimePlugin, AsyncCustomEndpointPlugin, AsyncDeveloperPlugin,
    AsyncExecuteTimePlugin)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties


def test_connect_time_plugin_accumulates_elapsed_time():
    async def _body() -> None:
        p = AsyncConnectTimePlugin()

        async def _connect() -> str:
            await asyncio.sleep(0.005)
            return "ok"

        await p.connect(
            target_driver_func=lambda: None,
            driver_dialect=MagicMock(),
            host_info=HostInfo(host="h", port=5432),
            props=Properties({}),
            is_initial_connection=True,
            connect_func=_connect,
        )
        assert p.connect_count == 1
        assert p.total_connect_time_ns > 0

    asyncio.run(_body())


def test_connect_time_plugin_records_even_when_connect_raises():
    async def _body() -> None:
        p = AsyncConnectTimePlugin()

        async def _raiser() -> None:
            raise RuntimeError("fail")

        with pytest.raises(RuntimeError):
            await p.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(host="h", port=5432),
                props=Properties({}),
                is_initial_connection=True,
                connect_func=_raiser,
            )
        assert p.connect_count == 1

    asyncio.run(_body())


def test_execute_time_plugin_accumulates_elapsed_time():
    async def _body() -> None:
        p = AsyncExecuteTimePlugin()

        async def _work() -> str:
            await asyncio.sleep(0.005)
            return "rows"

        assert await p.execute(object(), "Cursor.execute", _work) == "rows"
        assert p.execute_count == 1
        assert p.total_execute_time_ns > 0

    asyncio.run(_body())


def test_execute_time_plugin_subscribed_methods_covers_cursor_ops():
    p = AsyncExecuteTimePlugin()
    assert DbApiMethod.CURSOR_EXECUTE.method_name in p.subscribed_methods
    assert DbApiMethod.CURSOR_FETCHONE.method_name in p.subscribed_methods


def test_developer_plugin_injects_configured_exception_then_passes_through():
    async def _body() -> None:
        p = AsyncDeveloperPlugin()
        p.set_next_exception(ValueError("injected"))

        async def _work() -> str:
            return "clean"

        with pytest.raises(ValueError):
            await p.execute(object(), "Cursor.execute", _work)
        # Second call: exception was one-shot, so pass-through.
        assert await p.execute(object(), "Cursor.execute", _work) == "clean"

    asyncio.run(_body())


def test_developer_plugin_pass_through_when_no_injection():
    async def _body() -> None:
        p = AsyncDeveloperPlugin()

        async def _work() -> int:
            return 42

        assert await p.execute(object(), "Cursor.execute", _work) == 42

    asyncio.run(_body())


def test_custom_endpoint_plugin_is_stub():
    p = AsyncCustomEndpointPlugin()
    assert p.subscribed_methods == set()
