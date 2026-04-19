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

"""``AsyncDefaultPlugin`` -- the terminal plugin in every async pipeline.

Subscribes to every method. On ``connect`` it hands off to the driver
dialect's async ``connect``. On ``execute`` it just awaits the target
driver func.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Awaitable, Callable, Set

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncDefaultPlugin(AsyncPlugin):
    """Terminal plugin. Always last in the pipeline; drives the raw driver call."""

    @property
    def subscribed_methods(self) -> Set[str]:
        return {DbApiMethod.ALL.method_name}

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        # The pipeline terminates here. Open the connection via the driver
        # dialect rather than blindly calling connect_func -- the dialect
        # knows the target driver's async connect signature and props shape.
        return await driver_dialect.connect(host_info, props, target_driver_func)

    async def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable[..., Awaitable[Any]]) -> Any:
        return await driver_dialect.connect(host_info, props, target_driver_func)

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        return await execute_func()
