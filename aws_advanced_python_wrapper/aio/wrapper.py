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

"""``AsyncAwsWrapperConnection`` -- async facade over the plugin pipeline.

Parallel to sync :class:`AwsWrapperConnection`. Per SP-0 decision D2, this
class uses an explicit ``Async`` prefix matching psycopg's ``AsyncConnection``
convention.

This is an SP-0 stub: class header + method signatures only. Real async
connection behavior lands in SP-2 alongside the psycopg driver dialect.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Type, Union


class AsyncAwsWrapperConnection:
    """Async counterpart of :class:`AwsWrapperConnection`.

    In SP-2, this will own an ``AsyncPluginService`` + ``AsyncPluginManager``
    and delegate every DBAPI-async method through the plugin pipeline. For
    SP-0 the signatures are declared and every method raises
    ``NotImplementedError`` so the shape is locked and downstream sub-projects
    can type-check against it.
    """

    __module__ = "aws_advanced_python_wrapper.aio"

    @staticmethod
    async def connect(
            target: Union[None, str, Callable] = None,
            conninfo: str = "",
            *args: Any,
            **kwargs: Any) -> AsyncAwsWrapperConnection:
        """Open a new async wrapper connection. SP-2 delivers the implementation."""
        raise NotImplementedError(
            "AsyncAwsWrapperConnection.connect is a stub; SP-2 delivers it."
        )

    async def close(self) -> None:
        raise NotImplementedError

    async def cursor(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def commit(self) -> None:
        raise NotImplementedError

    async def rollback(self) -> None:
        raise NotImplementedError

    async def __aenter__(self) -> AsyncAwsWrapperConnection:
        raise NotImplementedError

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Any) -> None:
        raise NotImplementedError
