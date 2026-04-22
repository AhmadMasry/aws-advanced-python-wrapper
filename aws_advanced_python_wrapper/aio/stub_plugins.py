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

"""Pass-through stubs for plugins not yet ported to async.

Registered in :mod:`aws_advanced_python_wrapper.aio.plugin_factory` so
users can include these codes in ``plugins="..."`` config strings
without tripping unknown-plugin errors. Each stub logs a single warning
on construction to surface the gap at runtime; no pipeline hooks fire.

Actual async ports land as separate phases when consumers exercise
these features against the async wrapper.
"""

from __future__ import annotations

from typing import Set

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.utils.log import Logger

logger = Logger(__name__)


class _AsyncStubPlugin(AsyncPlugin):
    """Shared base for async plugin stubs.

    Subclasses set :attr:`_STUB_NAME` to the plugin code they stand in
    for. Construction logs a WARNING; the plugin subscribes to no
    pipeline methods, so the plugin manager skips it for every call.
    """

    _STUB_NAME: str = "unknown"

    def __init__(self) -> None:
        # Pre-format the message and pass it to the logger with no extra
        # args -- the wrapper Logger tries a resource-bundle lookup when
        # args are supplied, which would fail for our ad-hoc strings.
        # With no args it falls back to the raw message cleanly.
        logger.warning(
            "Plugin '{0}' is not implemented in the async wrapper; "
            "connections using it will behave as if the plugin were "
            "absent.".format(self._STUB_NAME)
        )

    @property
    def subscribed_methods(self) -> Set[str]:
        return set()


class AsyncBlueGreenStubPlugin(_AsyncStubPlugin):
    """Stub for sync BlueGreenPlugin ('bg')."""
    _STUB_NAME = "bg"


__all__ = [
    "AsyncBlueGreenStubPlugin",
]
