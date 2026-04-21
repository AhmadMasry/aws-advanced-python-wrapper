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

"""F3-B Phase H.2: stub plugins for unimplemented async codes.

Verifies that the six sync plugin codes without a real async port
register as pass-through stubs in ``PLUGIN_FACTORIES`` -- users can
keep their ``plugins="..."`` config identical across sync/async
wrappers without hitting 'unknown plugin' errors.
"""

from __future__ import annotations

import logging

from aws_advanced_python_wrapper.aio.plugin_factory import (
    PLUGIN_FACTORIES, resolve_plugin_factories)
from aws_advanced_python_wrapper.aio.stub_plugins import (
    AsyncAuroraInitialConnectionStrategyStubPlugin, AsyncBlueGreenStubPlugin,
    AsyncFastestResponseStrategyStubPlugin, AsyncLimitlessStubPlugin,
    AsyncSimpleReadWriteSplittingStubPlugin, AsyncStaleDnsStubPlugin)

STUB_CODES_AND_CLASSES = [
    ("srw", AsyncSimpleReadWriteSplittingStubPlugin),
    ("stale_dns", AsyncStaleDnsStubPlugin),
    ("initial_connection", AsyncAuroraInitialConnectionStrategyStubPlugin),
    ("limitless", AsyncLimitlessStubPlugin),
    ("bg", AsyncBlueGreenStubPlugin),
    ("fastest_response_strategy", AsyncFastestResponseStrategyStubPlugin),
]


def test_all_stub_codes_registered_in_plugin_factories():
    for code, _ in STUB_CODES_AND_CLASSES:
        assert code in PLUGIN_FACTORIES, (
            f"Expected plugin code '{code}' to be registered")


def test_each_stub_subscribes_to_nothing():
    """Stubs must not intercept any pipeline method."""
    for _, cls in STUB_CODES_AND_CLASSES:
        plugin = cls()
        assert plugin.subscribed_methods == set(), (
            f"{cls.__name__} must not subscribe to any method")


def test_stubs_log_warning_on_construction(caplog):
    caplog.set_level(logging.WARNING)
    for code, cls in STUB_CODES_AND_CLASSES:
        caplog.clear()
        cls()
        messages = [r.getMessage() for r in caplog.records
                    if r.levelno >= logging.WARNING]
        assert any(code in m for m in messages), (
            f"Expected warning for stub '{code}', got: {messages}")


def test_resolve_plugin_factories_returns_stub_factory_for_each_code():
    """resolve_plugin_factories round-trips the stub codes."""
    for code, _ in STUB_CODES_AND_CLASSES:
        factories = resolve_plugin_factories([code])
        assert len(factories) == 1
