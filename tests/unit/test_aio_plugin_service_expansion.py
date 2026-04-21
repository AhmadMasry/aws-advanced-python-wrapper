# tests/unit/test_aio_plugin_service_expansion.py
from __future__ import annotations

from typing import Optional
from unittest.mock import MagicMock

from aws_advanced_python_wrapper.aio.driver_dialect.base import \
    AsyncDriverDialect
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
from aws_advanced_python_wrapper.utils.properties import Properties


def _make_service(database_dialect: Optional[DatabaseDialect] = None) -> AsyncPluginServiceImpl:
    driver_dialect = MagicMock(spec=AsyncDriverDialect)
    driver_dialect.network_bound_methods = set()
    svc = AsyncPluginServiceImpl(Properties(), driver_dialect)
    if database_dialect is not None:
        svc.database_dialect = database_dialect
    return svc


def test_database_dialect_defaults_to_none():
    svc = _make_service()
    assert svc.database_dialect is None


def test_database_dialect_is_settable():
    dialect = MagicMock(spec=DatabaseDialect)
    svc = _make_service(database_dialect=dialect)
    assert svc.database_dialect is dialect
