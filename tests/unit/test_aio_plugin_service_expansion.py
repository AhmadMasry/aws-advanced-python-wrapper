# tests/unit/test_aio_plugin_service_expansion.py
from __future__ import annotations

from typing import Optional
from unittest.mock import MagicMock

from aws_advanced_python_wrapper.aio.driver_dialect.base import \
    AsyncDriverDialect
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
from aws_advanced_python_wrapper.host_availability import HostAvailability
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


def test_is_network_exception_returns_false_when_no_dialect():
    svc = _make_service()
    assert svc.is_network_exception(error=Exception("boom")) is False


def test_is_network_exception_delegates_to_dialect_handler():
    dialect = MagicMock(spec=DatabaseDialect)
    handler = MagicMock()
    handler.is_network_exception.return_value = True
    dialect.exception_handler = handler
    svc = _make_service(database_dialect=dialect)
    err = Exception("connection reset")
    assert svc.is_network_exception(error=err) is True
    handler.is_network_exception.assert_called_once_with(error=err, sql_state=None)


def test_is_login_exception_delegates_to_dialect_handler():
    dialect = MagicMock(spec=DatabaseDialect)
    handler = MagicMock()
    handler.is_login_exception.return_value = True
    dialect.exception_handler = handler
    svc = _make_service(database_dialect=dialect)
    err = Exception("auth failed")
    assert svc.is_login_exception(error=err, sql_state="28000") is True
    handler.is_login_exception.assert_called_once_with(error=err, sql_state="28000")


def test_get_availability_returns_none_when_unset():
    svc = _make_service()
    assert svc.get_availability("host-1.cluster.example") is None


def test_set_then_get_availability():
    svc = _make_service()
    svc.set_availability(frozenset({"host-1.cluster.example"}), HostAvailability.UNAVAILABLE)
    assert svc.get_availability("host-1.cluster.example") == HostAvailability.UNAVAILABLE


def test_set_availability_covers_all_aliases():
    svc = _make_service()
    aliases = frozenset({"h1.example", "h1-alias.example"})
    svc.set_availability(aliases, HostAvailability.UNAVAILABLE)
    for alias in aliases:
        assert svc.get_availability(alias) == HostAvailability.UNAVAILABLE
