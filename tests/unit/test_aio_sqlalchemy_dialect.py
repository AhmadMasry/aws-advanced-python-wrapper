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

"""F3-B SP-9: SQLAlchemy async dialect registration.

Verifies the async dialect class, entry-point registration under both
the bare and driver-slot URL forms, URL resolution via ``make_url``, and
the ``wrapper_plugins`` -> ``plugins`` URL translation.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql.psycopg import PGDialectAsync_psycopg
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine

import aws_advanced_python_wrapper.aio.psycopg as aio_wrapper_psycopg
from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection
from aws_advanced_python_wrapper.sqlalchemy_dialects.pg_async import (
    AwsWrapperAsyncPsycopgAdaptDBAPI, AwsWrapperPGPsycopgAsyncDialect)

# ---- Class-shape tests --------------------------------------------------


def test_async_dialect_subclasses_pgdialectasync_psycopg():
    assert issubclass(AwsWrapperPGPsycopgAsyncDialect, PGDialectAsync_psycopg)


def test_async_dialect_is_async_flag():
    assert AwsWrapperPGPsycopgAsyncDialect.is_async is True


def test_async_dialect_driver_attr():
    assert AwsWrapperPGPsycopgAsyncDialect.driver == "psycopg_async"


def test_async_dialect_import_dbapi_returns_adapter_wrapping_aio_submodule():
    adapter = AwsWrapperPGPsycopgAsyncDialect.import_dbapi()
    assert isinstance(adapter, AwsWrapperAsyncPsycopgAdaptDBAPI)
    # The adapter exposes the wrapped aio submodule via ``.psycopg`` for
    # parity with SA's own PsycopgAdaptDBAPI attribute name.
    assert adapter.psycopg is aio_wrapper_psycopg
    # PEP 249 surface (Error, apilevel, paramstyle, ...) copied onto adapter.
    assert adapter.Error is aio_wrapper_psycopg.Error
    assert adapter.apilevel == "2.0"


# ---- Registry tests -----------------------------------------------------


def test_registry_resolves_aws_wrapper_postgresql_async():
    cls = registry.load("aws_wrapper_postgresql_async")
    assert cls is AwsWrapperPGPsycopgAsyncDialect


def test_registry_resolves_aws_wrapper_postgresql_psycopg_async():
    cls = registry.load("aws_wrapper_postgresql.psycopg_async")
    assert cls is AwsWrapperPGPsycopgAsyncDialect


def test_url_get_dialect_async_bare_form():
    url = make_url(
        "aws_wrapper_postgresql_async://u:p@h:5432/db?wrapper_dialect=aurora-pg"
    )
    assert url.get_dialect() is AwsWrapperPGPsycopgAsyncDialect


def test_url_get_dialect_async_driver_slot_form():
    url = make_url(
        "aws_wrapper_postgresql+psycopg_async://u:p@h:5432/db?wrapper_dialect=aurora-pg"
    )
    assert url.get_dialect() is AwsWrapperPGPsycopgAsyncDialect


# ---- URL kwargs passthrough --------------------------------------------


def test_async_url_query_args_flow_through_to_async_wrapper_connect(mocker):
    """URL query args (incl. ``wrapper_plugins`` alias) reach
    ``AsyncAwsWrapperConnection.connect`` unaltered except for the
    alias rename."""
    fake_raw_conn = MagicMock()
    fake_raw_conn.close = AsyncMock()

    mock_connect = mocker.patch.object(
        AsyncAwsWrapperConnection,
        "connect",
        new_callable=AsyncMock,
        return_value=fake_raw_conn,
    )

    async def _body() -> None:
        engine = create_async_engine(
            "aws_wrapper_postgresql+psycopg_async://u:p@h:5432/db"
            "?wrapper_dialect=aurora-pg&wrapper_plugins=failover,efm"
        )
        try:
            async with engine.connect():
                pass
        except Exception:
            # The MagicMock conn may not satisfy every SA probe; we care only
            # that AsyncAwsWrapperConnection.connect was invoked with the right
            # kwargs.
            pass
        finally:
            await engine.dispose()

    asyncio.run(_body())

    assert mock_connect.called, "AsyncAwsWrapperConnection.connect was never invoked"
    _args, kwargs = mock_connect.call_args
    assert kwargs.get("wrapper_dialect") == "aurora-pg"
    assert kwargs.get("plugins") == "failover,efm"
    assert "wrapper_plugins" not in kwargs, (
        "dialect should have renamed wrapper_plugins -> plugins before the connect call"
    )


def test_async_dialect_create_connect_args_renames_wrapper_plugins():
    """Unit-level check: create_connect_args renames the alias even when
    invoked directly (no engine involved)."""
    dialect = AwsWrapperPGPsycopgAsyncDialect()
    url = make_url(
        "aws_wrapper_postgresql+psycopg_async://u:p@h:5432/db"
        "?wrapper_dialect=aurora-pg&wrapper_plugins=failover"
    )
    _args, kwargs = dialect.create_connect_args(url)
    assert kwargs.get("wrapper_dialect") == "aurora-pg"
    assert kwargs.get("plugins") == "failover"
    assert "wrapper_plugins" not in kwargs
