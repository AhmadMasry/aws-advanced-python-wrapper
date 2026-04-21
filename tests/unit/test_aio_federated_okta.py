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

"""Tasks 1-C + 1-D: async federated (SAML) + Okta auth plugins."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aws_advanced_python_wrapper.aio.federated_auth_plugins import (
    AsyncFederatedAuthPlugin, AsyncOktaAuthPlugin)
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties

_FAKE_SAML = "PHNhbWw6UmVzcG9uc2Ugeg=="  # noqa
_FAKE_CREDS = {
    "AccessKeyId": "AKIA123",
    "SecretAccessKey": "sek",
    "SessionToken": "tok",
}


def _svc(props: Properties) -> AsyncPluginServiceImpl:
    return AsyncPluginServiceImpl(
        props, MagicMock(), HostInfo("rds.example", 5432)
    )


def _federated_props(**overrides: str) -> Properties:
    base = {
        "host": "rds.example", "port": "5432",
        "db_user": "app_user",
        "idp_endpoint": "adfs.example.com",
        "idp_username": "alice",
        "idp_password": "s3cret",
        "iam_role_arn": "arn:aws:iam::1:role/R",
        "iam_idp_arn": "arn:aws:iam::1:saml-provider/Corp",
        "iam_region": "us-east-1",
        "iam_host": "rds.example",
        "ssl_secure": "true",
    }
    base.update(overrides)
    return Properties(base)


def _okta_props(**overrides: str) -> Properties:
    base = dict(_federated_props())
    base.update({
        "idp_endpoint": "mycorp.okta.com",
        "app_id": "abc123",
    })
    base.update(overrides)
    return Properties(base)


# ---- Federated plugin --------------------------------------------------


def test_federated_plugin_subscription():
    p = AsyncFederatedAuthPlugin(_svc(_federated_props()), _federated_props())
    assert p.subscribed_methods == {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.FORCE_CONNECT.method_name,
    }


def test_federated_plugin_resolves_credentials_end_to_end():
    async def _body() -> None:
        props = _federated_props()
        plugin = AsyncFederatedAuthPlugin(_svc(props), props)

        with patch.object(
                AsyncFederatedAuthPlugin, "_fetch_saml_assertion",
                new=AsyncMock(return_value=_FAKE_SAML),
        ), patch.object(
                AsyncFederatedAuthPlugin, "_sts_assume_role_with_saml_blocking",
                return_value=_FAKE_CREDS,
        ), patch.object(
                AsyncFederatedAuthPlugin, "_generate_rds_token_blocking",
                return_value="iam-token-xyz",
        ):
            raw = MagicMock()

            async def _cf() -> object:
                return raw

            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo("rds.example", 5432),
                props=props,
                is_initial_connection=True,
                connect_func=_cf,
            )
            assert props.get("user") == "app_user"
            assert props.get("password") == "iam-token-xyz"

    asyncio.run(_body())


def test_federated_plugin_caches_rds_token():
    async def _body() -> None:
        props = _federated_props()
        plugin = AsyncFederatedAuthPlugin(_svc(props), props)

        call_count = [0]

        def _gen(host, port, user, region, creds):
            call_count[0] += 1
            return f"tok-{call_count[0]}"

        with patch.object(
                AsyncFederatedAuthPlugin, "_fetch_saml_assertion",
                new=AsyncMock(return_value=_FAKE_SAML),
        ), patch.object(
                AsyncFederatedAuthPlugin, "_sts_assume_role_with_saml_blocking",
                return_value=_FAKE_CREDS,
        ), patch.object(
                AsyncFederatedAuthPlugin, "_generate_rds_token_blocking",
                side_effect=_gen,
        ):
            async def _cf() -> object:
                return MagicMock()

            for _ in range(3):
                fresh = Properties(dict(props))
                await plugin.connect(
                    target_driver_func=lambda: None,
                    driver_dialect=MagicMock(),
                    host_info=HostInfo("rds.example", 5432),
                    props=fresh,
                    is_initial_connection=True,
                    connect_func=_cf,
                )
        # Token cache hit for calls 2 and 3.
        assert call_count[0] == 1

    asyncio.run(_body())


def test_federated_plugin_missing_db_user_raises():
    async def _body() -> None:
        props = _federated_props()
        del props["db_user"]
        plugin = AsyncFederatedAuthPlugin(_svc(props), props)

        async def _cf() -> object:
            return MagicMock()

        with pytest.raises(AwsWrapperError):
            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo("rds.example", 5432),
                props=props,
                is_initial_connection=True,
                connect_func=_cf,
            )

    asyncio.run(_body())


def test_federated_plugin_missing_role_arn_raises():
    async def _body() -> None:
        props = _federated_props()
        del props["iam_role_arn"]
        plugin = AsyncFederatedAuthPlugin(_svc(props), props)

        with patch.object(
                AsyncFederatedAuthPlugin, "_fetch_saml_assertion",
                new=AsyncMock(return_value=_FAKE_SAML),
        ):
            async def _cf() -> object:
                return MagicMock()

            with pytest.raises(AwsWrapperError):
                await plugin.connect(
                    target_driver_func=lambda: None,
                    driver_dialect=MagicMock(),
                    host_info=HostInfo("rds.example", 5432),
                    props=props,
                    is_initial_connection=True,
                    connect_func=_cf,
                )

    asyncio.run(_body())


def test_federated_plugin_extracts_saml_assertion_from_html():
    html = (
        '<html><form>'
        '<input type="hidden" name="SAMLResponse" value="BASE64SAML=="/>'
        '<input name="foo" value="bar"/>'
        '</form></html>'
    )
    assert AsyncFederatedAuthPlugin._extract_saml_assertion(html) == "BASE64SAML=="


def test_federated_plugin_raises_when_saml_missing_from_html():
    html = "<html>no saml here</html>"
    with pytest.raises(AwsWrapperError):
        AsyncFederatedAuthPlugin._extract_saml_assertion(html)


# ---- Okta plugin -------------------------------------------------------


def test_okta_plugin_subscribed_is_connect_only():
    p = AsyncOktaAuthPlugin(_svc(_okta_props()), _okta_props())
    assert p.subscribed_methods == {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.FORCE_CONNECT.method_name,
    }


def test_okta_plugin_inherits_rds_token_path_from_federated():
    """Token resolution uses the same STS + RDS flow as Federated."""
    async def _body() -> None:
        props = _okta_props()
        plugin = AsyncOktaAuthPlugin(_svc(props), props)

        with patch.object(
                AsyncOktaAuthPlugin, "_fetch_saml_assertion",
                new=AsyncMock(return_value=_FAKE_SAML),
        ), patch.object(
                AsyncFederatedAuthPlugin, "_sts_assume_role_with_saml_blocking",
                return_value=_FAKE_CREDS,
        ), patch.object(
                AsyncFederatedAuthPlugin, "_generate_rds_token_blocking",
                return_value="okta-rds-token",
        ):
            async def _cf() -> object:
                return MagicMock()

            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo("rds.example", 5432),
                props=props,
                is_initial_connection=True,
                connect_func=_cf,
            )
            assert props.get("user") == "app_user"
            assert props.get("password") == "okta-rds-token"

    asyncio.run(_body())


def test_okta_plugin_missing_app_id_raises_during_saml_fetch():
    async def _body() -> None:
        # Force the real _fetch_saml_assertion (not patched) to run and
        # surface the missing-app_id check.
        props = _okta_props()
        del props["app_id"]
        plugin = AsyncOktaAuthPlugin(_svc(props), props)

        async def _cf() -> object:
            return MagicMock()

        with pytest.raises(AwsWrapperError):
            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo("rds.example", 5432),
                props=props,
                is_initial_connection=True,
                connect_func=_cf,
            )

    asyncio.run(_body())


# ---- Factory integration -----------------------------------------------


def test_factory_builds_federated_and_okta_plugins_from_string():
    from aws_advanced_python_wrapper.aio.plugin_factory import \
        build_async_plugins

    props = Properties({
        "host": "h", "port": "5432",
        "plugins": "federated_auth,okta",
    })
    plugins = build_async_plugins(_svc(props), props)
    types = {type(p).__name__ for p in plugins}
    assert "AsyncFederatedAuthPlugin" in types
    assert "AsyncOktaAuthPlugin" in types
