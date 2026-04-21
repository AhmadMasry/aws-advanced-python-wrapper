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

"""F3-B SP-7: async auth plugins (IAM + Secrets Manager)."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from aws_advanced_python_wrapper.aio.auth_plugins import (
    AsyncAwsSecretsManagerPlugin, AsyncIamAuthPlugin)
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties


def _svc(props: Properties) -> AsyncPluginServiceImpl:
    return AsyncPluginServiceImpl(props, MagicMock(), HostInfo(host="h", port=5432))


# ---- IAM plugin --------------------------------------------------------


def test_iam_plugin_subscription():
    props = Properties({"host": "h.example", "port": "5432", "user": "app"})
    p = AsyncIamAuthPlugin(_svc(props), props)
    assert p.subscribed_methods == {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.FORCE_CONNECT.method_name,
    }


def test_iam_plugin_generates_token_and_injects_as_password():
    async def _body() -> None:
        props = Properties({
            "host": "h.example", "port": "5432",
            "user": "db_user", "iam_region": "us-east-1",
        })
        plugin = AsyncIamAuthPlugin(_svc(props), props)

        # Patch the sync boto3 call; AsyncIamAuthPlugin awaits via to_thread.
        with patch.object(
            AsyncIamAuthPlugin,
            "_generate_token_blocking",
            return_value="iam-token-abc",
        ):
            async def _connect_func() -> object:
                return "conn-sentinel"

            result = await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(host="h.example", port=5432),
                props=props,
                is_initial_connection=True,
                connect_func=_connect_func,
            )
            assert result == "conn-sentinel"
            assert props.get("user") == "db_user"
            assert props.get("password") == "iam-token-abc"

    asyncio.run(_body())


def test_iam_plugin_caches_token_within_expiration_window():
    async def _body() -> None:
        props = Properties({
            "host": "h.example", "port": "5432",
            "user": "db_user",
        })
        plugin = AsyncIamAuthPlugin(_svc(props), props)

        call_count = [0]

        def _gen_token(host, port, user, region):
            call_count[0] += 1
            return f"tok-{call_count[0]}"

        with patch.object(
            AsyncIamAuthPlugin,
            "_generate_token_blocking",
            side_effect=_gen_token,
        ):
            async def _connect_func() -> object:
                return None

            # First connect triggers token generation.
            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(host="h.example", port=5432),
                props=Properties(dict(props)),
                is_initial_connection=True,
                connect_func=_connect_func,
            )
            # Second connect reuses cached token.
            second_props = Properties(dict(props))
            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(host="h.example", port=5432),
                props=second_props,
                is_initial_connection=False,
                connect_func=_connect_func,
            )
            assert call_count[0] == 1
            assert second_props.get("password") == "tok-1"

    asyncio.run(_body())


def test_iam_plugin_raises_when_user_is_missing():
    async def _body() -> None:
        props = Properties({"host": "h.example", "port": "5432"})
        plugin = AsyncIamAuthPlugin(_svc(props), props)

        async def _connect_func() -> None:
            return None

        with pytest.raises(AwsWrapperError):
            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(host="h.example", port=5432),
                props=props,
                is_initial_connection=True,
                connect_func=_connect_func,
            )

    asyncio.run(_body())


# ---- Secrets Manager plugin --------------------------------------------


def test_secrets_manager_subscription():
    props = Properties({
        "host": "h", "port": "5432",
        "secrets_manager_secret_id": "my-secret",
    })
    p = AsyncAwsSecretsManagerPlugin(_svc(props), props)
    assert p.subscribed_methods == {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.FORCE_CONNECT.method_name,
    }


def test_secrets_manager_fetches_and_injects_credentials():
    async def _body() -> None:
        props = Properties({
            "host": "h", "port": "5432",
            "secrets_manager_secret_id": "my-secret",
            "secrets_manager_region": "us-west-2",
        })
        plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)

        with patch.object(
            AsyncAwsSecretsManagerPlugin,
            "_fetch_secret_blocking",
            return_value={"username": "secret_user", "password": "secret_pwd"},
        ):
            async def _connect_func() -> object:
                return "opened"

            result = await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(host="h", port=5432),
                props=props,
                is_initial_connection=True,
                connect_func=_connect_func,
            )
            assert result == "opened"
            assert props.get("user") == "secret_user"
            assert props.get("password") == "secret_pwd"

    asyncio.run(_body())


def test_secrets_manager_uses_custom_keys():
    async def _body() -> None:
        props = Properties({
            "host": "h", "port": "5432",
            "secrets_manager_secret_id": "my-secret",
            "secrets_manager_secret_username_key": "db_login",
            "secrets_manager_secret_password_key": "db_pass",
        })
        plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)

        with patch.object(
            AsyncAwsSecretsManagerPlugin,
            "_fetch_secret_blocking",
            return_value={"db_login": "u", "db_pass": "p"},
        ):
            async def _connect_func() -> None:
                return None

            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(host="h", port=5432),
                props=props,
                is_initial_connection=True,
                connect_func=_connect_func,
            )
            assert props.get("user") == "u"
            assert props.get("password") == "p"

    asyncio.run(_body())


def test_secrets_manager_raises_when_secret_id_missing():
    async def _body() -> None:
        props = Properties({"host": "h", "port": "5432"})
        plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)

        async def _connect_func() -> None:
            return None

        with pytest.raises(AwsWrapperError):
            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(host="h", port=5432),
                props=props,
                is_initial_connection=True,
                connect_func=_connect_func,
            )

    asyncio.run(_body())


def test_secrets_manager_caches_result():
    async def _body() -> None:
        props = Properties({
            "host": "h", "port": "5432",
            "secrets_manager_secret_id": "my-secret",
        })
        plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)

        call_count = [0]

        def _fetch(secret_id, region):
            call_count[0] += 1
            return {"username": f"u{call_count[0]}", "password": f"p{call_count[0]}"}

        with patch.object(
            AsyncAwsSecretsManagerPlugin,
            "_fetch_secret_blocking",
            side_effect=_fetch,
        ):
            async def _connect_func() -> None:
                return None

            for _ in range(3):
                fresh = Properties(dict(props))
                await plugin.connect(
                    target_driver_func=lambda: None,
                    driver_dialect=MagicMock(),
                    host_info=HostInfo(host="h", port=5432),
                    props=fresh,
                    is_initial_connection=True,
                    connect_func=_connect_func,
                )
        assert call_count[0] == 1

    asyncio.run(_body())


# ---- AsyncAuthPluginBase: FORCE_CONNECT + retry-on-login (E.1) --------


def test_base_plugin_subscribes_to_connect_and_force_connect():
    from aws_advanced_python_wrapper.aio.auth_plugins import \
        AsyncAuthPluginBase

    class _Stub(AsyncAuthPluginBase):
        async def _resolve_credentials(self, host_info, props):
            return ("u", "p", False)

        def _invalidate_cache(self, host_info, props):
            pass

    props = Properties({"host": "h", "port": "5432"})
    plugin = _Stub(_svc(props), props)
    assert DbApiMethod.CONNECT.method_name in plugin.subscribed_methods
    assert DbApiMethod.FORCE_CONNECT.method_name in plugin.subscribed_methods


def test_base_plugin_retries_on_login_exception_when_cached():
    """Cached credentials + login exception -> invalidate + re-resolve + retry."""
    from aws_advanced_python_wrapper.aio.auth_plugins import \
        AsyncAuthPluginBase

    resolve_calls = []
    invalidate_calls = []

    class _Stub(AsyncAuthPluginBase):
        _next_was_cached = True

        async def _resolve_credentials(self, host_info, props):
            resolve_calls.append(self._next_was_cached)
            was_cached = self._next_was_cached
            self._next_was_cached = False  # next call returns fresh
            return ("u", f"token-{len(resolve_calls)}", was_cached)

        def _invalidate_cache(self, host_info, props):
            invalidate_calls.append(1)

    props = Properties({"host": "h", "port": "5432"})
    svc = _svc(props)
    svc.is_login_exception = MagicMock(return_value=True)
    plugin = _Stub(svc, props)

    attempt = [0]

    async def _connect_func():
        attempt[0] += 1
        if attempt[0] == 1:
            raise Exception("auth failed")
        return MagicMock(name="conn")

    host = HostInfo(host="h", port=5432)
    result = asyncio.run(plugin.connect(
        MagicMock(), MagicMock(), host, props, True, _connect_func))

    assert result is not None
    assert invalidate_calls == [1]
    assert len(resolve_calls) == 2
    assert attempt[0] == 2


def test_base_plugin_does_not_retry_when_credentials_were_fresh():
    """Fresh credentials + login exception -> propagate (no retry spin)."""
    from aws_advanced_python_wrapper.aio.auth_plugins import \
        AsyncAuthPluginBase

    class _Stub(AsyncAuthPluginBase):
        async def _resolve_credentials(self, host_info, props):
            return ("u", "fresh-token", False)

        def _invalidate_cache(self, host_info, props):
            pass

    props = Properties({"host": "h", "port": "5432"})
    svc = _svc(props)
    svc.is_login_exception = MagicMock(return_value=True)
    plugin = _Stub(svc, props)

    async def _connect_func():
        raise Exception("auth failed")

    host = HostInfo(host="h", port=5432)
    with pytest.raises(Exception, match="auth failed"):
        asyncio.run(plugin.connect(
            MagicMock(), MagicMock(), host, props, True, _connect_func))


def test_base_plugin_does_not_retry_on_non_login_exceptions():
    from aws_advanced_python_wrapper.aio.auth_plugins import \
        AsyncAuthPluginBase

    class _Stub(AsyncAuthPluginBase):
        async def _resolve_credentials(self, host_info, props):
            return ("u", "p", True)

        def _invalidate_cache(self, host_info, props):
            pass

    props = Properties({"host": "h", "port": "5432"})
    svc = _svc(props)
    svc.is_login_exception = MagicMock(return_value=False)
    plugin = _Stub(svc, props)

    async def _connect_func():
        raise Exception("network boom")

    host = HostInfo(host="h", port=5432)
    with pytest.raises(Exception, match="network boom"):
        asyncio.run(plugin.connect(
            MagicMock(), MagicMock(), host, props, True, _connect_func))


def test_base_plugin_force_connect_uses_same_retry_flow():
    from aws_advanced_python_wrapper.aio.auth_plugins import \
        AsyncAuthPluginBase

    class _Stub(AsyncAuthPluginBase):
        _cached = True

        async def _resolve_credentials(self, host_info, props):
            was = self._cached
            self._cached = False
            return ("u", "t", was)

        def _invalidate_cache(self, host_info, props):
            pass

    props = Properties({"host": "h", "port": "5432"})
    svc = _svc(props)
    svc.is_login_exception = MagicMock(return_value=True)
    plugin = _Stub(svc, props)

    attempt = [0]

    async def _fc():
        attempt[0] += 1
        if attempt[0] == 1:
            raise Exception("auth failed")
        return MagicMock()

    asyncio.run(plugin.force_connect(
        MagicMock(), MagicMock(), HostInfo(host="h", port=5432), props, True, _fc))
    assert attempt[0] == 2
