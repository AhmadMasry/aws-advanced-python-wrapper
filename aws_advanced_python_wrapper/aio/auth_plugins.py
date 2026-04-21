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

"""Async auth plugins: IAM, Secrets Manager.

The underlying AWS SDKs (boto3 for IAM token generation; botocore for
Secrets Manager) are sync-only. Running them directly would block the
event loop. This module wraps the blocking call in
``asyncio.to_thread`` so the plugin pipeline stays non-blocking even
though the SDK call itself runs on a thread.

3.0.0 ships async IAM + async Secrets Manager. Federated (SAML) and
Okta async ports depend on ``requests``/``aiohttp`` decisions that
warrant their own sub-project brainstorm; skeletons are provided so
users can subclass ``AsyncAuthPluginBase`` for custom flows.
"""

from __future__ import annotations

import asyncio
import json
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Dict, Optional,
                    Set, Tuple)

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.iam_utils import IamAuthUtils
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils
from aws_advanced_python_wrapper.utils.region_utils import (GdbRegionUtils,
                                                            RegionUtils)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncAuthPluginBase(AsyncPlugin):
    """Common shell for async auth plugins.

    Subclasses override :meth:`_resolve_credentials` to return a
    ``(user, password, was_cached)`` tuple. The base class handles
    plugin-pipeline wiring, credential injection, and retry-on-login
    when cached credentials fail authentication.
    """

    _SUBSCRIBED: Set[str] = {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.FORCE_CONNECT.method_name,
    }

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        return await self._connect_with_retry(host_info, props, connect_func)

    async def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable[..., Awaitable[Any]]) -> Any:
        return await self._connect_with_retry(host_info, props, force_connect_func)

    async def _connect_with_retry(
            self,
            host_info: HostInfo,
            props: Properties,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        """Resolve creds, inject, connect; retry once if cached creds
        cause a login failure."""
        user, password, was_cached = await self._resolve_credentials(host_info, props)
        self._inject_credentials(props, user, password)
        try:
            return await connect_func()
        except Exception as exc:
            if not was_cached:
                raise
            if not self._plugin_service.is_login_exception(error=exc):
                raise
            # Cached credentials failed auth -- invalidate, refetch, retry once.
            self._invalidate_cache(host_info, props)
            user, password, _ = await self._resolve_credentials(host_info, props)
            self._inject_credentials(props, user, password)
            return await connect_func()

    @staticmethod
    def _inject_credentials(
            props: Properties,
            user: Optional[str],
            password: Optional[str]) -> None:
        if user is not None:
            props["user"] = user
        if password is not None:
            props["password"] = password

    async def _resolve_credentials(
            self,
            host_info: HostInfo,
            props: Properties) -> Tuple[Optional[str], Optional[str], bool]:
        """Return ``(user, password, was_cached)`` for the given host.

        ``was_cached=True`` when the credentials were served from cache
        (so a login failure should trigger invalidation + one retry).
        """
        raise NotImplementedError

    def _invalidate_cache(
            self,
            host_info: HostInfo,
            props: Properties) -> None:
        """Drop any cached credentials for this (host, props) so a
        subsequent ``_resolve_credentials`` call generates fresh ones.

        Default no-op so subclasses that don't cache can ignore.
        """


class AsyncIamAuthPlugin(AsyncAuthPluginBase):
    """Async IAM DB Auth.

    Generates an RDS auth token via boto3 (sync SDK) executed in a thread
    so the event loop isn't blocked. Caches the generated token per
    (host, port, user, region) tuple until it expires.
    """

    _DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60  # 15 minutes
    _TOKEN_REGEN_GRACE_SEC = 60

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        super().__init__(plugin_service, props)
        self._token_cache: Dict[str, Tuple[str, float]] = {}

    def _default_port(self) -> int:
        dialect = self._plugin_service.database_dialect
        if dialect is not None:
            return dialect.default_port
        return 5432

    def _cache_key_for(
            self,
            host_info: HostInfo,
            props: Properties) -> Optional[str]:
        """Return the IAM-token cache key for ``(host_info, props)`` or
        ``None`` if the inputs don't contain enough info to build one.

        Encapsulates host / port / region derivation so
        ``_resolve_credentials`` and ``_invalidate_cache`` stay aligned.
        """
        user = WrapperProperties.USER.get(props)
        if not user:
            return None
        host = IamAuthUtils.get_iam_host(props, host_info)
        port = IamAuthUtils.get_port(props, host_info, self._default_port())
        rds_type = RdsUtils().identify_rds_type(host)
        region_utils = (GdbRegionUtils()
                        if rds_type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER
                        else RegionUtils())
        region = region_utils.get_region(
            props, WrapperProperties.IAM_REGION.name, host, host_info)
        if not region:
            return None
        return IamAuthUtils.get_cache_key(user, host, port, region)

    async def _resolve_credentials(
            self,
            host_info: HostInfo,
            props: Properties) -> Tuple[Optional[str], Optional[str], bool]:
        user = WrapperProperties.USER.get(props)
        if not user:
            raise AwsWrapperError(
                "IAM auth requires a 'user' connection property"
            )

        host = IamAuthUtils.get_iam_host(props, host_info)
        port = IamAuthUtils.get_port(props, host_info, self._default_port())

        rds_type = RdsUtils().identify_rds_type(host)
        region_utils = (GdbRegionUtils()
                        if rds_type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER
                        else RegionUtils())
        region = region_utils.get_region(
            props, WrapperProperties.IAM_REGION.name, host, host_info)
        if not region:
            raise AwsWrapperError(
                f"Could not resolve AWS region from host '{host}'. "
                "Set IAM_REGION explicitly."
            )

        cache_key = IamAuthUtils.get_cache_key(user, host, port, region)

        ttl_sec = WrapperProperties.IAM_EXPIRATION.get_int(props)
        if not ttl_sec:
            ttl_sec = self._DEFAULT_TOKEN_EXPIRATION_SEC

        now = asyncio.get_event_loop().time()
        cached = self._token_cache.get(cache_key)
        if cached is not None:
            token, expires_at = cached
            if now < expires_at - self._TOKEN_REGEN_GRACE_SEC:
                return user, token, True

        token = await asyncio.to_thread(
            self._generate_token_blocking, host, int(port), user, region
        )
        self._token_cache[cache_key] = (token, now + ttl_sec)
        return user, token, False

    def _invalidate_cache(
            self,
            host_info: HostInfo,
            props: Properties) -> None:
        """Drop the cached IAM token for this (host, port, user, region)
        so the next ``_resolve_credentials`` call regenerates it.

        Called by :class:`AsyncAuthPluginBase` when cached credentials
        fail authentication (retry-on-login path).
        """
        cache_key = self._cache_key_for(host_info, props)
        if cache_key is not None:
            self._token_cache.pop(cache_key, None)

    @staticmethod
    def _generate_token_blocking(
            host: str,
            port: int,
            user: str,
            region: Optional[str]) -> str:
        """Synchronous boto3 call to generate an RDS IAM auth token."""
        import boto3
        kwargs: dict = {}
        if region:
            kwargs["region_name"] = region
        client = boto3.client("rds", **kwargs)
        return client.generate_db_auth_token(
            DBHostname=host,
            Port=port,
            DBUsername=user,
        )


class AsyncAwsSecretsManagerPlugin(AsyncAuthPluginBase):
    """Async AWS Secrets Manager auth plugin.

    Fetches user/password from a named secret. Parses both Secrets
    Manager's default JSON shape (``{"username": "...", "password": "..."}``)
    and the common RDS-auto-created ``{"username": ..., "password": ...}``
    schema.
    """

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        super().__init__(plugin_service, props)
        self._secret_cache: dict = {}

    async def _resolve_credentials(
            self,
            host_info: HostInfo,
            props: Properties) -> Tuple[Optional[str], Optional[str], bool]:
        secret_id = WrapperProperties.SECRETS_MANAGER_SECRET_ID.get(props)
        if not secret_id:
            raise AwsWrapperError(
                "AWS Secrets Manager plugin requires 'secrets_manager_secret_id'"
            )
        region = WrapperProperties.SECRETS_MANAGER_REGION.get(props)

        cache_key = (secret_id, region)
        cached = self._secret_cache.get(cache_key)
        if cached is not None:
            user, password = cached
            return user, password, True

        secret = await asyncio.to_thread(
            self._fetch_secret_blocking, secret_id, region
        )
        # Allow custom field names via *_KEY properties (e.g. Terraform secrets
        # with non-default schemas).
        user_key = (
            WrapperProperties.SECRETS_MANAGER_SECRET_USERNAME_KEY.get(props)
            or "username"
        )
        password_key = (
            WrapperProperties.SECRETS_MANAGER_SECRET_PASSWORD_KEY.get(props)
            or "password"
        )
        user = secret.get(user_key)
        password = secret.get(password_key)

        self._secret_cache[cache_key] = (user, password)
        return user, password, False

    @staticmethod
    def _fetch_secret_blocking(
            secret_id: str,
            region: Optional[str]) -> dict:
        import boto3
        kwargs: dict = {}
        if region:
            kwargs["region_name"] = region
        client = boto3.client("secretsmanager", **kwargs)
        resp = client.get_secret_value(SecretId=secret_id)
        secret_str = resp.get("SecretString")
        if not secret_str:
            return {}
        try:
            return json.loads(secret_str)
        except json.JSONDecodeError:
            return {}
