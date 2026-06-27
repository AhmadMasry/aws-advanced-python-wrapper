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

import time
from typing import Any, ClassVar

import mysql.connector
import mysql.connector.django.base as base
from django.utils.asyncio import async_unsafe
from django.utils.functional import cached_property
from django.utils.regex_helper import _lazy_re_compile

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.utils import transient_connect

# This should match the numerical portion of the version numbers (we can treat
# versions like 5.0.24 and 5.0.24a as the same).
server_version_re = _lazy_re_compile(r"(\d{1,2})\.(\d{1,2})\.(\d{1,2})")


class DatabaseWrapper(base.DatabaseWrapper):
    """Custom MySQL Connector backend for Django"""

    # Django's own connection management opens a fresh DBAPI connection
    # via ``get_new_connection`` whenever the request cycle needs one,
    # bypassing any external pool. During Aurora's post-failover boot
    # window the new writer rejects connects with a transient error --
    # without retry, Django surfaces this straight back to the request
    # handler as a fatal error. Classification + backoff are centralised
    # in ``utils.transient_connect``. The retry budget is configurable via
    # ``connection_retry_max_attempts`` / ``connection_retry_max_backoff_s``
    # in Django's ``OPTIONS`` dict; defaults match ``transient_connect.DEFAULT_*``.
    _TRANSIENT_CONNECT_MAX_ATTEMPTS: ClassVar[int] = transient_connect.DEFAULT_MAX_ATTEMPTS

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._read_only = False

    @async_unsafe
    def get_new_connection(self, conn_params):
        if "converter_class" not in conn_params:
            conn_params["converter_class"] = base.DjangoMySQLConverter

        max_attempts = int(conn_params.get(
            "connection_retry_max_attempts", transient_connect.DEFAULT_MAX_ATTEMPTS))
        max_backoff = float(conn_params.get(
            "connection_retry_max_backoff_s", transient_connect.DEFAULT_MAX_BACKOFF_S))
        # Don't leak wrapper-only keys to mysql.connector.Connect — strip
        # before constructing the connection.
        conn_params.pop("connection_retry_max_attempts", None)
        conn_params.pop("connection_retry_max_backoff_s", None)

        last_exc: BaseException | None = None
        conn = None
        for attempt in range(max_attempts):
            try:
                conn = AwsWrapperConnection.connect(
                    mysql.connector.Connect,
                    **conn_params,
                )
                break
            except Exception as exc:
                last_exc = exc
                if transient_connect.is_transient_connect_error(exc) \
                        and attempt < max_attempts - 1:
                    time.sleep(transient_connect.compute_backoff(
                        attempt, max_backoff=max_backoff))
                    continue
                raise
        if conn is None:
            # Defensive: loop exits via ``break`` or ``raise`` above; this
            # branch is unreachable but keeps mypy from inferring an
            # implicit ``None`` return from ``get_new_connection``.
            assert last_exc is not None
            raise last_exc

        if not self._read_only:
            return conn
        else:
            conn.read_only = True
            return conn

    def get_connection_params(self):
        kwargs = super().get_connection_params()
        self._read_only = kwargs.pop("read_only", False)
        return kwargs

    @cached_property
    def mysql_server_info(self):
        return self.mysql_server_data["version"]

    @cached_property
    def mysql_version(self):
        match = server_version_re.match(self.mysql_server_info)
        if not match:
            raise Exception(
                "Unable to determine MySQL version from version string %r"
                % self.mysql_server_info
            )
        return tuple(int(x) for x in match.groups())

    @cached_property
    def mysql_is_mariadb(self):
        return "mariadb" in self.mysql_server_info.lower()

    @cached_property
    def sql_mode(self):
        sql_mode = self.mysql_server_data["sql_mode"]
        return set(sql_mode.split(",") if sql_mode else ())
