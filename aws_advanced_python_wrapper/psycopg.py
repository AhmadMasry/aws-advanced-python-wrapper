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

"""PEP 249 DBAPI module bound to psycopg v3.

Enables SQLAlchemy's creator-pattern:

    from sqlalchemy import create_engine
    from aws_advanced_python_wrapper.psycopg import connect

    engine = create_engine(
        "postgresql+psycopg://",
        creator=lambda: connect(
            "host=... user=... dbname=...",
            wrapper_dialect="aurora-pg",
        ),
    )
"""

from __future__ import annotations

import sys
from typing import Any

from psycopg import Connection as _PGConnection

from aws_advanced_python_wrapper import _dbapi
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection


def connect(conninfo: str = "", **kwargs: Any) -> AwsWrapperConnection:
    """PEP 249 `connect`, target-driver-bound to psycopg v3.

    Equivalent to::

        AwsWrapperConnection.connect(psycopg.Connection.connect, conninfo, **kwargs)
    """
    return AwsWrapperConnection.connect(_PGConnection.connect, conninfo, **kwargs)


_dbapi.install(sys.modules[__name__].__dict__, connect=connect)
